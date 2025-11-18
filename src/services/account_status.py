from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Mapping, Optional, Sequence

from src.db.repositories.session_repository import SessionRepository
from src.models.session import TelethonSession
from src.services.telethon_manager import SessionHealthReport, TelethonSessionManager


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class AccountStatusResult:
    """Represents a cached account status snapshot."""

    session_id: str
    owner_id: int
    active: bool
    status: str
    reason: str
    detail: Optional[str]
    latency_ms: int
    checked_at: datetime

    def to_payload(self) -> dict[str, object]:
        return {
            "active": self.active,
            "status": self.status,
            "reason": self.reason,
            "detail": self.detail,
            "latency_ms": self.latency_ms,
            "checked_at": self.checked_at.isoformat(),
        }


@dataclass(slots=True)
class _CacheEntry:
    result: AccountStatusResult
    cached_at: float


class AccountStatusService:
    """Performs concurrent Telethon account health checks with caching and persistence."""

    def __init__(
        self,
        *,
        session_manager: TelethonSessionManager,
        session_repository: SessionRepository,
        concurrency: int,
        timeout_seconds: float,
        cache_ttl_seconds: float,
        db_refresh_interval_seconds: float,
    ) -> None:
        self._session_manager = session_manager
        self._session_repository = session_repository
        self._concurrency = max(1, int(concurrency))
        self._timeout = max(0.5, float(timeout_seconds))
        self._cache_ttl = max(0.0, float(cache_ttl_seconds))
        self._db_refresh_interval = max(0.0, float(db_refresh_interval_seconds))
        self._cache: Dict[str, _CacheEntry] = {}
        self._cache_lock = asyncio.Lock()

    async def get_cached_snapshot(
        self,
        sessions: Sequence[TelethonSession],
    ) -> tuple[dict[str, AccountStatusResult], list[TelethonSession]]:
        """Return cached statuses and sessions requiring fresh checks."""

        now = time.monotonic()
        cached: dict[str, AccountStatusResult] = {}
        missing: list[TelethonSession] = []

        async with self._cache_lock:
            for session in self._unique_sessions(sessions).values():
                entry = self._cache.get(session.session_id)
                if entry and now - entry.cached_at <= self._cache_ttl:
                    cached[session.session_id] = entry.result
                else:
                    missing.append(session)
        return cached, missing

    async def refresh_session(
        self,
        session: TelethonSession,
        *,
        verify_dialog_access: bool = True,
        use_cache: bool = False,
    ) -> AccountStatusResult:
        results = await self.refresh_sessions(
            [session],
            verify_dialog_access=verify_dialog_access,
            use_cache=use_cache,
        )
        return results[session.session_id]

    async def refresh_sessions(
        self,
        sessions: Sequence[TelethonSession],
        *,
        verify_dialog_access: bool = True,
        use_cache: bool = True,
    ) -> dict[str, AccountStatusResult]:
        """Refresh statuses for provided sessions with bounded concurrency."""

        unique = self._unique_sessions(sessions)
        if not unique:
            return {}

        cached: dict[str, AccountStatusResult] = {}
        to_check: list[TelethonSession] = []

        if use_cache and self._cache_ttl > 0:
            now = time.monotonic()
            async with self._cache_lock:
                for session in unique.values():
                    entry = self._cache.get(session.session_id)
                    if entry and now - entry.cached_at <= self._cache_ttl:
                        cached[session.session_id] = entry.result
                    else:
                        to_check.append(session)
        else:
            to_check = list(unique.values())

        fresh: dict[str, AccountStatusResult] = {}
        if to_check:
            fresh = await self._probe_sessions(to_check, verify_dialog_access=verify_dialog_access)
            async with self._cache_lock:
                timestamp = time.monotonic()
                for session_id, result in fresh.items():
                    self._cache[session_id] = _CacheEntry(result=result, cached_at=timestamp)

        combined = dict(cached)
        combined.update(fresh)

        # Ensure ordering corresponds to the original input when possible.
        ordered: dict[str, AccountStatusResult] = {}
        for session in sessions:
            result = combined.get(session.session_id)
            if result:
                ordered[session.session_id] = result
        # Append any additional unique sessions that may not have been in the original list order.
        for session_id, result in combined.items():
            ordered.setdefault(session_id, result)
        return ordered

    async def _probe_sessions(
        self,
        sessions: Sequence[TelethonSession],
        *,
        verify_dialog_access: bool,
    ) -> dict[str, AccountStatusResult]:
        semaphore = asyncio.Semaphore(self._concurrency)
        tasks = [
            asyncio.create_task(self._probe_single(session, semaphore, verify_dialog_access))
            for session in sessions
        ]
        results = await asyncio.gather(*tasks)
        return {result.session_id: result for result in results}

    async def _probe_single(
        self,
        session: TelethonSession,
        semaphore: asyncio.Semaphore,
        verify_dialog_access: bool,
    ) -> AccountStatusResult:
        async with semaphore:
            report = await self._session_manager.check_session_health(
                session,
                timeout=self._timeout,
                verify_dialog_access=verify_dialog_access,
            )
            result = self._build_result(session, report)
            await self._persist_result(session, result)
            log_level = logging.DEBUG if result.active else logging.WARNING
            logger.log(
                log_level,
                "Account status probe completed",
                extra={
                    "session_id": session.session_id,
                    "owner_id": session.owner_id,
                    "status": result.status,
                    "status_reason": result.reason,
                    "status_detail": result.detail,
                    "latency_ms": result.latency_ms,
                },
            )
            return result

    def _build_result(self, session: TelethonSession, report: SessionHealthReport) -> AccountStatusResult:
        active = report.ok
        status = "active" if active else "inactive"
        detail = report.detail or report.code
        reason = self._translate_reason(report.code, detail)
        checked_at = datetime.utcnow()
        latency_ms = report.latency_ms
        return AccountStatusResult(
            session_id=session.session_id,
            owner_id=session.owner_id,
            active=active,
            status=status,
            reason=reason,
            detail=None if active else detail,
            latency_ms=latency_ms,
            checked_at=checked_at,
        )

    async def _persist_result(self, session: TelethonSession, result: AccountStatusResult) -> None:
        should_write = self._should_write(session, result)
        stored: Optional[TelethonSession] = None
        if should_write:
            try:
                stored = await self._session_repository.update_status_fields(
                    session.session_id,
                    is_active=result.active,
                    status=result.status,
                    last_checked_at=result.checked_at,
                    last_error=result.detail,
                )
            except Exception:
                logger.exception(
                    "Failed to persist session status",
                    extra={
                        "session_id": session.session_id,
                        "owner_id": session.owner_id,
                        "status": result.status,
                        "reason": result.reason,
                    },
                )
            if stored is not None:
                self._apply_session_snapshot(
                    session,
                    is_active=stored.is_active,
                    status=stored.status,
                    last_checked_at=stored.last_checked_at,
                    last_error=stored.last_error,
                )
                return
        # Fallback to reflecting the latest probe in memory even if there is no write.
        self._apply_session_snapshot(
            session,
            is_active=result.active,
            status=result.status,
            last_checked_at=result.checked_at,
            last_error=result.detail,
        )

    def _should_write(self, session: TelethonSession, result: AccountStatusResult) -> bool:
        if session.is_active != result.active:
            return True
        if (session.status or "").lower() != result.status:
            return True
        current_error = session.last_error
        if (current_error or None) != result.detail:
            return True
        last_checked = session.last_checked_at
        if last_checked is None:
            return True
        threshold = last_checked + timedelta(seconds=self._db_refresh_interval)
        return datetime.utcnow() >= threshold

    @staticmethod
    def _apply_session_snapshot(
        session: TelethonSession,
        *,
        is_active: bool,
        status: str,
        last_checked_at: datetime,
        last_error: Optional[str],
    ) -> None:
        session.is_active = is_active
        session.status = status
        session.last_checked_at = last_checked_at
        session.last_error = last_error

    @staticmethod
    def _unique_sessions(sessions: Sequence[TelethonSession]) -> Dict[str, TelethonSession]:
        unique: Dict[str, TelethonSession] = {}
        for session in sessions:
            if not session.session_id:
                continue
            unique.setdefault(session.session_id, session)
        return unique

    @staticmethod
    def _translate_reason(code: str, detail: Optional[str]) -> str:
        reason_map: Mapping[str, str] = {
            "ok": "активен",
            "timeout": "таймаут проверки Telegram",
            "not_authorized": "требуется повторный вход",
            "auth_error": "ошибка авторизации",
            "dialog_error": "не удалось получить список диалогов",
            "rpc_error": "ошибка Telegram",
            "flood_wait": "слишком частые запросы (FloodWait)",
            "unexpected_error": "непредвиденная ошибка",
            "client_init_error": "ошибка инициализации клиента",
        }
        base = reason_map.get(code, code.replace("_", " "))
        if detail and code != "ok":
            return f"{base} ({detail})"
        return base
