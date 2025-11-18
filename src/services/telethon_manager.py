from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from dataclasses import dataclass
from typing import Dict, Optional

from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError, SessionPasswordNeededError
from telethon.errors.rpcerrorlist import (
    AuthKeyUnregisteredError,
    SessionRevokedError,
    UserDeactivatedError,
    UserDeactivatedBanError,
)
from telethon.sessions import StringSession

from src.db.repositories.session_repository import SessionRepository
from src.models.session import SessionOwnerType, TelethonSession
from src.utils.telethon_reconnect import (
    TELETHON_NETWORK_EXCEPTIONS,
    run_with_exponential_backoff,
)


logger = logging.getLogger(__name__)

_SESSION_VALIDATION_TIMEOUT = 5.0
_AUTH_ERRORS = (
    AuthKeyUnregisteredError,
    SessionRevokedError,
    SessionPasswordNeededError,
    UserDeactivatedError,
    UserDeactivatedBanError,
)


class _UnauthorizedError(RuntimeError):
    """Internal marker for unauthorized session state."""


@dataclass(slots=True)
class SessionHealthReport:
    """Represents the outcome of a Telethon session health probe."""

    ok: bool
    code: str
    detail: Optional[str]
    latency_ms: int
    exception: Optional[Exception] = None


class TelethonSessionManager:
    """Coordinates Telethon client lifecycle and persistence."""

    def __init__(self, api_id: int, api_hash: str, session_repository: SessionRepository) -> None:
        self._api_id = api_id
        self._api_hash = api_hash
        self._session_repository = session_repository
        self._pooled_clients: Dict[str, TelegramClient] = {}
        self._client_locks: Dict[str, asyncio.Lock] = {}
        self._pool_guard = asyncio.Lock()

    async def create_temporary_client(self) -> TelegramClient:
        """Create and connect a fresh Telethon client for onboarding flows."""
        session = StringSession()
        client = TelegramClient(session, self._api_id, self._api_hash)
        await run_with_exponential_backoff(
            client.connect,
            label="telethon.session.temporary.connect",
            logger=logger,
            log_context={"client": "temporary"},
        )
        return client

    async def build_client_from_session(self, session: TelethonSession) -> TelegramClient:
        """Restore a Telethon client from stored session data."""
        if not session.session_data:
            raise ValueError("Session data is missing; cannot restore Telethon client")
        string_session = StringSession(session.session_data)
        client = TelegramClient(string_session, self._api_id, self._api_hash)
        await run_with_exponential_backoff(
            client.connect,
            label="telethon.session.restore.connect",
            logger=logger,
            log_context={"session_id": session.session_id},
        )
        return client

    async def persist_session(self, session: TelethonSession) -> TelethonSession:
        """Persist session metadata and payload to MongoDB."""
        stored = await self._session_repository.upsert_session(session)
        # Drop pooled client so subsequent checks use refreshed credentials.
        await self.drop_shared_client(stored.session_id)
        return stored

    async def _get_client_lock(self, session_id: str) -> asyncio.Lock:
        async with self._pool_guard:
            lock = self._client_locks.get(session_id)
            if lock is None:
                lock = asyncio.Lock()
                self._client_locks[session_id] = lock
            return lock

    async def acquire_shared_client(self, session: TelethonSession) -> TelegramClient:
        """Get a pooled Telethon client for session health checks."""

        if not session.session_data:
            raise ValueError("Session data is missing; cannot restore Telethon client")

        lock = await self._get_client_lock(session.session_id)
        async with lock:
            client = self._pooled_clients.get(session.session_id)
            if client is not None:
                if client.is_connected():
                    return client
                with contextlib.suppress(Exception):
                    await run_with_exponential_backoff(
                        client.connect,
                        label="telethon.session.pooled.reconnect",
                        logger=logger,
                        log_context={
                            "session_id": session.session_id,
                            "owner_id": session.owner_id,
                        },
                    )
                    if client.is_connected():
                        return client

            string_session = StringSession(session.session_data)
            client = TelegramClient(string_session, self._api_id, self._api_hash)
            await run_with_exponential_backoff(
                client.connect,
                label="telethon.session.pooled.connect",
                logger=logger,
                log_context={
                    "session_id": session.session_id,
                    "owner_id": session.owner_id,
                },
            )
            self._pooled_clients[session.session_id] = client
            return client

    async def drop_shared_client(self, session_id: str) -> None:
        """Remove a pooled client and close the connection if present."""

        lock = await self._get_client_lock(session_id)
        client: Optional[TelegramClient] = None
        async with lock:
            client = self._pooled_clients.pop(session_id, None)
        if client is not None:
            with contextlib.suppress(Exception):
                await self.close_client(client)

    async def check_session_health(
        self,
        session: TelethonSession,
        *,
        timeout: float,
        verify_dialog_access: bool,
    ) -> SessionHealthReport:
        """Probe a Telethon session for liveness and dialog availability."""

        started_at = time.perf_counter()
        try:
            client = await self.acquire_shared_client(session)
        except Exception as exc:  # pragma: no cover - defensive logging
            latency_ms = int((time.perf_counter() - started_at) * 1000)
            logger.exception(
                "Failed to acquire pooled Telethon client",
                extra={"session_id": session.session_id, "owner_id": session.owner_id},
            )
            return SessionHealthReport(
                ok=False,
                code="client_init_error",
                detail=exc.__class__.__name__,
                latency_ms=latency_ms,
                exception=exc,
            )

        async def _probe() -> None:
            authorized = await client.is_user_authorized()
            if not authorized:
                raise _UnauthorizedError()
            await client.get_me()
            if verify_dialog_access:
                await client.get_dialogs(limit=1)

        code = "ok"
        detail: Optional[str] = None
        error: Optional[Exception] = None

        try:
            await asyncio.wait_for(_probe(), timeout=timeout)
        except asyncio.TimeoutError:
            code = "timeout"
            detail = f">{timeout:.2f}s"
        except asyncio.CancelledError:
            raise
        except _UnauthorizedError as exc:
            code = "not_authorized"
            detail = "client reported unauthorized"
            error = exc
            await self.drop_shared_client(session.session_id)
        except _AUTH_ERRORS as exc:
            code = "auth_error"
            detail = exc.__class__.__name__
            error = exc
            await self.drop_shared_client(session.session_id)
        except FloodWaitError as exc:
            code = "flood_wait"
            seconds = getattr(exc, "seconds", None)
            detail = f"{seconds}s" if seconds is not None else exc.__class__.__name__
            error = exc
        except RPCError as exc:
            code = "dialog_error" if verify_dialog_access else "rpc_error"
            detail = exc.__class__.__name__
            error = exc
        except Exception as exc:  # pragma: no cover - defensive
            code = "dialog_error" if verify_dialog_access else "unexpected_error"
            detail = exc.__class__.__name__
            error = exc

        latency_ms = int((time.perf_counter() - started_at) * 1000)
        report = SessionHealthReport(ok=code == "ok", code=code, detail=detail, latency_ms=latency_ms, exception=error)

        log_extra = {
            "session_id": session.session_id,
            "owner_id": session.owner_id,
            "status_code": report.code,
            "status_detail": report.detail,
            "latency_ms": report.latency_ms,
        }
        if report.ok:
            logger.debug("Session health check succeeded", extra=log_extra)
        else:
            level = logging.WARNING if report.code in {"timeout", "not_authorized", "auth_error", "flood_wait", "dialog_error"} else logging.ERROR
            logger.log(level, "Session health check failed", extra=log_extra)

        return report

    async def get_active_sessions(
        self,
        owner_id: int,
        owner_type: SessionOwnerType = SessionOwnerType.USER,
        *,
        verify_live: bool = False,
        timeout: float | None = None,
    ) -> list[TelethonSession]:
        sessions_iter = await self._session_repository.get_active_sessions_for_owner(owner_id, owner_type)
        sessions = list(sessions_iter)
        if not verify_live or not sessions:
            return sessions

        timeout_value = timeout if timeout is not None else _SESSION_VALIDATION_TIMEOUT
        verified: list[TelethonSession] = []
        for session in sessions:
            is_live = await self.verify_session_status(session, timeout=timeout_value)
            if is_live:
                verified.append(session)
        return verified

    async def refresh_owner_sessions(
        self,
        owner_id: int,
        owner_type: SessionOwnerType = SessionOwnerType.USER,
        *,
        timeout: float | None = None,
    ) -> tuple[list[TelethonSession], list[TelethonSession]]:
        sessions = await self._session_repository.list_sessions_for_owner(owner_id, owner_type)
        if not sessions:
            return [], []

        timeout_value = timeout if timeout is not None else _SESSION_VALIDATION_TIMEOUT
        active: list[TelethonSession] = []
        inactive: list[TelethonSession] = []
        for session in sessions:
            is_live = await self.verify_session_status(session, timeout=timeout_value)
            target = active if is_live else inactive
            target.append(session)
        return active, inactive

    async def ensure_dialog_access(
        self,
        session: TelethonSession,
        *,
        limit: Optional[int] = None,
    ) -> bool:
        """Ensure account can fetch dialogs; returns True if successful."""
        # The limit parameter is retained for backward compatibility; pooled checks always use a minimal fetch.
        _ = limit  # pragma: no cover - compatibility no-op
        report = await self.check_session_health(
            session,
            timeout=_SESSION_VALIDATION_TIMEOUT,
            verify_dialog_access=True,
        )
        if report.ok:
            await self._set_session_active(session, True)
            return True
        await self._set_session_active(session, False)
        return False

    async def verify_session_status(
        self,
        session: TelethonSession,
        *,
        timeout: float = _SESSION_VALIDATION_TIMEOUT,
    ) -> bool:
        """Validate session against Telegram API and persist updated status."""
        report = await self.check_session_health(
            session,
            timeout=timeout,
            verify_dialog_access=False,
        )
        if report.ok:
            await self._set_session_active(session, True)
            return True
        if report.code == "timeout":
            # Preserve previous state on timeout to avoid flapping.
            return session.is_active
        await self._set_session_active(session, False)
        return False

    async def _set_session_active(self, session: TelethonSession, is_active: bool) -> None:
        if session.is_active == is_active:
            return
        updated = await self._session_repository.set_session_active(session.session_id, is_active)
        if updated is not None:
            session.is_active = updated.is_active
            session.metadata = updated.metadata
            session.phone = updated.phone
        else:
            session.is_active = is_active

    async def deactivate_session(self, session_id: str) -> Optional[TelethonSession]:
        """Mark a session as inactive without deleting it."""
        stored = await self._session_repository.get_by_session_id(session_id)
        if stored is None:
            return None
        stored.is_active = False
        updated = await self._session_repository.upsert_session(stored)
        with contextlib.suppress(Exception):
            await self.drop_shared_client(session_id)
        return updated

    async def remove_session(self, session: TelethonSession | str) -> bool:
        """Log out the Telethon client and remove the stored session."""

        if isinstance(session, str):
            session_obj = await self._session_repository.get_by_session_id(session)
            if session_obj is None:
                logger.warning("Не удалось найти сессию %s для удаления", session)
                return False
        else:
            session_obj = session

        client: Optional[TelegramClient] = None
        if session_obj.session_data:
            try:
                client = await self.build_client_from_session(session_obj)
                await client.log_out()
            except Exception:
                logger.exception(
                    "Не удалось корректно завершить сессию Telethon",
                    extra={"session_id": session_obj.session_id, "owner_id": session_obj.owner_id},
                )
                raise
            finally:
                if client is not None:
                    with contextlib.suppress(Exception):
                        await self.close_client(client)
        else:
            logger.warning(
                "Сессия %s не содержит session_data; пропускаем logout",
                session_obj.session_id,
            )

        try:
            deleted = await self._session_repository.delete_session(session_obj.session_id)
        except Exception:
            logger.exception(
                "Ошибка при удалении сессии из базы",
                extra={"session_id": session_obj.session_id, "owner_id": session_obj.owner_id},
            )
            raise

        if not deleted:
            logger.warning(
                "Документ сессии %s не найден при удалении",
                session_obj.session_id,
            )
            return False

        with contextlib.suppress(Exception):
            await self.drop_shared_client(session_obj.session_id)
        return True

    async def close_client(self, client: TelegramClient) -> None:
        """Gracefully disconnect a Telethon client."""
        if client.is_connected():
            try:
                await client.disconnect()
            except TELETHON_NETWORK_EXCEPTIONS as exc:
                logger.warning("Telethon disconnect reported network error: %s", exc)
