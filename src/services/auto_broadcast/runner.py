from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Mapping, Optional, Set, Tuple

from telethon import TelegramClient
from telethon.errors.rpcerrorlist import (
    AuthKeyUnregisteredError,
    SessionRevokedError,
    UserDeactivatedError,
    UserDeactivatedBanError,
)

from src.config.broadcast_settings import (
    BROADCAST_BATCH_PAUSE_SECONDS,
    BROADCAST_DELAY_MAX_SECONDS,
    BROADCAST_DELAY_MIN_SECONDS,
)
from src.db.repositories.account_repository import AccountRepository
from src.db.repositories.auto_broadcast_task_repository import AutoBroadcastTaskRepository
from src.db.repositories.session_repository import SessionRepository
from src.models.auto_broadcast import AccountMode, AccountStatus, AutoBroadcastTask, GroupTarget, TaskStatus
from src.models.session import TelethonSession
from src.services.auto_broadcast.payloads import ImagePayload, extract_image_metadata, prepare_image_payload
from src.services.broadcast_shared import (
    BroadcastImageData,
    DialogsFetchError,
    describe_content_payload,
    render_group_label,
    resolve_group_targets,
    resolved_target_identity,
    send_payload_to_group,
)
from src.services.telethon_manager import TelethonSessionManager
from src.services.account_status import AccountStatusService
from src.utils.timezone import format_moscow_time


logger = logging.getLogger(__name__)

SHUFFLE_RANDOM = random.SystemRandom()
SECONDARY_ACCOUNT_DELAY_MIN_SECONDS = 4.0
SECONDARY_ACCOUNT_DELAY_MAX_SECONDS = 9.0
AUTH_ERRORS: Tuple[type[BaseException], ...] = (
    AuthKeyUnregisteredError,
    SessionRevokedError,
    UserDeactivatedError,
    UserDeactivatedBanError,
)


@dataclass(slots=True)
class AccountCycleStats:
    account_id: str
    groups_planned: int
    expected_targets: int = 0
    attempts: int = 0
    sent: int = 0
    failed: int = 0
    skipped_duplicates: int = 0
    retried: bool = False


class AutoBroadcastRunner:
    """Executes periodic broadcast cycles for a single task."""

    def __init__(
        self,
        task_id: str,
        *,
        task_repository: AutoBroadcastTaskRepository,
        account_repository: AccountRepository,
        session_repository: SessionRepository,
        session_manager: TelethonSessionManager,
        account_status_service: AccountStatusService,
        bot_client: TelegramClient,
        worker_id: str,
        lock_ttl_seconds: int,
        max_delay_per_message: int,
        batch_pause_max_seconds: float,
        interval_safety_margin_seconds: float,
    ) -> None:
        self._task_id = task_id
        self._tasks = task_repository
        self._accounts = account_repository
        self._sessions = session_repository
        self._session_manager = session_manager
        self._account_status_service = account_status_service
        self._bot_client = bot_client
        self._worker_id = worker_id
        self._lock_ttl = lock_ttl_seconds
        self._max_delay = max_delay_per_message
        self._batch_pause_max = batch_pause_max_seconds
        self._interval_margin = interval_safety_margin_seconds
        self._stop_event = asyncio.Event()
        self._inactive_notified: Set[str] = set()
        self._auth_error_names: Set[str] = {error.__name__ for error in AUTH_ERRORS}
        self._health_check_interval = 30.0
        self._lock_refresh_interval = max(1.0, lock_ttl_seconds / 3.0)
        self._last_lock_refresh = time.monotonic()

    def stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        logger.debug("Auto broadcast runner started", extra={"task_id": self._task_id})
        try:
            while not self._stop_event.is_set():
                task = await self._tasks.get_by_task_id(self._task_id)
                if task is None:
                    logger.warning("Auto broadcast task removed during execution", extra={"task_id": self._task_id})
                    return
                if not task.enabled or task.status != TaskStatus.RUNNING:
                    logger.debug(
                        "Runner stopped because task status is %s", task.status.value, extra={"task_id": self._task_id}
                    )
                    return

                wait_seconds = self._seconds_until_due(task)
                if wait_seconds > 0:
                    try:
                        await asyncio.wait_for(self._stop_event.wait(), timeout=wait_seconds)
                        return
                    except asyncio.TimeoutError:
                        continue

                locked_task = await self._tasks.acquire_lock(self._task_id, self._worker_id, self._lock_ttl)
                if locked_task is None:
                    await self._delayed_wait(2.0)
                    continue

                try:
                    await self._execute_cycle(locked_task)
                finally:
                    await self._tasks.release_lock(self._task_id, self._worker_id)

                await self._delayed_wait(1.0)
        finally:
            logger.debug("Auto broadcast runner stopped", extra={"task_id": self._task_id})

    async def _delayed_wait(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=max(0.1, seconds))
        except asyncio.TimeoutError:
            return

    async def _refresh_task_lock(self) -> bool:
        now = time.monotonic()
        if now - self._last_lock_refresh < self._lock_refresh_interval:
            return True
        refreshed = await self._tasks.refresh_lock(self._task_id, self._worker_id)
        if refreshed is None:
            logger.warning(
                "Auto broadcast lock refresh failed",
                extra={"task_id": self._task_id, "worker_id": self._worker_id},
            )
            return False
        self._last_lock_refresh = now
        return True

    @staticmethod
    def _seconds_until_due(task: AutoBroadcastTask) -> float:
        if task.next_run_ts is None:
            return 0.0
        now = datetime.utcnow()
        if task.next_run_ts <= now:
            return 0.0
        delta = (task.next_run_ts - now).total_seconds()
        return max(0.0, delta)

    async def _execute_cycle(self, task: AutoBroadcastTask) -> None:
        logger.debug("Starting auto broadcast cycle", extra={"task_id": task.task_id, "user_id": task.user_id})
        cycle_started = time.monotonic()
        self._last_lock_refresh = time.monotonic()

        sessions = await self._resolve_sessions(task)
        if not sessions:
            await self._handle_no_sessions(task)
            return

        if task.account_mode == AccountMode.ALL:
            SHUFFLE_RANDOM.shuffle(sessions)

        per_account_target_counts: Dict[str, int] = {}
        for session in sessions:
            try:
                _, live_groups = await self._load_live_groups(session.session_id)
                per_account_target_counts[session.session_id] = len(live_groups)
            except Exception:
                logger.exception(
                    "Failed to read live groups for account",
                    extra={"task_id": task.task_id, "account_id": session.session_id},
                )
                per_account_target_counts[session.session_id] = 0

        logger.debug(
            "Planned per-account targets",
            extra={
                "task_id": task.task_id,
                "user_id": task.user_id,
                "per_account": per_account_target_counts,
            },
        )

        resume_account_id = task.current_account_id
        resume_batch_index = task.current_batch_index
        resume_group_index = task.current_group_index
        if resume_account_id:
            sessions = self._rotate_sessions_for_resume(sessions, resume_account_id)

        notify_task: Optional[asyncio.Task] = None
        if task.notify_each_cycle:
            notify_task = asyncio.create_task(
                self._notify_cycle_start(task, sessions, per_account_target_counts)
            )

        per_account_stats: Dict[str, AccountCycleStats] = {}
        pending_retries: List[Tuple[TelethonSession, int, int]] = []

        try:
            for account_index, session in enumerate(sessions):
                if self._stop_event.is_set():
                    break
                if not await self._is_account_available(session):
                    continue

                if session.session_id == resume_account_id:
                    batch_index = resume_batch_index
                    group_index = resume_group_index
                    resume_account_id = None
                    resume_batch_index = 0
                    resume_group_index = 0
                else:
                    batch_index = 0
                    group_index = 0

                await self._tasks.update_progress(
                    task.task_id,
                    current_account_id=session.session_id,
                    batch_index=batch_index,
                    group_index=group_index,
                )

                initial_version, planned_groups = await self._load_live_groups(session.session_id)
                planned_count = len(planned_groups)
                logger.debug(
                    "Auto broadcast account run starting",
                    extra={
                        "task_id": self._task_id,
                        "user_id": session.owner_id,
                        "account_id": session.session_id,
                        "groups_planned": planned_count,
                        "resume_batch": batch_index,
                        "resume_group": group_index,
                        "retry": False,
                    },
                )

                try:
                    stats = await self._process_account(
                        task,
                        session,
                        account_index=account_index,
                        resume_batch_index=batch_index,
                        resume_group_index=group_index,
                        is_retry=False,
                        initial_groups=planned_groups,
                        initial_version=initial_version,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.exception(
                        "Auto broadcast account processing failed",
                        extra={
                            "task_id": self._task_id,
                            "user_id": session.owner_id,
                            "account_id": session.session_id,
                        },
                    )
                    await self._notify_account_inactive(
                        session_id=session.session_id,
                        owner_id=session.owner_id,
                        session=session,
                        reason=f"unexpected_error: {exc.__class__.__name__}",
                        task=task,
                    )
                    stats = AccountCycleStats(
                        account_id=session.session_id,
                        groups_planned=planned_count,
                        expected_targets=planned_count,
                        sent=0,
                        failed=1,
                        attempts=0,
                    )

                per_account_stats[session.session_id] = stats

                logger.debug(
                    "Auto broadcast account run finished",
                    extra={
                        "task_id": self._task_id,
                        "user_id": session.owner_id,
                        "account_id": session.session_id,
                        "sent": stats.sent,
                        "failed": stats.failed,
                        "attempts": stats.attempts,
                        "expected_targets": stats.expected_targets,
                        "skipped_duplicates": stats.skipped_duplicates,
                        "retry": False,
                    },
                )

                if (
                    not self._stop_event.is_set()
                    and stats.groups_planned > 0
                    and stats.expected_targets > 0
                    and stats.attempts == 0
                ):
                    pending_retries.append((session, account_index, planned_count))
                    logger.warning(
                        "Detected account run with zero attempts",
                        extra={
                            "task_id": self._task_id,
                            "user_id": session.owner_id,
                            "account_id": session.session_id,
                                "groups_planned": planned_count,
                        },
                    )

                await self._tasks.reset_progress(task.task_id)

                if self._stop_event.is_set():
                    break

            if pending_retries and not self._stop_event.is_set():
                retry_account_ids = [session.session_id for session, _account_index, _planned_count in pending_retries]
                logger.warning(
                    "Retrying account runs with no activity",
                    extra={
                        "task_id": self._task_id,
                        "accounts": retry_account_ids,
                    },
                )
                for session, account_index, planned_count in pending_retries:
                    if self._stop_event.is_set():
                        break
                    try:
                        retry_stats = await self._process_account(
                            task,
                            session,
                            account_index=account_index,
                            resume_batch_index=0,
                            resume_group_index=0,
                            is_retry=True,
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        logger.exception(
                            "Retry run failed",
                            extra={
                                "task_id": self._task_id,
                                "user_id": session.owner_id,
                                "account_id": session.session_id,
                            },
                        )
                        await self._tasks.add_problem_account(self._task_id, session.session_id)
                        continue

                    retry_stats.retried = True
                    existing = per_account_stats.get(session.session_id)
                    if existing:
                        existing.sent += retry_stats.sent
                        existing.failed += retry_stats.failed
                        existing.attempts += retry_stats.attempts
                        existing.skipped_duplicates += retry_stats.skipped_duplicates
                        existing.expected_targets = max(
                            existing.expected_targets,
                            retry_stats.expected_targets,
                        )
                        existing.retried = True
                    else:
                        per_account_stats[session.session_id] = retry_stats

                    logger.debug(
                        "Auto broadcast account retry finished",
                        extra={
                            "task_id": self._task_id,
                            "user_id": session.owner_id,
                            "account_id": session.session_id,
                            "sent": retry_stats.sent,
                            "failed": retry_stats.failed,
                            "attempts": retry_stats.attempts,
                            "expected_targets": retry_stats.expected_targets,
                            "skipped_duplicates": retry_stats.skipped_duplicates,
                            "retry": True,
                        },
                    )

                    await self._tasks.reset_progress(task.task_id)

                    combined = per_account_stats[session.session_id]
                    if combined.expected_targets > 0 and combined.attempts == 0:
                        logger.warning(
                            "Account remained inactive after retry",
                            extra={
                                "task_id": self._task_id,
                                "user_id": session.owner_id,
                                "account_id": session.session_id,
                                "groups_planned": combined.groups_planned,
                            },
                        )
                        await self._tasks.add_problem_account(self._task_id, session.session_id)
        finally:
            if notify_task and not notify_task.done():
                notify_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await notify_task

        cycle_finished = time.monotonic()
        actual_cycle_seconds = max(0.1, cycle_finished - cycle_started)
        base_interval = (
            task.user_interval_seconds
            if math.isfinite(task.user_interval_seconds) and task.user_interval_seconds > 0
            else self._interval_margin
        )
        jitter_percent = random.uniform(0.05, 0.10)
        lower = max(self._interval_margin, base_interval * (1.0 - jitter_percent))
        upper = max(lower + 1.0, base_interval * (1.0 + jitter_percent))
        chosen_interval = random.uniform(lower, upper)
        minimal_gap = actual_cycle_seconds + self._interval_margin
        if chosen_interval < minimal_gap:
            chosen_interval = minimal_gap
        next_run_ts = datetime.utcnow() + timedelta(seconds=chosen_interval)

        total_sent = sum(stat.sent for stat in per_account_stats.values())
        total_failed = sum(stat.failed for stat in per_account_stats.values())
        total_expected = sum(stat.expected_targets for stat in per_account_stats.values())
        total_attempts = sum(stat.attempts for stat in per_account_stats.values())
        per_account_summary = {
            account_id: {
                "groups_planned": stats.groups_planned,
                "expected_targets": stats.expected_targets,
                "attempts": stats.attempts,
                "sent": stats.sent,
                "failed": stats.failed,
                "skipped_duplicates": stats.skipped_duplicates,
                "retried": stats.retried,
            }
            for account_id, stats in per_account_stats.items()
        }

        logger.debug(
            "Auto broadcast cycle summary",
            extra={
                "task_id": task.task_id,
                "user_id": task.user_id,
                "sent": total_sent,
                "failed": total_failed,
                "expected_targets": total_expected,
                "attempts": total_attempts,
                "per_account": per_account_summary,
            },
        )

        updated_task = await self._tasks.record_cycle_result(
            task.task_id,
            last_cycle_seconds=actual_cycle_seconds,
            next_run_ts=next_run_ts,
            totals_sent_delta=total_sent,
            totals_failed_delta=total_failed,
        )

        if task.notify_each_cycle:
            await self._notify_cycle_end(
                updated_task or task,
                sent=total_sent,
                failed=total_failed,
                duration_seconds=actual_cycle_seconds,
                next_run_ts=next_run_ts,
            )

    def _rotate_sessions_for_resume(
        self,
        sessions: List[TelethonSession],
        session_id: str,
    ) -> List[TelethonSession]:
        for idx, item in enumerate(sessions):
            if item.session_id == session_id:
                return sessions[idx:] + sessions[:idx]
        return sessions

    async def _resolve_sessions(self, task: AutoBroadcastTask) -> List[TelethonSession]:
        if task.account_mode == AccountMode.SINGLE:
            if not task.account_id:
                return []
            session = await self._sessions.get_by_session_id(task.account_id)
            if session is None or not session.is_active:
                return []
            status = await self._account_status_service.refresh_session(
                session,
                verify_dialog_access=True,
                use_cache=False,
            )
            if not status.active:
                await self._notify_account_inactive(
                    session_id=session.session_id,
                    owner_id=session.owner_id,
                    session=session,
                    reason=status.detail or "необходимо повторно авторизоваться",
                    task=task,
                )
                return []
            return [session]

        sessions = await self._session_manager.get_active_sessions(
            task.user_id,
            verify_live=False,
        )
        await self._accounts.bulk_sync_accounts(task.user_id, [entry.session_id for entry in sessions])
        filtered_sessions: List[TelethonSession] = []
        if not sessions:
            return filtered_sessions

        statuses = await self._account_status_service.refresh_sessions(
            sessions,
            verify_dialog_access=True,
            use_cache=False,
        )

        for session in sessions:
            status = statuses.get(session.session_id)
            if status is None or not status.active:
                reason = status.detail if status else "session_validation_failed"
                await self._notify_account_inactive(
                    session_id=session.session_id,
                    owner_id=session.owner_id,
                    session=session,
                    reason=reason,
                    task=task,
                )
                continue
            self._clear_inactive_marker(session.session_id)
            filtered_sessions.append(session)
        expected_ids = set(task.account_ids or [])
        live_ids = {session.session_id for session in filtered_sessions}
        missing = [session_id for session_id in expected_ids if session_id and session_id not in live_ids]
        for session_id in missing:
            await self._notify_account_inactive(
                session_id=session_id,
                owner_id=task.user_id,
                reason="необходимо повторно авторизоваться",
                task=task,
            )
        return filtered_sessions

    async def _notify_account_inactive(
        self,
        *,
        session_id: str,
        owner_id: int,
        reason: str,
        session: Optional[TelethonSession] = None,
        task: Optional[AutoBroadcastTask] = None,
    ) -> None:
        if not session_id:
            return
        if session_id in self._inactive_notified:
            return
        if not reason:
            reason = "status_unknown"
        if session is None:
            session = await self._sessions.get_by_session_id(session_id)
        username = None
        if session and isinstance(session.metadata, Mapping):
            raw_username = session.metadata.get("username")
            if isinstance(raw_username, str) and raw_username.strip():
                username = raw_username.strip()
        if username:
            label = f"@{username.lstrip('@')}"
        elif session is not None:
            label = session.display_name() or session.session_id
        else:
            label = session_id

        try:
            stored = await self._session_manager.deactivate_session(session_id)
            if stored is not None and session is not None:
                session.is_active = stored.is_active
        except Exception:
            logger.exception(
                "Failed to deactivate Telethon session",
                extra={"account_id": session_id, "owner_id": owner_id},
            )

        self._inactive_notified.add(session_id)
        if session is not None:
            session.is_active = False

        try:
            state = await self._accounts.mark_inactive(session_id, reason=reason)
            if state is None and session is not None:
                await self._accounts.upsert_account(
                    session_id,
                    owner_id,
                    session_id=session_id,
                    status=AccountStatus.INACTIVE,
                    blocked_reason=reason,
                    metadata=session.metadata,
                )
        except Exception:
            logger.exception(
                "Failed to persist inactive account state",
                extra={"account_id": session_id, "owner_id": owner_id, "reason": reason},
            )
        try:
            await self._tasks.add_problem_account(self._task_id, session_id)
        except Exception:
            logger.exception(
                "Failed to record problem account",
                extra={"account_id": session_id, "task_id": self._task_id},
            )

        pruned_task: Optional[AutoBroadcastTask] = None
        try:
            pruned_task = await self._tasks.remove_accounts_from_task(self._task_id, [session_id])
        except Exception:
            logger.exception(
                "Failed to prune inactive account from task",
                extra={"account_id": session_id, "task_id": self._task_id},
            )
        else:
            if pruned_task is not None and task is not None:
                task.account_id = pruned_task.account_id
                task.account_ids = list(pruned_task.account_ids)
                task.per_account_groups = dict(pruned_task.per_account_groups)
                task.current_account_id = pruned_task.current_account_id
                task.problem_accounts = list(pruned_task.problem_accounts)
                task.groups = list(pruned_task.groups)

        logger.warning(
            "Auto broadcast account became inactive",
            extra={
                "task_id": self._task_id,
                "user_id": owner_id,
                "account_id": session_id,
                "reason": reason,
            },
        )
        message = f"Аккаунт {label} стал неактивным, войдите снова."
        await self._safe_notify_user(owner_id, message)

    def _clear_inactive_marker(self, session_id: str) -> None:
        if session_id:
            self._inactive_notified.discard(session_id)

    @staticmethod
    def _is_auth_error(exc: Exception) -> bool:
        return isinstance(exc, AUTH_ERRORS)

    def _is_auth_error_reason(self, reason: Optional[str]) -> bool:
        if not reason:
            return False
        return reason in self._auth_error_names

    async def _handle_no_sessions(self, task: AutoBroadcastTask) -> None:
        message = (
            "Нет доступных аккаунтов для выполнения автозадачи. "
            "Авторизуйтесь снова через /login_phone или /login_qr."
        )
        await self._tasks.set_error_state(task.task_id, message)
        await self._safe_notify_user(task.user_id, message)
        logger.error(message, extra={"task_id": task.task_id, "user_id": task.user_id})

    async def _is_account_available(self, session: TelethonSession) -> bool:
        state = await self._accounts.get_by_account_id(session.session_id)
        if state is None:
            await self._accounts.upsert_account(session.session_id, session.owner_id, session_id=session.session_id)
            self._clear_inactive_marker(session.session_id)
            return True
        if state.status == AccountStatus.INACTIVE:
            await self._accounts.mark_active(session.session_id)
            self._clear_inactive_marker(session.session_id)
            return True
        if state.status == AccountStatus.BLOCKED:
            logger.warning(
                "Account is blocked, skipping",
                extra={"account_id": session.session_id, "owner_id": session.owner_id},
            )
            return False
        if state.status == AccountStatus.COOLDOWN and state.cooldown_until:
            if state.cooldown_until > datetime.utcnow():
                logger.debug(
                    "Account %s is on cooldown until %s",
                    session.session_id,
                    state.cooldown_until,
                )
                return False
            await self._accounts.clear_cooldown(session.session_id)
            self._clear_inactive_marker(session.session_id)
            return True
        self._clear_inactive_marker(session.session_id)
        return True

    def _groups_for_session(self, task: AutoBroadcastTask, session_id: str) -> List[GroupTarget]:
        # Deprecated: groups are now loaded from MongoDB on demand per account.
        return []

    async def _load_live_groups(self, session_id: str) -> tuple[int, List[GroupTarget]]:
        version, raw_groups = await self._sessions.get_broadcast_groups_with_version(session_id)
        groups: List[GroupTarget] = []
        for raw in raw_groups:
            if isinstance(raw, GroupTarget):
                groups.append(raw)
                continue
            if isinstance(raw, Mapping):
                try:
                    groups.append(GroupTarget.model_validate(raw))
                except Exception:
                    logger.warning(
                        "Failed to normalize broadcast group entry",
                        extra={"account_id": session_id, "group_payload": raw},
                    )
        return version, groups

    async def _process_account(
        self,
        task: AutoBroadcastTask,
        session: TelethonSession,
        *,
        account_index: int,
        resume_batch_index: int,
        resume_group_index: int,
        is_retry: bool,
        initial_groups: Optional[List[GroupTarget]] = None,
        initial_version: Optional[int] = None,
    ) -> AccountCycleStats:
        self._clear_inactive_marker(session.session_id)
        initial_count = len(initial_groups or [])
        stats = AccountCycleStats(
            account_id=session.session_id,
            groups_planned=initial_count,
            expected_targets=initial_count,
        )

        client: Optional[TelegramClient] = None
        try:
            client = await self._session_manager.build_client_from_session(session)
        except Exception as exc:
            logger.exception(
                "Failed to build Telethon client",
                extra={"task_id": task.task_id, "account_id": session.session_id, "retry": is_retry},
            )
            await self._notify_account_inactive(
                session_id=session.session_id,
                owner_id=session.owner_id,
                session=session,
                reason=f"ошибка восстановления сессии: {exc.__class__.__name__}",
                task=task,
            )
            return stats

        dialogs_cache: dict[str, list[object]] = {}
        batch_size = max(1, task.batch_size)
        resume_index = max(0, resume_batch_index * batch_size + resume_group_index)
        message_counter = resume_index
        account_label = session.display_name()
        session_inactive = False
        last_health_check = 0.0
        is_secondary_account = account_index >= 1
        delivered_peer_keys: Set[tuple[str, object | tuple]] = set()
        observed_targets: Set[tuple[str, object | tuple]] = set()
        current_version = initial_version
        groups: List[GroupTarget] = list(initial_groups) if initial_groups else []

        async def _ensure_account_active(force: bool = False) -> bool:
            nonlocal last_health_check, session_inactive
            now = time.monotonic()
            if not force and now - last_health_check < self._health_check_interval:
                return True
            last_health_check = now
            status = await self._account_status_service.refresh_session(
                session,
                verify_dialog_access=False,
                use_cache=False,
            )
            if status.active:
                self._clear_inactive_marker(session.session_id)
                return True
            await self._notify_account_inactive(
                session_id=session.session_id,
                owner_id=session.owner_id,
                session=session,
                reason=status.detail or "session_health_failed",
                task=task,
            )
            session_inactive = True
            return False

        async def _refresh_groups(force: bool = False) -> None:
            nonlocal current_version, groups
            try:
                version, loaded = await self._load_live_groups(session.session_id)
            except Exception:
                logger.exception(
                    "Failed to load broadcast groups",
                    extra={"task_id": self._task_id, "account_id": session.session_id},
                )
                if force:
                    groups = []
                return
            if force or current_version is None or version != current_version:
                if current_version is not None and version != current_version:
                    logger.info(
                        "Detected broadcast group update",
                        extra={
                            "task_id": self._task_id,
                            "account_id": session.session_id,
                            "old_version": current_version,
                            "new_version": version,
                            "group_count": len(loaded),
                        },
                    )
                current_version = version
                groups = loaded
            stats.groups_planned = len(groups)
            stats.expected_targets = max(stats.expected_targets, len(groups))

        try:
            if not await self._refresh_task_lock():
                logger.warning(
                    "Auto broadcast lock is no longer held",
                    extra={"task_id": task.task_id, "account_id": session.session_id},
                )
                self._stop_event.set()
                return stats

            if not groups:
                await _refresh_groups(force=True)

            if not groups:
                logger.warning(
                    "No groups configured for account",
                    extra={"task_id": task.task_id, "account_id": session.session_id},
                )
                return stats

            text, image_data = self._prepare_materials(session)
            if not text and image_data is None:
                logger.warning(
                    "Account %s has no broadcast materials, skipping",
                    session.session_id,
                )
                await self._safe_notify_user(
                    session.owner_id,
                    f"Аккаунт {session.display_name()} пропущен: нет текста или изображения для рассылки.",
                )
                return stats
            content_description = describe_content_payload(bool(text), image_data is not None)

            if not await _ensure_account_active(force=True):
                return stats

            index = resume_index
            while True:
                if self._stop_event.is_set() or session_inactive:
                    break

                if index >= len(groups):
                    await _refresh_groups(force=False)
                    if index >= len(groups):
                        break

                if not await _ensure_account_active():
                    break
                if not await self._refresh_task_lock():
                    logger.warning(
                        "Auto broadcast lock lost mid-run",
                        extra={"task_id": task.task_id, "account_id": session.session_id},
                    )
                    self._stop_event.set()
                    session_inactive = True
                    break

                try:
                    group = groups[index]
                except IndexError:
                    break

                group_payload: Mapping[str, object] = group.model_dump(mode="python", by_alias=True)
                try:
                    targets, duplicates_message = await resolve_group_targets(
                        client,
                        group_payload,
                        user_id=session.owner_id,
                        account_label=account_label,
                        account_session_id=session.session_id,
                        content_type=content_description,
                        dialogs_cache=dialogs_cache,
                    )
                except DialogsFetchError as exc:
                    await self._notify_account_inactive(
                        session_id=session.session_id,
                        owner_id=session.owner_id,
                        session=session,
                        reason=f"ошибка получения диалогов: {exc.error_type}",
                        task=task,
                    )
                    return stats
                except Exception as exc:
                    if self._is_auth_error(exc):
                        await self._notify_account_inactive(
                            session_id=session.session_id,
                            owner_id=session.owner_id,
                            session=session,
                            reason=f"ошибка доступа к чатам: {exc.__class__.__name__}",
                            task=task,
                        )
                        return stats
                    stats.failed += 1
                    await self._tasks.add_problem_account(self._task_id, session.session_id)
                    logger.exception(
                        "Auto broadcast: failed to resolve group",
                        extra={
                            "event_type": "auto_broadcast_target_error",
                            "task_id": self._task_id,
                            "user_id": session.owner_id,
                            "account_id": session.session_id,
                            "group_label": render_group_label(group_payload),
                            "error": str(exc),
                        },
                    )
                    index += 1
                    await _refresh_groups(force=False)
                    continue

                if not targets:
                    stats.failed += 1
                    await self._tasks.add_problem_account(self._task_id, session.session_id)
                    logger.warning(
                        "Auto broadcast: no accessible targets",
                        extra={
                            "event_type": "auto_broadcast_target_missing",
                            "task_id": self._task_id,
                            "user_id": session.owner_id,
                            "account_id": session.session_id,
                            "group_label": render_group_label(group_payload),
                        },
                    )
                    index += 1
                    await _refresh_groups(force=False)
                    continue

                for target_index, target in enumerate(targets):
                    if self._stop_event.is_set() or session_inactive:
                        break

                    identity = resolved_target_identity(target)
                    observed_targets.add(identity)

                    if identity in delivered_peer_keys:
                        stats.skipped_duplicates += 1
                        logger.debug(
                            "Auto broadcast duplicate target skipped",
                            extra={
                                "event_type": "auto_broadcast_duplicate_skip",
                                "task_id": self._task_id,
                                "user_id": session.owner_id,
                                "account_id": session.session_id,
                                "group_label": target.label,
                                "identity": repr(identity),
                            },
                        )
                        continue

                    delivered_peer_keys.add(identity)

                    payload_text = text
                    result = await send_payload_to_group(
                        session_client=client,
                        entity=target.entity,
                        text=payload_text,
                        image_data=image_data,
                        user_id=session.owner_id,
                        account_label=account_label,
                        account_session_id=session.session_id,
                        group=target.group,
                        group_label=target.label,
                        content_type=content_description,
                        extra_log_context=target.log_context,
                    )
                    message_counter += 1

                    stats.attempts += result.attempts

                    log_payload = {
                        "task_id": self._task_id,
                        "user_id": session.owner_id,
                        "account_id": session.session_id,
                        "group_label": target.label,
                        "identity": repr(identity),
                        "attempts": result.attempts,
                        "final_error": result.final_error,
                        "transient_errors": result.transient_errors or None,
                        "retry": is_retry,
                    }

                    if result.success:
                        stats.sent += 1
                        log_payload["event_type"] = "auto_broadcast_message_sent"
                        logger.debug("Auto broadcast message sent", extra=log_payload)
                    else:
                        if self._is_auth_error_reason(result.final_error):
                            await self._notify_account_inactive(
                                session_id=session.session_id,
                                owner_id=session.owner_id,
                                session=session,
                                reason=f"ошибка отправки сообщения: {result.final_error}",
                                task=task,
                            )
                            return stats
                        stats.failed += 1
                        await self._tasks.add_problem_account(self._task_id, session.session_id)
                        log_payload["event_type"] = "auto_broadcast_message_failed"
                        logger.warning("Auto broadcast message failed", extra=log_payload)

                    has_more_targets = (
                        target_index + 1 < len(targets)
                        or index + 1 < len(groups)
                    )
                    if has_more_targets and not self._stop_event.is_set():
                        await self._sleep_between_messages(
                            message_counter,
                            batch_size,
                            is_secondary_account=is_secondary_account,
                        )

                absolute_index = index + 1
                batch_index = absolute_index // batch_size
                group_index = absolute_index % batch_size

                await self._tasks.update_progress(
                    task.task_id,
                    current_account_id=session.session_id,
                    batch_index=batch_index,
                    group_index=group_index,
                )

                if duplicates_message:
                    logger.debug(
                        "Auto broadcast duplicates handled",
                        extra={
                            "event_type": "auto_broadcast_duplicates",
                            "task_id": self._task_id,
                            "user_id": session.owner_id,
                            "account_id": session.session_id,
                            "group_label": render_group_label(group_payload),
                            "note": duplicates_message,
                            "retry": is_retry,
                        },
                    )

                index += 1
                await _refresh_groups(force=False)

                if self._stop_event.is_set() or session_inactive:
                    break
        finally:
            if client is not None:
                await self._session_manager.close_client(client)
        stats.expected_targets = max(stats.groups_planned, len(observed_targets))
        return stats

    def _prepare_materials(self, session: TelethonSession) -> Tuple[Optional[str], Optional[BroadcastImageData]]:
        metadata = session.metadata or {}
        raw_text = metadata.get("broadcast_text")
        text = raw_text.strip() if isinstance(raw_text, str) else None
        image_meta = extract_image_metadata(metadata)
        if image_meta:
            payload = prepare_image_payload(image_meta)
            if payload.is_legacy and not payload.raw_bytes:
                payload = None
        else:
            payload = None
        return text, self._to_broadcast_image(payload)

    @staticmethod
    def _to_broadcast_image(image_payload: Optional[ImagePayload]) -> Optional[BroadcastImageData]:
        if image_payload is None:
            return None
        return BroadcastImageData(
            media=image_payload.media,
            force_document=image_payload.force_document,
            raw_bytes=image_payload.raw_bytes,
            file_name=image_payload.file_name,
            mime_type=image_payload.mime_type,
        )

    async def _sleep_between_messages(
        self,
        message_counter: int,
        batch_size: int,
        *,
        is_secondary_account: bool,
    ) -> None:
        if self._stop_event.is_set():
            return
        batch_size = max(1, batch_size)
        if message_counter % batch_size == 0:
            delay = self._random_batch_pause()
        else:
            delay = self._random_message_delay()
        await self._delayed_wait(delay)
        if is_secondary_account and not self._stop_event.is_set():
            extra_delay = self._random_secondary_account_delay()
            await self._delayed_wait(extra_delay)

    @staticmethod
    def _random_message_delay() -> float:
        return random.uniform(float(BROADCAST_DELAY_MIN_SECONDS), float(BROADCAST_DELAY_MAX_SECONDS))

    @staticmethod
    def _random_batch_pause() -> float:
        base = float(BROADCAST_BATCH_PAUSE_SECONDS)
        return random.uniform(base * 0.75, base * 1.25)

    @staticmethod
    def _random_secondary_account_delay() -> float:
        return random.uniform(SECONDARY_ACCOUNT_DELAY_MIN_SECONDS, SECONDARY_ACCOUNT_DELAY_MAX_SECONDS)

    async def _notify_cycle_start(
        self,
        task: AutoBroadcastTask,
        sessions: Iterable[TelethonSession],
        target_counts: Optional[Mapping[str, int]] = None,
    ) -> None:
        await asyncio.sleep(0)  # allow calling context to proceed
        session_list = list(sessions)
        labels = ", ".join(session.display_name() for session in session_list)
        metadata_map = task.metadata if isinstance(task.metadata, Mapping) else {}
        actual_map = metadata_map.get("per_account_actual_targets") if isinstance(metadata_map, Mapping) else None
        groups_total = 0
        for session in session_list:
            count: Optional[int] = None
            if target_counts is not None:
                raw_planned = target_counts.get(session.session_id)
                if raw_planned is not None:
                    try:
                        count = int(raw_planned)
                    except (TypeError, ValueError):
                        count = None
            if count is None and isinstance(actual_map, Mapping):
                raw_actual = actual_map.get(session.session_id)
                if raw_actual is not None:
                    try:
                        count = int(raw_actual)
                    except (TypeError, ValueError):
                        count = None
            if count is None or count <= 0:
                count = len(self._groups_for_session(task, session.session_id))
            groups_total += max(0, count)

        expected_seconds = max(1, groups_total) * BROADCAST_DELAY_MAX_SECONDS
        text = (
            "🚀 Новый цикл автосообщений запущен.\n"
            f"Аккаунты: {labels}.\n"
            f"Чатов в цикле: {groups_total}.\n"
            f"Ожидаемая длительность: ≈ {self._format_duration(expected_seconds)}"
        )
        logger.debug(
            "Auto broadcast cycle started",
            extra={
                "event_type": "auto_broadcast_cycle_start",
                "task_id": task.task_id,
                "user_id": task.user_id,
                "accounts": labels,
                "groups_total": groups_total,
                "expected_duration_seconds": expected_seconds,
                "target_counts": dict(target_counts or {}),
            },
        )
        await self._safe_notify_user(task.user_id, text)

    async def _notify_cycle_end(
        self,
        task: AutoBroadcastTask,
        *,
        sent: int,
        failed: int,
        duration_seconds: float,
        next_run_ts: datetime,
    ) -> None:
        formatted_next_run = format_moscow_time(next_run_ts)
        summary = (
            "✅ Цикл автосообщений завершён.\n"
            f"Успешно: {sent}, ошибок: {failed}.\n"
            f"Длительность: {self._format_duration(duration_seconds)}.\n"
            f"Следующий запуск: {formatted_next_run}"
        )
        logger.debug(
            "Auto broadcast cycle completed",
            extra={
                "event_type": "auto_broadcast_cycle_end",
                "task_id": task.task_id,
                "user_id": task.user_id,
                "sent": sent,
                "failed": failed,
                "duration_seconds": duration_seconds,
                "next_run_ts": next_run_ts.isoformat(),
            },
        )
        await self._safe_notify_user(task.user_id, summary)

    async def _safe_notify_user(self, user_id: int, message: str) -> None:
        try:
            await self._bot_client.send_message(user_id, message)
        except Exception:
            logger.exception("Failed to send notification", extra={"user_id": user_id})

    @staticmethod
    def _format_duration(seconds: float) -> str:
        total_seconds = int(max(0, round(seconds)))
        if total_seconds <= 0:
            return "< 1 сек"
        hours, remainder = divmod(total_seconds, 3600)
        minutes, secs = divmod(remainder, 60)
        parts: List[str] = []
        if hours:
            parts.append(f"{hours} ч")
        if minutes:
            parts.append(f"{minutes} мин")
        if secs or not parts:
            parts.append(f"{secs} сек")
        return " ".join(parts)
