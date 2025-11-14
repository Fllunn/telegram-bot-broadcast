from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import random
import time
from datetime import datetime, timedelta
from typing import Iterable, List, Mapping, Optional, Sequence, Tuple

from telethon import TelegramClient

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
    describe_content_payload,
    render_group_label,
    resolve_group_targets,
    send_payload_to_group,
)
from src.services.telethon_manager import TelethonSessionManager


logger = logging.getLogger(__name__)

ANTISPAM_SUFFIXES: Sequence[str] = ("\u2060", "\u200B", "\u200C", " .", " …", " 🙂")
SHUFFLE_RANDOM = random.SystemRandom()


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
        self._bot_client = bot_client
        self._worker_id = worker_id
        self._lock_ttl = lock_ttl_seconds
        self._max_delay = max_delay_per_message
        self._batch_pause_max = batch_pause_max_seconds
        self._interval_margin = interval_safety_margin_seconds
        self._stop_event = asyncio.Event()

    def stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        logger.info("Auto broadcast runner started", extra={"task_id": self._task_id})
        try:
            while not self._stop_event.is_set():
                task = await self._tasks.get_by_task_id(self._task_id)
                if task is None:
                    logger.warning("Auto broadcast task removed during execution", extra={"task_id": self._task_id})
                    return
                if not task.enabled or task.status != TaskStatus.RUNNING:
                    logger.info(
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
            logger.info("Auto broadcast runner stopped", extra={"task_id": self._task_id})

    async def _delayed_wait(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=max(0.1, seconds))
        except asyncio.TimeoutError:
            return

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
        logger.info("Starting auto broadcast cycle", extra={"task_id": task.task_id, "user_id": task.user_id})
        cycle_started = time.monotonic()
        total_sent = 0
        total_failed = 0

        sessions = await self._resolve_sessions(task)
        if not sessions:
            await self._handle_no_sessions(task)
            return

        if task.account_mode == AccountMode.ALL:
            SHUFFLE_RANDOM.shuffle(sessions)

        resume_account_id = task.current_account_id
        resume_batch_index = task.current_batch_index
        resume_group_index = task.current_group_index
        if resume_account_id:
            sessions = self._rotate_sessions_for_resume(sessions, resume_account_id)

        notify_task = asyncio.create_task(self._notify_cycle_start(task, sessions)) if task.notify_each_cycle else None

        try:
            for session in sessions:
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

                sent, failed = await self._process_account(
                    task,
                    session,
                    resume_batch_index=batch_index,
                    resume_group_index=group_index,
                )
                total_sent += sent
                total_failed += failed

                await self._tasks.reset_progress(task.task_id)

                if self._stop_event.is_set():
                    break
        finally:
            if notify_task and not notify_task.done():
                notify_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await notify_task

        cycle_finished = time.monotonic()
        actual_cycle_seconds = max(0.1, cycle_finished - cycle_started)
        base_interval = task.user_interval_seconds if math.isfinite(task.user_interval_seconds) and task.user_interval_seconds > 0 else self._interval_margin
        jitter_percent = random.uniform(0.05, 0.10)
        lower = max(self._interval_margin, base_interval * (1.0 - jitter_percent))
        upper = max(lower + 1.0, base_interval * (1.0 + jitter_percent))
        chosen_interval = random.uniform(lower, upper)
        minimal_gap = actual_cycle_seconds + self._interval_margin
        if chosen_interval < minimal_gap:
            chosen_interval = minimal_gap
        next_run_ts = datetime.utcnow() + timedelta(seconds=chosen_interval)

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
            return [session]

        sessions_iter = await self._session_manager.get_active_sessions(task.user_id)
        sessions = list(sessions_iter)
        await self._accounts.bulk_sync_accounts(task.user_id, [entry.session_id for entry in sessions])
        return sessions

    async def _handle_no_sessions(self, task: AutoBroadcastTask) -> None:
        message = "Нет доступных аккаунтов для выполнения автозадачи."
        await self._tasks.set_error_state(task.task_id, message)
        await self._safe_notify_user(task.user_id, message)
        logger.error(message, extra={"task_id": task.task_id, "user_id": task.user_id})

    async def _is_account_available(self, session: TelethonSession) -> bool:
        state = await self._accounts.get_by_account_id(session.session_id)
        if state is None:
            await self._accounts.upsert_account(session.session_id, session.owner_id, session_id=session.session_id)
            return True
        if state.status == AccountStatus.BLOCKED:
            logger.warning(
                "Account is blocked, skipping",
                extra={"account_id": session.session_id, "owner_id": session.owner_id},
            )
            return False
        if state.status == AccountStatus.COOLDOWN and state.cooldown_until:
            if state.cooldown_until > datetime.utcnow():
                logger.info(
                    "Account %s is on cooldown until %s",
                    session.session_id,
                    state.cooldown_until,
                )
                return False
            await self._accounts.clear_cooldown(session.session_id)
        return True

    def _groups_for_session(self, task: AutoBroadcastTask, session_id: str) -> List[GroupTarget]:
        groups = task.per_account_groups.get(session_id)
        if groups:
            return groups
        return task.groups

    async def _process_account(
        self,
        task: AutoBroadcastTask,
        session: TelethonSession,
        *,
        resume_batch_index: int,
        resume_group_index: int,
    ) -> Tuple[int, int]:
        try:
            client = await self._session_manager.build_client_from_session(session)
        except Exception as exc:
            logger.exception(
                "Failed to build Telethon client",
                extra={"task_id": task.task_id, "account_id": session.session_id},
            )
            await self._safe_notify_user(
                session.owner_id,
                f"Не удалось восстановить аккаунт {session.display_name()} для рассылки: {exc}",
            )
            return 0, 0
        sent = 0
        failed = 0
        dialogs_cache: dict[str, list[object]] = {}
        batch_size = max(1, task.batch_size)
        resume_index = max(0, resume_batch_index * batch_size + resume_group_index)
        message_counter = resume_index
        account_label = session.display_name()
        try:
            groups = self._groups_for_session(task, session.session_id)
            if not groups:
                logger.warning(
                    "No groups configured for account", extra={"task_id": task.task_id, "account_id": session.session_id}
                )
                return sent, failed

            text, image_data = self._prepare_materials(session)
            if not text and image_data is None:
                logger.warning(
                    "Account %s has no broadcast materials, skipping", session.session_id
                )
                await self._safe_notify_user(
                    session.owner_id,
                    f"Аккаунт {session.display_name()} пропущен: нет текста или изображения для рассылки.",
                )
                return sent, failed
            content_description = describe_content_payload(bool(text), image_data is not None)

            for index, group in enumerate(groups):
                if index < resume_index:
                    continue
                if self._stop_event.is_set():
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
                except Exception as exc:
                    failed += 1
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
                    continue

                if not targets:
                    failed += 1
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
                    continue

                for target_index, target in enumerate(targets):
                    if self._stop_event.is_set():
                        break

                    payload_text = self._append_suffix(text)
                    success, reason = await send_payload_to_group(
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

                    log_payload = {
                        "task_id": self._task_id,
                        "user_id": session.owner_id,
                        "account_id": session.session_id,
                        "group_label": target.label,
                        "reason": reason,
                    }

                    if success:
                        sent += 1
                        log_payload["event_type"] = "auto_broadcast_message_sent"
                        logger.info("Auto broadcast message sent", extra=log_payload)
                    else:
                        failed += 1
                        await self._tasks.add_problem_account(self._task_id, session.session_id)
                        log_payload["event_type"] = "auto_broadcast_message_failed"
                        logger.warning("Auto broadcast message failed", extra=log_payload)

                    has_more_targets = (
                        target_index + 1 < len(targets)
                        or index + 1 < len(groups)
                    )
                    if has_more_targets and not self._stop_event.is_set():
                        await self._sleep_between_messages(message_counter, batch_size)

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
                    logger.info(
                        "Auto broadcast duplicates handled",
                        extra={
                            "event_type": "auto_broadcast_duplicates",
                            "task_id": self._task_id,
                            "user_id": session.owner_id,
                            "account_id": session.session_id,
                            "group_label": render_group_label(group_payload),
                            "note": duplicates_message,
                        },
                    )

                if self._stop_event.is_set():
                    break
        finally:
            await self._session_manager.close_client(client)
        return sent, failed

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

    @staticmethod
    def _append_suffix(text: Optional[str]) -> Optional[str]:
        if not text:
            return text
        return f"{text}{random.choice(ANTISPAM_SUFFIXES)}"

    async def _sleep_between_messages(self, message_counter: int, batch_size: int) -> None:
        if self._stop_event.is_set():
            return
        batch_size = max(1, batch_size)
        if message_counter % batch_size == 0:
            delay = self._random_batch_pause()
        else:
            delay = self._random_message_delay()
        await self._delayed_wait(delay)

    @staticmethod
    def _random_message_delay() -> float:
        return random.uniform(float(BROADCAST_DELAY_MIN_SECONDS), float(BROADCAST_DELAY_MAX_SECONDS))

    @staticmethod
    def _random_batch_pause() -> float:
        base = float(BROADCAST_BATCH_PAUSE_SECONDS)
        return random.uniform(base * 0.75, base * 1.25)

    async def _notify_cycle_start(self, task: AutoBroadcastTask, sessions: Iterable[TelethonSession]) -> None:
        await asyncio.sleep(0)  # allow calling context to proceed
        session_list = list(sessions)
        labels = ", ".join(session.display_name() for session in session_list)
        groups_total = sum(len(self._groups_for_session(task, session.session_id)) for session in session_list)
        expected_seconds = max(1, groups_total) * BROADCAST_DELAY_MAX_SECONDS
        text = (
            "🚀 Новый цикл автосообщений запущен.\n"
            f"Аккаунты: {labels}.\n"
            f"Чатов в цикле: {groups_total}.\n"
            f"Ожидаемая длительность: ≈ {self._format_duration(expected_seconds)}"
        )
        logger.info(
            "Auto broadcast cycle started",
            extra={
                "event_type": "auto_broadcast_cycle_start",
                "task_id": task.task_id,
                "user_id": task.user_id,
                "accounts": labels,
                "groups_total": groups_total,
                "expected_duration_seconds": expected_seconds,
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
        summary = (
            "✅ Цикл автосообщений завершён.\n"
            f"Успешно: {sent}, ошибок: {failed}.\n"
            f"Длительность: {self._format_duration(duration_seconds)}.\n"
            f"Следующий запуск: {next_run_ts:%d.%m %H:%M:%S}"
        )
        logger.info(
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
