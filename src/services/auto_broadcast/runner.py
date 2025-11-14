from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import time
from datetime import datetime, timedelta
from io import BytesIO
from typing import Iterable, List, Optional, Sequence, Tuple

from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError
from telethon.errors.rpcerrorlist import ChatWriteForbiddenError

from src.db.repositories.account_repository import AccountRepository
from src.db.repositories.auto_broadcast_task_repository import AutoBroadcastTaskRepository
from src.db.repositories.session_repository import SessionRepository
from src.models.auto_broadcast import AccountMode, AccountStatus, AutoBroadcastTask, GroupTarget, TaskStatus
from src.models.session import TelethonSession
from src.services.auto_broadcast.payloads import ImagePayload, extract_image_metadata, prepare_image_payload
from src.services.telethon_manager import TelethonSessionManager


logger = logging.getLogger(__name__)

INVISIBLE_SUFFIXES: Sequence[str] = ("\u200B", "\u200C", "\u200D", "\u2060", "\uFEFF")
INTER_MESSAGE_DELAY_RANGE: Tuple[int, int] = (4, 9)
BATCH_PAUSE_RANGE: Tuple[float, float] = (10.0, 15.0)
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
        dynamic_spread = max(5.0, actual_cycle_seconds * 0.05)
        lower = max(5.0, task.user_interval_seconds - dynamic_spread)
        upper = task.user_interval_seconds + dynamic_spread
        chosen_interval = random.uniform(lower, max(lower + 1, upper))
        next_run_ts = datetime.utcnow() + timedelta(seconds=chosen_interval)

        await self._tasks.record_cycle_result(
            task.task_id,
            last_cycle_seconds=actual_cycle_seconds,
            next_run_ts=next_run_ts,
            totals_sent_delta=total_sent,
            totals_failed_delta=total_failed,
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
        try:
            groups = self._groups_for_session(task, session.session_id)
            if not groups:
                logger.warning(
                    "No groups configured for account", extra={"task_id": task.task_id, "account_id": session.session_id}
                )
                return sent, failed

            resume_index = max(0, resume_batch_index * task.batch_size + resume_group_index)

            text, image_payload = self._prepare_materials(session)
            if not text and image_payload is None:
                logger.warning(
                    "Account %s has no broadcast materials, skipping", session.session_id
                )
                await self._safe_notify_user(
                    session.owner_id,
                    f"Аккаунт {session.display_name()} пропущен: нет текста или изображения для рассылки.",
                )
                return sent, failed

            for index, group in enumerate(groups):
                if index < resume_index:
                    continue
                if self._stop_event.is_set():
                    break

                success = await self._deliver_to_group(
                    client,
                    session,
                    group,
                    text,
                    image_payload,
                )
                absolute_index = index + 1
                batch_index = absolute_index // task.batch_size
                group_index = absolute_index % task.batch_size

                await self._tasks.update_progress(
                    task.task_id,
                    current_account_id=session.session_id,
                    batch_index=batch_index,
                    group_index=group_index,
                )

                if success:
                    sent += 1
                else:
                    failed += 1

                if self._stop_event.is_set():
                    break

                if absolute_index < len(groups):
                    await asyncio.sleep(random.randint(*INTER_MESSAGE_DELAY_RANGE))
                    if absolute_index % task.batch_size == 0:
                        await asyncio.sleep(random.uniform(*BATCH_PAUSE_RANGE))
        finally:
            await self._session_manager.close_client(client)
        return sent, failed

    def _prepare_materials(self, session: TelethonSession) -> Tuple[Optional[str], Optional[ImagePayload]]:
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
        return text, payload

    async def _deliver_to_group(
        self,
        client: TelegramClient,
        session: TelethonSession,
        group: GroupTarget,
        text: Optional[str],
        image_payload: Optional[ImagePayload],
    ) -> bool:
        try:
            entity = await self._resolve_entity(client, group)
        except Exception as exc:
            logger.exception(
                "Failed to resolve group entity",
                extra={"task_id": self._task_id, "group": group.model_dump()},
            )
            await self._tasks.add_problem_account(self._task_id, session.session_id)
            await self._safe_notify_user(
                session.owner_id,
                f"Не удалось получить группу {self._render_group_label(group)}: {exc}",
            )
            return False

        try:
            await self._send_payload(client, entity, text, image_payload)
            logger.info(
                "Message sent",
                extra={
                    "task_id": self._task_id,
                    "account_id": session.session_id,
                    "group": self._render_group_label(group),
                },
            )
            return True
        except FloodWaitError as exc:
            wait_seconds = int(getattr(exc, "seconds", 0) or 0)
            if wait_seconds < 180:
                await asyncio.sleep(wait_seconds)
                return await self._send_with_retry(client, entity, text, image_payload)
            cooldown_hours = random.randint(1, 3)
            cooldown_until = datetime.utcnow() + timedelta(hours=cooldown_hours)
            await self._accounts.mark_cooldown(session.session_id, cooldown_until=cooldown_until, reason=str(exc))
            await self._safe_notify_user(
                session.owner_id,
                (
                    f"Аккаунт {session.display_name()} отправлен на паузу до {cooldown_until:%d.%m %H:%M} из-за FloodWait "
                    f"на {wait_seconds} секунд."
                ),
            )
            return False
        except ChatWriteForbiddenError as exc:
            logger.warning(
                "Write forbidden",
                extra={
                    "task_id": self._task_id,
                    "account_id": session.session_id,
                    "group": self._render_group_label(group),
                    "error": str(exc),
                },
            )
            return False
        except RPCError as exc:
            logger.error(
                "RPC error during broadcast",
                extra={
                    "task_id": self._task_id,
                    "account_id": session.session_id,
                    "group": self._render_group_label(group),
                    "error": str(exc),
                },
            )
            return False
        except Exception:
            logger.exception(
                "Unexpected error during broadcast",
                extra={
                    "task_id": self._task_id,
                    "account_id": session.session_id,
                    "group": self._render_group_label(group),
                },
            )
            return False

    async def _send_with_retry(
        self,
        client: TelegramClient,
        entity: object,
        text: Optional[str],
        image_payload: Optional[ImagePayload],
    ) -> bool:
        try:
            await self._send_payload(client, entity, text, image_payload)
            return True
        except Exception:
            logger.exception("Retry after FloodWait failed")
            return False

    async def _send_payload(
        self,
        client: TelegramClient,
        entity: object,
        text: Optional[str],
        image_payload: Optional[ImagePayload],
    ) -> None:
        suffix_text = self._append_suffix(text)
        if image_payload is None:
            if suffix_text:
                await client.send_message(entity, suffix_text, parse_mode="html", link_preview=False)
            else:
                raise RuntimeError("No payload to send")
            return

        if image_payload.media is not None:
            await client.send_file(
                entity,
                file=image_payload.media,
                caption=suffix_text or None,
                parse_mode="html",
                force_document=image_payload.force_document,
                link_preview=False,
            )
            return

        if image_payload.raw_bytes is not None:
            buffer = BytesIO(image_payload.raw_bytes)
            if image_payload.file_name:
                buffer.name = image_payload.file_name
            await client.send_file(
                entity,
                file=buffer,
                caption=suffix_text or None,
                parse_mode="html",
                force_document=image_payload.force_document,
                link_preview=False,
            )
            return

        raise RuntimeError("Image payload is empty")

    @staticmethod
    def _append_suffix(text: Optional[str]) -> Optional[str]:
        if not text:
            return text
        return f"{text}{random.choice(INVISIBLE_SUFFIXES)}"

    async def _resolve_entity(self, client: TelegramClient, group: GroupTarget) -> object:
        if group.chat_id is not None:
            try:
                return await client.get_input_entity(group.chat_id)
            except Exception:
                logger.debug("Failed to resolve by chat_id", exc_info=True)
        if group.username:
            try:
                return await client.get_input_entity(group.username)
            except Exception:
                logger.debug("Failed to resolve by username", exc_info=True)
        if group.link:
            try:
                return await client.get_input_entity(group.link)
            except Exception:
                logger.debug("Failed to resolve by link", exc_info=True)
        raise RuntimeError(f"Не удалось получить доступ к группе {self._render_group_label(group)}")

    @staticmethod
    def _render_group_label(group: GroupTarget) -> str:
        if group.name:
            return group.name
        if group.username:
            return f"@{group.username.lstrip('@')}"
        if group.chat_id is not None:
            return str(group.chat_id)
        if group.link:
            return group.link
        return "неизвестная группа"

    async def _notify_cycle_start(self, task: AutoBroadcastTask, sessions: Iterable[TelethonSession]) -> None:
        await asyncio.sleep(0)  # allow calling context to proceed
        labels = ", ".join(session.display_name() for session in sessions)
        groups_total = 0
        for session in sessions:
            groups_total += len(self._groups_for_session(task, session.session_id))
        expected_seconds = max(1, groups_total) * self._max_delay
        text = (
            "Новый цикл автосообщений запущен.\n"
            f"Аккаунты: {labels}.\n"
            f"Ожидаемая длительность: {self._format_duration(expected_seconds)}"
        )
        await self._safe_notify_user(task.user_id, text)

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
