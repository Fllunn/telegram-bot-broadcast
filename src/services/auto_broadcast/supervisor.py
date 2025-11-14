from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass
from typing import Dict, Optional

from telethon import TelegramClient

from src.db.repositories.account_repository import AccountRepository
from src.db.repositories.auto_broadcast_task_repository import AutoBroadcastTaskRepository
from src.db.repositories.session_repository import SessionRepository
from src.models.auto_broadcast import AutoBroadcastTask, TaskStatus
from src.services.auto_broadcast.runner import AutoBroadcastRunner
from src.services.telethon_manager import TelethonSessionManager


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class RunnerHandle:
    """Tracks a running AutoBroadcastRunner and its metadata."""

    runner: AutoBroadcastRunner
    task: asyncio.Task
    restart_attempts: int = 0
    restart_task: Optional[asyncio.Task] = None
    latest_snapshot: Optional[AutoBroadcastTask] = None

    def cancel_restart(self) -> None:
        if self.restart_task and not self.restart_task.done():
            self.restart_task.cancel()


class AutoBroadcastSupervisor:
    """Supervises background runners and restarts them on failure."""

    def __init__(
        self,
        *,
        task_repository: AutoBroadcastTaskRepository,
        account_repository: AccountRepository,
        session_repository: SessionRepository,
        session_manager: TelethonSessionManager,
        bot_client: TelegramClient,
        worker_id: str,
        lock_ttl_seconds: int,
        poll_interval: float,
        max_delay_per_message: int,
        max_restart_attempts: int = 5,
        base_backoff: float = 5.0,
        max_backoff: float = 300.0,
    ) -> None:
        self._tasks = task_repository
        self._accounts = account_repository
        self._sessions = session_repository
        self._session_manager = session_manager
        self._bot_client = bot_client
        self._worker_id = worker_id
        self._lock_ttl = lock_ttl_seconds
        self._poll_interval = poll_interval
        self._max_delay = max_delay_per_message
        self._max_restart_attempts = max_restart_attempts
        self._base_backoff = base_backoff
        self._max_backoff = max_backoff
        self._wake_event = asyncio.Event()
        self._handles: Dict[str, RunnerHandle] = {}
        self._monitor_task: Optional[asyncio.Task] = None
        self._stopped = False

    async def start(self) -> None:
        if self._monitor_task is not None and not self._monitor_task.done():
            return
        self._stopped = False
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("Auto broadcast supervisor started")

    async def stop(self) -> None:
        self._stopped = True
        self._wake_event.set()
        for handle in list(self._handles.values()):
            handle.cancel_restart()
            handle.runner.stop()
            if not handle.task.done():
                handle.task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await handle.task
        self._handles.clear()
        if self._monitor_task is not None:
            self._monitor_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._monitor_task
            self._monitor_task = None
        logger.info("Auto broadcast supervisor stopped")

    def request_refresh(self) -> None:
        self._wake_event.set()

    async def _monitor_loop(self) -> None:
        while not self._stopped:
            try:
                await self._sync_active_tasks()
            except Exception:
                logger.exception("Failed to synchronize auto broadcast tasks")
            try:
                await asyncio.wait_for(self._wake_event.wait(), timeout=self._poll_interval)
                self._wake_event.clear()
            except asyncio.TimeoutError:
                continue

    async def _sync_active_tasks(self) -> None:
        active_tasks = await self._tasks.list_active_tasks()
        active_ids = {task.task_id for task in active_tasks}

        for task in active_tasks:
            handle = self._handles.get(task.task_id)
            if handle is None or handle.task.done():
                await self._launch_runner(task)
            else:
                handle.latest_snapshot = task

        for task_id in list(self._handles.keys()):
            if task_id not in active_ids:
                await self._stop_runner(task_id)

    async def _launch_runner(self, task: AutoBroadcastTask) -> None:
        runner = AutoBroadcastRunner(
            task.task_id,
            task_repository=self._tasks,
            account_repository=self._accounts,
            session_repository=self._sessions,
            session_manager=self._session_manager,
            bot_client=self._bot_client,
            worker_id=self._worker_id,
            lock_ttl_seconds=int(self._lock_ttl),
            max_delay_per_message=self._max_delay,
        )
        runner_task = asyncio.create_task(runner.run())
        handle = RunnerHandle(runner=runner, task=runner_task, latest_snapshot=task)
        self._handles[task.task_id] = handle
        runner_task.add_done_callback(
            lambda fut, task_id=task.task_id: asyncio.create_task(self._handle_runner_completion(task_id, fut))
        )
        logger.info("Launched auto broadcast runner", extra={"task_id": task.task_id, "user_id": task.user_id})

    async def _stop_runner(self, task_id: str) -> None:
        handle = self._handles.pop(task_id, None)
        if handle is None:
            return
        handle.cancel_restart()
        handle.runner.stop()
        if not handle.task.done():
            handle.task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await handle.task
        logger.info("Stopped auto broadcast runner", extra={"task_id": task_id})

    async def _handle_runner_completion(self, task_id: str, future: asyncio.Future) -> None:
        handle = self._handles.get(task_id)
        if handle is None:
            return
        if self._stopped:
            return
        if future.cancelled():
            logger.info("Runner cancelled", extra={"task_id": task_id})
            return
        exc = future.exception()
        if exc is None:
            handle.restart_attempts = 0
            logger.info("Runner finished gracefully", extra={"task_id": task_id})
            return
        handle.restart_attempts += 1
        logger.exception("Auto broadcast runner crashed", exc_info=exc, extra={"task_id": task_id})
        if handle.restart_attempts > self._max_restart_attempts:
            snapshot = handle.latest_snapshot or await self._tasks.get_by_task_id(task_id)
            if snapshot:
                await self._tasks.set_error_state(task_id, f"Runner crashed: {exc}")
                await self._notify_error(snapshot, str(exc))
            await self._stop_runner(task_id)
            return
        delay = min(self._base_backoff * (2 ** (handle.restart_attempts - 1)), self._max_backoff)
        handle.cancel_restart()
        handle.restart_task = asyncio.create_task(self._schedule_restart(task_id, delay))
        logger.info("Scheduled runner restart", extra={"task_id": task_id, "delay": delay})

    async def _schedule_restart(self, task_id: str, delay: float) -> None:
        try:
            await asyncio.sleep(delay)
            task = await self._tasks.get_by_task_id(task_id)
            if task is None or task.status != TaskStatus.RUNNING or not task.enabled:
                return
            await self._launch_runner(task)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Failed to restart runner", extra={"task_id": task_id})

    async def _notify_error(self, task: AutoBroadcastTask, message: str) -> None:
        text = (
            "Автозадача приостановлена из-за ошибки.\n"
            f"Task ID: {task.task_id}\n"
            f"Причина: {message}"
        )
        try:
            await self._bot_client.send_message(task.user_id, text)
        except Exception:
            logger.exception("Failed to notify user about runner error", extra={"task_id": task.task_id})
