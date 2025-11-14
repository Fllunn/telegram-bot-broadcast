from __future__ import annotations

import logging
import secrets
from datetime import datetime
from typing import Dict, Iterable, List, Mapping, Optional, Sequence

from telethon import TelegramClient

from src.db.repositories.account_repository import AccountRepository
from src.db.repositories.auto_broadcast_task_repository import AutoBroadcastTaskRepository
from src.db.repositories.session_repository import SessionRepository
from src.models.auto_broadcast import AccountMode, AutoBroadcastTask, GroupTarget, TaskStatus
from src.models.session import TelethonSession
from src.services.auto_broadcast.state_manager import AutoTaskStateManager
from src.services.auto_broadcast.supervisor import AutoBroadcastSupervisor
from src.services.telethon_manager import TelethonSessionManager


logger = logging.getLogger(__name__)


class InvalidIntervalError(ValueError):
    """Raised when user interval is below the required minimum."""

    def __init__(self, minimum_seconds: float) -> None:
        super().__init__("Interval is below required minimum")
        self.minimum_seconds = minimum_seconds


class AutoBroadcastService:
    """Facade for managing auto broadcast tasks and supervisor lifecycle."""

    def __init__(
        self,
        *,
        task_repository: AutoBroadcastTaskRepository,
        account_repository: AccountRepository,
        session_repository: SessionRepository,
        session_manager: TelethonSessionManager,
        bot_client: TelegramClient,
        worker_id: str,
        poll_interval: float,
        lock_ttl_seconds: int,
        max_delay_per_message: int,
    ) -> None:
        self._tasks = task_repository
        self._accounts = account_repository
        self._sessions = session_repository
        self._session_manager = session_manager
        self._bot_client = bot_client
        self._max_delay = max_delay_per_message
        self._supervisor = AutoBroadcastSupervisor(
            task_repository=task_repository,
            account_repository=account_repository,
            session_repository=session_repository,
            session_manager=session_manager,
            bot_client=bot_client,
            worker_id=worker_id,
            lock_ttl_seconds=lock_ttl_seconds,
            poll_interval=poll_interval,
            max_delay_per_message=max_delay_per_message,
        )
        self.state_manager = AutoTaskStateManager()

    async def start(self) -> None:
        await self._supervisor.start()

    async def stop(self) -> None:
        await self._supervisor.stop()

    async def list_tasks_for_user(self, user_id: int) -> List[AutoBroadcastTask]:
        return await self._tasks.list_for_user(user_id)

    async def get_task(self, task_id: str) -> Optional[AutoBroadcastTask]:
        return await self._tasks.get_by_task_id(task_id)

    async def pause_task(self, task_id: str) -> Optional[AutoBroadcastTask]:
        task = await self._tasks.update_status(task_id, status=TaskStatus.PAUSED, enabled=False)
        if task:
            self._supervisor.request_refresh()
        return task

    async def resume_task(self, task_id: str) -> Optional[AutoBroadcastTask]:
        task = await self._tasks.get_by_task_id(task_id)
        if task is None:
            return None
        await self._tasks.update_status(task_id, status=TaskStatus.RUNNING, enabled=True)
        await self._tasks.update_next_run(task_id, datetime.utcnow())
        self._supervisor.request_refresh()
        return await self._tasks.get_by_task_id(task_id)

    async def stop_task(self, task_id: str) -> Optional[AutoBroadcastTask]:
        task = await self._tasks.update_status(task_id, status=TaskStatus.STOPPED, enabled=False)
        if task:
            self._supervisor.request_refresh()
        return task

    async def toggle_notifications(self, task_id: str, enabled: bool) -> Optional[AutoBroadcastTask]:
        return await self._tasks.update_notify_flag(task_id, enabled)

    async def create_task(
        self,
        *,
        user_id: int,
        account_mode: AccountMode,
        session_ids: Sequence[str],
        user_interval_seconds: float,
        notify_each_cycle: bool,
        batch_size: int,
    ) -> AutoBroadcastTask:
        if not session_ids:
            raise ValueError("Не выбран ни один аккаунт для автозадачи")
        sessions = await self._load_sessions(user_id, session_ids)
        if not sessions:
            raise ValueError("Нет доступных аккаунтов для создания автозадачи")

        groups_by_account = self._extract_groups(sessions)
        total_groups = sum(len(groups) for groups in groups_by_account.values())
        if total_groups == 0:
            raise ValueError("Для выбранных аккаунтов не настроены группы для рассылки")
        minimum_interval = self._calculate_minimum_interval(groups_by_account)
        if user_interval_seconds <= minimum_interval:
            raise InvalidIntervalError(minimum_interval)

        union_groups = self._build_union_groups(groups_by_account)
        account_ids = [session.session_id for session in sessions]
        primary_account = account_ids[0] if account_mode == AccountMode.SINGLE else None

        task = AutoBroadcastTask(
            task_id=self._generate_task_id(),
            user_id=user_id,
            account_mode=account_mode,
            account_id=primary_account,
            account_ids=account_ids,
            groups=union_groups,
            per_account_groups=groups_by_account,
            user_interval_seconds=user_interval_seconds,
            enabled=True,
            status=TaskStatus.RUNNING,
            next_run_ts=datetime.utcnow(),
            notify_each_cycle=notify_each_cycle,
            batch_size=batch_size,
        )

        stored = await self._tasks.create_task(task)
        for session in sessions:
            await self._accounts.upsert_account(
                session.session_id,
                session.owner_id,
                session_id=session.session_id,
                metadata=session.metadata,
            )
        self._supervisor.request_refresh()
        logger.info("Auto broadcast task created", extra={"task_id": stored.task_id, "user_id": user_id})
        return stored

    async def load_active_sessions(self, user_id: int) -> List[TelethonSession]:
        result = await self._session_manager.get_active_sessions(user_id)
        return list(result)

    def minimum_interval_seconds(self, groups_by_account: Mapping[str, Sequence[GroupTarget]]) -> float:
        return self._calculate_minimum_interval(groups_by_account)

    def humanize_interval(self, seconds: float) -> str:
        total_seconds = int(round(max(0, seconds)))
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

    def build_group_targets(self, raw_groups: Iterable[Mapping[str, object]]) -> List[GroupTarget]:
        targets: List[GroupTarget] = []
        for entry in raw_groups:
            targets.append(
                GroupTarget(
                    chat_id=self._maybe_int(entry.get("chat_id")),
                    username=self._maybe_str(entry.get("username")),
                    link=self._maybe_str(entry.get("link")),
                    name=self._maybe_str(entry.get("name")),
                    source_session_id=self._maybe_str(entry.get("source_session_id")),
                    metadata=dict(entry) if isinstance(entry, Mapping) else {},
                )
            )
        return targets

    async def _load_sessions(self, user_id: int, session_ids: Sequence[str]) -> List[TelethonSession]:
        sessions: List[TelethonSession] = []
        for session_id in session_ids:
            session = await self._sessions.get_by_session_id(session_id)
            if session is not None and session.owner_id == user_id and session.is_active:
                sessions.append(session)
        if not sessions:
            raise ValueError("Указанные аккаунты недоступны или отключены")
        return sessions

    def _extract_groups(self, sessions: Sequence[TelethonSession]) -> Dict[str, List[GroupTarget]]:
        result: Dict[str, List[GroupTarget]] = {}
        for session in sessions:
            metadata = session.metadata or {}
            raw_groups = metadata.get("broadcast_groups") if isinstance(metadata, Mapping) else None
            if not isinstance(raw_groups, list):
                raw_groups = []
            targets = [target for target in self.build_group_targets(raw_groups) if self._is_valid_group(target)]
            result[session.session_id] = targets
        return result

    def _calculate_minimum_interval(self, groups_by_account: Mapping[str, Sequence[GroupTarget]]) -> float:
        total_groups = sum(len(groups) for groups in groups_by_account.values())
        return float(total_groups * self._max_delay)

    def _build_union_groups(self, groups_by_account: Mapping[str, Sequence[GroupTarget]]) -> List[GroupTarget]:
        seen: Dict[tuple, GroupTarget] = {}
        for session_id, groups in groups_by_account.items():
            for group in groups:
                key = (
                    group.chat_id,
                    group.username,
                    group.link,
                )
                if key not in seen:
                    seen[key] = group
        return list(seen.values())

    @staticmethod
    def _generate_task_id() -> str:
        return secrets.token_hex(8)

    @staticmethod
    def _is_valid_group(group: GroupTarget) -> bool:
        return bool(group.chat_id or group.username or group.link)

    def is_valid_group(self, group: GroupTarget) -> bool:
        return self._is_valid_group(group)

    @staticmethod
    def _maybe_int(value: object) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _maybe_str(value: object) -> Optional[str]:
        if value is None:
            return None
        string = str(value).strip()
        return string or None
