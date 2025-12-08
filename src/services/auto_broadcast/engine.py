from __future__ import annotations

import logging
import secrets
from datetime import datetime
import math
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set

from telethon import TelegramClient

from src.db.repositories.account_repository import AccountRepository
from src.db.repositories.auto_broadcast_task_repository import AutoBroadcastTaskRepository
from src.db.repositories.session_repository import SessionRepository
from src.models.auto_broadcast import AccountMode, AccountStatus, AutoBroadcastTask, GroupTarget, TaskStatus
from src.models.session import TelethonSession
from src.services.auto_broadcast.state_manager import AutoTaskStateManager
from src.services.auto_broadcast.intervals import MAX_INTERVAL_SECONDS, NORMALIZED_MAX_INTERVAL
from src.services.auto_broadcast.supervisor import AutoBroadcastSupervisor
from src.services.auto_broadcast.payloads import extract_image_metadata
from src.services.telethon_manager import TelethonSessionManager
from src.services.account_status import AccountStatusService
from src.services.broadcast_shared import deduplicate_broadcast_groups


logger = logging.getLogger(__name__)


class InvalidIntervalError(ValueError):
    """Raised when user interval is below the required minimum."""

    def __init__(self, minimum_seconds: float) -> None:
        super().__init__("Interval is below required minimum")
        self.minimum_seconds = minimum_seconds


class AccountInUseError(RuntimeError):
    """Raised when attempting to create an auto task for an occupied account."""


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
        batch_pause_max_seconds: float = 15.0,
        interval_safety_margin_seconds: float = 5.0,
        account_status_service: AccountStatusService,
    ) -> None:
        self._tasks = task_repository
        self._accounts = account_repository
        self._sessions = session_repository
        self._session_manager = session_manager
        self._account_status_service = account_status_service
        self._bot_client = bot_client
        self._max_delay = max_delay_per_message
        self._batch_pause_max = max(0.0, float(batch_pause_max_seconds))
        self._interval_safety_margin = max(1.0, float(interval_safety_margin_seconds))
        self._default_batch_size = 20
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
            batch_pause_max_seconds=self._batch_pause_max,
            interval_safety_margin_seconds=self._interval_safety_margin,
            account_status_service=account_status_service,
        )
        self.state_manager = AutoTaskStateManager()

    async def start(self) -> None:
        await self._self_heal_all_tasks()
        await self._supervisor.start()

    async def stop(self) -> None:
        await self._supervisor.stop()

    async def list_tasks_for_user(self, user_id: int, *, active_only: bool = False) -> List[AutoBroadcastTask]:
        tasks = await self._tasks.list_for_user(user_id)
        cleaned = await self._clean_user_tasks(user_id, tasks)
        if active_only:
            cleaned = [task for task in cleaned if self._is_task_active(task)]
        return cleaned

    async def list_active_tasks(self, user_id: int) -> List[AutoBroadcastTask]:
        return [task for task in await self.list_tasks_for_user(user_id, active_only=True) if self._is_task_active(task)]

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

    async def remove_task(self, *, task_id: str, user_id: int) -> bool:
        task = await self._tasks.get_by_task_id(task_id)
        if task is None or task.user_id != user_id:
            return False
        await self._supervisor.remove_task(task_id)
        deleted = await self._tasks.delete_task(task_id)
        if deleted:
            self._supervisor.request_refresh()
        return deleted

    async def remove_tasks(self, *, user_id: int, task_ids: Optional[Sequence[str]] = None) -> int:
        stopped, _ = await self.stop_tasks(user_id=user_id, task_ids=task_ids)
        return stopped

    async def stop_tasks(
        self,
        *,
        user_id: int,
        task_ids: Optional[Sequence[str]] = None,
    ) -> tuple[int, int]:
        """
        Stop and remove auto broadcast tasks for the user.

        Returns tuple (stopped_count, total_requested).
        """
        tasks = await self._tasks.list_for_user(user_id)
        tasks = await self._clean_user_tasks(user_id, tasks)
        if task_ids is not None:
            id_set = {task_id for task_id in task_ids if task_id}
            filtered = [task for task in tasks if task.task_id in id_set]
        else:
            filtered = list(tasks)
        total_requested = len(filtered)
        if not filtered:
            return 0, total_requested
        stopped = 0
        for task in filtered:
            await self._supervisor.remove_task(task.task_id)
            deleted = await self._tasks.delete_task(task.task_id)
            if deleted:
                stopped += 1
        if stopped:
            self._supervisor.request_refresh()
        return stopped, total_requested

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

        existing_tasks = await self._tasks.list_for_user(user_id)
        await self._clean_user_tasks(user_id, existing_tasks)
        await self._ensure_accounts_available(user_id, sessions)

        if not math.isfinite(user_interval_seconds) or user_interval_seconds <= 0:
            raise ValueError("Укажите корректный интервал между циклами")

        if user_interval_seconds > MAX_INTERVAL_SECONDS:
            raise ValueError(f"Интервал слишком большой. Максимум — {NORMALIZED_MAX_INTERVAL}.")

        groups_by_account = self._extract_groups(sessions)
        total_groups = sum(len(groups) for groups in groups_by_account.values())
        if total_groups == 0:
            raise ValueError(
                "Невозможно создать автозадачу: нет доступных групп. Добавьте хотя бы одну группу и попробуйте снова."
            )

        materials_presence: Dict[str, bool] = {}
        for session in sessions:
            metadata = session.metadata or {}
            mapping = metadata if isinstance(metadata, Mapping) else {}
            raw_text = mapping.get("broadcast_text") if isinstance(mapping, Mapping) else None
            text_value = None
            if isinstance(raw_text, str):
                text_value = raw_text.strip()
            elif raw_text is not None:
                text_value = str(raw_text).strip()
            has_text = bool(text_value)
            has_image = bool(extract_image_metadata(mapping))
            materials_presence[session.session_id] = has_text or has_image
            if not materials_presence[session.session_id]:
                logger.warning(
                    "Auto-task account missing materials",
                    extra={
                        "user_id": user_id,
                        "session_id": session.session_id,
                    },
                )

        if not any(materials_presence.values()):
            raise ValueError("Нет сохранённого текста или изображения для автозадачи")
        minimum_interval = self._calculate_minimum_interval(groups_by_account, batch_size)
        if user_interval_seconds <= minimum_interval:
            raise InvalidIntervalError(minimum_interval)

        if minimum_interval > MAX_INTERVAL_SECONDS:
            raise ValueError(
                "Минимальный безопасный интервал для выбранных параметров превышает лимит {0}. "
                "Уменьшите количество групп или разбейте рассылку на несколько задач.".format(NORMALIZED_MAX_INTERVAL)
            )

        self._default_batch_size = max(1, batch_size)

        union_groups = self._build_union_groups(groups_by_account)
        account_ids = [session.session_id for session in sessions]
        primary_account = account_ids[0] if account_mode == AccountMode.SINGLE else None

        per_account_actual_targets: Dict[str, int] = {}
        for session in sessions:
            metadata = session.metadata or {}
            mapping = metadata if isinstance(metadata, Mapping) else {}
            stats_payload = mapping.get("broadcast_groups_stats") if isinstance(mapping, Mapping) else None
            actual = None
            if isinstance(stats_payload, Mapping):
                actual = stats_payload.get("actual_targets")
            actual_value = None
            if actual is not None:
                try:
                    actual_value = int(actual)
                except (TypeError, ValueError):
                    actual_value = None
            if actual_value is None or actual_value <= 0:
                account_groups_list = groups_by_account.get(session.session_id) or []
                actual_value = len(account_groups_list)
            per_account_actual_targets[session.session_id] = max(0, actual_value)

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
            metadata={
                "per_account_actual_targets": per_account_actual_targets,
            },
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
        logger.debug("Auto broadcast task created", extra={"task_id": stored.task_id, "user_id": user_id})
        return stored

    async def load_active_sessions(self, user_id: int, *, ensure_fresh_metadata: bool = False) -> List[TelethonSession]:
        sessions = await self._session_manager.get_active_sessions(
            user_id,
            verify_live=False,
        )
        if not sessions:
            return []

        statuses = await self._account_status_service.refresh_sessions(
            sessions,
            verify_dialog_access=True,
            use_cache=False,
        )
        validated: List[TelethonSession] = []
        for session in sessions:
            status = statuses.get(session.session_id)
            if status is None or not status.active:
                await self._session_manager.deactivate_session(session.session_id)
                await self.mark_account_inactive(
                    session.session_id,
                    owner_id=session.owner_id,
                    reason=(status.detail if status else "dialog_fetch_failed"),
                    metadata=session.metadata,
                )
                continue
            await self.mark_account_active(
                session.session_id,
                owner_id=session.owner_id,
                metadata=session.metadata,
            )
            validated.append(session)
        if not ensure_fresh_metadata:
            return validated

        refreshed: List[TelethonSession] = []
        for session in validated:
            metadata = session.metadata or {}
            groups = metadata.get("broadcast_groups") if isinstance(metadata, Mapping) else None
            if groups:
                refreshed.append(session)
                continue

            latest = await self._sessions.get_by_session_id(session.session_id)
            if latest is None:
                refreshed.append(session)
                continue
            status = await self._account_status_service.refresh_session(
                latest,
                verify_dialog_access=True,
                use_cache=False,
            )
            if not status.active:
                await self._session_manager.deactivate_session(latest.session_id)
                await self.mark_account_inactive(
                    latest.session_id,
                    owner_id=latest.owner_id,
                    reason=status.detail or "dialog_fetch_failed",
                    metadata=latest.metadata,
                )
                continue

            await self.mark_account_active(
                latest.session_id,
                owner_id=latest.owner_id,
                metadata=latest.metadata,
            )
            refreshed.append(latest)
        return refreshed

    async def mark_account_active(
        self,
        account_id: str,
        *,
        owner_id: Optional[int] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> None:
        try:
            state = await self._accounts.mark_active(account_id)
            if state is None and owner_id is not None:
                await self._accounts.upsert_account(
                    account_id,
                    owner_id,
                    session_id=account_id,
                    status=AccountStatus.ACTIVE,
                    metadata=dict(metadata or {}),
                )
        except Exception:
            logger.exception(
                "Failed to mark account active",
                extra={"account_id": account_id, "owner_id": owner_id},
            )

    async def mark_account_inactive(
        self,
        account_id: str,
        *,
        owner_id: int,
        reason: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> None:
        try:
            state = await self._accounts.mark_inactive(account_id, reason=reason)
            if state is None:
                await self._accounts.upsert_account(
                    account_id,
                    owner_id,
                    session_id=account_id,
                    status=AccountStatus.INACTIVE,
                    blocked_reason=reason,
                    metadata=dict(metadata or {}),
                )
        except Exception:
            logger.exception(
                "Failed to mark account inactive",
                extra={"account_id": account_id, "owner_id": owner_id, "reason": reason},
            )

    def minimum_interval_seconds(self, groups_by_account: Mapping[str, Sequence[GroupTarget]], batch_size: int = 20) -> float:
        return self._calculate_minimum_interval(groups_by_account, batch_size)

    def humanize_interval(self, seconds: float) -> str:
        corrected = False
        if seconds is None or not math.isfinite(seconds) or seconds <= 0:
            logger.warning(
                "Invalid interval value received for humanize",
                extra={"seconds": seconds},
            )
            seconds = self._interval_safety_margin
            corrected = True
        minimum = max(self._interval_safety_margin, 1.0)
        total_seconds = int(round(max(minimum, seconds)))
        hours, remainder = divmod(total_seconds, 3600)
        minutes, secs = divmod(remainder, 60)
        parts: List[str] = []
        if hours:
            parts.append(f"{hours} ч")
        if minutes:
            parts.append(f"{minutes} мин")
        if secs or not parts:
            parts.append(f"{secs} сек")
        result = " ".join(parts)
        if corrected:
            result = f"{result} (минимум)"
        return result

    def build_group_targets(self, raw_groups: Optional[Iterable[Any]]) -> List[GroupTarget]:
        if raw_groups is None:
            logger.debug("Received None for raw_groups; returning empty list")
            return []

        if isinstance(raw_groups, (list, tuple)):
            container = list(raw_groups)
        else:
            try:
                container = list(raw_groups)
            except TypeError:
                logger.warning(
                    "Unsupported raw_groups container",
                    extra={"container_type": type(raw_groups).__name__},
                )
                return []

        if not container:
            logger.debug("Received empty broadcast group list; nothing to build")
            return []

        targets: List[GroupTarget] = []
        for entry in container:
            try:
                target = self._coerce_group_target(entry)
            except Exception:
                logger.exception(
                    "Failed to normalize broadcast group entry",
                    extra={"entry_type": type(entry).__name__},
                )
                continue

            if target is None:
                continue
            targets.append(target)

        if not targets:
            logger.warning(
                "No valid broadcast groups parsed from input",
                extra={"raw_length": len(container)},
            )
        return targets

    @staticmethod
    def _metadata_groups(metadata: Mapping[str, Any]) -> List[dict[str, Any]]:
        if not isinstance(metadata, Mapping):
            return []
        unique_source = metadata.get("broadcast_groups_unique")
        if isinstance(unique_source, list) and unique_source:
            prepared: List[dict[str, Any]] = []
            for entry in unique_source:
                if isinstance(entry, Mapping):
                    prepared.append(dict(entry))
            if prepared:
                return prepared
        raw_source = metadata.get("broadcast_groups")
        raw_groups: List[Mapping[str, Any]] = []
        if isinstance(raw_source, list) and raw_source:
            for entry in raw_source:
                if isinstance(entry, Mapping):
                    raw_groups.append(entry)
        if not raw_groups:
            return []
        deduplicated = deduplicate_broadcast_groups(raw_groups)
        return [dict(entry) for entry in deduplicated]

    @staticmethod
    def _normalize_chat_id(value: Any) -> Optional[int]:
        if value is None:
            return None
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        try:
            string = str(value).strip()
        except Exception:
            return None
        if not string:
            return None
        if string.endswith(".0"):
            string = string[:-2]
        try:
            return int(string)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _normalize_username(value: Any) -> Optional[str]:
        if value is None:
            return None
        username = str(value).strip()
        if not username:
            return None
        username = username.lstrip("@")
        return username or None

    @staticmethod
    def _normalize_link(value: Any) -> Optional[str]:
        if value is None:
            return None
        link = str(value).strip()
        return link or None

    @staticmethod
    def _normalize_name(value: Any) -> Optional[str]:
        if value is None:
            return None
        name = str(value).strip()
        return name or None

    @staticmethod
    def _normalize_metadata(value: Any) -> Dict[str, Any]:
        if isinstance(value, Mapping):
            return dict(value)
        return {}

    def _coerce_group_target(self, entry: Any) -> Optional[GroupTarget]:
        if isinstance(entry, GroupTarget):
            copy = entry.model_copy(deep=True)
            copy.chat_id = self._normalize_chat_id(copy.chat_id)
            copy.username = self._normalize_username(copy.username)
            copy.link = self._normalize_link(copy.link)
            copy.name = self._normalize_name(copy.name)
            copy.metadata = self._normalize_metadata(copy.metadata)
            return copy

        payload: Dict[str, Any]
        if isinstance(entry, Mapping):
            payload = dict(entry)
        elif hasattr(entry, "model_dump"):
            payload = entry.model_dump()
        else:
            logger.debug(
                "Skipping unsupported group entry",
                extra={"entry_type": type(entry).__name__},
            )
            return None

        metadata_payload = payload.pop("metadata", {})
        metadata = self._normalize_metadata(metadata_payload)

        alias_map = {
            "chat_id": "chat_id",
            "chatid": "chat_id",
            "chat": "chat_id",
            "username": "username",
            "user_name": "username",
            "link": "link",
            "invite_link": "link",
            "url": "link",
            "name": "name",
            "title": "name",
            "source_session_id": "source_session_id",
            "sourceid": "source_session_id",
        }
        known_keys = set(alias_map.values())
        known_values: Dict[str, Any] = {}
        for key in list(payload.keys()):
            key_str = str(key)
            lower_key = key_str.lower()
            normalized_key = alias_map.get(lower_key)
            if normalized_key in known_keys:
                known_values[normalized_key] = payload.pop(key)

        if payload:
            metadata.update(payload)

        raw_chat_id = known_values.get("chat_id")
        raw_username = known_values.get("username")
        raw_link = known_values.get("link")
        raw_name = known_values.get("name")

        chat_id = self._normalize_chat_id(raw_chat_id)
        username = self._normalize_username(raw_username)
        link = self._normalize_link(raw_link)
        name = self._normalize_name(raw_name)

        if raw_chat_id is not None and chat_id is None:
            metadata.setdefault("raw_chat_id", raw_chat_id)
        if raw_username and username and raw_username != username:
            metadata.setdefault("raw_username", raw_username)
        if raw_link and link and raw_link != link:
            metadata.setdefault("raw_link", raw_link)
        if raw_name and name and raw_name != name:
            metadata.setdefault("raw_name", raw_name)

        target = GroupTarget(
            chat_id=chat_id,
            username=username,
            link=link,
            name=name,
            source_session_id=known_values.get("source_session_id") or None,
            metadata=metadata,
        )

        return target

    async def _load_sessions(self, user_id: int, session_ids: Sequence[str]) -> List[TelethonSession]:
        fetched: List[TelethonSession] = []
        for session_id in session_ids:
            session = await self._sessions.get_by_session_id(session_id)
            if session is None or session.owner_id != user_id:
                continue
            fetched.append(session)

        if not fetched:
            raise ValueError("Указанные аккаунты недоступны или отключены")

        statuses = await self._account_status_service.refresh_sessions(
            fetched,
            verify_dialog_access=True,
            use_cache=False,
        )

        sessions: List[TelethonSession] = []
        for session in fetched:
            status = statuses.get(session.session_id)
            if status is None or not status.active:
                await self._session_manager.deactivate_session(session.session_id)
                await self.mark_account_inactive(
                    session.session_id,
                    owner_id=session.owner_id,
                    reason=(status.detail if status else "session_validation_failed"),
                    metadata=session.metadata,
                )
                continue
            await self.mark_account_active(
                session.session_id,
                owner_id=session.owner_id,
                metadata=session.metadata,
            )
            sessions.append(session)

        if not sessions:
            raise ValueError("Указанные аккаунты недоступны или отключены")
        return sessions

    async def _ensure_accounts_available(self, user_id: int, sessions: Sequence[TelethonSession]) -> None:
        account_ids = [session.session_id for session in sessions]
        existing = await self._tasks.find_active_for_accounts(account_ids, user_id=user_id)
        if not existing:
            return
        occupied: Set[str] = set()
        for task in existing:
            occupied.update(self._task_account_ids(task) & set(account_ids))
        for session in sessions:
            if session.session_id in occupied:
                label = self._format_account_label(session)
                raise AccountInUseError(
                    "На аккаунт {label} уже запущена авторассылка.\nВы можете остановить её через кнопку \"Остановить авторассылку\".".format(
                        label=label
                    )
                )

    async def _clean_user_tasks(self, user_id: int, tasks: List[AutoBroadcastTask]) -> List[AutoBroadcastTask]:
        if not tasks:
            return []
        active_session_list = await self._session_manager.get_active_sessions(
            user_id,
            verify_live=True,
        )
        active_sessions = {session.session_id: session for session in active_session_list}
        tasks_sorted = sorted(
            tasks,
            key=lambda t: (
                self._is_task_active(t),
                t.created_at or t.updated_at or datetime.utcnow(),
            ),
            reverse=True,
        )
        accounts_seen: Set[str] = set()
        keep: List[AutoBroadcastTask] = []
        to_remove: List[AutoBroadcastTask] = []
        pruned = False
        for task in tasks_sorted:
            account_ids = self._task_account_ids(task)
            if not account_ids:
                to_remove.append(task)
                continue

            inactive_ids = [account_id for account_id in account_ids if account_id not in active_sessions]
            if inactive_ids:
                try:
                    updated_task = await self._tasks.remove_accounts_from_task(task.task_id, inactive_ids)
                except Exception:
                    logger.exception(
                        "Failed to prune inactive accounts from task",
                        extra={"task_id": task.task_id, "user_id": user_id},
                    )
                else:
                    if updated_task is not None:
                        task = updated_task
                        account_ids = self._task_account_ids(task)
                        pruned = True

            active_ids = [account_id for account_id in account_ids if account_id in active_sessions]
            if not active_ids:
                to_remove.append(task)
                continue
            if any(account_id in accounts_seen for account_id in active_ids):
                to_remove.append(task)
                continue
            accounts_seen.update(active_ids)
            keep.append(task)
        if to_remove:
            for task in to_remove:
                await self._supervisor.remove_task(task.task_id)
            await self._tasks.delete_tasks_for_user(user_id, [task.task_id for task in to_remove])
            for task in to_remove:
                logger.warning(
                    "Auto-task removed during cleanup", extra={"task_id": task.task_id, "user_id": user_id}
                )
            self._supervisor.request_refresh()
        elif pruned:
            self._supervisor.request_refresh()
        ordered_keep = sorted(keep, key=lambda t: t.created_at or t.updated_at or datetime.utcnow())
        return ordered_keep

    async def _self_heal_all_tasks(self) -> None:
        active_tasks = await self._tasks.list_active_tasks()
        if not active_tasks:
            return
        per_user: Dict[int, List[AutoBroadcastTask]] = {}
        for task in active_tasks:
            per_user.setdefault(task.user_id, []).append(task)
        for user_id in per_user:
            all_tasks = await self._tasks.list_for_user(user_id)
            await self._clean_user_tasks(user_id, all_tasks)

    @staticmethod
    def _task_account_ids(task: AutoBroadcastTask) -> Set[str]:
        ids: List[str] = []
        if task.account_id:
            ids.append(task.account_id)
        ids.extend(task.account_ids or [])
        if task.current_account_id:
            ids.append(task.current_account_id)
        return {account_id for account_id in ids if account_id}

    @staticmethod
    def _is_task_active(task: AutoBroadcastTask) -> bool:
        return bool(task.enabled and task.status == TaskStatus.RUNNING)

    @staticmethod
    def _format_account_label(session: TelethonSession) -> str:
        metadata = session.metadata or {}
        username = metadata.get("username") if isinstance(metadata, Mapping) else None
        label = AutoBroadcastService._normalize_username(username)
        if label:
            return f"@{label}"
        phone = session.phone or (metadata.get("phone") if isinstance(metadata, Mapping) else None)
        normalized_phone = AutoBroadcastService._normalize_phone(phone)
        if normalized_phone:
            return normalized_phone
        return "аккаунт"

    @staticmethod
    def _normalize_username(username: Optional[str]) -> Optional[str]:
        if not username:
            return None
        value = str(username).strip().lstrip("@")
        return value or None

    @staticmethod
    def _normalize_phone(phone: Optional[str]) -> Optional[str]:
        if not phone:
            return None
        digits = "".join(ch for ch in str(phone).strip() if ch not in {" ", "-", "(", ")"})
        if not digits:
            return None
        formatted = digits if digits.startswith("+") else f"+{digits.lstrip('+')}"
        return formatted if len(formatted) >= 4 else None
    def _extract_groups(self, sessions: Sequence[TelethonSession]) -> Dict[str, List[GroupTarget]]:
        result: Dict[str, List[GroupTarget]] = {}
        for session in sessions:
            metadata = session.metadata or {}
            metadata_mapping: Mapping[str, Any] = metadata if isinstance(metadata, Mapping) else {}
            group_payloads = self._metadata_groups(metadata_mapping)
            prepared: List[GroupTarget] = []
            for target in self.build_group_targets(group_payloads):
                if self._is_valid_group(target):
                    prepared.append(target)
            if group_payloads and not prepared:
                logger.debug(
                    "Metadata provided broadcast groups but none validated",
                    extra={
                        "session_id": session.session_id,
                        "user_id": session.owner_id,
                        "groups_provided": len(group_payloads),
                    },
                )
            for target in prepared:
                target.source_session_id = session.session_id
            result[session.session_id] = prepared
        return result

    def _calculate_minimum_interval(self, groups_by_account: Mapping[str, Sequence[GroupTarget]], batch_size: int) -> float:
        ceiling = self._estimate_cycle_ceiling(groups_by_account, batch_size)
        return ceiling + self._interval_safety_margin

    def _estimate_cycle_ceiling(self, groups_by_account: Mapping[str, Sequence[GroupTarget]], batch_size: int) -> float:
        batch_factor = max(1, batch_size)
        total = 0.0
        for groups in groups_by_account.values():
            count = len(groups)
            if count == 0:
                continue
            total += count * self._max_delay
            batches = max(0, math.ceil(count / batch_factor) - 1)
            total += batches * self._batch_pause_max
        return max(total, self._interval_safety_margin)

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
        return bool(group.chat_id or group.username or group.link or (group.name and group.name.strip()))

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

    @property
    def default_batch_size(self) -> int:
        return self._default_batch_size
