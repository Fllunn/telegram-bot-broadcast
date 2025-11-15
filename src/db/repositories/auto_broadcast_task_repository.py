from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Iterable, List, Optional, Sequence, Set

from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from src.models.auto_broadcast import AutoBroadcastTask, TaskStatus


class AutoBroadcastTaskRepository:
    """Persistence layer for auto broadcast task documents."""

    def __init__(self, database: AsyncIOMotorDatabase, collection_name: str) -> None:
        self._collection: AsyncIOMotorCollection = database.get_collection(collection_name)
        self._logger = logging.getLogger(__name__)

    async def ensure_indexes(self) -> None:
        await self._collection.create_index("task_id", unique=True)
        await self._collection.create_index([("user_id", 1), ("status", 1)])
        await self._collection.create_index("next_run_ts")
        await self._collection.create_index("enabled")
        await self._collection.create_index("locked_by")

    @staticmethod
    def _deserialize(document: Optional[dict]) -> Optional[AutoBroadcastTask]:
        if document is None:
            return None
        return AutoBroadcastTask.model_validate(AutoBroadcastTaskRepository._stringify_object_id(document))

    @classmethod
    def _stringify_object_id(cls, document: dict) -> dict:
        if document is None:
            return {}
        normalized = dict(document)
        if "_id" in normalized and normalized["_id"] is not None and not isinstance(normalized["_id"], str):
            try:
                normalized["_id"] = str(normalized["_id"])
            except Exception as exc:  # pragma: no cover - defensive logging
                logging.getLogger(__name__).warning(
                    "Failed to stringify task document _id", exc_info=exc
                )
                normalized["_id"] = repr(normalized["_id"])
        return normalized

    async def create_task(self, task: AutoBroadcastTask) -> AutoBroadcastTask:
        payload = task.model_dump(by_alias=True, exclude_none=True)
        payload.pop("_id", None)
        payload.pop("id", None)
        now = datetime.utcnow()
        payload.setdefault("created_at", now)
        payload["updated_at"] = now
        try:
            result = await self._collection.insert_one(payload)
        except DuplicateKeyError as exc:  # pragma: no cover - motor translates unique index violation
            raise ValueError(f"Task with id {task.task_id} already exists") from exc
        payload["_id"] = str(result.inserted_id)
        return AutoBroadcastTask.model_validate(payload)

    def _serialize_for_update(self, task: AutoBroadcastTask, *, include_none: bool = True) -> dict[str, Any]:
        payload = task.model_dump(by_alias=True, exclude_none=not include_none)
        payload.pop("_id", None)
        payload.pop("id", None)
        payload["updated_at"] = datetime.utcnow()
        return payload

    async def replace_task(self, task: AutoBroadcastTask) -> AutoBroadcastTask:
        payload = self._serialize_for_update(task)
        document = await self._collection.find_one_and_update(
            {"task_id": task.task_id},
            {"$set": payload},
            return_document=ReturnDocument.AFTER,
        )
        if document is None:
            raise ValueError(f"Task {task.task_id} not found for update")
        return AutoBroadcastTask.model_validate(self._stringify_object_id(document))

    async def get_by_task_id(self, task_id: str) -> Optional[AutoBroadcastTask]:
        document = await self._collection.find_one({"task_id": task_id})
        return self._deserialize(document)

    async def list_for_user(self, user_id: int) -> List[AutoBroadcastTask]:
        cursor = self._collection.find({"user_id": user_id}).sort("created_at", 1)
        tasks: List[AutoBroadcastTask] = []
        async for document in cursor:
            tasks.append(AutoBroadcastTask.model_validate(self._stringify_object_id(document)))
        return tasks

    async def list_active_tasks(self) -> List[AutoBroadcastTask]:
        cursor = self._collection.find(
            {
                "enabled": True,
                "status": TaskStatus.RUNNING.value,
            }
        )
        tasks: List[AutoBroadcastTask] = []
        async for document in cursor:
            tasks.append(AutoBroadcastTask.model_validate(self._stringify_object_id(document)))
        return tasks

    async def find_active_for_accounts(
        self,
        account_ids: Sequence[str],
        *,
        user_id: Optional[int] = None,
    ) -> List[AutoBroadcastTask]:
        ids = [account_id for account_id in account_ids if account_id]
        if not ids:
            return []
        query: dict = {
            "$and": [
                {"enabled": True},
                {"status": {"$in": [TaskStatus.RUNNING.value, TaskStatus.PAUSED.value]}},
                {
                    "$or": [
                        {"account_id": {"$in": ids}},
                        {"account_ids": {"$in": ids}},
                    ]
                },
            ]
        }
        if user_id is not None:
            query["$and"].append({"user_id": user_id})
        cursor = self._collection.find(query)
        tasks: List[AutoBroadcastTask] = []
        async for document in cursor:
            tasks.append(AutoBroadcastTask.model_validate(self._stringify_object_id(document)))
        return tasks

    async def acquire_lock(self, task_id: str, worker_id: str, lock_ttl_seconds: int) -> Optional[AutoBroadcastTask]:
        threshold = datetime.utcnow() - timedelta(seconds=max(1, lock_ttl_seconds))
        document = await self._collection.find_one_and_update(
            {
                "task_id": task_id,
                "enabled": True,
                "$or": [
                    {"locked_by": None},
                    {"lock_ts": {"$lte": threshold}},
                    {"locked_by": worker_id},
                ],
            },
            {
                "$set": {
                    "locked_by": worker_id,
                    "lock_ts": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        return self._deserialize(document)

    async def release_lock(self, task_id: str, worker_id: str) -> None:
        await self._collection.update_one(
            {"task_id": task_id, "locked_by": worker_id},
            {
                "$set": {
                    "locked_by": None,
                    "lock_ts": None,
                    "updated_at": datetime.utcnow(),
                }
            },
        )

    async def update_status(self, task_id: str, *, status: TaskStatus, enabled: Optional[bool] = None) -> Optional[AutoBroadcastTask]:
        update: dict = {
            "status": status.value,
            "updated_at": datetime.utcnow(),
        }
        if enabled is not None:
            update["enabled"] = enabled
        document = await self._collection.find_one_and_update(
            {"task_id": task_id},
            {"$set": update},
            return_document=ReturnDocument.AFTER,
        )
        return self._deserialize(document)

    async def update_notify_flag(self, task_id: str, notify: bool) -> Optional[AutoBroadcastTask]:
        document = await self._collection.find_one_and_update(
            {"task_id": task_id},
            {
                "$set": {
                    "notify_each_cycle": notify,
                    "updated_at": datetime.utcnow(),
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        return self._deserialize(document)

    async def update_progress(
        self,
        task_id: str,
        *,
        current_account_id: Optional[str],
        batch_index: int,
        group_index: int,
    ) -> None:
        await self._collection.update_one(
            {"task_id": task_id},
            {
                "$set": {
                    "current_account_id": current_account_id,
                    "current_batch_index": batch_index,
                    "current_group_index": group_index,
                    "updated_at": datetime.utcnow(),
                }
            },
        )

    async def reset_progress(self, task_id: str) -> None:
        await self._collection.update_one(
            {"task_id": task_id},
            {
                "$set": {
                    "current_account_id": None,
                    "current_batch_index": 0,
                    "current_group_index": 0,
                    "updated_at": datetime.utcnow(),
                }
            },
        )

    async def record_cycle_result(
        self,
        task_id: str,
        *,
        last_cycle_seconds: float,
        next_run_ts: datetime,
        totals_sent_delta: int,
        totals_failed_delta: int,
    ) -> Optional[AutoBroadcastTask]:
        document = await self._collection.find_one({"task_id": task_id})
        if document is None:
            return None

        task = AutoBroadcastTask.model_validate(self._stringify_object_id(document))
        cycles_completed = task.cycles_completed + 1
        total_sent = task.total_sent + totals_sent_delta
        total_failed = task.total_failed + totals_failed_delta
        average_cycle_time: Optional[float]
        if task.average_cycle_time is None:
            average_cycle_time = last_cycle_seconds
        else:
            previous_total = task.average_cycle_time * task.cycles_completed
            average_cycle_time = (previous_total + last_cycle_seconds) / max(1, cycles_completed)

        updated = await self._collection.find_one_and_update(
            {"task_id": task_id},
            {
                "$set": {
                    "last_cycle_time_seconds": last_cycle_seconds,
                    "next_run_ts": next_run_ts,
                    "last_run_at": datetime.utcnow(),
                    "cycles_completed": cycles_completed,
                    "average_cycle_time": average_cycle_time,
                    "total_sent": total_sent,
                    "total_failed": total_failed,
                    "current_account_id": None,
                    "current_batch_index": 0,
                    "current_group_index": 0,
                    "updated_at": datetime.utcnow(),
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        return self._deserialize(updated)

    async def set_error_state(self, task_id: str, message: str) -> Optional[AutoBroadcastTask]:
        document = await self._collection.find_one_and_update(
            {"task_id": task_id},
            {
                "$set": {
                    "status": TaskStatus.ERROR.value,
                    "enabled": False,
                    "last_error": message,
                    "last_error_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        return self._deserialize(document)

    async def add_problem_account(self, task_id: str, account_id: str) -> None:
        await self._collection.update_one(
            {"task_id": task_id},
            {
                "$addToSet": {"problem_accounts": account_id},
                "$set": {"updated_at": datetime.utcnow()},
            },
        )

    async def update_next_run(self, task_id: str, next_run_ts: datetime) -> None:
        await self._collection.update_one(
            {"task_id": task_id},
            {
                "$set": {
                    "next_run_ts": next_run_ts,
                    "updated_at": datetime.utcnow(),
                }
            },
        )

    async def bulk_update_accounts(self, task_id: str, account_ids: Iterable[str]) -> None:
        await self._collection.update_one(
            {"task_id": task_id},
            {
                "$set": {
                    "account_ids": list(account_ids),
                    "updated_at": datetime.utcnow(),
                }
            },
        )

    async def remove_accounts_from_task(self, task_id: str, account_ids: Sequence[str]) -> Optional[AutoBroadcastTask]:
        ids: Set[str] = {account_id for account_id in account_ids if account_id}
        if not ids:
            return await self.get_by_task_id(task_id)

        document = await self._collection.find_one({"task_id": task_id})
        if document is None:
            return None

        task = AutoBroadcastTask.model_validate(self._stringify_object_id(document))
        changed = False

        if task.account_id and task.account_id in ids:
            task.account_id = None
            changed = True

        if task.account_ids:
            filtered = [account_id for account_id in task.account_ids if account_id not in ids]
            if filtered != task.account_ids:
                task.account_ids = filtered
                changed = True

        if task.per_account_groups:
            filtered_groups = {
                account_id: groups
                for account_id, groups in task.per_account_groups.items()
                if account_id not in ids
            }
            if len(filtered_groups) != len(task.per_account_groups):
                task.per_account_groups = filtered_groups
                changed = True

        if task.groups:
            filtered_union = [
                group
                for group in task.groups
                if getattr(group, "source_session_id", None) not in ids
            ]
            if len(filtered_union) != len(task.groups):
                task.groups = filtered_union
                changed = True

        if task.problem_accounts:
            remaining_problems = [account_id for account_id in task.problem_accounts if account_id not in ids]
            if remaining_problems != task.problem_accounts:
                task.problem_accounts = remaining_problems
                changed = True

        if task.current_account_id and task.current_account_id in ids:
            task.current_account_id = None
            changed = True

        if not changed:
            return task

        task.updated_at = datetime.utcnow()
        payload = self._serialize_for_update(task)
        document = await self._collection.find_one_and_update(
            {"task_id": task_id},
            {"$set": payload},
            return_document=ReturnDocument.AFTER,
        )
        if document is None:
            return None
        return AutoBroadcastTask.model_validate(self._stringify_object_id(document))

    async def delete_task(self, task_id: str) -> bool:
        result = await self._collection.delete_one({"task_id": task_id})
        return bool(result.deleted_count)

    async def delete_tasks_for_user(self, user_id: int, task_ids: Optional[Sequence[str]] = None) -> int:
        query: dict[str, object] = {"user_id": user_id}
        if task_ids is not None:
            ids = [task_id for task_id in task_ids if task_id]
            if not ids:
                return 0
            query["task_id"] = {"$in": ids}
        result = await self._collection.delete_many(query)
        return result.deleted_count or 0
