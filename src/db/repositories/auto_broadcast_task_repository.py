from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Iterable, List, Optional

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
        now = datetime.utcnow()
        payload.setdefault("created_at", now)
        payload["updated_at"] = now
        try:
            result = await self._collection.insert_one(payload)
        except DuplicateKeyError as exc:  # pragma: no cover - motor translates unique index violation
            raise ValueError(f"Task with id {task.task_id} already exists") from exc
        payload["_id"] = str(result.inserted_id)
        return AutoBroadcastTask.model_validate(payload)

    async def replace_task(self, task: AutoBroadcastTask) -> AutoBroadcastTask:
        payload = task.model_dump(by_alias=True, exclude_none=True)
        payload["updated_at"] = datetime.utcnow()
        document = await self._collection.find_one_and_replace(
            {"task_id": task.task_id},
            payload,
            return_document=ReturnDocument.AFTER,
            upsert=False,
        )
        if document is None:
            raise ValueError(f"Task {task.task_id} not found for replacement")
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
