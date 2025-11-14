from __future__ import annotations

from datetime import datetime
from typing import Iterable, List, Optional

from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase
from pymongo import ReturnDocument

from src.models.auto_broadcast import AccountState, AccountStatus


class AccountRepository:
    """Stores runtime state of accounts involved in broadcasts."""

    def __init__(self, database: AsyncIOMotorDatabase, collection_name: str) -> None:
        self._collection: AsyncIOMotorCollection = database.get_collection(collection_name)

    async def ensure_indexes(self) -> None:
        await self._collection.create_index("account_id", unique=True)
        await self._collection.create_index([("owner_id", 1), ("status", 1)])
        await self._collection.create_index("cooldown_until")

    @staticmethod
    def _deserialize(document: Optional[dict]) -> Optional[AccountState]:
        if document is None:
            return None
        return AccountState.model_validate(document)

    async def upsert_account(
        self,
        account_id: str,
        owner_id: int,
        *,
        session_id: Optional[str] = None,
        status: AccountStatus = AccountStatus.ACTIVE,
        cooldown_until: Optional[datetime] = None,
        blocked_reason: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> AccountState:
        document = await self._collection.find_one_and_update(
            {"account_id": account_id},
            {
                "$set": {
                    "owner_id": owner_id,
                    "session_id": session_id,
                    "status": status.value,
                    "cooldown_until": cooldown_until,
                    "blocked_reason": blocked_reason,
                    "metadata": metadata or {},
                    "updated_at": datetime.utcnow(),
                },
                "$setOnInsert": {
                    "created_at": datetime.utcnow(),
                },
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        if document is None:
            raise RuntimeError("Failed to upsert account state")
        return AccountState.model_validate(document)

    async def get_by_account_id(self, account_id: str) -> Optional[AccountState]:
        document = await self._collection.find_one({"account_id": account_id})
        return self._deserialize(document)

    async def list_for_owner(self, owner_id: int) -> List[AccountState]:
        cursor = self._collection.find({"owner_id": owner_id})
        states: List[AccountState] = []
        async for document in cursor:
            states.append(AccountState.model_validate(document))
        return states

    async def mark_cooldown(
        self,
        account_id: str,
        *,
        cooldown_until: datetime,
        reason: Optional[str] = None,
    ) -> Optional[AccountState]:
        document = await self._collection.find_one_and_update(
            {"account_id": account_id},
            {
                "$set": {
                    "status": AccountStatus.COOLDOWN.value,
                    "cooldown_until": cooldown_until,
                    "blocked_reason": reason,
                    "updated_at": datetime.utcnow(),
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        return self._deserialize(document)

    async def clear_cooldown(self, account_id: str) -> Optional[AccountState]:
        document = await self._collection.find_one_and_update(
            {"account_id": account_id},
            {
                "$set": {
                    "status": AccountStatus.ACTIVE.value,
                    "cooldown_until": None,
                    "updated_at": datetime.utcnow(),
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        return self._deserialize(document)

    async def mark_blocked(self, account_id: str, reason: Optional[str] = None) -> Optional[AccountState]:
        document = await self._collection.find_one_and_update(
            {"account_id": account_id},
            {
                "$set": {
                    "status": AccountStatus.BLOCKED.value,
                    "blocked_reason": reason,
                    "updated_at": datetime.utcnow(),
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        return self._deserialize(document)

    async def mark_active(self, account_id: str) -> Optional[AccountState]:
        document = await self._collection.find_one_and_update(
            {"account_id": account_id},
            {
                "$set": {
                    "status": AccountStatus.ACTIVE.value,
                    "blocked_reason": None,
                    "updated_at": datetime.utcnow(),
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        return self._deserialize(document)

    async def bulk_sync_accounts(
        self,
        owner_id: int,
        account_ids: Iterable[str],
    ) -> None:
        ids = list(account_ids)
        now = datetime.utcnow()
        if not ids:
            return
        await self._collection.update_many(
            {"account_id": {"$nin": ids}, "owner_id": owner_id},
            {
                "$set": {
                    "status": AccountStatus.BLOCKED.value,
                    "blocked_reason": "account missing from session repository",
                    "updated_at": now,
                }
            },
        )
        await self._collection.update_many(
            {"account_id": {"$in": ids}, "owner_id": owner_id},
            {
                "$set": {
                    "status": AccountStatus.ACTIVE.value,
                    "updated_at": now,
                }
            },
        )
