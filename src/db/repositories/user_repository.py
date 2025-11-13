from __future__ import annotations

from datetime import datetime
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase
from pymongo import ReturnDocument

from src.models.user import User


class UserRepository:
    """Provides CRUD access for Telegram users."""

    def __init__(self, database: AsyncIOMotorDatabase, collection_name: str) -> None:
        self._collection: AsyncIOMotorCollection = database.get_collection(collection_name)

    async def ensure_indexes(self) -> None:
        await self._collection.create_index("telegram_id", unique=True)

    async def upsert_user(self, user: User) -> User:
        payload = user.model_dump(by_alias=True, exclude_none=True)
        created_at = payload.pop("created_at", datetime.utcnow())
        payload["updated_at"] = datetime.utcnow()

        result = await self._collection.find_one_and_update(
            {"telegram_id": user.telegram_id},
            {
                "$set": payload,
                "$setOnInsert": {"created_at": created_at},
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        if result is None:
            raise RuntimeError("Failed to upsert user document")
        if result.get("_id") is not None:
            result["_id"] = str(result["_id"])
        return User.model_validate(result)

    async def get_by_telegram_id(self, telegram_id: int) -> Optional[User]:
        document = await self._collection.find_one({"telegram_id": telegram_id})
        if document is None:
            return None
        if document.get("_id") is not None:
            document["_id"] = str(document["_id"])
        return User.model_validate(document)
