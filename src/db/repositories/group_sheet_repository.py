from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase
from pymongo import ReturnDocument

logger = logging.getLogger(__name__)


class GroupSheetRepository:
    """Persists Google Sheets links and their last synced state per account.

    Document schema (group_sheet_links):
        {
            _id: ObjectId,
            session_id: str,
            owner_id: int,
            url: str,
            spreadsheet_id: str,
            gid: str,
            content_hash: str | None,
            last_sync_ts: datetime | None,
            last_error_ts: datetime | None,
            last_error: str | None,
            created_at: datetime,
            updated_at: datetime,
        }
    """

    def __init__(self, database: AsyncIOMotorDatabase, collection_name: str = "group_sheet_links") -> None:
        self._collection: AsyncIOMotorCollection = database.get_collection(collection_name)

    async def ensure_indexes(self) -> None:
        await self._collection.create_index([("session_id", 1), ("owner_id", 1)], unique=True)
        await self._collection.create_index("owner_id")

    @staticmethod
    def _normalize(document: dict[str, Any]) -> dict[str, Any]:
        if document.get("_id") is not None:
            document["_id"] = str(document["_id"])
        return document

    async def upsert_link(
        self,
        *,
        session_id: str,
        owner_id: int,
        url: str,
        spreadsheet_id: str,
        gid: str,
        content_hash: Optional[str] = None,
        last_sync_ts: Optional[datetime] = None,
    ) -> dict[str, Any]:
        payload = {
            "session_id": session_id,
            "owner_id": owner_id,
            "url": url,
            "spreadsheet_id": spreadsheet_id,
            "gid": gid,
        }
        if content_hash is not None:
            payload["content_hash"] = content_hash
        if last_sync_ts is not None:
            payload["last_sync_ts"] = last_sync_ts
        try:
            result = await self._collection.find_one_and_update(
                {"session_id": session_id, "owner_id": owner_id},
                {
                    "$set": payload,
                    "$setOnInsert": {"created_at": datetime.utcnow()},
                    "$unset": {"last_error": "", "last_error_ts": ""},
                    "$currentDate": {"updated_at": True},
                },
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )
        except Exception:  # noqa: BLE001
            logger.exception(
                "Failed to upsert sheet link",
                extra={"session_id": session_id, "owner_id": owner_id},
            )
            raise
        if result is None:
            raise RuntimeError("Upsert returned empty result")
        return self._normalize(result)

    async def update_state(
        self,
        *,
        session_id: str,
        owner_id: int,
        content_hash: str,
        last_sync_ts: datetime,
    ) -> Optional[dict[str, Any]]:
        update = {
            "content_hash": content_hash,
            "last_sync_ts": last_sync_ts,
            "updated_at": datetime.utcnow(),
        }
        try:
            result = await self._collection.find_one_and_update(
                {"session_id": session_id, "owner_id": owner_id},
                {"$set": update, "$unset": {"last_error": "", "last_error_ts": ""}},
                return_document=ReturnDocument.AFTER,
            )
        except Exception:
            logger.exception(
                "Failed to update sheet state",
                extra={"session_id": session_id, "owner_id": owner_id},
            )
            return None
        if result is None:
            return None
        return self._normalize(result)

    async def update_error(
        self,
        *,
        session_id: str,
        owner_id: int,
        error_message: str,
        when: Optional[datetime] = None,
    ) -> Optional[dict[str, Any]]:
        update = {
            "last_error": error_message,
            "last_error_ts": when or datetime.utcnow(),
            "updated_at": datetime.utcnow(),
        }
        try:
            result = await self._collection.find_one_and_update(
                {"session_id": session_id, "owner_id": owner_id},
                {"$set": update},
                return_document=ReturnDocument.AFTER,
            )
        except Exception:
            logger.exception(
                "Failed to update sheet error state",
                extra={"session_id": session_id, "owner_id": owner_id},
            )
            return None
        if result is None:
            return None
        return self._normalize(result)

    async def list_all_links(self) -> list[dict[str, Any]]:
        links: list[dict[str, Any]] = []
        try:
            cursor = self._collection.find({})
            async for document in cursor:
                links.append(self._normalize(document))
        except Exception:
            logger.exception("Failed to list sheet links")
        return links
