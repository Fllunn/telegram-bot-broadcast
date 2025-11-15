from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, Optional, Sequence

from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase
from pymongo import ReturnDocument

from src.models.session import SessionOwnerType, TelethonSession


class SessionRepository:
    """Handles persistence of Telethon sessions."""

    def __init__(self, database: AsyncIOMotorDatabase, collection_name: str) -> None:
        self._collection: AsyncIOMotorCollection = database.get_collection(collection_name)

    @staticmethod
    def _normalize_document(document: dict[str, Any]) -> dict[str, Any]:
        if document.get("_id") is not None:
            document["_id"] = str(document["_id"])
        metadata = document.get("metadata") or {}
        phone = document.get("phone") or metadata.get("phone")
        document["phone"] = phone or "не указан"
        return document

    async def ensure_indexes(self) -> None:
        await self._collection.create_index("session_id", unique=True)
        await self._collection.create_index([("owner_id", 1), ("owner_type", 1)])

    async def upsert_session(self, session: TelethonSession) -> TelethonSession:
        payload = session.model_dump(by_alias=True, exclude_none=True)
        created_at = payload.pop("created_at", datetime.utcnow())
        payload["updated_at"] = datetime.utcnow()

        result = await self._collection.find_one_and_update(
            {"session_id": session.session_id},
            {
                "$set": payload,
                "$setOnInsert": {"created_at": created_at},
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        if result is None:
            raise RuntimeError("Failed to upsert session document")
        normalized = self._normalize_document(result)
        return TelethonSession.model_validate(normalized)

    async def get_by_session_id(self, session_id: str) -> Optional[TelethonSession]:
        document = await self._collection.find_one({"session_id": session_id})
        if document is None:
            return None
        normalized = self._normalize_document(document)
        return TelethonSession.model_validate(normalized)

    async def get_by_session_ids(self, session_ids: Sequence[str]) -> list[TelethonSession]:
        ids = [session_id for session_id in session_ids if session_id]
        if not ids:
            return []
        cursor = self._collection.find({"session_id": {"$in": ids}})
        sessions: list[TelethonSession] = []
        async for document in cursor:
            normalized = self._normalize_document(document)
            sessions.append(TelethonSession.model_validate(normalized))
        return sessions

    async def get_active_sessions_for_owner(
        self,
        owner_id: int,
        owner_type: SessionOwnerType = SessionOwnerType.USER,
    ) -> Iterable[TelethonSession]:
        cursor = self._collection.find({"owner_id": owner_id, "owner_type": owner_type.value, "is_active": True})
        sessions: list[TelethonSession] = []
        async for document in cursor:
            normalized = self._normalize_document(document)
            sessions.append(TelethonSession.model_validate(normalized))
        return sessions

    async def list_sessions_for_owner(
        self,
        owner_id: int,
        owner_type: SessionOwnerType = SessionOwnerType.USER,
    ) -> list[TelethonSession]:
        cursor = self._collection.find({"owner_id": owner_id, "owner_type": owner_type.value})
        sessions: list[TelethonSession] = []
        async for document in cursor:
            normalized = self._normalize_document(document)
            sessions.append(TelethonSession.model_validate(normalized))
        return sessions

    async def set_session_active(self, session_id: str, is_active: bool) -> Optional[TelethonSession]:
        document = await self._collection.find_one_and_update(
            {"session_id": session_id},
            {
                "$set": {
                    "is_active": bool(is_active),
                    "updated_at": datetime.utcnow(),
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        if document is None:
            return None
        normalized = self._normalize_document(document)
        return TelethonSession.model_validate(normalized)

    async def delete_session(self, session_id: str) -> bool:
        result = await self._collection.delete_one({"session_id": session_id})
        return result.deleted_count > 0

    async def set_broadcast_texts(self, session_ids: Sequence[str], text: str) -> int:
        ids = [session_id for session_id in session_ids if session_id]
        if not ids:
            return 0
        result = await self._collection.update_many(
            {"session_id": {"$in": ids}},
            {
                "$set": {
                    "metadata.broadcast_text": text,
                    "updated_at": datetime.utcnow(),
                }
            },
        )
        return result.matched_count or 0

    async def set_broadcast_images(self, session_ids: Sequence[str], image_payload: dict[str, Any]) -> int:
        ids = [session_id for session_id in session_ids if session_id]
        if not ids or not image_payload:
            return 0
        result = await self._collection.update_many(
            {"session_id": {"$in": ids}},
            {
                "$set": {
                    "metadata.broadcast_image": image_payload,
                    "metadata.broadcast_image_file_id": None,
                    "updated_at": datetime.utcnow(),
                }
            },
        )
        return result.matched_count or 0

    async def set_broadcast_groups(self, session_id: str, groups: Sequence[dict[str, Any]]) -> bool:
        if not session_id:
            return False
        update = await self._collection.update_one(
            {"session_id": session_id},
            {
                "$set": {
                    "metadata.broadcast_groups": list(groups),
                    "updated_at": datetime.utcnow(),
                }
            },
        )
        return update.matched_count > 0

    async def set_broadcast_groups_bulk(self, session_ids: Sequence[str], groups: Sequence[dict[str, Any]]) -> int:
        ids = [session_id for session_id in session_ids if session_id]
        if not ids:
            return 0
        result = await self._collection.update_many(
            {"session_id": {"$in": ids}},
            {
                "$set": {
                    "metadata.broadcast_groups": list(groups),
                    "updated_at": datetime.utcnow(),
                }
            },
        )
        return result.matched_count or 0
