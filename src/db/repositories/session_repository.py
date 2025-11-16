from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Iterable, Mapping, Optional, Sequence

from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase
from pymongo import ReturnDocument

from src.models.session import SessionOwnerType, TelethonSession


logger = logging.getLogger(__name__)


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
        document.setdefault("status", None)
        document.setdefault("last_checked_at", None)
        document.setdefault("last_error", None)
        return document

    async def ensure_indexes(self) -> None:
        await self._collection.create_index("session_id", unique=True)
        await self._collection.create_index([("owner_id", 1), ("owner_type", 1)])

    async def upsert_session(self, session: TelethonSession) -> TelethonSession:
        payload = session.model_dump(by_alias=True, exclude_none=True)
        payload.pop("_id", None)
        payload.pop("id", None)
        created_at = payload.pop("created_at", datetime.utcnow())
        payload["updated_at"] = datetime.utcnow()

        try:
            result = await self._collection.find_one_and_update(
                {"session_id": session.session_id},
                {
                    "$set": payload,
                    "$setOnInsert": {"created_at": created_at},
                },
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )
        except Exception:
            logger.exception(
                "Failed to upsert session document",
                extra={"session_id": session.session_id, "owner_id": session.owner_id},
            )
            raise

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
                    "status": "active" if is_active else "inactive",
                    "updated_at": datetime.utcnow(),
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        if document is None:
            return None
        normalized = self._normalize_document(document)
        return TelethonSession.model_validate(normalized)

    async def update_status_fields(
        self,
        session_id: str,
        *,
        is_active: bool,
        status: str,
        last_checked_at: datetime,
        last_error: Optional[str],
    ) -> Optional[TelethonSession]:
        payload = {
            "is_active": bool(is_active),
            "status": status,
            "last_checked_at": last_checked_at,
            "last_error": last_error,
            "updated_at": datetime.utcnow(),
        }
        try:
            document = await self._collection.find_one_and_update(
                {"session_id": session_id},
                {"$set": payload},
                return_document=ReturnDocument.AFTER,
            )
        except Exception:
            logger.exception(
                "Failed to update session status fields",
                extra={"session_id": session_id},
            )
            raise
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

    async def set_broadcast_groups(
        self,
        session_id: str,
        groups: Sequence[dict[str, Any]],
        *,
        owner_id: int,
        unique_groups: Optional[Sequence[dict[str, Any]]] = None,
        stats: Optional[Mapping[str, Any]] = None,
    ) -> bool:
        if not session_id:
            return False
        unique_payload = list(unique_groups) if unique_groups is not None else []
        stats_payload = dict(stats or {})
        stats_payload.setdefault("file_rows", len(groups))
        stats_payload.setdefault("unique_groups", len(unique_payload) if unique_payload else len(groups))
        stats_payload.setdefault(
            "actual_targets",
            stats_payload.get("unique_groups") if isinstance(stats_payload.get("unique_groups"), int) else (len(unique_payload) if unique_payload else len(groups)),
        )
        update = await self._collection.update_one(
            {"session_id": session_id, "owner_id": owner_id},
            {
                "$set": {
                    "metadata.broadcast_groups": list(groups),
                    "metadata.broadcast_groups_unique": unique_payload,
                    "metadata.broadcast_groups_stats": stats_payload,
                    "updated_at": datetime.utcnow(),
                }
            },
        )
        return update.matched_count > 0

    async def set_broadcast_groups_bulk(
        self,
        session_ids: Sequence[str],
        groups: Sequence[dict[str, Any]],
        *,
        owner_id: int,
        unique_groups: Optional[Sequence[dict[str, Any]]] = None,
        stats: Optional[Mapping[str, Any]] = None,
    ) -> int:
        ids = [session_id for session_id in session_ids if session_id]
        if not ids:
            return 0
        unique_payload = list(unique_groups) if unique_groups is not None else []
        stats_payload = dict(stats or {})
        stats_payload.setdefault("file_rows", len(groups))
        stats_payload.setdefault("unique_groups", len(unique_payload) if unique_payload else len(groups))
        stats_payload.setdefault(
            "actual_targets",
            stats_payload.get("unique_groups") if isinstance(stats_payload.get("unique_groups"), int) else (len(unique_payload) if unique_payload else len(groups)),
        )
        result = await self._collection.update_many(
            {"session_id": {"$in": ids}, "owner_id": owner_id},
            {
                "$set": {
                    "metadata.broadcast_groups": list(groups),
                    "metadata.broadcast_groups_unique": unique_payload,
                    "metadata.broadcast_groups_stats": stats_payload,
                    "updated_at": datetime.utcnow(),
                }
            },
        )
        return result.matched_count or 0
