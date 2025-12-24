from __future__ import annotations

from datetime import datetime
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import ASCENDING, IndexModel


class AutoInvasionRepository:

    def __init__(self, database: AsyncIOMotorDatabase) -> None:
        self._db = database
        self._groups = database.get_collection("invasion_groups")
        self._settings = database.get_collection("invasion_settings")

    async def ensure_indexes(self) -> None:
        # Drop old indexes that conflict with new schema
        try:
            await self._groups.drop_index("link_1")
        except Exception:
            pass
        
        try:
            await self._groups.drop_index("session_id_1_link_1")
        except Exception:
            pass
        
        try:
            existing_indexes = await self._groups.index_information()
            for idx_name in existing_indexes:
                if idx_name != "_id_" and "session_id" not in str(existing_indexes[idx_name]):
                    try:
                        await self._groups.drop_index(idx_name)
                    except Exception:
                        pass
        except Exception:
            pass
        
        await self._groups.create_indexes([
            IndexModel([("user_id", ASCENDING), ("session_id", ASCENDING), ("link", ASCENDING)], unique=True),
            IndexModel([("user_id", ASCENDING), ("joined", ASCENDING)]),
            IndexModel([("session_id", ASCENDING), ("joined", ASCENDING)]),
            IndexModel([("next_attempt_at", ASCENDING)]),
        ])
        await self._settings.create_indexes([
            IndexModel([("key", ASCENDING)], unique=True),
        ])

    async def add_group(self, user_id: int, session_id: str, link: str) -> None:
        await self._groups.update_one(
            {"link": link, "user_id": user_id, "session_id": session_id},
            {
                "$setOnInsert": {
                    "link": link,
                    "user_id": user_id,
                    "session_id": session_id,
                    "joined": False,
                    "error_count": 0,
                    "attempts_count": 0,
                    "last_attempt_at": None,
                    "last_error_at": None,
                    "next_attempt_at": None,
                }
            },
            upsert=True,
        )

    async def get_next_group(self, user_id: int, now: datetime) -> Optional[dict]:
        return await self._groups.find_one(
            {
                "user_id": user_id,
                "joined": False,
                "$or": [
                    {"next_attempt_at": None},
                    {"next_attempt_at": {"$lte": now}},
                ],
            },
            sort=[("next_attempt_at", ASCENDING)],
        )

    async def mark_joined(self, user_id: int, session_id: str, link: str) -> None:
        await self._groups.update_one(
            {"link": link, "user_id": user_id, "session_id": session_id},
            {
                "$set": {
                    "joined": True,
                    "error_count": 0,
                },
            },
        )

    async def is_group_joined(self, user_id: int, session_id: str, link: str) -> bool:
        doc = await self._groups.find_one({"link": link, "user_id": user_id, "session_id": session_id})
        if not doc:
            return False
        return bool(doc.get("joined", False))

    async def update_error(
        self,
        link: str,
        error_count: int,
        attempts_count: int,
        next_attempt_at: datetime,
        last_error_at: datetime,
        last_attempt_at: datetime,
    ) -> None:
        await self._groups.update_one(
            {"link": link},
            {
                "$set": {
                    "error_count": error_count,
                    "attempts_count": attempts_count,
                    "next_attempt_at": next_attempt_at,
                    "last_error_at": last_error_at,
                    "last_attempt_at": last_attempt_at,
                },
            },
        )

    async def is_active(self, user_id: int) -> bool:
        doc = await self._settings.find_one({"key": f"auto_invasion_active_{user_id}"})
        return doc.get("value", False) if doc else False

    async def set_active(self, user_id: int, active: bool, started_at: Optional[datetime] = None) -> None:
        update_fields = {"value": active}
        if started_at is not None:
            update_fields["started_at"] = started_at
        await self._settings.update_one(
            {"key": f"auto_invasion_active_{user_id}"},
            {"$set": update_fields},
            upsert=True,
        )

    async def get_active_users(self) -> list[int]:
        cursor = self._settings.find({"key": {"$regex": "^auto_invasion_active_"}, "value": True})
        user_ids = []
        async for doc in cursor:
            key = doc.get("key", "")
            if key.startswith("auto_invasion_active_"):
                try:
                    user_id = int(key.replace("auto_invasion_active_", ""))
                    user_ids.append(user_id)
                except ValueError:
                    pass
        return user_ids

    async def has_unjoined_groups(self, user_id: int) -> bool:
        count = await self._groups.count_documents({"user_id": user_id, "joined": False}, limit=1)
        return count > 0

    async def count_groups(self, user_id: int) -> dict:
        total = await self._groups.count_documents({"user_id": user_id})
        joined = await self._groups.count_documents({"user_id": user_id, "joined": True})
        return {"total": total, "joined": joined, "pending": total - joined}

    async def sync_session_groups(self, user_id: int, session_id: str, links: list[str]) -> None:
        """Remove groups from invasion_groups that are no longer in the session's broadcast_groups.
        
        This ensures that when broadcast_groups are updated, old entries don't clutter invasion_groups.
        """
        if not links:
            # If no groups provided, remove all invasion entries for this session
            await self._groups.delete_many({"user_id": user_id, "session_id": session_id})
            return
        
        # Remove entries for links that are no longer in the broadcast_groups
        await self._groups.delete_many({
            "user_id": user_id,
            "session_id": session_id,
            "link": {"$nin": links}
        })

    async def reset_join_status_for_session(self, user_id: int, session_id: str, links: list[str]) -> None:
        """Reset join status to force re-check for the provided links within a session.
        
        This is used when the operator performs a full replace of broadcast_groups so that
        previously joined groups are re-validated.
        """
        if not links:
            return
        await self._groups.update_many(
            {"user_id": user_id, "session_id": session_id, "link": {"$in": links}},
            {
                "$set": {
                    "joined": False,
                    "error_count": 0,
                    "attempts_count": 0,
                    "next_attempt_at": None,
                    "last_error_at": None,
                    "last_attempt_at": None,
                }
            },
        )

    async def cleanup_user_sessions(self, user_id: int, valid_session_ids: list[str]) -> None:
        """Remove invasion entries for a user that belong to unknown session_ids.

        This cleans up artifacts created under incorrect session identifiers.
        """
        try:
            await self._groups.delete_many({
                "user_id": user_id,
                "session_id": {"$nin": valid_session_ids},
            })
        except Exception:
            pass
