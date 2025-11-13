from __future__ import annotations

from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase


class MongoManager:
    """Encapsulates MongoDB connection lifecycle."""

    def __init__(self, dsn: str, app_name: str) -> None:
        self._dsn = dsn
        self._app_name = app_name
        self._client: Optional[AsyncIOMotorClient] = None

    async def connect(self) -> None:
        if self._client is not None:
            return
        self._client = AsyncIOMotorClient(self._dsn, appname=self._app_name)

    def get_database(self, name: str) -> AsyncIOMotorDatabase:
        if self._client is None:
            raise RuntimeError("MongoManager is not connected")
        return self._client.get_database(name)

    async def close(self) -> None:
        if self._client is None:
            return
        self._client.close()
        self._client = None
