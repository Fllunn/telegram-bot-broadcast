from __future__ import annotations

import asyncio
import logging
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

logger = logging.getLogger(__name__)


class MongoManager:
    """Encapsulates MongoDB connection lifecycle."""

    def __init__(
        self,
        dsn: str,
        app_name: str,
        *,
        retry_seconds: float = 5.0,
        max_attempts: int = 0,
    ) -> None:
        self._dsn = dsn
        self._app_name = app_name
        self._retry_seconds = retry_seconds
        self._max_attempts = max_attempts
        self._client: Optional[AsyncIOMotorClient] = None

    async def connect(self) -> None:
        if self._client is not None:
            return
        attempt = 0
        while True:
            attempt += 1
            self._client = AsyncIOMotorClient(
                self._dsn,
                appname=self._app_name,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
            )
            try:
                await self._client.admin.command("ping")
                logger.info("MongoDB connection established")
                return
            except (ServerSelectionTimeoutError, ConnectionFailure) as exc:
                logger.warning(
                    "MongoDB unavailable (attempt %s). Retrying in %.1f seconds.",
                    attempt,
                    self._retry_seconds,
                )
                self._client.close()
                self._client = None
                if self._max_attempts > 0 and attempt >= self._max_attempts:
                    raise RuntimeError(
                        "MongoDB is unavailable. Ensure MongoDB is running and MONGO_DSN points to it."
                    ) from exc
                await asyncio.sleep(self._retry_seconds)

    def get_database(self, name: str) -> AsyncIOMotorDatabase:
        if self._client is None:
            raise RuntimeError("MongoManager is not connected")
        return self._client.get_database(name)

    async def close(self) -> None:
        if self._client is None:
            return
        self._client.close()
        self._client = None
