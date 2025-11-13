from __future__ import annotations

import contextlib
import logging
from typing import Iterable, Optional

from telethon import TelegramClient
from telethon.sessions import StringSession

from src.db.repositories.session_repository import SessionRepository
from src.models.session import SessionOwnerType, TelethonSession


logger = logging.getLogger(__name__)


class TelethonSessionManager:
    """Coordinates Telethon client lifecycle and persistence."""

    def __init__(self, api_id: int, api_hash: str, session_repository: SessionRepository) -> None:
        self._api_id = api_id
        self._api_hash = api_hash
        self._session_repository = session_repository

    async def create_temporary_client(self) -> TelegramClient:
        """Create and connect a fresh Telethon client for onboarding flows."""
        session = StringSession()
        client = TelegramClient(session, self._api_id, self._api_hash)
        await client.connect()
        return client

    async def build_client_from_session(self, session: TelethonSession) -> TelegramClient:
        """Restore a Telethon client from stored session data."""
        if not session.session_data:
            raise ValueError("Session data is missing; cannot restore Telethon client")
        string_session = StringSession(session.session_data)
        client = TelegramClient(string_session, self._api_id, self._api_hash)
        await client.connect()
        return client

    async def persist_session(self, session: TelethonSession) -> TelethonSession:
        """Persist session metadata and payload to MongoDB."""
        return await self._session_repository.upsert_session(session)

    async def get_active_sessions(
        self,
        owner_id: int,
        owner_type: SessionOwnerType = SessionOwnerType.USER,
    ) -> Iterable[TelethonSession]:
        return await self._session_repository.get_active_sessions_for_owner(owner_id, owner_type)

    async def deactivate_session(self, session_id: str) -> Optional[TelethonSession]:
        """Mark a session as inactive without deleting it."""
        stored = await self._session_repository.get_by_session_id(session_id)
        if stored is None:
            return None
        stored.is_active = False
        return await self._session_repository.upsert_session(stored)

    async def remove_session(self, session: TelethonSession | str) -> bool:
        """Log out the Telethon client and remove the stored session."""

        if isinstance(session, str):
            session_obj = await self._session_repository.get_by_session_id(session)
            if session_obj is None:
                logger.warning("Не удалось найти сессию %s для удаления", session)
                return False
        else:
            session_obj = session

        client: Optional[TelegramClient] = None
        if session_obj.session_data:
            try:
                client = await self.build_client_from_session(session_obj)
                await client.log_out()
            except Exception:
                logger.exception(
                    "Не удалось корректно завершить сессию Telethon",
                    extra={"session_id": session_obj.session_id, "owner_id": session_obj.owner_id},
                )
                raise
            finally:
                if client is not None:
                    with contextlib.suppress(Exception):
                        await self.close_client(client)
        else:
            logger.warning(
                "Сессия %s не содержит session_data; пропускаем logout",
                session_obj.session_id,
            )

        try:
            deleted = await self._session_repository.delete_session(session_obj.session_id)
        except Exception:
            logger.exception(
                "Ошибка при удалении сессии из базы",
                extra={"session_id": session_obj.session_id, "owner_id": session_obj.owner_id},
            )
            raise

        if not deleted:
            logger.warning(
                "Документ сессии %s не найден при удалении",
                session_obj.session_id,
            )
            return False

        return True

    async def close_client(self, client: TelegramClient) -> None:
        """Gracefully disconnect a Telethon client."""
        if client.is_connected():
            await client.disconnect()
