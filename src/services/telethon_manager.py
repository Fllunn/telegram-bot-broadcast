from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Iterable, Optional

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors.rpcerrorlist import (
    AuthKeyUnregisteredError,
    SessionRevokedError,
    UserDeactivatedError,
    UserDeactivatedBanError,
)

from src.db.repositories.session_repository import SessionRepository
from src.models.session import SessionOwnerType, TelethonSession


logger = logging.getLogger(__name__)

_SESSION_VALIDATION_TIMEOUT = 5.0
_AUTH_ERRORS = (
    AuthKeyUnregisteredError,
    SessionRevokedError,
    UserDeactivatedError,
    UserDeactivatedBanError,
)


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
        *,
        verify_live: bool = False,
        timeout: float | None = None,
    ) -> list[TelethonSession]:
        sessions_iter = await self._session_repository.get_active_sessions_for_owner(owner_id, owner_type)
        sessions = list(sessions_iter)
        if not verify_live or not sessions:
            return sessions

        timeout_value = timeout if timeout is not None else _SESSION_VALIDATION_TIMEOUT
        verified: list[TelethonSession] = []
        for session in sessions:
            is_live = await self.verify_session_status(session, timeout=timeout_value)
            if is_live:
                verified.append(session)
        return verified

    async def refresh_owner_sessions(
        self,
        owner_id: int,
        owner_type: SessionOwnerType = SessionOwnerType.USER,
        *,
        timeout: float | None = None,
    ) -> tuple[list[TelethonSession], list[TelethonSession]]:
        sessions = await self._session_repository.list_sessions_for_owner(owner_id, owner_type)
        if not sessions:
            return [], []

        timeout_value = timeout if timeout is not None else _SESSION_VALIDATION_TIMEOUT
        active: list[TelethonSession] = []
        inactive: list[TelethonSession] = []
        for session in sessions:
            is_live = await self.verify_session_status(session, timeout=timeout_value)
            target = active if is_live else inactive
            target.append(session)
        return active, inactive

    async def ensure_dialog_access(
        self,
        session: TelethonSession,
        *,
        limit: Optional[int] = None,
    ) -> bool:
        """Ensure account can fetch dialogs; returns True if successful."""

        client: Optional[TelegramClient] = None
        try:
            client = await self.build_client_from_session(session)
        except _AUTH_ERRORS as exc:
            logger.warning(
                "Dialog access failed: authorization error",
                extra={"session_id": session.session_id, "owner_id": session.owner_id, "error": str(exc)},
            )
            await self._set_session_active(session, False)
            return False
        except Exception as exc:
            logger.exception(
                "Dialog access failed: client initialization error",
                extra={"session_id": session.session_id, "owner_id": session.owner_id, "error": str(exc)},
            )
            await self._set_session_active(session, False)
            return False

        try:
            await client.get_dialogs(limit=limit)
        except _AUTH_ERRORS as exc:
            logger.warning(
                "Dialog access failed: authorization error during get_dialogs",
                extra={"session_id": session.session_id, "owner_id": session.owner_id, "error": str(exc)},
            )
            await self._set_session_active(session, False)
            return False
        except Exception as exc:
            logger.exception(
                "Dialog access failed: unexpected error",
                extra={"session_id": session.session_id, "owner_id": session.owner_id, "error": str(exc)},
            )
            await self._set_session_active(session, False)
            return False
        finally:
            if client is not None:
                with contextlib.suppress(Exception):
                    await self.close_client(client)

        await self._set_session_active(session, True)
        return True

    async def verify_session_status(
        self,
        session: TelethonSession,
        *,
        timeout: float = _SESSION_VALIDATION_TIMEOUT,
    ) -> bool:
        """Validate session against Telegram API and persist updated status."""

        client: Optional[TelegramClient] = None
        try:
            client = await self.build_client_from_session(session)
        except _AUTH_ERRORS as exc:
            logger.warning(
                "Session authorization error during validation",
                extra={"session_id": session.session_id, "owner_id": session.owner_id, "error": str(exc)},
            )
            await self._set_session_active(session, False)
            return False
        except Exception as exc:
            logger.exception(
                "Failed to rebuild Telethon client for validation",
                extra={"session_id": session.session_id, "owner_id": session.owner_id, "error": str(exc)},
            )
            await self._set_session_active(session, False)
            return False

        try:
            try:
                authorized = client.is_user_authorized()
            except Exception as exc:
                logger.exception(
                    "Failed to determine authorization state",
                    extra={"session_id": session.session_id, "owner_id": session.owner_id, "error": str(exc)},
                )
                authorized = False

            if not authorized:
                logger.warning(
                    "Session reported as unauthorized",
                    extra={"session_id": session.session_id, "owner_id": session.owner_id},
                )
                await self._set_session_active(session, False)
                return False

            try:
                await asyncio.wait_for(client.get_me(), timeout=timeout)
            except asyncio.TimeoutError:
                logger.warning(
                    "Timed out while validating session",
                    extra={"session_id": session.session_id, "owner_id": session.owner_id, "timeout": timeout},
                )
                # Keep previous state if timeout occurs to avoid flapping due to transient delays.
                return session.is_active
            except _AUTH_ERRORS as exc:
                logger.warning(
                    "Authorization error while calling get_me",
                    extra={"session_id": session.session_id, "owner_id": session.owner_id, "error": str(exc)},
                )
                await self._set_session_active(session, False)
                return False
            except Exception as exc:
                logger.exception(
                    "Unexpected error during session validation",
                    extra={"session_id": session.session_id, "owner_id": session.owner_id, "error": str(exc)},
                )
                return session.is_active

            await self._set_session_active(session, True)
            return True
        finally:
            if client is not None:
                with contextlib.suppress(Exception):
                    await self.close_client(client)

    async def _set_session_active(self, session: TelethonSession, is_active: bool) -> None:
        if session.is_active == is_active:
            return
        updated = await self._session_repository.set_session_active(session.session_id, is_active)
        if updated is not None:
            session.is_active = updated.is_active
            session.metadata = updated.metadata
            session.phone = updated.phone
        else:
            session.is_active = is_active

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
