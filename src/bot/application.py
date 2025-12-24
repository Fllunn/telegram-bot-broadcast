from __future__ import annotations

import asyncio
import logging
from typing import Optional

from telethon import TelegramClient

from src.bot.context import BotContext
from src.bot.router import register_commands
from src.db.repositories.session_repository import SessionRepository
from src.db.repositories.group_sheet_repository import GroupSheetRepository
from src.db.repositories.user_repository import UserRepository
from src.db.repositories.auto_invasion_repository import AutoInvasionRepository
from src.services.telethon_manager import TelethonSessionManager
from src.services.auth_state import AuthStateManager
from src.services.auto_broadcast import AutoBroadcastService
from src.services.account_status import AccountStatusService
from src.services.sheet_monitor import GroupSheetMonitorService
from src.services.broadcast_state import BroadcastRunStateManager, BroadcastStateManager
from src.services.groups_state import GroupUploadStateManager, GroupViewStateManager
from src.utils.telethon_reconnect import (
    TELETHON_NETWORK_EXCEPTIONS,
    run_with_exponential_backoff,
)


class BotApplication:
    """Bootstraps the Telethon bot client and routes commands."""

    def __init__(self, api_id: int, api_hash: str, bot_token: str, bot_session_name: str) -> None:
        self._bot_token = bot_token
        self._client = TelegramClient(bot_session_name, api_id, api_hash)
        self._context: Optional[BotContext] = None
        self._handlers_registered = False
        self._stop_event: asyncio.Event | None = None
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    async def start(
        self,
        user_repository: UserRepository,
        session_repository: SessionRepository,
        session_manager: TelethonSessionManager,
        auto_broadcast_service: AutoBroadcastService,
        account_status_service: AccountStatusService,
        group_sheet_repository: GroupSheetRepository | None = None,
        group_sheet_monitor: GroupSheetMonitorService | None = None,
        invasion_repository: AutoInvasionRepository | None = None,
        invasion_worker = None,
    ) -> None:
        """Start the Telethon client and register command handlers."""
        if self._stop_event is None:
            self._stop_event = asyncio.Event()
        else:
            self._stop_event.clear()

        if self._context is None:
            auth_manager = AuthStateManager()
            broadcast_manager = BroadcastStateManager()
            broadcast_run_manager = BroadcastRunStateManager()
            groups_manager = GroupUploadStateManager()
            group_view_manager = GroupViewStateManager()

            self._context = BotContext(
                user_repository=user_repository,
                session_repository=session_repository,
                session_manager=session_manager,
                auth_manager=auth_manager,
                broadcast_manager=broadcast_manager,
                broadcast_run_manager=broadcast_run_manager,
                groups_manager=groups_manager,
                group_view_manager=group_view_manager,
                auto_broadcast_service=auto_broadcast_service,
                account_status_service=account_status_service,
                group_sheet_repository=group_sheet_repository,
                group_sheet_monitor=group_sheet_monitor,
                invasion_repository=invasion_repository,
                invasion_worker=invasion_worker,
            )

        context = self._context
        if context is None:  # defensive guard for type checkers
            raise RuntimeError("Bot context failed to initialize")

        await run_with_exponential_backoff(
            lambda: self._client.start(bot_token=self._bot_token),
            label="telethon.bot.start",
            logger=self._logger,
            log_context={"client": "bot"},
        )

        if not self._handlers_registered:
            register_commands(self._client, context)
            self._handlers_registered = True

        # Start monitor after handlers (does not depend on them but ensures client running)
        if context.group_sheet_monitor is not None:
            try:
                await context.group_sheet_monitor.start()
            except Exception:
                self._logger.exception("Failed to start group sheet monitor service")

    async def idle(self) -> None:
        """Block until the bot is disconnected."""
        if self._stop_event is None:
            self._stop_event = asyncio.Event()

        reconnect_attempt = 0
        while True:
            try:
                await self._client.run_until_disconnected()
            except asyncio.CancelledError:
                raise
            except TELETHON_NETWORK_EXCEPTIONS as exc:
                self._logger.warning(
                    "Bot client stopped due to network issue: %s",
                    exc,
                    extra={"client": "bot"},
                )
            except Exception:
                self._logger.exception(
                    "Bot client stopped unexpectedly",
                    extra={"client": "bot"},
                )

            if self._stop_event.is_set():
                break

            reconnect_attempt += 1
            delay = min(2 ** reconnect_attempt, 60)
            log_extra = {
                "client": "bot",
                "reconnect_attempt": reconnect_attempt,
                "reconnect_delay": delay,
            }
            self._logger.warning(
                "Bot client disconnected; reconnecting in %s seconds",
                delay,
                extra=log_extra,
            )
            await asyncio.sleep(delay)

            if self._stop_event.is_set():
                break

            await run_with_exponential_backoff(
                lambda: self._client.start(bot_token=self._bot_token),
                label="telethon.bot.reconnect",
                logger=self._logger,
                log_context={
                    "client": "bot",
                    "reconnect_attempt": reconnect_attempt,
                },
            )
            reconnect_attempt = 0

    async def stop(self) -> None:
        """Disconnect the Telethon bot client."""
        if self._stop_event is None:
            self._stop_event = asyncio.Event()
        self._stop_event.set()

        if self._client.is_connected():
            try:
                await self._client.disconnect()
            except TELETHON_NETWORK_EXCEPTIONS as exc:
                self._logger.warning(
                    "Bot client disconnect reported network error: %s",
                    exc,
                    extra={"client": "bot"},
                )

    @property
    def client(self) -> TelegramClient:
        return self._client
