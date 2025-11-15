from __future__ import annotations

from typing import Optional

from telethon import TelegramClient

from src.bot.context import BotContext
from src.bot.router import register_commands
from src.db.repositories.session_repository import SessionRepository
from src.db.repositories.user_repository import UserRepository
from src.services.telethon_manager import TelethonSessionManager
from src.services.auth_state import AuthStateManager
from src.services.auto_broadcast import AutoBroadcastService
from src.services.account_status import AccountStatusService
from src.services.broadcast_state import BroadcastRunStateManager, BroadcastStateManager
from src.services.groups_state import GroupUploadStateManager, GroupViewStateManager


class BotApplication:
    """Bootstraps the Telethon bot client and routes commands."""

    def __init__(self, api_id: int, api_hash: str, bot_token: str, bot_session_name: str) -> None:
        self._bot_token = bot_token
        self._client = TelegramClient(bot_session_name, api_id, api_hash)
        self._context: Optional[BotContext] = None

    async def start(
        self,
        user_repository: UserRepository,
        session_repository: SessionRepository,
        session_manager: TelethonSessionManager,
        auto_broadcast_service: AutoBroadcastService,
        account_status_service: AccountStatusService,
    ) -> None:
        """Start the Telethon client and register command handlers."""
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
            )

        await self._client.start(bot_token=self._bot_token)
        register_commands(self._client, self._context)

    async def idle(self) -> None:
        """Block until the bot is disconnected."""
        await self._client.run_until_disconnected()

    async def stop(self) -> None:
        """Disconnect the Telethon bot client."""
        if self._client.is_connected():
            await self._client.disconnect()

    @property
    def client(self) -> TelegramClient:
        return self._client
