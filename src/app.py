from __future__ import annotations

from contextlib import AsyncExitStack
from dataclasses import dataclass
from typing import Optional
from uuid import uuid4

from src.bot.application import BotApplication
from src.config.broadcast_settings import BROADCAST_DELAY_MAX_SECONDS
from src.config.settings import settings, Settings
from src.db.client import MongoManager
from src.db.repositories.account_repository import AccountRepository
from src.db.repositories.auto_broadcast_task_repository import AutoBroadcastTaskRepository
from src.db.repositories.session_repository import SessionRepository
from src.db.repositories.user_repository import UserRepository
from src.services.auto_broadcast import AutoBroadcastService
from src.services.telethon_manager import TelethonSessionManager


@dataclass(slots=True)
class Application:
    """Coordinates lifecycle for infrastructure and bot runtime."""

    settings: Settings
    mongo_manager: MongoManager
    bot_application: BotApplication

    async def run(self) -> None:
        async with AsyncExitStack() as stack:
            await self.mongo_manager.connect()
            stack.push_async_callback(self.mongo_manager.close)

            database = self.mongo_manager.get_database(self.settings.mongo_database)

            user_repository = UserRepository(database, collection_name=self.settings.user_collection)
            session_repository = SessionRepository(database, collection_name=self.settings.session_collection)
            task_repository = AutoBroadcastTaskRepository(database, collection_name=self.settings.auto_task_collection)
            account_repository = AccountRepository(database, collection_name=self.settings.auto_account_collection)

            # Ensure indexes before serving requests.
            await user_repository.ensure_indexes()
            await session_repository.ensure_indexes()
            await task_repository.ensure_indexes()
            await account_repository.ensure_indexes()

            telethon_manager = TelethonSessionManager(
                api_id=self.settings.telegram_api_id,
                api_hash=self.settings.telegram_api_hash,
                session_repository=session_repository,
            )

            worker_id = f"{self.settings.app_name}-{uuid4().hex[:8]}"
            auto_broadcast_service = AutoBroadcastService(
                task_repository=task_repository,
                account_repository=account_repository,
                session_repository=session_repository,
                session_manager=telethon_manager,
                bot_client=self.bot_application.client,
                worker_id=worker_id,
                poll_interval=float(self.settings.auto_task_poll_interval_seconds),
                lock_ttl_seconds=self.settings.auto_task_lock_ttl_seconds,
                max_delay_per_message=BROADCAST_DELAY_MAX_SECONDS,
            )

            await self.bot_application.start(
                user_repository=user_repository,
                session_repository=session_repository,
                session_manager=telethon_manager,
                auto_broadcast_service=auto_broadcast_service,
            )
            stack.push_async_callback(self.bot_application.stop)

            await auto_broadcast_service.start()
            stack.push_async_callback(auto_broadcast_service.stop)

            await self.bot_application.idle()


def create_application(custom_settings: Optional[Settings] = None) -> Application:
    """Factory that wires the application with configured dependencies."""
    app_settings = custom_settings or settings

    mongo_manager = MongoManager(
        dsn=app_settings.mongo_dsn,
        app_name=app_settings.app_name,
    )

    bot_application = BotApplication(
        api_id=app_settings.telegram_api_id,
        api_hash=app_settings.telegram_api_hash,
        bot_token=app_settings.telegram_bot_token,
        bot_session_name=app_settings.bot_session_name,
    )

    return Application(
        settings=app_settings,
        mongo_manager=mongo_manager,
        bot_application=bot_application,
    )
