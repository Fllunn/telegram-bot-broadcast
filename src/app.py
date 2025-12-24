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
from src.db.repositories.group_sheet_repository import GroupSheetRepository
from src.db.repositories.user_repository import UserRepository
from src.db.repositories.auto_invasion_repository import AutoInvasionRepository
from src.services.auto_broadcast import AutoBroadcastService
from src.services.telethon_manager import TelethonSessionManager
from src.services.account_status import AccountStatusService
from src.services.sheet_monitor import GroupSheetMonitorService
from src.services.auto_invasion.worker import AutoInvasionWorker
from src.bot.commands.auto_invasion import set_worker_instance


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
            group_sheet_repository = GroupSheetRepository(database)
            task_repository = AutoBroadcastTaskRepository(database, collection_name=self.settings.auto_task_collection)
            account_repository = AccountRepository(database, collection_name=self.settings.auto_account_collection)
            invasion_repository = AutoInvasionRepository(database)

            await user_repository.ensure_indexes()
            await session_repository.ensure_indexes()
            await group_sheet_repository.ensure_indexes()
            await task_repository.ensure_indexes()
            await account_repository.ensure_indexes()
            await invasion_repository.ensure_indexes()

            telethon_manager = TelethonSessionManager(
                api_id=self.settings.telegram_api_id,
                api_hash=self.settings.telegram_api_hash,
                session_repository=session_repository,
            )

            account_status_service = AccountStatusService(
                session_manager=telethon_manager,
                session_repository=session_repository,
                concurrency=self.settings.account_status_concurrency,
                timeout_seconds=self.settings.account_status_timeout_seconds,
                cache_ttl_seconds=self.settings.account_status_cache_ttl_seconds,
                db_refresh_interval_seconds=self.settings.account_status_db_refresh_seconds,
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
                account_status_service=account_status_service,
            )

            group_sheet_monitor = GroupSheetMonitorService(
                repository=group_sheet_repository,
                session_repository=session_repository,
                bot_client=self.bot_application.client,
                interval_seconds=600.0,
            )

            invasion_worker = AutoInvasionWorker(
                invasion_repository=invasion_repository,
                session_repository=session_repository,
                session_manager=telethon_manager,
            )
            set_worker_instance(invasion_worker)

            await self.bot_application.start(
                user_repository=user_repository,
                session_repository=session_repository,
                session_manager=telethon_manager,
                auto_broadcast_service=auto_broadcast_service,
                account_status_service=account_status_service,
                group_sheet_repository=group_sheet_repository,
                group_sheet_monitor=group_sheet_monitor,
                invasion_repository=invasion_repository,
                invasion_worker=invasion_worker,
            )
            stack.push_async_callback(self.bot_application.stop)

            await auto_broadcast_service.start()
            stack.push_async_callback(auto_broadcast_service.stop)

            await invasion_worker.start()
            stack.push_async_callback(invasion_worker.stop)

            stack.push_async_callback(group_sheet_monitor.stop)

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
