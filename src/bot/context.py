from __future__ import annotations

from dataclasses import dataclass

from src.db.repositories.session_repository import SessionRepository
from src.db.repositories.user_repository import UserRepository
from src.services.auth_state import AuthStateManager
from src.services.auto_broadcast import AutoBroadcastService
from src.services.account_status import AccountStatusService
from src.services.telethon_manager import TelethonSessionManager
from src.services.broadcast_state import BroadcastRunStateManager, BroadcastStateManager
from src.services.groups_state import GroupUploadStateManager, GroupViewStateManager
from src.db.repositories.group_sheet_repository import GroupSheetRepository
from src.services.sheet_monitor import GroupSheetMonitorService


@dataclass(slots=True)
class BotContext:
    """Aggregates bot dependencies accessible to command handlers."""

    user_repository: UserRepository
    session_repository: SessionRepository
    session_manager: TelethonSessionManager
    auth_manager: AuthStateManager
    broadcast_manager: BroadcastStateManager
    broadcast_run_manager: BroadcastRunStateManager
    groups_manager: GroupUploadStateManager
    group_view_manager: GroupViewStateManager
    auto_broadcast_service: AutoBroadcastService
    account_status_service: AccountStatusService
    group_sheet_repository: GroupSheetRepository | None = None
    group_sheet_monitor: GroupSheetMonitorService | None = None
