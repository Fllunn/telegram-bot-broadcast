from __future__ import annotations

from dataclasses import dataclass

from src.db.repositories.session_repository import SessionRepository
from src.db.repositories.user_repository import UserRepository
from src.services.auth_state import AuthStateManager
from src.services.telethon_manager import TelethonSessionManager
from src.services.broadcast_state import BroadcastStateManager


@dataclass(slots=True)
class BotContext:
    """Aggregates bot dependencies accessible to command handlers."""

    user_repository: UserRepository
    session_repository: SessionRepository
    session_manager: TelethonSessionManager
    auth_manager: AuthStateManager
    broadcast_manager: BroadcastStateManager
