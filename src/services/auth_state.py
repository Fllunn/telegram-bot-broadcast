from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional

from telethon import TelegramClient


class AuthStep(str, Enum):
    """Represents the current step in the phone login flow."""

    IDLE = "idle"
    WAITING_PHONE = "waiting_phone"
    WAITING_CODE = "waiting_code"
    WAITING_PASSWORD = "waiting_password"


@dataclass(slots=True)
class AuthSession:
    """Holds mutable state for a single user's login flow."""

    step: AuthStep = AuthStep.IDLE
    phone: Optional[str] = None
    phone_code_hash: Optional[str] = None
    client: Optional[TelegramClient] = None
    last_message_id: Optional[int] = None


class AuthStateManager:
    """Manages per-user authorization states for multi-step flows."""

    def __init__(self) -> None:
        self._states: Dict[int, AuthSession] = {}

    def get(self, user_id: int) -> Optional[AuthSession]:
        return self._states.get(user_id)

    def begin(self, user_id: int, *, step: AuthStep, last_message_id: Optional[int] = None) -> AuthSession:
        state = AuthSession(step=step, last_message_id=last_message_id)
        self._states[user_id] = state
        return state

    def update(self, user_id: int, **kwargs) -> AuthSession:
        state = self._states.get(user_id)
        if state is None:
            state = AuthSession()
            self._states[user_id] = state
        for key, value in kwargs.items():
            setattr(state, key, value)
        return state

    def clear(self, user_id: int) -> Optional[AuthSession]:
        return self._states.pop(user_id, None)

    def has_active_flow(self, user_id: int) -> bool:
        state = self._states.get(user_id)
        return state is not None and state.step != AuthStep.IDLE
