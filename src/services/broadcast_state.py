from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional


class BroadcastStep(str, Enum):
    """Represents the current step in the broadcast text flow."""

    IDLE = "idle"
    CHOOSING_SCOPE = "choosing_scope"
    CHOOSING_ACCOUNT = "choosing_account"
    CONFIRMING_REPLACE = "confirming_replace"
    WAITING_TEXT = "waiting_text"


@dataclass(slots=True)
class BroadcastSession:
    """Stores transient state for managing broadcast text updates."""

    step: BroadcastStep = BroadcastStep.IDLE
    apply_to_all: bool = False
    target_session_ids: List[str] = field(default_factory=list)
    last_message_id: Optional[int] = None


class BroadcastStateManager:
    """Tracks per-user broadcast flows to avoid conflicts."""

    def __init__(self) -> None:
        self._states: Dict[int, BroadcastSession] = {}

    def get(self, user_id: int) -> Optional[BroadcastSession]:
        return self._states.get(user_id)

    def begin(
        self,
        user_id: int,
        *,
        step: BroadcastStep,
        apply_to_all: bool = False,
        session_ids: Optional[List[str]] = None,
        last_message_id: Optional[int] = None,
    ) -> BroadcastSession:
        state = BroadcastSession(
            step=step,
            apply_to_all=apply_to_all,
            target_session_ids=list(session_ids or []),
            last_message_id=last_message_id,
        )
        self._states[user_id] = state
        return state

    def update(self, user_id: int, **kwargs) -> BroadcastSession:
        state = self._states.get(user_id)
        if state is None:
            state = BroadcastSession()
            self._states[user_id] = state
        for key, value in kwargs.items():
            setattr(state, key, value)
        return state

    def clear(self, user_id: int) -> Optional[BroadcastSession]:
        return self._states.pop(user_id, None)

    def has_active_flow(self, user_id: int) -> bool:
        state = self._states.get(user_id)
        return state is not None and state.step != BroadcastStep.IDLE
