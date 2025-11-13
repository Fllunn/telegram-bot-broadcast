from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional


class GroupUploadStep(str, Enum):
    """Represents the current step in the group upload flow."""

    IDLE = "idle"
    CHOOSING_ACCOUNT = "choosing_account"
    CONFIRMING_REPLACE = "confirming_replace"
    WAITING_FILE = "waiting_file"


@dataclass(slots=True)
class GroupUploadSession:
    """Stores transient state for managing group upload interactions."""

    step: GroupUploadStep = GroupUploadStep.IDLE
    target_session_id: Optional[str] = None
    last_message_id: Optional[int] = None


class GroupUploadStateManager:
    """Tracks per-user group upload state to prevent conflicting flows."""

    def __init__(self) -> None:
        self._states: Dict[int, GroupUploadSession] = {}

    def get(self, user_id: int) -> Optional[GroupUploadSession]:
        return self._states.get(user_id)

    def begin(
        self,
        user_id: int,
        *,
        step: GroupUploadStep,
        target_session_id: Optional[str] = None,
        last_message_id: Optional[int] = None,
    ) -> GroupUploadSession:
        session = GroupUploadSession(
            step=step,
            target_session_id=target_session_id,
            last_message_id=last_message_id,
        )
        self._states[user_id] = session
        return session

    def update(self, user_id: int, **kwargs) -> GroupUploadSession:
        session = self._states.get(user_id)
        if session is None:
            session = GroupUploadSession()
            self._states[user_id] = session
        for key, value in kwargs.items():
            setattr(session, key, value)
        return session

    def clear(self, user_id: int) -> Optional[GroupUploadSession]:
        return self._states.pop(user_id, None)

    def has_active_flow(self, user_id: int) -> bool:
        session = self._states.get(user_id)
        return session is not None and session.step != GroupUploadStep.IDLE
