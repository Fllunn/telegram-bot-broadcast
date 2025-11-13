from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Dict, List, Optional

if TYPE_CHECKING:
    from src.models.session import TelethonSession


class GroupUploadStep(str, Enum):
    """Represents the current step in the group upload flow."""

    IDLE = "idle"
    CHOOSING_SCOPE = "choosing_scope"
    CHOOSING_ACCOUNT = "choosing_account"
    CONFIRMING_REPLACE = "confirming_replace"
    WAITING_FILE = "waiting_file"


class GroupUploadScope(str, Enum):
    """Determines whether the upload targets one or all accounts."""

    SINGLE = "single"
    ALL = "all"


@dataclass(slots=True)
class GroupUploadSession:
    """Stores transient state for managing group upload interactions."""

    step: GroupUploadStep = GroupUploadStep.IDLE
    scope: GroupUploadScope = GroupUploadScope.SINGLE
    target_session_ids: List[str] = field(default_factory=list)
    target_session_id: Optional[str] = None  # backward compatibility during transition
    last_message_id: Optional[int] = None
    sessions: Dict[str, "TelethonSession"] = field(default_factory=dict)


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
        scope: GroupUploadScope = GroupUploadScope.SINGLE,
        target_session_ids: Optional[List[str]] = None,
        sessions: Optional[Dict[str, "TelethonSession"]] = None,
        last_message_id: Optional[int] = None,
    ) -> GroupUploadSession:
        session = GroupUploadSession(
            step=step,
            scope=scope,
            target_session_ids=list(target_session_ids or []),
            sessions=dict(sessions or {}),
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


class GroupViewStep(str, Enum):
    """Represents the current step in the group view flow."""

    IDLE = "idle"
    CHOOSING_SCOPE = "choosing_scope"
    CHOOSING_ACCOUNT = "choosing_account"
    VIEWING = "viewing"


class GroupViewScope(str, Enum):
    """Determines the scope for viewing stored broadcast groups."""

    SINGLE = "single"
    ALL = "all"


@dataclass(slots=True)
class GroupViewSession:
    """Stores transient state for managing group view interactions."""

    step: GroupViewStep = GroupViewStep.IDLE
    scope: Optional[GroupViewScope] = None
    session_ids: List[str] = field(default_factory=list)
    sessions: Dict[str, "TelethonSession"] = field(default_factory=dict)
    last_message_id: Optional[int] = None
    pagination_tokens: Dict[str, str] = field(default_factory=dict)


class GroupViewStateManager:
    """Tracks per-user state for browsing stored broadcast groups."""

    def __init__(self) -> None:
        self._states: Dict[int, GroupViewSession] = {}

    def get(self, user_id: int) -> Optional[GroupViewSession]:
        return self._states.get(user_id)

    def begin(
        self,
        user_id: int,
        *,
        step: GroupViewStep,
        scope: Optional[GroupViewScope] = None,
        session_ids: Optional[List[str]] = None,
        sessions: Optional[Dict[str, "TelethonSession"]] = None,
        last_message_id: Optional[int] = None,
    ) -> GroupViewSession:
        session = GroupViewSession(
            step=step,
            scope=scope,
            session_ids=list(session_ids or []),
            sessions=dict(sessions or {}),
            last_message_id=last_message_id,
            pagination_tokens={},
        )
        self._states[user_id] = session
        return session

    def update(self, user_id: int, **kwargs) -> GroupViewSession:
        session = self._states.get(user_id)
        if session is None:
            session = GroupViewSession()
            self._states[user_id] = session
        for key, value in kwargs.items():
            setattr(session, key, value)
        return session

    def clear(self, user_id: int) -> Optional[GroupViewSession]:
        return self._states.pop(user_id, None)

    def has_active_flow(self, user_id: int) -> bool:
        session = self._states.get(user_id)
        return session is not None and session.step != GroupViewStep.IDLE
