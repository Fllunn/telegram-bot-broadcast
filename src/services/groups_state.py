from __future__ import annotations

import secrets
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Sequence

if TYPE_CHECKING:
    from src.models.session import TelethonSession


@dataclass(slots=True)
class UploadAccountSnapshot:
    """Lightweight description of an upload target account."""

    session_id: str
    owner_id: int
    label: str
    cached_session: Optional["TelethonSession"] = None


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


def _generate_flow_token() -> str:
    return secrets.token_hex(8)


@dataclass(slots=True)
class GroupUploadSession:
    """Stores transient state for managing group upload interactions."""

    flow_id: str = field(default_factory=_generate_flow_token)
    step: GroupUploadStep = GroupUploadStep.IDLE
    scope: GroupUploadScope = GroupUploadScope.SINGLE
    target_session_ids: List[str] = field(default_factory=list)
    selected_session_id: Optional[str] = None
    last_message_id: Optional[int] = None
    sessions: Dict[str, UploadAccountSnapshot] = field(default_factory=dict)
    allowed_session_ids: List[str] = field(default_factory=list)
    confirmation_tokens: Dict[str, str] = field(default_factory=dict)


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
        sessions: Optional[Dict[str, UploadAccountSnapshot]] = None,
        allowed_session_ids: Optional[Sequence[str]] = None,
        last_message_id: Optional[int] = None,
    ) -> GroupUploadSession:
        # Reset any residual state before starting a new flow to avoid stale values.
        self._states.pop(user_id, None)

        session_map = {session_id: snapshot for session_id, snapshot in (sessions or {}).items() if session_id}
        allowed_ids = list(allowed_session_ids or session_map.keys())
        sanitized_targets = [session_id for session_id in (target_session_ids or []) if session_id in allowed_ids]

        session = GroupUploadSession(
            step=step,
            scope=scope,
            target_session_ids=sanitized_targets,
            sessions=session_map,
            allowed_session_ids=list(allowed_ids),
            last_message_id=last_message_id,
        )
        if session.scope == GroupUploadScope.SINGLE and session.target_session_ids:
            session.selected_session_id = session.target_session_ids[0]
        self._states[user_id] = session
        return session

    def update(self, user_id: int, **kwargs) -> GroupUploadSession:
        session = self._states.get(user_id)
        if session is None:
            session = GroupUploadSession()
            self._states[user_id] = session
        for key, value in kwargs.items():
            setattr(session, key, value)
            if key == "sessions" and isinstance(value, dict):
                session.allowed_session_ids = [session_id for session_id in value.keys() if session_id]
            if key == "allowed_session_ids" and isinstance(value, Iterable):
                session.allowed_session_ids = [session_id for session_id in value if session_id]
        return session

    def reset_targets(self, user_id: int) -> Optional[GroupUploadSession]:
        session = self._states.get(user_id)
        if session is None:
            return None
        session.target_session_ids = []
        session.selected_session_id = None
        session.scope = GroupUploadScope.SINGLE
        session.confirmation_tokens.clear()
        return session

    def set_single_target(self, user_id: int, session_id: str) -> Optional[GroupUploadSession]:
        session = self._states.get(user_id)
        if session is None:
            return None
        if session.allowed_session_ids and session_id not in session.allowed_session_ids:
            return None
        session.scope = GroupUploadScope.SINGLE
        session.target_session_ids = [session_id]
        session.selected_session_id = session_id
        session.confirmation_tokens.clear()
        return session

    def set_all_targets(self, user_id: int, session_ids: Sequence[str]) -> Optional[GroupUploadSession]:
        session = self._states.get(user_id)
        if session is None:
            return None
        session.scope = GroupUploadScope.ALL
        allowed = session.allowed_session_ids or list(session.sessions.keys())
        if not allowed:
            session.target_session_ids = []
        else:
            sanitized = [session_id for session_id in session_ids if session_id in allowed]
            session.target_session_ids = sanitized if sanitized else list(allowed)
        session.selected_session_id = None
        session.confirmation_tokens.clear()
        return session

    def select_targets(self, user_id: int, *, scope: GroupUploadScope, session_ids: Iterable[str]) -> Optional[GroupUploadSession]:
        session = self._states.get(user_id)
        if session is None:
            return None
        allowed = session.allowed_session_ids or list(session.sessions.keys())
        sanitized = [session_id for session_id in session_ids if session_id and (not allowed or session_id in allowed)]
        session.scope = scope
        session.target_session_ids = sanitized
        session.selected_session_id = sanitized[0] if scope == GroupUploadScope.SINGLE and sanitized else None
        session.confirmation_tokens.clear()
        return session

    def neutralize(self, user_id: int) -> Optional[GroupUploadSession]:
        session = self._states.get(user_id)
        if session is None:
            return None
        session.step = GroupUploadStep.IDLE
        session.scope = GroupUploadScope.SINGLE
        session.target_session_ids = []
        session.selected_session_id = None
        session.last_message_id = None
        session.allowed_session_ids = []
        session.sessions.clear()
        session.confirmation_tokens.clear()
        session.flow_id = _generate_flow_token()
        return session

    def register_confirmation_token(self, user_id: int, session_id: str) -> Optional[str]:
        session = self._states.get(user_id)
        if session is None:
            return None
        token = secrets.token_hex(4)
        session.confirmation_tokens[token] = session_id
        return token

    def resolve_confirmation_token(self, user_id: int, token: str) -> Optional[str]:
        session = self._states.get(user_id)
        if session is None:
            return None
        return session.confirmation_tokens.get(token)

    def consume_confirmation_token(self, user_id: int, token: str) -> Optional[str]:
        session = self._states.get(user_id)
        if session is None:
            return None
        return session.confirmation_tokens.pop(token, None)

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
        if "step" in kwargs and kwargs["step"] != GroupViewStep.VIEWING:
            session.pagination_tokens.clear()
        return session

    def clear(self, user_id: int) -> Optional[GroupViewSession]:
        session = self._states.pop(user_id, None)
        if session is not None:
            session.pagination_tokens.clear()
        return session

    def has_active_flow(self, user_id: int) -> bool:
        session = self._states.get(user_id)
        return session is not None and session.step != GroupViewStep.IDLE
