from __future__ import annotations

from asyncio import Task
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from src.models.session import TelethonSession


class BroadcastStep(str, Enum):
    """Represents the current step in the broadcast text flow."""

    IDLE = "idle"
    CHOOSING_SCOPE = "choosing_scope"
    CHOOSING_ACCOUNT = "choosing_account"
    CONFIRMING_REPLACE = "confirming_replace"
    WAITING_TEXT = "waiting_text"
    WAITING_IMAGE = "waiting_image"


class BroadcastFlow(str, Enum):
    """Indicates which kind of broadcast content is being configured."""

    TEXT = "text"
    IMAGE = "image"


@dataclass(slots=True)
class BroadcastSession:
    """Stores transient state for managing broadcast text updates."""

    step: BroadcastStep = BroadcastStep.IDLE
    flow: BroadcastFlow = BroadcastFlow.TEXT
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
        flow: BroadcastFlow,
        apply_to_all: bool = False,
        session_ids: Optional[List[str]] = None,
        last_message_id: Optional[int] = None,
    ) -> BroadcastSession:
        state = BroadcastSession(
            step=step,
            flow=flow,
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


class BroadcastRunStep(str, Enum):
    """Represents the current step in the broadcast sending flow."""

    IDLE = "idle"
    CHOOSING_SCOPE = "choosing_scope"
    CHOOSING_ACCOUNT = "choosing_account"
    CONFIRMING = "confirming"
    RUNNING = "running"


class BroadcastRunScope(str, Enum):
    """Determines whether the broadcast targets one or all accounts."""

    SINGLE = "single"
    ALL = "all"


@dataclass(slots=True)
class BroadcastRunSession:
    """Stores transient state for managing broadcast execution."""

    step: BroadcastRunStep = BroadcastRunStep.IDLE
    scope: BroadcastRunScope = BroadcastRunScope.SINGLE
    target_session_ids: List[str] = field(default_factory=list)
    last_message_id: Optional[int] = None
    last_trigger_message_id: Optional[int] = None
    task: Optional[Task[None]] = None
    cancel_requested: bool = False
    progress_message_id: Optional[int] = None
    plan: Optional[object] = None
    sessions: Dict[str, "TelethonSession"] = field(default_factory=dict)


class BroadcastRunStateManager:
    """Tracks per-user broadcast sending sessions."""

    def __init__(self) -> None:
        self._states: Dict[int, BroadcastRunSession] = {}

    def get(self, user_id: int) -> Optional[BroadcastRunSession]:
        return self._states.get(user_id)

    def begin(
        self,
        user_id: int,
        *,
        step: BroadcastRunStep,
        scope: BroadcastRunScope = BroadcastRunScope.SINGLE,
        target_session_ids: Optional[List[str]] = None,
        sessions: Optional[Dict[str, "TelethonSession"]] = None,
        last_message_id: Optional[int] = None,
        trigger_message_id: Optional[int] = None,
    ) -> BroadcastRunSession:
        session = BroadcastRunSession(
            step=step,
            scope=scope,
            target_session_ids=list(target_session_ids or []),
            sessions=dict(sessions or {}),
            last_message_id=last_message_id,
            last_trigger_message_id=trigger_message_id,
        )
        self._states[user_id] = session
        return session

    def update(self, user_id: int, **kwargs) -> BroadcastRunSession:
        session = self._states.get(user_id)
        if session is None:
            session = BroadcastRunSession()
            self._states[user_id] = session
        for key, value in kwargs.items():
            setattr(session, key, value)
        return session

    def clear(self, user_id: int) -> Optional[BroadcastRunSession]:
        return self._states.pop(user_id, None)

    def has_active_flow(self, user_id: int) -> bool:
        session = self._states.get(user_id)
        if session is None:
            return False
        if session.step != BroadcastRunStep.IDLE:
            return True
        if session.task is not None and not session.task.done():
            return True
        return False
