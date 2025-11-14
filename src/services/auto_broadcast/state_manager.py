from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional

from src.models.auto_broadcast import AccountMode


class AutoTaskSetupStep(str, Enum):
    """States for interactive auto broadcast task setup."""

    IDLE = "idle"
    CHOOSING_MODE = "choosing_mode"
    CHOOSING_ACCOUNT = "choosing_account"
    ENTERING_INTERVAL = "entering_interval"
    CONFIRMATION = "confirmation"


@dataclass(slots=True)
class AutoTaskSetupState:
    """In-memory FSM state for a user's auto task setup flow."""

    step: AutoTaskSetupStep = AutoTaskSetupStep.IDLE
    account_mode: Optional[AccountMode] = None
    available_account_ids: List[str] = field(default_factory=list)
    selected_account_id: Optional[str] = None
    per_account_group_counts: Dict[str, int] = field(default_factory=dict)
    account_labels: Dict[str, str] = field(default_factory=dict)
    account_groups: Dict[str, List[object]] = field(default_factory=dict)
    total_groups: int = 0
    user_interval_seconds: Optional[float] = None
    notify_each_cycle: bool = False
    batch_size: int = 20
    last_message_id: Optional[int] = None

    def is_active(self) -> bool:
        return self.step != AutoTaskSetupStep.IDLE


class AutoTaskStateManager:
    """Stores per-user auto task setup states."""

    def __init__(self) -> None:
        self._states: Dict[int, AutoTaskSetupState] = {}

    def get(self, user_id: int) -> Optional[AutoTaskSetupState]:
        return self._states.get(user_id)

    def begin(
        self,
        user_id: int,
        *,
        step: AutoTaskSetupStep,
        account_mode: Optional[AccountMode] = None,
        available_account_ids: Optional[List[str]] = None,
        per_account_group_counts: Optional[Dict[str, int]] = None,
        account_labels: Optional[Dict[str, str]] = None,
        account_groups: Optional[Dict[str, List[object]]] = None,
        total_groups: int = 0,
        last_message_id: Optional[int] = None,
    ) -> AutoTaskSetupState:
        state = AutoTaskSetupState(
            step=step,
            account_mode=account_mode,
            available_account_ids=list(available_account_ids or []),
            per_account_group_counts=dict(per_account_group_counts or {}),
            account_labels=dict(account_labels or {}),
            account_groups=dict(account_groups or {}),
            total_groups=total_groups,
            last_message_id=last_message_id,
        )
        self._states[user_id] = state
        return state

    def update(self, user_id: int, **kwargs) -> AutoTaskSetupState:
        state = self._states.get(user_id)
        if state is None:
            state = AutoTaskSetupState()
            self._states[user_id] = state
        for field_name, value in kwargs.items():
            setattr(state, field_name, value)
        return state

    def clear(self, user_id: int) -> Optional[AutoTaskSetupState]:
        return self._states.pop(user_id, None)

    def has_active_flow(self, user_id: int) -> bool:
        state = self._states.get(user_id)
        return bool(state and state.is_active())
