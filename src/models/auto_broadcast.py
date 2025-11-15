from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class AccountMode(str, Enum):
    """Determines how many accounts participate in the auto broadcast."""

    SINGLE = "single"
    ALL = "all"


class TaskStatus(str, Enum):
    """Represents runtime status of an auto broadcast task."""

    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"
    ERROR = "error"


class AccountStatus(str, Enum):
    """Tracks operational status of a user account used for broadcasts."""

    ACTIVE = "active"
    BLOCKED = "blocked"
    COOLDOWN = "cooldown"
    INACTIVE = "inactive"


class RetryPolicy(BaseModel):
    """Retry policy configuration for handling recoverable failures."""

    model_config = ConfigDict(populate_by_name=True)

    max_attempts: int = Field(default=3, ge=1, le=10)
    base_delay_seconds: int = Field(default=30, ge=1)
    backoff_multiplier: float = Field(default=2.0, ge=1.0)
    max_delay_seconds: int = Field(default=600, ge=1)


class GroupTarget(BaseModel):
    """Represents a target group/channel for broadcast delivery."""

    model_config = ConfigDict(populate_by_name=True)

    chat_id: Optional[int] = None
    username: Optional[str] = None
    link: Optional[str] = None
    name: Optional[str] = None
    source_session_id: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class AutoBroadcastTask(BaseModel):
    """MongoDB document schema for auto broadcast tasks."""

    model_config = ConfigDict(populate_by_name=True)

    id: Optional[str] = Field(default=None, alias="_id")
    task_id: str
    user_id: int
    account_mode: AccountMode
    account_id: Optional[str] = None
    account_ids: List[str] = Field(default_factory=list)
    groups: List[GroupTarget] = Field(default_factory=list)
    per_account_groups: Dict[str, List[GroupTarget]] = Field(default_factory=dict)
    user_interval_seconds: float = Field(gt=0)
    enabled: bool = True
    status: TaskStatus = TaskStatus.RUNNING
    next_run_ts: Optional[datetime] = None
    last_cycle_time_seconds: Optional[float] = None
    last_run_at: Optional[datetime] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    notify_each_cycle: bool = False
    current_batch_index: int = 0
    current_group_index: int = 0
    current_account_id: Optional[str] = None
    batch_size: int = Field(default=20, ge=1)
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)
    locked_by: Optional[str] = None
    lock_ts: Optional[datetime] = None
    last_error: Optional[str] = None
    last_error_at: Optional[datetime] = None
    total_sent: int = 0
    total_failed: int = 0
    average_cycle_time: Optional[float] = None
    cycles_completed: int = 0
    problem_accounts: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class AccountState(BaseModel):
    """MongoDB document schema for account runtime state."""

    model_config = ConfigDict(populate_by_name=True)

    id: Optional[str] = Field(default=None, alias="_id")
    account_id: str
    owner_id: int
    session_id: Optional[str] = None
    status: AccountStatus = AccountStatus.ACTIVE
    cooldown_until: Optional[datetime] = None
    blocked_reason: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


__all__ = [
    "AccountMode",
    "TaskStatus",
    "AccountStatus",
    "RetryPolicy",
    "GroupTarget",
    "AutoBroadcastTask",
    "AccountState",
]
