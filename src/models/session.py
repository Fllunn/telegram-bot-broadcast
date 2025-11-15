from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


class SessionOwnerType(str, Enum):
    """Indicates the owner type of a Telethon session."""

    USER = "user"
    BOT = "bot"


class TelethonSession(BaseModel):
    """Represents a stored Telethon session for a user account."""

    model_config = ConfigDict(populate_by_name=True)

    id: Optional[str] = Field(default=None, alias="_id")
    owner_id: int
    owner_type: SessionOwnerType = SessionOwnerType.USER
    session_id: str
    session_data: Optional[str] = None
    is_active: bool = True
    client_type: Literal["user", "bot"] = "user"
    phone: str = Field(..., min_length=1)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: dict[str, Any] = Field(default_factory=dict)
    status: Optional[str] = None
    last_checked_at: Optional[datetime] = None
    last_error: Optional[str] = None

    def display_name(self) -> str:
        username = self.metadata.get("username") if self.metadata else None
        first_name = self.metadata.get("first_name") if self.metadata else None
        last_name = self.metadata.get("last_name") if self.metadata else None
        if username:
            return f"@{username}"
        if first_name or last_name:
            return " ".join(part for part in [first_name, last_name] if part)
        return self.session_id
