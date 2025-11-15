from __future__ import annotations

from datetime import datetime, timedelta, timezone

MOSCOW_TIMEZONE = timezone(timedelta(hours=3))
UTC_TIMEZONE = timezone.utc


def ensure_utc(dt: datetime) -> datetime:
    """Return a timezone-aware datetime in UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC_TIMEZONE)
    return dt.astimezone(UTC_TIMEZONE)


def to_moscow_time(dt: datetime) -> datetime:
    """Convert the provided datetime to the Moscow timezone."""
    return ensure_utc(dt).astimezone(MOSCOW_TIMEZONE)


def format_moscow_time(dt: datetime | None, fallback: str = "—") -> str:
    """Render datetime in `DD.MM HH:MM (МСК)` format for user-facing messages."""
    if dt is None:
        return fallback
    local_dt = to_moscow_time(dt)
    return f"{local_dt:%d.%m %H:%M} (МСК)"
