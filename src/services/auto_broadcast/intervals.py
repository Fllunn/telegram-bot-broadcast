from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

MAX_INTERVAL_SECONDS = 7 * 24 * 60 * 60  # 7 days
NORMALIZED_MAX_INTERVAL = "168:00:00"


class IntervalValidationError(ValueError):
    """Represents an invalid user-defined interval value."""

    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code
        self.user_message = message


@dataclass(slots=True)
class IntervalParseResult:
    total_seconds: int
    normalized_text: str


def _strip_part(part: str) -> str:
    return part.strip() if part else part


def parse_interval_input(value: Optional[str]) -> IntervalParseResult:
    raw = (value or "").strip()
    if not raw:
        raise IntervalValidationError("Интервал должен быть больше нуля.", code="empty")

    parts = raw.split(":")
    if len(parts) != 3:
        raise IntervalValidationError(
            "Используйте формат ЧЧ:ММ:СС. Например: 01:30:00.",
            code="format",
        )

    hours_raw, minutes_raw, seconds_raw = (_strip_part(part) for part in parts)

    if any(part.startswith("-") for part in (hours_raw, minutes_raw, seconds_raw)):
        raise IntervalValidationError("Время не может содержать отрицательные значения.", code="negative")

    if not all(part.isdigit() for part in (hours_raw, minutes_raw, seconds_raw)):
        raise IntervalValidationError(
            "Используйте формат ЧЧ:ММ:СС. Например: 01:30:00.",
            code="format",
        )

    hours = int(hours_raw)
    minutes = int(minutes_raw)
    seconds = int(seconds_raw)

    if minutes > 59 or seconds > 59:
        raise IntervalValidationError(
            "Минуты и секунды должны быть в диапазоне от 0 до 59.",
            code="minute_second_range",
        )

    total_seconds = hours * 60 * 60 + minutes * 60 + seconds
    if total_seconds <= 0:
        raise IntervalValidationError("Интервал должен быть больше нуля.", code="non_positive")

    if total_seconds > MAX_INTERVAL_SECONDS:
        raise IntervalValidationError(
            "Интервал слишком большой. Максимум — 168:00:00.",
            code="too_large",
        )

    normalized = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return IntervalParseResult(total_seconds=total_seconds, normalized_text=normalized)


def format_interval_hms(seconds: Optional[float]) -> str:
    if seconds is None:
        return "—"
    try:
        total_seconds = int(seconds)
    except (TypeError, ValueError):
        return "—"
    if total_seconds < 0:
        return "—"
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


__all__ = [
    "IntervalParseResult",
    "IntervalValidationError",
    "MAX_INTERVAL_SECONDS",
    "NORMALIZED_MAX_INTERVAL",
    "parse_interval_input",
    "format_interval_hms",
]
