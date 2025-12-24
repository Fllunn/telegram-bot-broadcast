from __future__ import annotations

import random
from datetime import datetime, timedelta


def calculate_next_attempt(
    error_count: int,
    last_error_at: datetime,
    now: datetime,
) -> datetime:
    if error_count >= 5:
        hours_since_last_error = (now - last_error_at).total_seconds() / 3600
        if hours_since_last_error > 10:
            error_count = 1
    
    if error_count == 1:
        hours = random.uniform(1, 2)
    elif error_count == 2:
        hours = random.uniform(2, 3)
    elif error_count == 3:
        hours = random.uniform(3, 4)
    elif error_count == 4:
        hours = random.uniform(4, 5)
    else:
        hours = random.uniform(5, 6)
    
    return now + timedelta(hours=hours)


def calculate_long_pause(now: datetime) -> datetime:
    hours = random.uniform(24, 48)
    return now + timedelta(hours=hours)
