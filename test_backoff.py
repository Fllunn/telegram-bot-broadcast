import asyncio
from datetime import datetime, timezone
from src.services.auto_invasion.backoff_calculator import calculate_next_attempt

now = datetime.now(timezone.utc)

print("Testing backoff calculator:")
for error_count in range(1, 7):
    next_attempt = calculate_next_attempt(error_count, now, now)
    hours_to_wait = (next_attempt - now).total_seconds() / 3600
    print(f"Error count {error_count}: Wait ~{hours_to_wait:.1f} hours")
