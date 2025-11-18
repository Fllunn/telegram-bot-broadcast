from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

T = TypeVar("T")

TELETHON_NETWORK_EXCEPTIONS: tuple[type[BaseException], ...] = (
    ConnectionError,
    asyncio.TimeoutError,
    OSError,
)


async def run_with_exponential_backoff(
    operation: Callable[[], Awaitable[T]],
    *,
    label: str,
    logger: logging.Logger,
    log_context: dict[str, Any] | None = None,
    max_delay_seconds: int = 60,
    retry_exceptions: tuple[type[BaseException], ...] = TELETHON_NETWORK_EXCEPTIONS,
) -> T:
    """Execute *operation* and retry with exponential backoff on network failures.

    Each retry delay is calculated as min(2 ** attempt, max_delay_seconds), where
    attempt starts from 1. All retry attempts and their delays are logged so the
    operator can observe reconnection behaviour in production.
    """

    attempt = 0
    context = dict(log_context or {})

    while True:
        try:
            result = await operation()
        except asyncio.CancelledError:
            raise
        except retry_exceptions as exc:
            attempt += 1
            delay = min(2 ** attempt, max_delay_seconds)
            log_extra = {**context, "backoff_attempt": attempt, "backoff_delay": delay}
            logger.warning(
                "%s failed due to %s: %s. Retrying in %s seconds",
                label,
                exc.__class__.__name__,
                exc,
                delay,
                extra=log_extra,
            )
            await asyncio.sleep(delay)
            continue
        except Exception:
            logger.exception("%s failed with unexpected error", label, extra=context)
            raise
        else:
            if attempt:
                log_extra = {**context, "recovery_attempts": attempt}
                logger.info(
                    "%s recovered after %s retries",
                    label,
                    attempt,
                    extra=log_extra,
                )
            return result