"""Simple async retry utility."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


async def retry(
    func: Callable[..., Awaitable[T]],
    max_attempts: int = 3,
    delay: float = 5.0,
    backoff: float = 2.0,
) -> T:
    """Retry an async function with exponential backoff.

    Args:
        func: Async callable to retry.
        max_attempts: Maximum number of attempts.
        delay: Initial delay between retries in seconds.
        backoff: Multiplier applied to delay after each retry.

    Returns:
        The result of the successful function call.

    Raises:
        The last exception if all attempts fail.
    """
    last_exc: Exception | None = None
    current_delay = delay

    for attempt in range(1, max_attempts + 1):
        try:
            return await func()
        except Exception as exc:
            last_exc = exc
            if attempt < max_attempts:
                logger.warning(
                    "Attempt %d/%d failed: %s. Retrying in %.1fs...",
                    attempt, max_attempts, exc, current_delay,
                )
                await asyncio.sleep(current_delay)
                current_delay *= backoff
            else:
                logger.error(
                    "All %d attempts failed. Last error: %s", max_attempts, exc,
                )

    raise last_exc  # type: ignore[misc]
