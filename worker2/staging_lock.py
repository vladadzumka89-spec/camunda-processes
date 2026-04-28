"""Staging sync lock — prevents deploys to staging during nightly DB sync.

In-memory flag with 4-hour stale-lock protection.
Acquire at staging-dump start, release in staging-export finally block.
"""

from __future__ import annotations

import time

_sync_start_time: float | None = None
_LOCK_MAX_AGE = 4 * 3600  # 4h — stale lock threshold


def acquire() -> None:
    global _sync_start_time
    _sync_start_time = time.time()


def release() -> None:
    global _sync_start_time
    _sync_start_time = None


def is_active() -> bool:
    if _sync_start_time is None:
        return False
    if time.time() - _sync_start_time > _LOCK_MAX_AGE:
        release()
        return False
    return True
