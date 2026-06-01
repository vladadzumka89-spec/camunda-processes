"""Small on-disk runtime state used by Docker healthcheck."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

HEALTH_FILE = Path(os.getenv("WORKER_HEALTH_FILE", "/tmp/worker2-health.json"))
POLL_WRITE_INTERVAL_SECONDS = float(os.getenv("WORKER_POLL_HEALTH_WRITE_INTERVAL_SECONDS", "5"))

_last_poll_write_at = 0.0


def _now() -> float:
    return time.time()


def _read_state() -> dict[str, Any]:
    try:
        return json.loads(HEALTH_FILE.read_text())
    except Exception:
        return {}


def write_state(**updates: Any) -> None:
    """Atomically merge updates into the health state file."""
    state = _read_state()
    state.update(updates)
    state["updated_at"] = _now()
    tmp = HEALTH_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, sort_keys=True))
    tmp.replace(HEALTH_FILE)


def mark_worker(status: str, **updates: Any) -> None:
    write_state(worker_status=status, worker_heartbeat_at=_now(), **updates)


def mark_heartbeat() -> None:
    write_state(worker_heartbeat_at=_now())


def mark_poll_attempt(task_type: str) -> None:
    global _last_poll_write_at

    now = _now()
    if now - _last_poll_write_at < POLL_WRITE_INTERVAL_SECONDS:
        return
    _last_poll_write_at = now
    write_state(
        last_poll_attempt_at=now,
        last_poll_task_type=task_type,
    )


def mark_job_started(job: Any) -> None:
    write_state(
        current_job_started_at=_now(),
        current_job_key=str(getattr(job, "key", "")),
        current_job_type=str(getattr(job, "type", "")),
        current_job_status="running",
        last_job_started_at=_now(),
        last_job_started_key=str(getattr(job, "key", "")),
        last_job_started_type=str(getattr(job, "type", "")),
    )


def mark_job_finished(job: Any, status: str) -> None:
    key = str(getattr(job, "key", ""))
    state = _read_state()
    updates: dict[str, Any] = {}
    if str(state.get("current_job_key") or "") == key:
        updates.update(
            current_job_started_at=0,
            current_job_key="",
            current_job_type="",
            current_job_status="",
        )
    write_state(
        last_job_finished_at=_now(),
        last_job_finished_key=key,
        last_job_finished_type=str(getattr(job, "type", "")),
        last_job_finished_status=status,
        last_job_status=status,
        last_job_key=key,
        last_job_type=str(getattr(job, "type", "")),
        **updates,
    )


def mark_orphan_stale_job(job_key: Any) -> None:
    state = _read_state()
    keys = [str(key) for key in state.get("orphan_stale_job_keys", []) if key]
    key = str(job_key or "")
    if key and key not in keys:
        keys.append(key)
    write_state(orphan_stale_job_keys=keys[-100:])


def orphan_stale_job_keys() -> set[str]:
    state = _read_state()
    return {str(key) for key in state.get("orphan_stale_job_keys", []) if key}


def job_health_status(job: Any) -> str:
    """Map pyzeebe's local status to a healthcheck-safe status string."""
    status = getattr(job, "status", "")
    status_name = str(getattr(status, "value", status)).lower()
    if status_name == "completed":
        return "completed"
    if status_name == "failed":
        return "failed"
    if status_name == "errorthrown":
        return "error"
    return f"not_completed:{status_name or 'unknown'}"
