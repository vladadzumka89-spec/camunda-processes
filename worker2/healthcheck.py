"""Docker healthcheck for worker2.

Checks both Zeebe connectivity and the worker heartbeat written by worker.py.
It also reports stale CREATED jobs so they show up in container health logs.
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import grpc
import httpx

HEALTH_FILE = Path(os.getenv("WORKER_HEALTH_FILE", "/tmp/worker2-health.json"))
MAX_HEARTBEAT_AGE = int(os.getenv("WORKER_HEALTH_MAX_AGE_SECONDS", "120"))
STALE_JOB_ALERT_SECONDS = int(os.getenv("WORKER_STALE_JOB_ALERT_SECONDS", "900"))


def _check_grpc() -> tuple[bool, str]:
    target = os.getenv("ZEEBE_ADDRESS", "orchestration:26500")
    try:
        channel = grpc.insecure_channel(target)
        future = grpc.channel_ready_future(channel)
        future.result(timeout=5)
        channel.close()
        return True, "grpc ok"
    except Exception as exc:
        return False, f"grpc failed: {exc}"


def _check_heartbeat() -> tuple[bool, str]:
    try:
        state = json.loads(HEALTH_FILE.read_text())
    except Exception as exc:
        return False, f"heartbeat missing: {exc}"

    status = state.get("worker_status")
    heartbeat_at = float(state.get("worker_heartbeat_at") or 0)
    age = time.time() - heartbeat_at
    if status != "running":
        return False, f"worker status is {status!r}"
    if age > MAX_HEARTBEAT_AGE:
        return False, f"worker heartbeat stale: {age:.0f}s"
    return True, f"heartbeat ok: {age:.0f}s"


def _check_polling() -> tuple[bool, str]:
    try:
        state = json.loads(HEALTH_FILE.read_text())
    except Exception as exc:
        return False, f"polling heartbeat missing: {exc}"

    poll_at = float(state.get("last_poll_attempt_at") or 0)
    age = time.time() - poll_at
    if not poll_at:
        return False, "job polling has not started"
    if age > MAX_HEARTBEAT_AGE:
        return False, f"job polling stale: {age:.0f}s"

    task_type = state.get("last_poll_task_type") or "unknown"
    current_job = state.get("current_job_key")
    if current_job:
        current_type = state.get("current_job_type") or "unknown"
        current_status = state.get("current_job_status") or "running"
        return True, (
            f"job polling ok: {age:.0f}s, last poll={task_type}, "
            f"current job={current_job}/{current_type}/{current_status}"
        )
    last_finished = state.get("last_job_finished_key") or state.get("last_job_key")
    if last_finished:
        finished_type = state.get("last_job_finished_type") or state.get("last_job_type") or "unknown"
        status = state.get("last_job_finished_status") or state.get("last_job_status") or "unknown"
        return True, (
            f"job polling ok: {age:.0f}s, last poll={task_type}, "
            f"last finished={last_finished}/{finished_type}/{status}"
        )
    return True, f"job polling ok: {age:.0f}s, last poll={task_type}, no jobs handled yet"


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, (int, float)):
        timestamp = value / 1000 if value > 10_000_000_000 else value
        return datetime.fromtimestamp(timestamp, timezone.utc)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _worker_task_types() -> set[str]:
    try:
        state = json.loads(HEALTH_FILE.read_text())
    except Exception:
        return set()
    return {str(item) for item in state.get("task_types", []) if item}


def _orphan_stale_job_keys() -> set[str]:
    try:
        state = json.loads(HEALTH_FILE.read_text())
    except Exception:
        return set()
    return {str(item) for item in state.get("orphan_stale_job_keys", []) if item}


def _current_job_keys() -> set[str]:
    try:
        state = json.loads(HEALTH_FILE.read_text())
    except Exception:
        return set()
    return {str(item) for item in (state.get("current_job_key"),) if item}


def _is_unclaimed_or_expired(job: dict[str, Any]) -> bool:
    worker = str(job.get("worker") or "")
    deadline = _parse_time(job.get("deadline"))
    if not worker:
        return True
    return deadline is not None and deadline <= datetime.now(timezone.utc)


def _oauth_token(client: httpx.Client) -> str | None:
    token_url = os.getenv("ZEEBE_TOKEN_URL", "")
    client_secret = os.getenv("ZEEBE_CLIENT_SECRET", "")
    if not token_url or not client_secret:
        return None
    resp = client.post(
        token_url,
        data={
            "grant_type": "client_credentials",
            "client_id": os.getenv("ZEEBE_CLIENT_ID", "orchestration"),
            "client_secret": client_secret,
        },
        timeout=5,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _stale_created_jobs() -> list[dict[str, Any]]:
    if STALE_JOB_ALERT_SECONDS <= 0:
        return []
    rest_url = os.getenv("CAMUNDA_REST_URL", "http://orchestration:8080")
    with httpx.Client(timeout=8) as client:
        token = _oauth_token(client)
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        resp = client.post(
            f"{rest_url}/v2/jobs/search",
            headers=headers,
            json={"filter": {"state": "CREATED"}, "page": {"limit": 100}},
        )
        if resp.status_code != 200:
            return []

    stale: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)
    task_types = _worker_task_types()
    orphan_keys = _orphan_stale_job_keys()
    current_keys = _current_job_keys()
    for job in resp.json().get("items", []):
        job_key = str(job.get("jobKey") or "")
        if job_key in orphan_keys or job_key in current_keys:
            continue
        if task_types and job.get("type") not in task_types:
            continue
        if not _is_unclaimed_or_expired(job):
            continue
        created_at = _parse_time(job.get("creationTime") or job.get("lastUpdateTime"))
        if not created_at:
            continue
        age = (now - created_at).total_seconds()
        if age >= STALE_JOB_ALERT_SECONDS:
            job["ageSeconds"] = int(age)
            stale.append(job)
    return stale


def check() -> bool:
    checks = [_check_grpc(), _check_heartbeat(), _check_polling()]
    ok = all(result for result, _ in checks)
    for result, message in checks:
        stream = sys.stdout if result else sys.stderr
        print(message, file=stream)

    stale_jobs = _stale_created_jobs()
    if stale_jobs:
        for job in stale_jobs:
            print(
                "stale CREATED job: "
                f"{job.get('jobKey')} {job.get('processDefinitionId')}/"
                f"{job.get('elementId')} type={job.get('type')} "
                f"age={job.get('ageSeconds')}s",
                file=sys.stderr,
            )
        if os.getenv("WORKER_HEALTH_FAIL_ON_STALE_JOBS", "false").lower() == "true":
            ok = False

    return ok


if __name__ == "__main__":
    sys.exit(0 if check() else 1)
