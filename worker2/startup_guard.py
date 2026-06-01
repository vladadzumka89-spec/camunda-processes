"""Startup guard that prevents stale Zeebe jobs from running after restart."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Iterable

import httpx

from .http_request_smart import _camunda_rest_request
from .runtime_state import mark_orphan_stale_job, orphan_stale_job_keys

logger = logging.getLogger(__name__)

DEFAULT_MAX_AGE_SECONDS = 6 * 60 * 60
PRODUCTION_VALUES = {"production", "prod", "main"}


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, (int, float)):
        # Camunda may expose deadlines as epoch milliseconds.
        timestamp = value / 1000 if value > 10_000_000_000 else value
        return datetime.fromtimestamp(timestamp, timezone.utc)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _job_age_seconds(job: dict[str, Any]) -> float | None:
    timestamp = _parse_time(job.get("creationTime") or job.get("lastUpdateTime"))
    if not timestamp:
        return None
    return (datetime.now(timezone.utc) - timestamp).total_seconds()


def _job_label(job: dict[str, Any]) -> str:
    return (
        f"{job.get('jobKey')} {job.get('processDefinitionId')}/"
        f"{job.get('elementId')} type={job.get('type')} "
        f"state={job.get('state')} retries={job.get('retries')}"
    )


def _is_unclaimed_or_expired(job: dict[str, Any]) -> bool:
    """Only treat CREATED jobs as stale when no worker owns them anymore."""
    worker = str(job.get("worker") or "")
    deadline = _parse_time(job.get("deadline"))
    if not worker:
        return True
    return deadline is not None and deadline <= datetime.now(timezone.utc)


async def _search_jobs(client: httpx.AsyncClient, state: str) -> list[dict[str, Any]]:
    resp = await _camunda_rest_request(
        client,
        "POST",
        "/v2/jobs/search",
        json={"filter": {"state": state}, "page": {"limit": 200}},
    )
    if resp.status_code != 200:
        raise RuntimeError(f"jobs search {state} failed: HTTP {resp.status_code} {resp.text[:300]}")
    return resp.json().get("items", [])


async def _fetch_variables(client: httpx.AsyncClient, process_instance_key: str) -> dict[str, Any]:
    resp = await _camunda_rest_request(
        client,
        "POST",
        "/v2/variables/search",
        json={
            "filter": {"processInstanceKey": process_instance_key},
            "page": {"limit": 100},
        },
    )
    if resp.status_code != 200:
        return {}
    variables: dict[str, Any] = {}
    for item in resp.json().get("items", []):
        name = item.get("name", "")
        value = item.get("value", "")
        if isinstance(value, str):
            value = value.strip('"')
        variables[name] = value
    return variables


async def _fetch_parent_process_key(client: httpx.AsyncClient, process_instance_key: str) -> str | None:
    try:
        key: str | int = int(process_instance_key)
    except ValueError:
        key = process_instance_key

    resp = await _camunda_rest_request(
        client,
        "POST",
        "/v1/process-instances/search",
        json={"filter": {"key": key}},
    )
    if resp.status_code != 200:
        logger.warning(
            "Could not inspect parent for process %s: HTTP %s %s",
            process_instance_key,
            resp.status_code,
            resp.text[:300],
        )
        return None

    items = resp.json().get("items", [])
    if not items:
        return None
    parent_key = items[0].get("parentKey")
    if parent_key in (None, "", -1, 0, "-1", "0"):
        return None
    return str(parent_key)


async def _root_process_key(client: httpx.AsyncClient, process_instance_key: str) -> str:
    root_key = str(process_instance_key)
    seen = {root_key}
    for _ in range(10):
        parent_key = await _fetch_parent_process_key(client, root_key)
        if not parent_key or parent_key in seen:
            return root_key
        seen.add(parent_key)
        root_key = parent_key
    return root_key


def _is_production_job(job: dict[str, Any], variables: dict[str, Any]) -> bool:
    process_id = str(job.get("processDefinitionId", "")).lower()
    server_host = str(variables.get("server_host", "")).lower()
    branch = str(variables.get("branch", "")).lower()
    if process_id == "production-nightly-deploy":
        return True
    return server_host in {"production", "prod"} or branch == "main"


async def guard_stale_jobs(
    task_types: Iterable[str],
    *,
    active_job_keys: Iterable[int | str] = (),
    context: str = "Startup",
) -> None:
    """Cancel stale worker2 jobs before polling starts.

    Stale production jobs are never auto-cancelled here; the worker refuses to
    start so a human can inspect them first.
    """
    max_age = int(os.getenv("WORKER_STALE_JOB_MAX_AGE_SECONDS", str(DEFAULT_MAX_AGE_SECONDS)))
    if max_age <= 0:
        logger.info("Startup stale-job guard disabled")
        return

    policy = os.getenv("WORKER_STALE_JOB_POLICY", "cancel").lower()
    worker_task_types = set(task_types)
    active_keys = {str(key) for key in active_job_keys}
    known_orphan_keys = orphan_stale_job_keys()
    stale_jobs: list[dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=15) as client:
        for state in ("CREATED", "FAILED"):
            for job in await _search_jobs(client, state):
                job_key = str(job.get("jobKey") or "")
                if job_key in active_keys or job_key in known_orphan_keys:
                    continue
                if job.get("type") not in worker_task_types:
                    continue
                if state == "FAILED" and int(job.get("retries") or 0) <= 0:
                    continue
                if state == "CREATED" and not _is_unclaimed_or_expired(job):
                    continue
                age = _job_age_seconds(job)
                if age is None or age < max_age:
                    continue
                stale_jobs.append(job)

        if not stale_jobs:
            logger.info("%s stale-job guard: no stale worker2 jobs", context)
            return

        logger.warning("%s stale-job guard found %d stale worker2 job(s)", context, len(stale_jobs))

        for job in stale_jobs:
            process_key = str(job.get("processInstanceKey") or "")
            variables = await _fetch_variables(client, process_key) if process_key else {}
            label = _job_label(job)

            if _is_production_job(job, variables):
                raise RuntimeError(f"Refusing to start with stale production job: {label}")

            if policy == "refuse":
                raise RuntimeError(f"Refusing to start with stale job: {label}")
            if policy not in {"cancel", "warn"}:
                raise RuntimeError(f"Unknown WORKER_STALE_JOB_POLICY={policy!r}")
            if policy == "warn":
                logger.warning("Stale job left in place by policy=warn: %s", label)
                continue

            if not process_key:
                logger.warning("Cannot cancel stale job without process key: %s", label)
                continue

            cancel_key = await _root_process_key(client, process_key)
            resp = await _camunda_rest_request(
                client,
                "POST",
                f"/v2/process-instances/{cancel_key}/cancellation",
                json={},
            )
            if resp.status_code in (200, 204):
                if cancel_key != process_key:
                    logger.warning(
                        "Cancelled stale root process %s (child %s) for job %s",
                        cancel_key,
                        process_key,
                        label,
                    )
                else:
                    logger.warning("Cancelled stale process %s for job %s", process_key, label)
            elif resp.status_code == 404:
                mark_orphan_stale_job(job.get("jobKey"))
                logger.warning("Ignoring stale orphan index job not present in broker: %s", label)
            else:
                raise RuntimeError(
                    f"Could not cancel stale process {cancel_key} for {label}: "
                    f"HTTP {resp.status_code} {resp.text[:300]}"
                )
