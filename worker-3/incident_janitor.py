"""Incident Janitor — automatically cancel processes with stale incidents.

Periodically queries Zeebe REST API for ACTIVE incidents and cancels
process instances where the incident is older than a configurable threshold.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

import httpx

from .config import AppConfig

logger = logging.getLogger(__name__)

INCIDENT_MAX_AGE_HOURS = int(os.getenv("INCIDENT_MAX_AGE_HOURS", "48"))
JANITOR_INTERVAL_SECONDS = 3600  # 1 hour


def _build_zeebe_rest_url(config: AppConfig) -> str:
    """Build Zeebe REST base URL from gateway address (same logic as webhook.py)."""
    gw = config.zeebe.gateway_address  # e.g. "orchestration:26500"
    zeebe_host = gw.split(":")[0] if ":" in gw else gw
    return f"http://{zeebe_host}:8080"


async def cleanup_stale_incidents(config: AppConfig) -> int:
    """Find ACTIVE incidents older than threshold and cancel their processes.

    Returns the number of cancelled process instances.
    """
    zeebe_rest = _build_zeebe_rest_url(config)
    auth = ("demo", "demo")
    cancelled = 0

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            # 1. Fetch all active incidents
            resp = await client.post(
                f"{zeebe_rest}/v2/incidents/search",
                auth=auth,
                headers={"Content-Type": "application/json"},
                json={"filter": {"state": "ACTIVE"}},
            )
            if resp.status_code != 200:
                logger.error(
                    "Failed to search incidents: HTTP %d %s",
                    resp.status_code, resp.text,
                )
                return 0

            data = resp.json()
            incidents = data.get("items", [])
            if not incidents:
                logger.debug("No active incidents found")
                return 0

            logger.info("Found %d active incident(s), checking age...", len(incidents))
            now = datetime.now(timezone.utc)

            # 2. Check each incident's age and cancel if stale
            seen_keys: set[int] = set()
            for incident in incidents:
                creation_time = incident.get("creationTime")
                process_key = incident.get("processInstanceKey")

                if not creation_time or not process_key:
                    continue

                # Avoid cancelling the same process twice
                if process_key in seen_keys:
                    continue

                created = datetime.fromisoformat(creation_time)
                age_hours = (now - created).total_seconds() / 3600

                if age_hours < INCIDENT_MAX_AGE_HOURS:
                    continue

                # 3. Cancel the stale process instance
                cancel_resp = await client.post(
                    f"{zeebe_rest}/v2/process-instances/{process_key}/cancellation",
                    auth=auth,
                    headers={"Content-Type": "application/json"},
                    content="{}",
                )

                seen_keys.add(process_key)

                if cancel_resp.status_code in (200, 204):
                    cancelled += 1
                    logger.info(
                        "Cancelled stale process %s (incident age: %.0fh)",
                        process_key, age_hours,
                    )
                elif cancel_resp.status_code == 404:
                    logger.info(
                        "Process %s already terminated (404)", process_key,
                    )
                else:
                    logger.warning(
                        "Failed to cancel process %s: HTTP %d %s",
                        process_key, cancel_resp.status_code, cancel_resp.text,
                    )

    except Exception as exc:
        logger.error("Incident janitor error: %s", exc)

    if cancelled:
        logger.info("Incident janitor: cancelled %d stale process(es)", cancelled)

    return cancelled
