"""HTTP Smart Request handler — universal HTTP proxy for BPMN Service Tasks.

Handles job type 'http-request-smart' used by Odoo-facing BPMN processes
(task creation, server-action calls, etc.). Logic is identical to the
production worker on main branch.
"""

from __future__ import annotations

import logging

import httpx
from pyzeebe import Job, ZeebeWorker

from ..config import AppConfig

logger = logging.getLogger(__name__)


async def _get_user_task_key(
    camunda_rest_url: str,
    process_instance_key: str,
    element_id: str,
    auth: tuple[str, str] | None = None,
) -> str | None:
    """Look up the user task key via Camunda REST API."""
    url = f"{camunda_rest_url}/v2/user-tasks/search"
    headers = {"Content-Type": "application/json"}
    payload = {
        "filter": {
            "processInstanceKey": int(process_instance_key),
            "elementId": element_id,
        }
    }

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                url, json=payload, headers=headers,
                auth=auth, timeout=10.0,
            )
            if response.status_code == 200:
                data = response.json()
                items = data.get("items", [])
                if items:
                    user_task_key = str(items[0].get("userTaskKey"))
                    logger.info("Found user_task_key: %s", user_task_key)
                    return user_task_key
                else:
                    logger.warning(
                        "No user tasks found for process %s, element %s",
                        process_instance_key, element_id,
                    )
            else:
                logger.warning(
                    "User task search failed with status %d: %s",
                    response.status_code, response.text,
                )
        except Exception as exc:
            logger.warning("Failed to get user_task_key: %s", exc)

    return None


def register_http_smart_handlers(
    worker: ZeebeWorker,
    config: AppConfig,
) -> None:
    """Register the http-request-smart handler."""

    # Build Camunda REST URL from gateway address
    gw = config.zeebe.gateway_address
    zeebe_host = gw.split(":")[0] if ":" in gw else gw
    camunda_rest_url = f"http://{zeebe_host}:8080"
    auth = ("demo", "demo")

    @worker.task(task_type="http-request-smart", timeout_ms=30_000)
    async def handle_smart_http_request(
        job: Job,
        url: str,
        method: str = "POST",
        body: dict = None,
        headers: dict = None,
        result_variable_name: str = None,
    ) -> dict | None:
        is_task_listener = not job.element_instance_key or job.element_instance_key == 0

        user_task_key = None

        if job.custom_headers and "io.camunda.zeebe:userTaskKey" in job.custom_headers:
            user_task_key = str(job.custom_headers["io.camunda.zeebe:userTaskKey"])
            logger.info("Got user_task_key from custom_headers: %s", user_task_key)

        elif hasattr(job, "user_task_key") and job.user_task_key:
            user_task_key = str(job.user_task_key)
            logger.info("Got user_task_key from job attribute: %s", user_task_key)

        elif is_task_listener and hasattr(job, "element_id") and job.element_id:
            user_task_key = await _get_user_task_key(
                camunda_rest_url,
                str(job.process_instance_key),
                job.element_id,
                auth=auth,
            )
            logger.info("Got user_task_key from REST API: %s", user_task_key)

        metadata = {
            "process_instance_key": job.process_instance_key,
            "element_instance_key": job.element_instance_key if job.element_instance_key else None,
            "bpmn_process_id": job.bpmn_process_id,
            "element_id": job.element_id if hasattr(job, "element_id") else None,
            "job_key": job.key,
            "user_task_key": user_task_key,
        }

        payload = body if body else {}
        payload.update(metadata)

        req_headers = headers if headers else {}
        req_headers["Content-Type"] = "application/json"

        logger.info("[%s] Sending %s to %s", job.process_instance_key, method, url)
        logger.info("[%s] Payload: %s", job.process_instance_key, payload)

        async with httpx.AsyncClient() as client:
            try:
                response = await client.request(
                    method=method.upper(),
                    url=url,
                    json=payload,
                    headers=req_headers,
                    timeout=30.0,
                )

                try:
                    response_body = response.json() if response.content else {}
                except Exception:
                    response_body = response.text if response.content else ""

                if response.status_code >= 400:
                    error_msg = f"HTTP {response.status_code}: {response_body}"
                    logger.error(error_msg)
                    raise Exception(error_msg)

                logger.info("Success. Status: %s", response.status_code)

                if result_variable_name:
                    if is_task_listener:
                        logger.warning(
                            "Task Listener detected - skipping variable return to avoid loop"
                        )
                        return None

                    logger.info("Returning data into variable: '%s'", result_variable_name)
                    return {result_variable_name: response_body}

            except httpx.RequestError as exc:
                logger.error("Network error: %s", exc)
                raise Exception(f"Network error: {exc}") from exc

        return None
