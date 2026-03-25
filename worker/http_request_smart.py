"""Handler for http-request-smart job type.

Executes HTTP requests to Odoo webhooks/APIs from BPMN Service Tasks.
Integrated into the main worker via register_http_smart_handlers().
"""

import logging
import os

import httpx
from pyzeebe import Job

logger = logging.getLogger(__name__)

CAMUNDA_REST_URL = os.getenv("CAMUNDA_REST_URL", "http://orchestration:8080")
CAMUNDA_REST_USER = os.getenv("CAMUNDA_REST_USER", "demo")
CAMUNDA_REST_PASSWORD = os.getenv("CAMUNDA_REST_PASSWORD", "demo")
ZEEBE_CLIENT_ID = os.getenv("ZEEBE_CLIENT_ID", "orchestration")
ZEEBE_CLIENT_SECRET = os.getenv("ZEEBE_CLIENT_SECRET", "")
ZEEBE_TOKEN_URL = os.getenv("ZEEBE_TOKEN_URL", "http://keycloak:18080/auth/realms/camunda-platform/protocol/openid-connect/token")


async def _get_oauth_token(client: httpx.AsyncClient) -> str:
    """Get OAuth2 token for Operate API."""
    response = await client.post(
        ZEEBE_TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": ZEEBE_CLIENT_ID,
            "client_secret": ZEEBE_CLIENT_SECRET,
        },
        timeout=10.0,
    )
    response.raise_for_status()
    return response.json()["access_token"]


async def get_parent_process_instance_key(process_instance_key) -> str:
    """Check if this process instance is a subprocess (Call Activity) and return parent key.
    Uses Operate API v1 which reliably returns parentKey for subprocesses.
    """
    url = f"{CAMUNDA_REST_URL}/v1/process-instances/search"

    async with httpx.AsyncClient() as client:
        try:
            token = await _get_oauth_token(client)
            response = await client.post(
                url,
                json={"filter": {"key": int(process_instance_key)}},
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                timeout=5.0,
            )
            if response.status_code == 200:
                data = response.json()
                items = data.get("items", [])
                if items:
                    parent_key = items[0].get("parentKey")
                    if parent_key and parent_key not in (-1, 0):
                        logger.info(f"Subprocess detected: parent_key={parent_key}")
                        return str(parent_key)
            else:
                logger.warning(f"Operate API returned {response.status_code}")
        except Exception as e:
            logger.warning(f"Failed to check parent process: {e}")

    return None


async def get_user_task_key(process_instance_key: str, element_id: str) -> str:
    """Look up user_task_key via Camunda REST API (basic auth)."""
    url = f"{CAMUNDA_REST_URL}/v2/user-tasks/search"
    payload = {
        "filter": {
            "processInstanceKey": int(process_instance_key),
            "elementId": element_id
        }
    }

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                url, json=payload,
                auth=(CAMUNDA_REST_USER, CAMUNDA_REST_PASSWORD),
                timeout=10.0,
            )
            if response.status_code == 200:
                data = response.json()
                items = data.get("items", [])
                if items:
                    user_task_key = str(items[0].get("userTaskKey"))
                    logger.info(f"Found user_task_key: {user_task_key}")
                    return user_task_key
                else:
                    logger.warning(f"No user tasks found for process {process_instance_key}, element {element_id}")
            else:
                logger.warning(f"User task search failed with status {response.status_code}: {response.text}")
        except Exception as e:
            logger.warning(f"Failed to get user_task_key: {e}")

    return None


def register_http_smart_handlers(worker, config=None):
    """Register http-request-smart handler with an existing worker."""

    @worker.task(task_type="http-request-smart", timeout_ms=30_000)
    async def handle_smart_http_request(
        job: Job,
        url: str,
        method: str = "POST",
        body: dict = None,
        headers: dict = None,
        result_variable_name: str = None
    ):
        is_task_listener = not job.element_instance_key or job.element_instance_key == 0

        user_task_key = None

        if job.custom_headers and "io.camunda.zeebe:userTaskKey" in job.custom_headers:
            is_task_listener = True
            user_task_key = str(job.custom_headers["io.camunda.zeebe:userTaskKey"])
            logger.info(f"Got user_task_key from custom_headers: {user_task_key}")

        elif hasattr(job, 'user_task_key') and job.user_task_key:
            user_task_key = str(job.user_task_key)
            logger.info(f"Got user_task_key from job attribute: {user_task_key}")

        elif is_task_listener and hasattr(job, 'element_id') and job.element_id:
            user_task_key = await get_user_task_key(
                str(job.process_instance_key),
                job.element_id
            )
            logger.info(f"Got user_task_key from REST API: {user_task_key}")

        metadata = {
            "process_instance_key": job.process_instance_key,
            "element_instance_key": job.element_instance_key if job.element_instance_key else None,
            "bpmn_process_id": job.bpmn_process_id,
            "element_id": job.element_id if hasattr(job, 'element_id') else None,
            "job_key": job.key,
            "user_task_key": user_task_key
        }

        payload = body if body else {}
        payload.update(metadata)

        parent_key = await get_parent_process_instance_key(job.process_instance_key)
        if parent_key:
            payload["parent_process_instance_key"] = parent_key

        req_headers = headers if headers else {}
        req_headers['Content-Type'] = 'application/json'

        logger.info(f"[{job.process_instance_key}] Sending {method} to {url}")
        logger.info(f"[{job.process_instance_key}] Payload: {payload}")

        async with httpx.AsyncClient() as client:
            try:
                response = await client.request(
                    method=method.upper(),
                    url=url,
                    json=payload,
                    headers=req_headers,
                    timeout=30.0
                )

                try:
                    response_body = response.json() if response.content else {}
                except Exception:
                    response_body = response.text if response.content else ""

                if response.status_code >= 400:
                    error_msg = f"HTTP {response.status_code}: {response_body}"
                    logger.error(error_msg)
                    raise Exception(error_msg)

                logger.info(f"Success. Status: {response.status_code}")

                result = {"process_instance_key": job.process_instance_key}

                if is_task_listener:
                    logger.warning(f"Task Listener detected - skipping variable return to avoid loop")
                    return

                if result_variable_name:
                    logger.info(f"Returning data into variable: '{result_variable_name}'")
                    result[result_variable_name] = response_body

                return result

            except httpx.RequestError as e:
                logger.error(f"Network error: {e}")
                raise Exception(f"Network error: {e}")
