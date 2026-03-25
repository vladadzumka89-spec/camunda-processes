"""Handler for http-request-cicd job type.

Executes HTTP requests to Odoo webhooks/APIs from CI/CD BPMN Service Tasks.
Uses 'http-request-cicd' task type to avoid conflicts with python-worker's 'http-request-smart'.
"""

import asyncio
import logging
import os

import httpx
from pyzeebe import Job

from .errors import ConfigError, OdooWebhookError

logger = logging.getLogger(__name__)


class TaskListenerCompleted(Exception):
    """Raised after task listener job is completed via REST API to skip pyzeebe's gRPC completion."""
    pass

CAMUNDA_REST_URL = os.getenv("CAMUNDA_REST_URL", "http://orchestration:8080")
ZEEBE_CLIENT_ID = os.getenv("ZEEBE_CLIENT_ID", "orchestration")
ZEEBE_CLIENT_SECRET = os.getenv("ZEEBE_CLIENT_SECRET", "")
ZEEBE_TOKEN_URL = os.getenv("ZEEBE_TOKEN_URL", "")

_cached_token = {"access_token": None, "expires_at": 0}
_token_lock = asyncio.Lock()


async def _get_oauth_token(client: httpx.AsyncClient) -> str:
    """Get OAuth2 token from Keycloak, with simple caching."""
    import time
    if _cached_token["access_token"] and time.time() < _cached_token["expires_at"] - 30:
        return _cached_token["access_token"]

    async with _token_lock:
        # Double-check after acquiring lock
        if _cached_token["access_token"] and time.time() < _cached_token["expires_at"] - 30:
            return _cached_token["access_token"]

        resp = await client.post(
            ZEEBE_TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": ZEEBE_CLIENT_ID,
                "client_secret": ZEEBE_CLIENT_SECRET,
            },
            timeout=10.0,
        )
        data = resp.json()
        _cached_token["access_token"] = data["access_token"]
        _cached_token["expires_at"] = time.time() + data.get("expires_in", 300)
        return data["access_token"]


async def _camunda_rest_request(client: httpx.AsyncClient, method: str, path: str, **kwargs) -> httpx.Response:
    """Make authenticated request to Camunda REST API."""
    token = await _get_oauth_token(client)
    return await client.request(
        method, f"{CAMUNDA_REST_URL}{path}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=10.0,
        **kwargs,
    )


async def get_user_task_key(process_instance_key: str, element_id: str) -> str:
    """Look up user_task_key via Camunda REST API."""
    async with httpx.AsyncClient() as client:
        try:
            response = await _camunda_rest_request(
                client, "POST", "/v2/user-tasks/search",
                json={"filter": {"processInstanceKey": int(process_instance_key), "elementId": element_id}},
            )
            if response.status_code == 200:
                items = response.json().get("items", [])
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


def register_http_smart_handlers(worker, config=None):
    """Register http-request-smart handler with an existing worker."""

    @worker.task(task_type="http-request-cicd", timeout_ms=30_000)
    async def handle_smart_http_request(
        job: Job,
        url: str,
        method: str = "POST",
        body: dict = None,
        headers: dict = None,
        result_variable_name: str = None,
        ignore_errors: bool = False,
        **kwargs,
    ):
        # Detect task listener: check for userTaskKey in custom_headers (set by Zeebe for task listeners)
        is_task_listener = bool(
            job.custom_headers and "io.camunda.zeebe:userTaskKey" in job.custom_headers
        )
        logger.info(f"[{job.process_instance_key}] is_task_listener={is_task_listener}, custom_headers={job.custom_headers}")

        user_task_key = None

        if job.custom_headers and "io.camunda.zeebe:userTaskKey" in job.custom_headers:
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

        # Validate URL early — BPMN must provide it via input mapping
        if not url:
            raise ConfigError(
                f"url is required but got None — check BPMN input mapping "
                f"for element '{job.element_id}' in process '{job.bpmn_process_id}'"
            )

        payload = body if body else {}
        payload["process_instance_key"] = job.process_instance_key
        payload["element_instance_key"] = job.element_instance_key
        payload["bpmn_process_id"] = job.bpmn_process_id
        payload["element_id"] = job.element_id if hasattr(job, 'element_id') else None
        payload["job_key"] = job.key
        if user_task_key:
            payload["user_task_key"] = user_task_key

        # Inject Odoo project ID if BPMN didn't provide it
        if "_id" not in payload and config and config.odoo.project_id:
            payload["_id"] = config.odoo.project_id

        parent_key = await get_parent_process_instance_key(job.process_instance_key)
        if parent_key:
            payload["parent_process_instance_key"] = parent_key

        req_headers = headers if headers else {}
        req_headers['Content-Type'] = 'application/json'

        logger.info(f"[{job.process_instance_key}] Sending {method} to {url}")
        logger.info(f"[{job.process_instance_key}] Payload: {payload}")

        max_retries = 3
        retry_delay = 2.0

        try:
            async with httpx.AsyncClient() as client:
                last_error = None

                for attempt in range(1, max_retries + 1):
                    try:
                        response = await client.request(
                            method=method.upper(),
                            url=url,
                            json=payload,
                            headers=req_headers,
                            timeout=30.0
                        )
                    except httpx.RequestError as net_err:
                        if attempt < max_retries:
                            logger.warning(
                                f"[{job.process_instance_key}] Network error: {net_err}, "
                                f"retrying in {retry_delay}s (attempt {attempt}/{max_retries})"
                            )
                            await asyncio.sleep(retry_delay)
                            retry_delay *= 2
                            continue
                        raise OdooWebhookError(f"Network error after {max_retries} attempts: {net_err}")

                    try:
                        response_body = response.json() if response.content else {}
                    except Exception:
                        response_body = response.text if response.content else ""

                    # Retry on 5xx (server error, transient); fail on 4xx (client error, permanent)
                    if response.status_code >= 500 and attempt < max_retries:
                        logger.warning(
                            f"[{job.process_instance_key}] HTTP {response.status_code} from {url}, "
                            f"retrying in {retry_delay}s (attempt {attempt}/{max_retries})"
                        )
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 2
                        continue

                    if response.status_code >= 400:
                        error_msg = f"HTTP {response.status_code}: {response_body}"
                        if ignore_errors:
                            logger.warning(f"[{job.process_instance_key}] {error_msg} (ignore_errors=True, continuing)")
                            return {"process_instance_key": job.process_instance_key}
                        raise OdooWebhookError(error_msg)

                    break  # success

                logger.info(f"Success. Status: {response.status_code}")

                # Task Listeners in Camunda 8.8 do not support returning variables via gRPC
                # Complete via REST API without variables, then raise to skip pyzeebe's completion
                if is_task_listener:
                    logger.info("Task Listener detected - completing via REST API without variables")
                    complete_resp = await _camunda_rest_request(
                        client, "POST", f"/v2/jobs/{job.key}/completion",
                        json={},
                    )
                    logger.info(f"REST API job completion status: {complete_resp.status_code}")
                    raise TaskListenerCompleted()

                result = {"process_instance_key": job.process_instance_key}

                if result_variable_name:
                    logger.info(f"Returning data into variable: '{result_variable_name}'")
                    result[result_variable_name] = response_body

                return result

        except TaskListenerCompleted:
            raise
        except OdooWebhookError:
            raise
        except ConfigError:
            raise
        except Exception as e:
            if ignore_errors:
                logger.warning(f"[{job.process_instance_key}] {e} (ignore_errors=True, continuing)")
                return {"process_instance_key": job.process_instance_key}
            logger.error(f"[{job.process_instance_key}] {e}")
            raise
