import asyncio
import logging
import os
import threading
import time
import requests
import grpc
import httpx
from pyzeebe import ZeebeWorker, Job

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

ZEEBE_ADDRESS = os.getenv("ZEEBE_ADDRESS", "orchestration:26500")
CLIENT_ID = os.getenv("ZEEBE_CLIENT_ID", "orchestration")
CLIENT_SECRET = os.getenv("ZEEBE_CLIENT_SECRET", "")
TOKEN_URL = os.getenv("ZEEBE_TOKEN_URL", "http://keycloak:18080/auth/realms/camunda-platform/protocol/openid-connect/token")
AUDIENCE = os.getenv("ZEEBE_AUDIENCE", "zeebe-api")
CAMUNDA_REST_URL = os.getenv("CAMUNDA_REST_URL", "http://camunda25.a.local:8088")


class TokenManager:
    def __init__(self, token_url: str, client_id: str, client_secret: str, audience: str):
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.audience = audience
        self._token = None
        self._token_expiry = 0
        self._lock = threading.Lock()

    def _fetch_new_token(self):
        response = requests.post(
            self.token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "audience": self.audience,
            }
        )
        response.raise_for_status()
        data = response.json()
        self._token = data["access_token"]
        expires_in = data.get("expires_in", 300)
        self._token_expiry = time.time() + expires_in
        logger.info(f"OAuth2 token obtained successfully (expires in {expires_in}s)")
        return self._token

    def get_token(self) -> str:
        with self._lock:
            if self._token is None or time.time() >= (self._token_expiry - 60):
                return self._fetch_new_token()
            return self._token

    def refresh_token(self):
        with self._lock:
            return self._fetch_new_token()


token_manager = TokenManager(TOKEN_URL, CLIENT_ID, CLIENT_SECRET, AUDIENCE)


class AuthClientInterceptor(
    grpc.aio.UnaryUnaryClientInterceptor,
    grpc.aio.UnaryStreamClientInterceptor
):
    def _get_metadata(self, metadata):
        token = token_manager.get_token()
        new_metadata = list(metadata) if metadata else []
        new_metadata.append(("authorization", f"Bearer {token}"))
        return new_metadata

    async def intercept_unary_unary(self, continuation, client_call_details, request):
        new_metadata = self._get_metadata(client_call_details.metadata)
        new_details = grpc.aio.ClientCallDetails(
            method=client_call_details.method,
            timeout=client_call_details.timeout,
            metadata=new_metadata,
            credentials=client_call_details.credentials,
            wait_for_ready=client_call_details.wait_for_ready,
        )
        return await continuation(new_details, request)

    async def intercept_unary_stream(self, continuation, client_call_details, request):
        new_metadata = self._get_metadata(client_call_details.metadata)
        new_details = grpc.aio.ClientCallDetails(
            method=client_call_details.method,
            timeout=client_call_details.timeout,
            metadata=new_metadata,
            credentials=client_call_details.credentials,
            wait_for_ready=client_call_details.wait_for_ready,
        )
        return await continuation(new_details, request)


class InterceptedChannel:
    def __init__(self, channel):
        self._channel = channel
        self._interceptor = AuthClientInterceptor()

    def unary_unary(self, method, request_serializer=None, response_deserializer=None):
        return _InterceptedUnaryUnaryMultiCallable(
            self._channel.unary_unary(method, request_serializer, response_deserializer),
            method
        )

    def unary_stream(self, method, request_serializer=None, response_deserializer=None):
        return _InterceptedUnaryStreamMultiCallable(
            self._channel.unary_stream(method, request_serializer, response_deserializer),
            method
        )

    async def close(self):
        await self._channel.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


class _InterceptedUnaryUnaryMultiCallable:
    def __init__(self, multi_callable, method):
        self._multi_callable = multi_callable
        self._method = method

    def __call__(self, request, **kwargs):
        metadata = kwargs.get('metadata', [])
        token = token_manager.get_token()
        new_metadata = list(metadata) if metadata else []
        new_metadata.append(("authorization", f"Bearer {token}"))
        kwargs['metadata'] = new_metadata
        return self._multi_callable(request, **kwargs)


class _InterceptedUnaryStreamMultiCallable:
    def __init__(self, multi_callable, method):
        self._multi_callable = multi_callable
        self._method = method

    def __call__(self, request, **kwargs):
        metadata = kwargs.get('metadata', [])
        token = token_manager.get_token()
        new_metadata = list(metadata) if metadata else []
        new_metadata.append(("authorization", f"Bearer {token}"))
        kwargs['metadata'] = new_metadata
        return self._multi_callable(request, **kwargs)


async def get_user_task_key(process_instance_key: str, element_id: str) -> str:
    token = token_manager.get_token()
    url = f"{CAMUNDA_REST_URL}/v2/user-tasks/search"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    payload = {
        "filter": {
            "processInstanceKey": int(process_instance_key),
            "elementId": element_id
        }
    }

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(url, json=payload, headers=headers, timeout=10.0)
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


async def create_worker():
    logger.info(f"Connecting to Zeebe at {ZEEBE_ADDRESS}")
    logger.info(f"Using client ID: {CLIENT_ID}")

    token_manager.get_token()

    base_channel = grpc.aio.insecure_channel(ZEEBE_ADDRESS)
    channel = InterceptedChannel(base_channel)

    worker = ZeebeWorker(channel)

    @worker.task(task_type="http-request-smart")
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
                except:
                    response_body = response.text if response.content else ""

                if response.status_code >= 400:
                    error_msg = f"HTTP {response.status_code}: {response_body}"
                    logger.error(error_msg)
                    raise Exception(error_msg)

                logger.info(f"Success. Status: {response.status_code}")

                if result_variable_name:
                    if is_task_listener:
                        logger.warning(f"Task Listener detected - skipping variable return to avoid loop")
                        return

                    logger.info(f"Returning data into variable: '{result_variable_name}'")
                    return {result_variable_name: response_body}

            except httpx.RequestError as e:
                logger.error(f"Network error: {e}")
                raise Exception(f"Network error: {e}")

    return worker


async def main():
    logger.info("Starting Python Worker...")

    while True:
        try:
            worker = await create_worker()
            logger.info("Worker created successfully. Waiting for jobs...")
            await worker.work()
        except Exception as e:
            logger.error(f"Worker error: {e}")
            logger.info("Refreshing token and restarting worker in 5 seconds...")
            token_manager.refresh_token()
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
