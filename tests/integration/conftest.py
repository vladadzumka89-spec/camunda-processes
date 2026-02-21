"""Fixtures for integration tests against a real Zeebe instance."""

from __future__ import annotations

import asyncio
import logging
import subprocess
import time
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
from pyzeebe import ZeebeClient, ZeebeWorker, create_insecure_channel

logger = logging.getLogger(__name__)

COMPOSE_DIR = Path(__file__).parent
BPMN_DIR = Path(__file__).parents[2] / "bpmn"
ZEEBE_GRPC = "localhost:26500"
ZEEBE_REST = "http://localhost:8088"
AUTH = ("demo", "demo")

STACK_STARTUP_TIMEOUT = 120  # seconds
DEPLOY_TIMEOUT = 10
PROCESS_COMPLETE_TIMEOUT = 60


# ---------------------------------------------------------------------------
# Docker stack lifecycle (session-scoped, sync)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def zeebe_stack():
    """Start Zeebe + Elasticsearch via docker compose, tear down after session."""
    compose_file = str(COMPOSE_DIR / "docker-compose.yaml")
    base_cmd = ["docker", "compose", "-f", compose_file, "-p", "camunda-integration"]

    # Start stack
    subprocess.run([*base_cmd, "up", "-d", "--wait"], check=True, timeout=STACK_STARTUP_TIMEOUT)

    # Wait for REST API + broker partitions
    deadline = time.monotonic() + STACK_STARTUP_TIMEOUT
    while time.monotonic() < deadline:
        try:
            r = httpx.get(f"{ZEEBE_REST}/v2/topology", auth=AUTH, timeout=5)
            if r.status_code == 200:
                data = r.json()
                brokers = data.get("brokers", [])
                if brokers and brokers[0].get("partitions"):
                    logger.info("Zeebe REST API ready (broker has partitions)")
                    break
                logger.info("Zeebe REST API up but broker has no partitions yet...")
        except (httpx.ConnectError, httpx.ReadTimeout):
            pass
        time.sleep(2)
    else:
        subprocess.run([*base_cmd, "logs"], check=False)
        raise TimeoutError("Zeebe REST API did not become ready")

    yield

    subprocess.run([*base_cmd, "down", "-v", "--remove-orphans"], check=False, timeout=60)


# ---------------------------------------------------------------------------
# BPMN deployment (session-scoped, sync)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def deploy_bpmn(zeebe_stack):
    """Deploy BPMN processes and forms to Zeebe."""
    resources = [
        BPMN_DIR / "feature-to-production.bpmn",
        BPMN_DIR / "deploy-process.bpmn",
        *(BPMN_DIR / "forms").glob("*.form"),
    ]
    deadline = time.monotonic() + 30
    last_error = None
    while time.monotonic() < deadline:
        files = [
            ("resources", (f.name, f.read_bytes(), "application/octet-stream"))
            for f in resources
        ]
        resp = httpx.post(
            f"{ZEEBE_REST}/v2/deployments",
            files=files,
            auth=AUTH,
            timeout=DEPLOY_TIMEOUT,
        )
        if resp.status_code == 200:
            logger.info("Deployed: %s", [f.name for f in resources])
            return resp.json()
        last_error = f"{resp.status_code} {resp.text}"
        logger.warning("Deploy attempt failed: %s, retrying...", last_error)
        time.sleep(3)
    raise AssertionError(f"Deploy failed after retries: {last_error}")


# ---------------------------------------------------------------------------
# gRPC channel + pyzeebe clients (function-scoped)
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def zeebe_channel(deploy_bpmn):
    """gRPC channel to Zeebe — created per-test on the test's event loop."""
    channel = create_insecure_channel(ZEEBE_GRPC)
    yield channel
    await channel.close()


@pytest_asyncio.fixture
async def zeebe_client(zeebe_channel):
    """pyzeebe ZeebeClient for publishing messages."""
    return ZeebeClient(zeebe_channel)


@pytest_asyncio.fixture
async def mock_worker(zeebe_channel):
    """Create a ZeebeWorker per test."""
    worker = ZeebeWorker(zeebe_channel)
    yield worker


# ---------------------------------------------------------------------------
# REST client (function-scoped, for process instance search only)
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def rest_client(deploy_bpmn):
    """httpx async client for Zeebe REST API (process instance queries)."""
    async with httpx.AsyncClient(base_url=ZEEBE_REST, auth=AUTH, timeout=30) as client:
        yield client


# ---------------------------------------------------------------------------
# Helper: wait for process instance to complete (via REST/ES search)
# ---------------------------------------------------------------------------


async def wait_process_completed(
    client: httpx.AsyncClient,
    process_instance_key: int,
    *,
    timeout: int = PROCESS_COMPLETE_TIMEOUT,
) -> dict | None:
    """Poll until process instance reaches COMPLETED state."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        resp = await client.post(
            "/v2/process-instances/search",
            json={
                "filter": {
                    "processInstanceKey": process_instance_key,
                    "state": "COMPLETED",
                },
            },
        )
        if resp.status_code == 200:
            items = resp.json().get("items", [])
            if items:
                return items[0]
        await asyncio.sleep(1)
    return None


async def find_process_instance(
    client: httpx.AsyncClient,
    *,
    process_id: str = "feature-to-production",
    timeout: int = 60,
) -> int:
    """Wait for a process instance (any state) and return its key."""
    deadline = asyncio.get_event_loop().time() + timeout
    seen_keys: set[int] = set()
    while asyncio.get_event_loop().time() < deadline:
        resp = await client.post(
            "/v2/process-instances/search",
            json={
                "filter": {"processDefinitionId": process_id},
                "sort": [{"field": "startDate", "order": "DESC"}],
                "page": {"limit": 5},
            },
        )
        if resp.status_code == 200:
            items = resp.json().get("items", [])
            for item in items:
                key = item["processInstanceKey"]
                if key not in seen_keys:
                    # Return the newest unseen instance
                    return key
        await asyncio.sleep(1)
    raise TimeoutError(f"No process instance for '{process_id}' within {timeout}s")
