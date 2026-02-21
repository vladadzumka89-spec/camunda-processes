"""
Integration tests for the feature-to-production BPMN pipeline.

Runs against a real Zeebe instance (docker-compose).
All service task handlers AND user tasks are handled via the gRPC job worker.
User tasks in this BPMN are job-based (type io.camunda.zeebe:userTask).
"""

from __future__ import annotations

import asyncio
import logging

import pytest
from pyzeebe import ZeebeClient, ZeebeWorker

from .conftest import find_process_instance  # noqa: F401 (available for debugging)

logger = logging.getLogger(__name__)

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.timeout(180),
]

# ---------------------------------------------------------------------------
# Shared process variables
# ---------------------------------------------------------------------------

BASE_VARIABLES = {
    "pr_number": 42,
    "pr_url": "https://github.com/test/pr/42",
    "repository": "tut-ua/odoo-enterprise",
    "base_branch": "staging",
    "head_branch": "feat/test",
    "pr_title": "Test Feature",
    "pr_author": "dev",
    # Staging server vars
    "staging_host": "staging.test",
    "staging_ssh_user": "deploy",
    "staging_repo_dir": "/opt/odoo",
    "staging_db": "odoo19",
    "staging_container": "odoo19",
    # Production server vars
    "production_host": "production.test",
    "production_ssh_user": "deploy",
    "production_repo_dir": "/opt/odoo",
    "production_db": "odoo19prod",
    "production_container": "odoo19prod",
}


# ---------------------------------------------------------------------------
# Default mock responses for service task types
# ---------------------------------------------------------------------------

SERVICE_TASK_RESPONSES: dict[str, dict] = {
    "pr-agent-review": {"review_score": 8, "has_critical_issues": False},
    "github-merge": {},
    "github-comment": {},
    "github-create-pr": {"pr_url": "https://github.com/test/pr/99", "pr_number": 99},
    "send-notification": {"odoo_task_id": 1},
    "clickbot-test": {"clickbot_passed": True, "clickbot_report": "All OK"},
    "rollback": {},
    # deploy-process handlers:
    "git-pull": {"old_commit": "aaa111", "new_commit": "bbb222", "has_changes": True},
    "detect-modules": {"changed_modules": "sale_management", "docker_build_needed": False},
    "docker-build": {},
    "docker-up": {},
    "module-update": {"modules_updated": "sale_management"},
    "cache-clear": {},
    "smoke-test": {"smoke_passed": True},
    "http-verify": {},
    "save-deploy-state": {},
}

# Job type for BPMN user tasks without <zeebe:userTask /> element
USER_TASK_JOB_TYPE = "io.camunda.zeebe:userTask"


# ---------------------------------------------------------------------------
# Register mock handlers on a ZeebeWorker
# ---------------------------------------------------------------------------


def register_handlers(
    worker: ZeebeWorker,
    service_overrides: dict[str, list[dict]] | None = None,
    user_task_responses: dict[str, list[dict]] | None = None,
) -> dict[str, int]:
    """
    Register mock handlers for all service tasks AND user tasks.

    *service_overrides*: task_type → list of response dicts (sequential).
    *user_task_responses*: element_id → list of response dicts (sequential).

    Returns a shared call_count dict (keys = task_type or "ut:<element_id>").
    """
    call_counts: dict[str, int] = {}
    service_overrides = service_overrides or {}
    user_task_responses = user_task_responses or {}

    # Register service task handlers
    for task_type, default_response in SERVICE_TASK_RESPONSES.items():
        _register_service_task(
            worker, task_type, default_response,
            service_overrides.get(task_type), call_counts,
        )

    # Register user task handler (routes by element_id)
    _register_user_task_handler(worker, user_task_responses, call_counts)

    return call_counts


def _register_service_task(
    worker: ZeebeWorker,
    task_type: str,
    default_response: dict,
    response_sequence: list[dict] | None,
    call_counts: dict[str, int],
) -> None:
    @worker.task(task_type=task_type)
    async def handler(**kwargs) -> dict:
        call_counts[task_type] = call_counts.get(task_type, 0) + 1
        idx = call_counts[task_type] - 1
        if response_sequence and idx < len(response_sequence):
            resp = response_sequence[idx]
        elif response_sequence:
            resp = response_sequence[-1]
        else:
            resp = default_response
        logger.info("Mock %s (#%d) → %s", task_type, call_counts[task_type], resp)
        return resp


def _register_user_task_handler(
    worker: ZeebeWorker,
    responses_by_element: dict[str, list[dict]],
    call_counts: dict[str, int],
) -> None:
    """Register a single handler for all user tasks, routing by element_id.

    Uses a `before` decorator to capture the element_id from the Job object
    (pyzeebe handlers only receive variables as kwargs, not the raw Job).
    """
    # Shared state to pass element_id from the before hook to the handler
    _current_element: dict[str, str] = {}

    async def capture_element_id(job):
        _current_element["id"] = job.element_id
        return job

    @worker.task(task_type=USER_TASK_JOB_TYPE, before=[capture_element_id])
    async def user_task_handler(**kwargs) -> dict:
        element_id = _current_element.get("id", "unknown")
        count_key = f"ut:{element_id}"
        call_counts[count_key] = call_counts.get(count_key, 0) + 1
        idx = call_counts[count_key] - 1

        responses = responses_by_element.get(element_id, [{}])
        if idx < len(responses):
            resp = responses[idx]
        else:
            resp = responses[-1]

        logger.info("UserTask %s (#%d) → %s", element_id, call_counts[count_key], resp)
        return resp


# ---------------------------------------------------------------------------
# Helper: start pipeline
# ---------------------------------------------------------------------------


async def start_pipeline(
    zeebe_client: ZeebeClient,
    worker: ZeebeWorker,
    rest_client,
    variables: dict | None = None,
    service_overrides: dict[str, list[dict]] | None = None,
    user_task_responses: dict[str, list[dict]] | None = None,
) -> tuple[dict[str, int], asyncio.Task]:
    """
    Register handlers, publish msg_pr_event, start worker.
    Returns (call_counts, worker_task).
    """
    call_counts = register_handlers(worker, service_overrides, user_task_responses)

    worker_task = asyncio.create_task(worker.work())

    await zeebe_client.publish_message(
        name="msg_pr_event",
        correlation_key="",
        variables=variables or BASE_VARIABLES,
    )

    return call_counts, worker_task


async def wait_for_handler(
    call_counts: dict[str, int],
    key: str,
    min_calls: int = 1,
    timeout: int = 60,
) -> None:
    """Wait until a specific handler has been called at least min_calls times."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if call_counts.get(key, 0) >= min_calls:
            return
        await asyncio.sleep(0.5)
    raise TimeoutError(
        f"Handler '{key}' not called {min_calls} time(s) within {timeout}s "
        f"(actual: {call_counts.get(key, 0)})"
    )


async def stop_worker(worker_task: asyncio.Task) -> None:
    worker_task.cancel()
    try:
        await worker_task
    except (asyncio.CancelledError, BaseExceptionGroup):
        pass


# ---------------------------------------------------------------------------
# Test 1: Happy path
# ---------------------------------------------------------------------------


class TestHappyPath:
    """PR → review(8) → merge → deploy staging → clickbot → notify →
    verify staging(OK) → create PR main → merge main → deploy prod →
    verify prod(OK) → END SUCCESS"""

    async def test_happy_path(self, zeebe_client, mock_worker, rest_client):
        call_counts, worker_task = await start_pipeline(
            zeebe_client, mock_worker, rest_client,
            user_task_responses={
                "user_verify_staging": [{"staging_approved": True}],
                "user_merge_main": [{"merge_confirmed": True}],
                "user_verify_prod": [{"prod_approved": True}],
            },
        )

        try:
            # Wait for the last user task to complete (signals process end)
            await wait_for_handler(call_counts, "ut:user_verify_prod")

            # Verify all key handlers were called
            assert call_counts.get("pr-agent-review", 0) >= 1
            assert call_counts.get("github-merge", 0) >= 1
            assert call_counts.get("git-pull", 0) >= 2  # staging + production
            assert call_counts.get("clickbot-test", 0) >= 1
            assert call_counts.get("github-create-pr", 0) >= 1
            assert call_counts.get("ut:user_verify_staging", 0) >= 1
            assert call_counts.get("ut:user_merge_main", 0) >= 1
            assert call_counts.get("ut:user_verify_prod", 0) >= 1
        finally:
            await stop_worker(worker_task)


# ---------------------------------------------------------------------------
# Test 2: Score below threshold
# ---------------------------------------------------------------------------


class TestScoreBelowThreshold:
    """PR → review(4) → comment → wait msg_pr_updated → re-review(8) →
    merge → ... → END SUCCESS"""

    async def test_score_below_threshold(self, zeebe_client, mock_worker, rest_client):
        call_counts, worker_task = await start_pipeline(
            zeebe_client, mock_worker, rest_client,
            variables={**BASE_VARIABLES, "pr_number": 43},
            service_overrides={
                "pr-agent-review": [
                    {"review_score": 4, "has_critical_issues": False},
                    {"review_score": 8, "has_critical_issues": False},
                ],
            },
            user_task_responses={
                "user_verify_staging": [{"staging_approved": True}],
                "user_merge_main": [{"merge_confirmed": True}],
                "user_verify_prod": [{"prod_approved": True}],
            },
        )

        try:
            # Wait for github-comment (score < 7 path)
            await wait_for_handler(call_counts, "github-comment", timeout=30)

            # Let event-based gateway activate
            await asyncio.sleep(3)

            # Publish msg_pr_updated → triggers re-review
            await zeebe_client.publish_message(
                name="msg_pr_updated",
                correlation_key="43",
                variables={"pr_number": 43},
            )

            # Wait for process to complete through the full pipeline
            await wait_for_handler(call_counts, "ut:user_verify_prod")
            assert call_counts["pr-agent-review"] >= 2
        finally:
            await stop_worker(worker_task)


# ---------------------------------------------------------------------------
# Test 3: Staging rejected → rework → re-review → happy path
# ---------------------------------------------------------------------------


class TestStagingRejectedRework:
    """PR → review(8) → merge → deploy staging → clickbot → notify →
    verify staging(REJECT) → comment rework → msg_pr_updated →
    re-review(8) → merge → deploy staging → ... → verify prod(OK) → END"""

    async def test_staging_rejected_rework(self, zeebe_client, mock_worker, rest_client):
        call_counts, worker_task = await start_pipeline(
            zeebe_client, mock_worker, rest_client,
            variables={**BASE_VARIABLES, "pr_number": 44},
            user_task_responses={
                "user_verify_staging": [
                    {"staging_approved": False, "rejection_reason": "Broken layout"},
                    {"staging_approved": True},
                ],
                "user_merge_main": [{"merge_confirmed": True}],
                "user_verify_prod": [{"prod_approved": True}],
            },
        )

        try:
            # Wait for rework comment (staging rejected path)
            await wait_for_handler(call_counts, "github-comment", timeout=30)

            # Let event-based gateway activate
            await asyncio.sleep(3)

            # Publish msg_pr_updated — developer pushed fixes
            await zeebe_client.publish_message(
                name="msg_pr_updated",
                correlation_key="44",
                variables={"pr_number": 44},
            )

            # Wait for full pipeline completion
            await wait_for_handler(call_counts, "ut:user_verify_prod")
            assert call_counts["pr-agent-review"] >= 2
            assert call_counts["git-pull"] >= 3  # 2x staging + 1x prod
        finally:
            await stop_worker(worker_task)


# ---------------------------------------------------------------------------
# Test 4: Production rollback
# ---------------------------------------------------------------------------


class TestProductionRollback:
    """... → verify prod(REJECT) → rollback → notify rollback → END ROLLBACK"""

    async def test_production_rollback(self, zeebe_client, mock_worker, rest_client):
        call_counts, worker_task = await start_pipeline(
            zeebe_client, mock_worker, rest_client,
            variables={**BASE_VARIABLES, "pr_number": 45},
            user_task_responses={
                "user_verify_staging": [{"staging_approved": True}],
                "user_merge_main": [{"merge_confirmed": True}],
                "user_verify_prod": [
                    {"prod_approved": False, "rollback_reason": "Performance regression"},
                ],
            },
        )

        try:
            # Wait for rollback handler + notification (rollback path)
            await wait_for_handler(call_counts, "rollback", min_calls=1)
            await wait_for_handler(call_counts, "send-notification", min_calls=2)
            assert call_counts.get("rollback", 0) >= 1
            assert call_counts.get("ut:user_verify_prod", 0) >= 1
        finally:
            await stop_worker(worker_task)
