"""
Integration tests for standalone deploy-process (v2.0).

Tests that deploy-process starts from msg_deploy_trigger,
runs full deploy pipeline, and publishes msg_deploy_done.
"""

from __future__ import annotations

import asyncio
import logging

import pytest
from pyzeebe import ZeebeClient, ZeebeWorker

logger = logging.getLogger(__name__)

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.timeout(120),
]

# Default deploy variables (simulates webhook payload)
DEPLOY_VARIABLES = {
    "trigger_sha": "abc123def456",
    "server_host": "staging",
    "ssh_user": "deploy",
    "repo_dir": "/opt/odoo-enterprise",
    "db_name": "odoo19",
    "container": "odoo19",
    "branch": "staging",
    "run_smoke_test": True,
    "test_mode": "full",
    "odoo_project_id": 252,
}

# Service task responses for deploy-process
DEPLOY_TASKS = {
    "git-pull": {"old_commit": "aaa111", "new_commit": "bbb222", "has_changes": True},
    "detect-modules": {"changed_modules": "sale_management", "docker_build_needed": False},
    "docker-build": {},
    "docker-up": {},
    "module-update": {"modules_updated": "sale_management"},
    "cache-clear": {},
    "smoke-test": {"smoke_passed": True},
    "http-verify": {},
    "clickbot-test": {"clickbot_passed": True, "clickbot_report": "All OK"},
    "save-deploy-state": {},
    "rollback": {},
    "publish-message": {"message_published": True},
    "http-request-smart": {},  # for clickbot report -> Odoo
}


def register_deploy_handlers(
    worker: ZeebeWorker,
    service_overrides: dict[str, list[dict]] | None = None,
) -> dict[str, int]:
    """Register mock handlers for deploy-process service tasks."""
    call_counts: dict[str, int] = {}
    overrides = service_overrides or {}

    for task_type, default_response in DEPLOY_TASKS.items():
        _seq = overrides.get(task_type)

        @worker.task(task_type=task_type)
        async def handler(
            _tt=task_type, _default=default_response, _seq=_seq, **kwargs,
        ) -> dict:
            call_counts[_tt] = call_counts.get(_tt, 0) + 1
            idx = call_counts[_tt] - 1
            if _seq and idx < len(_seq):
                resp = _seq[idx]
            elif _seq:
                resp = _seq[-1]
            else:
                resp = _default
            logger.info("Mock %s (#%d) -> %s", _tt, call_counts[_tt], resp)
            return resp

    return call_counts


async def wait_handler(counts, key, min_calls=1, timeout=60):
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if counts.get(key, 0) >= min_calls:
            return
        await asyncio.sleep(0.5)
    raise TimeoutError(f"Handler '{key}' not called {min_calls}x within {timeout}s (got {counts.get(key, 0)})")


class TestDeployStandalone:
    """deploy-process starts from msg_deploy_trigger, deploys, publishes msg_deploy_done."""

    async def test_deploy_happy_path(self, zeebe_client, mock_worker, rest_client):
        """Full deploy: git-pull -> detect -> build -> update -> smoke -> clickbot -> publish."""
        call_counts = register_deploy_handlers(mock_worker)
        worker_task = asyncio.create_task(mock_worker.work())

        try:
            # Trigger deploy via message (simulates push to staging)
            await zeebe_client.publish_message(
                name="msg_deploy_trigger",
                correlation_key="abc123def456",
                variables=DEPLOY_VARIABLES,
            )

            # Wait for publish-message (last step before end)
            await wait_handler(call_counts, "publish-message", timeout=60)

            # Verify full deploy flow executed
            assert call_counts["git-pull"] == 1
            assert call_counts["detect-modules"] == 1
            assert call_counts["module-update"] == 1
            assert call_counts["smoke-test"] == 1
            assert call_counts["http-verify"] == 1
            assert call_counts["clickbot-test"] == 1
            assert call_counts["save-deploy-state"] == 1
            assert call_counts["publish-message"] >= 1
            # docker-build should NOT be called (docker_build_needed=False)
            assert call_counts.get("docker-build", 0) == 0
        finally:
            worker_task.cancel()
            try:
                await worker_task
            except (asyncio.CancelledError, BaseExceptionGroup):
                pass

    async def test_deploy_no_changes(self, zeebe_client, mock_worker, rest_client):
        """No changes -> publish msg_deploy_done with no_changes=true -> end early."""
        call_counts = register_deploy_handlers(
            mock_worker,
            service_overrides={
                "git-pull": [{"old_commit": "aaa", "new_commit": "aaa", "has_changes": False}],
            },
        )
        worker_task = asyncio.create_task(mock_worker.work())

        try:
            await zeebe_client.publish_message(
                name="msg_deploy_trigger",
                correlation_key="nochanges123",
                variables={**DEPLOY_VARIABLES, "trigger_sha": "nochanges123"},
            )

            # Wait for publish-message (no-changes path also publishes)
            await wait_handler(call_counts, "publish-message", timeout=30)

            # Only git-pull and publish should be called
            assert call_counts["git-pull"] == 1
            assert call_counts["publish-message"] == 1
            assert call_counts.get("detect-modules", 0) == 0
            assert call_counts.get("module-update", 0) == 0
        finally:
            worker_task.cancel()
            try:
                await worker_task
            except (asyncio.CancelledError, BaseExceptionGroup):
                pass

    async def test_deploy_with_docker_build(self, zeebe_client, mock_worker, rest_client):
        """Docker build needed -> build runs before module update."""
        call_counts = register_deploy_handlers(
            mock_worker,
            service_overrides={
                "detect-modules": [{"changed_modules": "base", "docker_build_needed": True}],
            },
        )
        worker_task = asyncio.create_task(mock_worker.work())

        try:
            await zeebe_client.publish_message(
                name="msg_deploy_trigger",
                correlation_key="dockerbuild123",
                variables={**DEPLOY_VARIABLES, "trigger_sha": "dockerbuild123"},
            )

            await wait_handler(call_counts, "publish-message", timeout=60)

            assert call_counts["docker-build"] == 1
            assert call_counts["module-update"] == 1
            assert call_counts["publish-message"] >= 1
        finally:
            worker_task.cancel()
            try:
                await worker_task
            except (asyncio.CancelledError, BaseExceptionGroup):
                pass
