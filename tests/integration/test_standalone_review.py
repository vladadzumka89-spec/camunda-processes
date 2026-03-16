"""
Integration tests for standalone pr-review process (v2.0).

Tests that pr-review starts from msg_pr_event, runs PR-Agent review,
and publishes msg_review_done when score >= 7.
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


# Service task responses for pr-review process
PR_REVIEW_TASKS = {
    "pr-agent-review": {"review_score": 8, "has_critical_issues": False},
    "github-comment": {},
    "publish-message": {"message_published": True},
}


def register_review_handlers(
    worker: ZeebeWorker,
    service_overrides: dict[str, list[dict]] | None = None,
) -> dict[str, int]:
    """Register mock handlers for pr-review service tasks."""
    call_counts: dict[str, int] = {}
    overrides = service_overrides or {}

    for task_type, default_response in PR_REVIEW_TASKS.items():
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


class TestPrReviewStandalone:
    """pr-review starts from msg_pr_event, reviews, publishes msg_review_done."""

    async def test_review_happy_path(self, zeebe_client, mock_worker, rest_client):
        """Score >= 7 -> publish msg_review_done -> end."""
        call_counts = register_review_handlers(mock_worker)
        worker_task = asyncio.create_task(mock_worker.work())

        try:
            # Trigger pr-review via message (simulates GitHub webhook)
            await zeebe_client.publish_message(
                name="msg_pr_event",
                correlation_key="feat/review-test",
                variables={
                    "pr_number": 100,
                    "pr_url": "https://github.com/test/pr/100",
                    "repository": "tut-ua/odoo-enterprise",
                    "head_branch": "feat/review-test",
                },
            )

            # Wait for publish-message handler (last step before end)
            await wait_handler(call_counts, "publish-message", timeout=30)

            # Verify flow
            assert call_counts["pr-agent-review"] == 1
            assert call_counts["publish-message"] == 1
            # github-comment should NOT be called (score >= 7)
            assert call_counts.get("github-comment", 0) == 0
        finally:
            worker_task.cancel()
            try:
                await worker_task
            except (asyncio.CancelledError, BaseExceptionGroup):
                pass

    async def test_review_low_score_then_rework(self, zeebe_client, mock_worker, rest_client):
        """Score < 7 -> comment -> wait for update -> re-review(8) -> publish."""
        call_counts = register_review_handlers(
            mock_worker,
            service_overrides={
                "pr-agent-review": [
                    {"review_score": 4, "has_critical_issues": False},  # 1st: low
                    {"review_score": 9, "has_critical_issues": False},  # 2nd: pass
                ],
            },
        )
        worker_task = asyncio.create_task(mock_worker.work())

        try:
            await zeebe_client.publish_message(
                name="msg_pr_event",
                correlation_key="feat/rework-test",
                variables={
                    "pr_number": 101,
                    "pr_url": "https://github.com/test/pr/101",
                    "repository": "tut-ua/odoo-enterprise",
                    "head_branch": "feat/rework-test",
                },
            )

            # Wait for github-comment (score < 7)
            await wait_handler(call_counts, "github-comment", timeout=30)
            assert call_counts["pr-agent-review"] == 1

            # Simulate developer push (PR synchronize event)
            await asyncio.sleep(2)
            await zeebe_client.publish_message(
                name="msg_pr_updated",
                correlation_key="101",
                variables={"pr_updated": True, "head_sha": "newsha"},
            )

            # Wait for publish-message (re-review passed)
            await wait_handler(call_counts, "publish-message", timeout=30)
            assert call_counts["pr-agent-review"] == 2
            assert call_counts["github-comment"] >= 1
            assert call_counts["publish-message"] == 1
        finally:
            worker_task.cancel()
            try:
                await worker_task
            except (asyncio.CancelledError, BaseExceptionGroup):
                pass
