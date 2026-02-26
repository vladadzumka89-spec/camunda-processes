"""Tests for worker.handlers.notify — Odoo notification handlers."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from worker.config import AppConfig
from worker.handlers.notify import register_notify_handlers


def _make_mock_job() -> MagicMock:
    """Create a mock pyzeebe Job with required attributes."""
    job = MagicMock()
    job.process_instance_key = 2251799813793035
    job.element_instance_key = 2251799813793040
    job.bpmn_process_id = "upstream-sync"
    return job


def _extract_handlers(config: AppConfig, odoo: MagicMock) -> dict:
    handlers = {}

    def task_decorator(task_type: str, **kwargs):
        def wrapper(fn):
            handlers[task_type] = fn
            return fn
        return wrapper

    worker = MagicMock()
    worker.task = task_decorator
    register_notify_handlers(worker, config, odoo)
    return handlers


@pytest.fixture
def handlers(app_config: AppConfig, mock_odoo: MagicMock) -> dict:
    return _extract_handlers(app_config, mock_odoo)


# ── send-notification ─────────────────────────────────────


@pytest.mark.asyncio
async def test_send_notification(handlers: dict, mock_odoo: MagicMock) -> None:
    job = _make_mock_job()
    result = await handlers["send-notification"](
        job=job,
        notification_type="staging_ready",
        message_body="All good",
    )
    assert result["odoo_task_id"] == 42
    mock_odoo.create_task.assert_called_once()
    call_kwargs = mock_odoo.create_task.call_args[1]
    assert "Staging готовий" in call_kwargs["name"]


@pytest.mark.asyncio
async def test_send_notification_types(handlers: dict, mock_odoo: MagicMock) -> None:
    job = _make_mock_job()
    await handlers["send-notification"](job=job, notification_type="staging_ready")
    name1 = mock_odoo.create_task.call_args[1]["name"]
    assert "[deploy]" in name1

    mock_odoo.create_task.reset_mock()

    await handlers["send-notification"](job=job, notification_type="deploy_failed")
    name2 = mock_odoo.create_task.call_args[1]["name"]
    assert "провалився" in name2


# ── create-odoo-task ──────────────────────────────────────


@pytest.mark.asyncio
async def test_create_odoo_task_resolve_conflicts(handlers: dict, mock_odoo: MagicMock) -> None:
    job = _make_mock_job()
    result = await handlers["create-odoo-task"](
        job=job,
        odoo_task_type="resolve_conflicts",
        affected_custom_count=3,
        impact_table="| mod | dep |",
    )
    assert result["odoo_task_id"] == "42"
    call_kwargs = mock_odoo.create_task.call_args[1]
    assert "3" in call_kwargs["name"]
    assert "impact" in call_kwargs["description"].lower() or "custom" in call_kwargs["description"].lower()


@pytest.mark.asyncio
async def test_create_odoo_task_returns_string_id(handlers: dict, mock_odoo: MagicMock) -> None:
    job = _make_mock_job()
    mock_odoo.create_task.return_value = 123
    result = await handlers["create-odoo-task"](job=job, odoo_task_type="review_sync")
    assert result["odoo_task_id"] == "123"
    assert isinstance(result["odoo_task_id"], str)


@pytest.mark.asyncio
async def test_create_odoo_task_zero_id_uses_pik(handlers: dict, mock_odoo: MagicMock) -> None:
    """When Odoo returns task_id=0, correlation falls back to process_instance_key."""
    job = _make_mock_job()
    mock_odoo.create_task.return_value = 0
    result = await handlers["create-odoo-task"](job=job, odoo_task_type="resolve_conflicts")
    assert result["odoo_task_id"] == str(job.process_instance_key)
