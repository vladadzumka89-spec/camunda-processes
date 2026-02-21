"""Tests for worker.handlers.notify — Odoo notification handlers."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from worker.config import AppConfig
from worker.handlers.notify import register_notify_handlers


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
    result = await handlers["send-notification"](
        notification_type="staging_ready",
        message_body="All good",
    )
    assert result["odoo_task_id"] == 42
    mock_odoo.create_task.assert_called_once()
    call_kwargs = mock_odoo.create_task.call_args[1]
    assert "Staging готовий" in call_kwargs["name"]


@pytest.mark.asyncio
async def test_send_notification_types(handlers: dict, mock_odoo: MagicMock) -> None:
    await handlers["send-notification"](notification_type="staging_ready")
    name1 = mock_odoo.create_task.call_args[1]["name"]
    assert "[deploy]" in name1

    mock_odoo.create_task.reset_mock()

    await handlers["send-notification"](notification_type="deploy_failed")
    name2 = mock_odoo.create_task.call_args[1]["name"]
    assert "провалився" in name2


# ── create-odoo-task ──────────────────────────────────────


@pytest.mark.asyncio
async def test_create_odoo_task_resolve_conflicts(handlers: dict, mock_odoo: MagicMock) -> None:
    result = await handlers["create-odoo-task"](
        odoo_task_type="resolve_conflicts",
        affected_custom_count=3,
        impact_table="| mod | dep |",
    )
    assert result["odoo_task_id"] == "42"
    call_kwargs = mock_odoo.create_task.call_args[1]
    assert "3" in call_kwargs["name"]
    assert "Impact analysis" in call_kwargs["description"] or "impact" in call_kwargs["description"].lower()


@pytest.mark.asyncio
async def test_create_odoo_task_returns_string_id(handlers: dict, mock_odoo: MagicMock) -> None:
    mock_odoo.create_task.return_value = 123
    result = await handlers["create-odoo-task"](odoo_task_type="review_sync")
    assert result["odoo_task_id"] == "123"
    assert isinstance(result["odoo_task_id"], str)
