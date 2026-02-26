"""Tests for worker.odoo_client — OdooClient HTTP webhook operations."""

from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest

from worker.config import OdooConfig
from worker.odoo_client import OdooClient


@pytest.fixture
def odoo_config() -> OdooConfig:
    return OdooConfig(
        webhook_url="https://o.tut.ua/web/hook/67f62d7c-2612-444c-baf3-ad409c769bbe",
        project_id=252,
        assignee_id=10,
    )


@pytest.fixture
def odoo_client(odoo_config: OdooConfig) -> OdooClient:
    return OdooClient(odoo_config)


@patch("worker.odoo_client.httpx.post")
def test_create_task(mock_post: MagicMock, odoo_client: OdooClient) -> None:
    mock_post.return_value = MagicMock(
        status_code=200,
        json=MagicMock(return_value={"id": 99}),
        raise_for_status=MagicMock(),
    )
    task_id = odoo_client.create_task(name="Test task")
    assert task_id == 99

    call_kwargs = mock_post.call_args
    assert call_kwargs[0][0] == "https://o.tut.ua/web/hook/67f62d7c-2612-444c-baf3-ad409c769bbe"
    body = call_kwargs[1]["json"]
    assert body["name"] == "Test task"
    assert body["_model"] == "project.project"
    assert body["_id"] == 252
    assert "description" not in body


@patch("worker.odoo_client.httpx.post")
def test_create_task_with_description(mock_post: MagicMock, odoo_client: OdooClient) -> None:
    mock_post.return_value = MagicMock(
        status_code=200,
        json=MagicMock(return_value={"id": 100}),
        raise_for_status=MagicMock(),
    )
    task_id = odoo_client.create_task(name="Task", description="Details here")
    assert task_id == 100
    body = mock_post.call_args[1]["json"]
    assert body["description"] == "Details here"


@patch("worker.odoo_client.httpx.post")
def test_create_task_with_assignee(mock_post: MagicMock, odoo_client: OdooClient) -> None:
    mock_post.return_value = MagicMock(
        status_code=200,
        json=MagicMock(return_value={"id": 101}),
        raise_for_status=MagicMock(),
    )
    task_id = odoo_client.create_task(name="Assigned task")
    assert task_id == 101
    body = mock_post.call_args[1]["json"]
    assert body["x_studio_camunda_user_ids"] == 10


@patch("worker.odoo_client.httpx.post")
def test_create_task_with_process_instance_key(mock_post: MagicMock, odoo_client: OdooClient) -> None:
    mock_post.return_value = MagicMock(
        status_code=200,
        json=MagicMock(return_value={"id": 102}),
        raise_for_status=MagicMock(),
    )
    task_id = odoo_client.create_task(name="Tracked task", process_instance_key=2251799813688185)
    assert task_id == 102
    body = mock_post.call_args[1]["json"]
    assert body["process_instance_key"] == 2251799813688185


@patch("worker.odoo_client.httpx.post")
def test_create_task_without_process_instance_key(mock_post: MagicMock, odoo_client: OdooClient) -> None:
    mock_post.return_value = MagicMock(
        status_code=200,
        json=MagicMock(return_value={"id": 103}),
        raise_for_status=MagicMock(),
    )
    task_id = odoo_client.create_task(name="No key task")
    assert task_id == 103
    body = mock_post.call_args[1]["json"]
    assert "process_instance_key" not in body
