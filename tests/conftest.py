"""Shared fixtures for Camunda worker tests."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from worker.config import (
    AppConfig,
    GitHubConfig,
    OdooConfig,
    ServerConfig,
    WebhookConfig,
    ZeebeConfig,
)


@pytest.fixture
def staging_server() -> ServerConfig:
    return ServerConfig(
        host="staging.example.com",
        ssh_user="deploy",
        repo_dir="/opt/odoo-enterprise",
        db_name="odoo19",
        container="odoo19",
    )


@pytest.fixture
def production_server() -> ServerConfig:
    return ServerConfig(
        host="prod.example.com",
        ssh_user="deploy",
        repo_dir="/opt/odoo-enterprise",
        db_name="odoo19prod",
        container="odoo19prod",
    )


@pytest.fixture
def app_config(staging_server: ServerConfig) -> AppConfig:
    return AppConfig(
        zeebe=ZeebeConfig(gateway_address="localhost:26500"),
        github=GitHubConfig(
            token="ghp_test",
            deploy_pat="ghp_deploy",
            webhook_secret="test-secret-123",
            repository="tut-ua/odoo-enterprise",
        ),
        webhook=WebhookConfig(
            host="0.0.0.0",
            port=9001,
            odoo_webhook_token="odoo-token-456",
        ),
        odoo=OdooConfig(
            webhook_url="https://o.tut.ua/web/hook/67f62d7c-2612-444c-baf3-ad409c769bbe",
        ),
        servers={"staging": staging_server},
    )


@pytest.fixture
def app_config_with_production(
    staging_server: ServerConfig,
    production_server: ServerConfig,
) -> AppConfig:
    return AppConfig(
        zeebe=ZeebeConfig(gateway_address="localhost:26500"),
        github=GitHubConfig(
            token="ghp_test",
            deploy_pat="ghp_deploy",
            webhook_secret="test-secret-123",
            repository="tut-ua/odoo-enterprise",
        ),
        webhook=WebhookConfig(
            host="0.0.0.0",
            port=9001,
            odoo_webhook_token="odoo-token-456",
        ),
        odoo=OdooConfig(
            webhook_url="https://o.tut.ua/web/hook/67f62d7c-2612-444c-baf3-ad409c769bbe",
        ),
        servers={
            "staging": staging_server,
            "production": production_server,
        },
    )


@pytest.fixture
def mock_odoo() -> MagicMock:
    odoo = MagicMock()
    odoo.create_task = MagicMock(return_value=42)
    return odoo


@pytest.fixture
def mock_worker() -> MagicMock:
    worker = MagicMock()
    _handlers: dict = {}

    def task_decorator(task_type: str, **kwargs):
        def wrapper(fn):
            _handlers[task_type] = fn
            return fn
        return wrapper

    worker.task = task_decorator
    worker._handlers = _handlers
    return worker


@pytest.fixture
def mock_zeebe_client() -> AsyncMock:
    client = AsyncMock()
    client.publish_message = AsyncMock()
    return client


@pytest.fixture
def mock_ssh() -> AsyncMock:
    ssh = AsyncMock()
    ssh.run = AsyncMock()
    return ssh


@pytest.fixture
def mock_github() -> AsyncMock:
    github = AsyncMock()
    github.get_pr = AsyncMock(return_value={})
    github.comment_pr = AsyncMock()
    github.merge_pr = AsyncMock()
    github.create_pr = AsyncMock(return_value={"html_url": "", "number": 0})
    github.get_bot_review_comment = AsyncMock(return_value=None)
    github.mark_pr_ready = AsyncMock()
    return github
