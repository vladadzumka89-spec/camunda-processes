"""Tests for camunda.config — AppConfig.from_env() with mocked env."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from worker.config import AppConfig


@pytest.fixture
def env_vars() -> dict[str, str]:
    return {
        "ZEEBE_ADDRESS": "zeebe-test:26500",
        "ZEEBE_CLIENT_ID": "test-client",
        "ZEEBE_CLIENT_SECRET": "test-secret",
        "ZEEBE_TOKEN_URL": "https://auth.example.com/token",
        "ZEEBE_AUDIENCE": "zeebe-api",
        "GITHUB_TOKEN": "ghp_test_token",
        "DEPLOY_PAT": "ghp_deploy_pat",
        "GITHUB_WEBHOOK_SECRET": "webhook-secret",
        "REPOSITORY": "test-org/test-repo",
        "WEBHOOK_HOST": "127.0.0.1",
        "WEBHOOK_PORT": "8080",
        "ODOO_WEBHOOK_TOKEN": "odoo-tok",
        "ODOO_WEBHOOK_URL": "https://odoo.test/web/hook/test-uuid",
        "ODOO_PROJECT_ID": "5",
        "ODOO_ASSIGNEE_ID": "10",
        "OPENROUTER_API_KEY": "or-key",
        "STAGING_HOST": "staging.test",
        "STAGING_SSH_USER": "stg",
        "STAGING_DB_NAME": "stg_db",
        "STAGING_CONTAINER": "stg_container",
        "STAGING_PORT": "8070",
        "STAGING_SSH_PORT": "2222",
        "PRODUCTION_HOST": "prod.test",
    }


def test_from_env_full(env_vars: dict[str, str]) -> None:
    with patch.dict("os.environ", env_vars, clear=False):
        config = AppConfig.from_env()

    assert config.zeebe.gateway_address == "zeebe-test:26500"
    assert config.zeebe.client_id == "test-client"
    assert config.github.token == "ghp_test_token"
    assert config.github.webhook_secret == "webhook-secret"
    assert config.github.repository == "test-org/test-repo"
    assert config.webhook.host == "127.0.0.1"
    assert config.webhook.port == 8080
    assert config.webhook.odoo_webhook_token == "odoo-tok"
    assert config.odoo.webhook_url == "https://odoo.test/web/hook/test-uuid"
    assert config.odoo.project_id == 5
    assert config.openrouter_api_key == "or-key"

    assert "staging" in config.servers
    stg = config.servers["staging"]
    assert stg.host == "staging.test"
    assert stg.ssh_user == "stg"
    assert stg.db_name == "stg_db"
    assert stg.port == 8070
    assert stg.ssh_port == 2222

    assert "production" in config.servers
    assert config.servers["production"].host == "prod.test"


def test_from_env_defaults() -> None:
    with patch.dict("os.environ", {}, clear=True):
        config = AppConfig.from_env()

    assert config.zeebe.gateway_address == "zeebe:26500"
    assert config.github.repository == "tut-ua/odoo-enterprise"
    assert config.webhook.port == 9001
    assert config.servers == {}


def test_get_server_missing() -> None:
    config = AppConfig()
    with pytest.raises(ValueError, match="not configured"):
        config.get_server("staging")


def test_resolve_server_by_name(app_config: AppConfig) -> None:
    server = app_config.resolve_server("staging")
    assert server.host == "staging.example.com"


def test_resolve_server_by_host(app_config: AppConfig) -> None:
    server = app_config.resolve_server("staging.example.com")
    assert server.ssh_user == "deploy"


def test_resolve_server_missing(app_config: AppConfig) -> None:
    with pytest.raises(ValueError, match="No server config"):
        app_config.resolve_server("unknown-host")
