"""Tests for camunda.webhook — HMAC verification, event routing, message publish."""

from __future__ import annotations

import hashlib
import hmac
import json
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer

from worker.config import AppConfig
from worker.webhook import WebhookServer


def _sign(body: bytes, secret: str) -> str:
    """Compute GitHub HMAC-SHA256 signature."""
    digest = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


@pytest.fixture
def webhook(app_config: AppConfig) -> WebhookServer:
    return WebhookServer(app_config)


@pytest_asyncio.fixture
async def client(webhook: WebhookServer) -> TestClient:
    """Create aiohttp test client for the webhook app."""
    server = TestServer(webhook._app)
    client = TestClient(server)
    await client.start_server()
    yield client
    await client.close()


# ── Health check ──────────────────────────────────────────

@pytest.mark.asyncio
async def test_health(client: TestClient) -> None:
    resp = await client.get("/health")
    assert resp.status == 200
    data = await resp.json()
    assert data["status"] == "ok"


# ── HMAC verification ────────────────────────────────────

@pytest.mark.asyncio
async def test_github_invalid_signature(client: TestClient) -> None:
    body = json.dumps({"action": "opened"}).encode()
    resp = await client.post(
        "/webhook/github",
        data=body,
        headers={
            "X-GitHub-Event": "pull_request",
            "X-Hub-Signature-256": "sha256=invalid",
            "Content-Type": "application/json",
        },
    )
    assert resp.status == 401


@pytest.mark.asyncio
async def test_github_missing_signature(client: TestClient) -> None:
    body = json.dumps({"action": "opened"}).encode()
    resp = await client.post(
        "/webhook/github",
        data=body,
        headers={
            "X-GitHub-Event": "pull_request",
            "Content-Type": "application/json",
        },
    )
    assert resp.status == 401


@pytest.mark.asyncio
async def test_github_valid_signature(client: TestClient, app_config: AppConfig) -> None:
    payload = {
        "action": "opened",
        "pull_request": {
            "number": 42,
            "title": "Test PR",
            "html_url": "https://github.com/tut-ua/odoo-enterprise/pull/42",
            "user": {"login": "dev"},
            "base": {"ref": "staging"},
            "head": {"ref": "feat/test", "sha": "abc123"},
        },
        "repository": {"full_name": "tut-ua/odoo-enterprise"},
    }
    body = json.dumps(payload).encode()
    sig = _sign(body, app_config.github.webhook_secret)

    with patch.object(WebhookServer, "_create_zeebe_client") as mock_factory:
        mock_client = AsyncMock()
        mock_client.publish_message = AsyncMock()
        mock_factory.return_value = mock_client

        resp = await client.post(
            "/webhook/github",
            data=body,
            headers={
                "X-GitHub-Event": "pull_request",
                "X-Hub-Signature-256": sig,
                "Content-Type": "application/json",
            },
        )
        assert resp.status == 200
        data = await resp.json()
        assert data["message"] == "msg_pr_event"
        assert data["pr_number"] == 42

        mock_client.publish_message.assert_awaited_once()
        call_kwargs = mock_client.publish_message.call_args[1]
        assert call_kwargs["name"] == "msg_pr_event"
        assert call_kwargs["correlation_key"] == "feat/test"
        assert call_kwargs["variables"]["pr_number"] == 42


# ── Event routing ─────────────────────────────────────────

@pytest.mark.asyncio
async def test_ignores_non_staging_pr(client: TestClient, app_config: AppConfig) -> None:
    payload = {
        "action": "opened",
        "pull_request": {
            "number": 10,
            "title": "Fix",
            "html_url": "",
            "user": {"login": "dev"},
            "base": {"ref": "main"},
            "head": {"ref": "fix/bug"},
        },
        "repository": {"full_name": "tut-ua/odoo-enterprise"},
    }
    body = json.dumps(payload).encode()
    sig = _sign(body, app_config.github.webhook_secret)

    resp = await client.post(
        "/webhook/github",
        data=body,
        headers={
            "X-GitHub-Event": "pull_request",
            "X-Hub-Signature-256": sig,
            "Content-Type": "application/json",
        },
    )
    assert resp.status == 200
    data = await resp.json()
    assert data["status"] == "ignored"


@pytest.mark.asyncio
async def test_synchronize_publishes_pr_updated(
    client: TestClient, app_config: AppConfig,
) -> None:
    payload = {
        "action": "synchronize",
        "pull_request": {
            "number": 42,
            "title": "Test PR",
            "html_url": "",
            "user": {"login": "dev"},
            "base": {"ref": "staging"},
            "head": {"ref": "feat/test", "sha": "def456"},
        },
        "repository": {"full_name": "tut-ua/odoo-enterprise"},
    }
    body = json.dumps(payload).encode()
    sig = _sign(body, app_config.github.webhook_secret)

    with patch.object(WebhookServer, "_create_zeebe_client") as mock_factory:
        mock_client = AsyncMock()
        mock_client.publish_message = AsyncMock()
        mock_factory.return_value = mock_client

        resp = await client.post(
            "/webhook/github",
            data=body,
            headers={
                "X-GitHub-Event": "pull_request",
                "X-Hub-Signature-256": sig,
                "Content-Type": "application/json",
            },
        )
        assert resp.status == 200
        data = await resp.json()
        assert data["message"] == "msg_pr_updated"

        call_kwargs = mock_client.publish_message.call_args[1]
        assert call_kwargs["name"] == "msg_pr_updated"
        assert call_kwargs["correlation_key"] == "42"


@pytest.mark.asyncio
async def test_non_pr_event_ignored(client: TestClient, app_config: AppConfig) -> None:
    payload = {"action": "created", "comment": {"body": "test"}}
    body = json.dumps(payload).encode()
    sig = _sign(body, app_config.github.webhook_secret)

    resp = await client.post(
        "/webhook/github",
        data=body,
        headers={
            "X-GitHub-Event": "issue_comment",
            "X-Hub-Signature-256": sig,
            "Content-Type": "application/json",
        },
    )
    assert resp.status == 200
    data = await resp.json()
    assert data["status"] == "ignored"


# ── Odoo webhook ──────────────────────────────────────────

@pytest.mark.asyncio
async def test_odoo_valid_token(client: TestClient) -> None:
    with patch.object(WebhookServer, "_create_zeebe_client") as mock_factory:
        mock_client = AsyncMock()
        mock_client.publish_message = AsyncMock()
        mock_factory.return_value = mock_client

        resp = await client.post(
            "/webhook/odoo",
            json={"task_id": 123},
            headers={"Authorization": "Bearer odoo-token-456"},
        )
        assert resp.status == 200
        data = await resp.json()
        assert data["message"] == "msg_odoo_task_done"
        assert data["correlation_key"] == "123"

        call_kwargs = mock_client.publish_message.call_args[1]
        assert call_kwargs["name"] == "msg_odoo_task_done"
        assert call_kwargs["correlation_key"] == "123"


@pytest.mark.asyncio
async def test_odoo_process_instance_key_fallback(client: TestClient) -> None:
    """When task_id is missing, process_instance_key is used for correlation."""
    with patch.object(WebhookServer, "_create_zeebe_client") as mock_factory:
        mock_client = AsyncMock()
        mock_client.publish_message = AsyncMock()
        mock_factory.return_value = mock_client

        resp = await client.post(
            "/webhook/odoo",
            json={"process_instance_key": "2251799813793035"},
            headers={"Authorization": "Bearer odoo-token-456"},
        )
        assert resp.status == 200
        data = await resp.json()
        assert data["correlation_key"] == "2251799813793035"

        call_kwargs = mock_client.publish_message.call_args[1]
        assert call_kwargs["correlation_key"] == "2251799813793035"


@pytest.mark.asyncio
async def test_odoo_invalid_token(client: TestClient) -> None:
    resp = await client.post(
        "/webhook/odoo",
        json={"task_id": 123},
        headers={"Authorization": "Bearer wrong-token"},
    )
    assert resp.status == 401


@pytest.mark.asyncio
async def test_odoo_missing_task_id_and_pik(client: TestClient) -> None:
    resp = await client.post(
        "/webhook/odoo",
        json={},
        headers={"Authorization": "Bearer odoo-token-456"},
    )
    assert resp.status == 400


# ── Verify signature helper ──────────────────────────────

def test_verify_github_signature_valid() -> None:
    body = b'{"test": true}'
    secret = "my-secret"
    sig = _sign(body, secret)
    assert WebhookServer._verify_github_signature(body, secret, sig) is True


def test_verify_github_signature_invalid() -> None:
    body = b'{"test": true}'
    assert WebhookServer._verify_github_signature(body, "my-secret", "sha256=bad") is False


def test_verify_github_signature_no_prefix() -> None:
    assert WebhookServer._verify_github_signature(b"test", "secret", "nope") is False


# ── Production variable injection ─────────────────────────


@pytest.mark.asyncio
async def test_pr_event_includes_production_vars(
    app_config_with_production: AppConfig,
) -> None:
    """When production server is configured, variables include production_* keys."""
    webhook = WebhookServer(app_config_with_production)
    server = TestServer(webhook._app)
    client = TestClient(server)
    await client.start_server()

    try:
        payload = {
            "action": "opened",
            "pull_request": {
                "number": 42,
                "title": "Test PR",
                "html_url": "https://github.com/tut-ua/odoo-enterprise/pull/42",
                "user": {"login": "dev"},
                "base": {"ref": "staging"},
                "head": {"ref": "feat/test", "sha": "abc123"},
            },
            "repository": {"full_name": "tut-ua/odoo-enterprise"},
        }
        body = json.dumps(payload).encode()
        sig = _sign(body, app_config_with_production.github.webhook_secret)

        with patch.object(WebhookServer, "_create_zeebe_client") as mock_factory:
            mock_client = AsyncMock()
            mock_client.publish_message = AsyncMock()
            mock_factory.return_value = mock_client

            resp = await client.post(
                "/webhook/github",
                data=body,
                headers={
                    "X-GitHub-Event": "pull_request",
                    "X-Hub-Signature-256": sig,
                    "Content-Type": "application/json",
                },
            )
            assert resp.status == 200

            variables = mock_client.publish_message.call_args[1]["variables"]
            assert "production_host" in variables
            assert variables["production_host"] == "prod.example.com"
            assert "production_ssh_user" in variables
            assert "production_repo_dir" in variables
            assert "production_db" in variables
            assert "production_container" in variables
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_pr_event_without_production(
    client: TestClient, app_config: AppConfig,
) -> None:
    """Without production config, variables should not contain production_* keys."""
    payload = {
        "action": "opened",
        "pull_request": {
            "number": 42,
            "title": "Test PR",
            "html_url": "",
            "user": {"login": "dev"},
            "base": {"ref": "staging"},
            "head": {"ref": "feat/test", "sha": "abc123"},
        },
        "repository": {"full_name": "tut-ua/odoo-enterprise"},
    }
    body = json.dumps(payload).encode()
    sig = _sign(body, app_config.github.webhook_secret)

    with patch.object(WebhookServer, "_create_zeebe_client") as mock_factory:
        mock_client = AsyncMock()
        mock_client.publish_message = AsyncMock()
        mock_factory.return_value = mock_client

        resp = await client.post(
            "/webhook/github",
            data=body,
            headers={
                "X-GitHub-Event": "pull_request",
                "X-Hub-Signature-256": sig,
                "Content-Type": "application/json",
            },
        )
        assert resp.status == 200
        variables = mock_client.publish_message.call_args[1]["variables"]
        assert "production_host" not in variables


@pytest.mark.asyncio
async def test_pr_event_includes_staging_vars(
    client: TestClient, app_config: AppConfig,
) -> None:
    """Staging server config should be injected into variables."""
    payload = {
        "action": "opened",
        "pull_request": {
            "number": 42,
            "title": "Test PR",
            "html_url": "",
            "user": {"login": "dev"},
            "base": {"ref": "staging"},
            "head": {"ref": "feat/test", "sha": "abc123"},
        },
        "repository": {"full_name": "tut-ua/odoo-enterprise"},
    }
    body = json.dumps(payload).encode()
    sig = _sign(body, app_config.github.webhook_secret)

    with patch.object(WebhookServer, "_create_zeebe_client") as mock_factory:
        mock_client = AsyncMock()
        mock_client.publish_message = AsyncMock()
        mock_factory.return_value = mock_client

        resp = await client.post(
            "/webhook/github",
            data=body,
            headers={
                "X-GitHub-Event": "pull_request",
                "X-Hub-Signature-256": sig,
                "Content-Type": "application/json",
            },
        )
        assert resp.status == 200
        variables = mock_client.publish_message.call_args[1]["variables"]
        assert variables["staging_host"] == "staging.example.com"
        assert variables["staging_ssh_user"] == "deploy"
        assert variables["staging_repo_dir"] == "/opt/odoo-enterprise"
