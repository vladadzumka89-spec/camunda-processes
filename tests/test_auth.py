"""Tests for worker.auth — ZeebeAuthConfig, TokenManager, create_channel."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from worker.auth import TokenManager, ZeebeAuthConfig, create_channel


# ── ZeebeAuthConfig ───────────────────────────────────────


def test_auth_config_use_oauth_true() -> None:
    cfg = ZeebeAuthConfig(
        client_id="id",
        client_secret="secret",
        token_url="https://auth.example.com/token",
    )
    assert cfg.use_oauth is True


def test_auth_config_use_oauth_false() -> None:
    cfg = ZeebeAuthConfig()
    assert cfg.use_oauth is False


def test_auth_config_use_oauth_false_partial() -> None:
    cfg = ZeebeAuthConfig(client_id="id")
    assert cfg.use_oauth is False


# ── create_channel ────────────────────────────────────────


def test_create_channel_insecure() -> None:
    cfg = ZeebeAuthConfig(gateway_address="localhost:26500")
    with patch("grpc.aio.insecure_channel") as mock_insecure:
        mock_insecure.return_value = MagicMock()
        channel = create_channel(cfg)
    mock_insecure.assert_called_once()
    args, kwargs = mock_insecure.call_args
    assert args[0] == "localhost:26500"
    assert "options" in kwargs


def test_create_channel_oauth() -> None:
    cfg = ZeebeAuthConfig(
        gateway_address="zeebe.cloud:443",
        client_id="id",
        client_secret="secret",
        token_url="https://auth.example.com/token",
        audience="zeebe-api",
    )
    with patch("worker.auth.TokenManager.refresh_token", return_value="test-token"):
        channel = create_channel(cfg)
        # OAuth path creates a TokenManager and calls refresh_token
        from worker.auth import get_token_manager
        mgr = get_token_manager()
        assert mgr is not None
        assert mgr._client_id == "id"
        assert mgr._audience == "zeebe-api"
    # OAuth path returns grpc.aio secure channel, not insecure
    import grpc
    assert isinstance(channel, grpc.aio.Channel)


# ── TokenManager ──────────────────────────────────────────


def test_token_manager_refresh() -> None:
    mgr = TokenManager(
        client_id="id",
        client_secret="secret",
        token_url="https://auth.example.com/token",
        audience="zeebe-api",
    )
    mock_response = MagicMock()
    mock_response.json.return_value = {"access_token": "new-token-123"}
    mock_response.raise_for_status = MagicMock()

    with patch("httpx.post", return_value=mock_response) as mock_post:
        token = mgr.refresh_token()

    assert token == "new-token-123"
    assert mgr.token == "new-token-123"
    mock_post.assert_called_once()
    call_kwargs = mock_post.call_args
    assert call_kwargs[0][0] == "https://auth.example.com/token"
    assert call_kwargs[1]["data"]["client_id"] == "id"
    assert call_kwargs[1]["data"]["audience"] == "zeebe-api"
