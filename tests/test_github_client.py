"""Tests for worker.github_client — GitHubClient async HTTP operations."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from worker.github_client import GitHubClient


@pytest.fixture
def github() -> GitHubClient:
    return GitHubClient(token="ghp_test", deploy_pat="ghp_deploy")


@pytest.fixture
def github_no_deploy_pat() -> GitHubClient:
    return GitHubClient(token="ghp_test")


def _mock_response(status_code: int = 200, json_data: dict | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    resp.raise_for_status = MagicMock()
    return resp


@pytest.mark.asyncio
async def test_get_pr(github: GitHubClient) -> None:
    mock_resp = _mock_response(json_data={"number": 42, "title": "Test"})
    with patch("httpx.AsyncClient") as MockClient:
        instance = AsyncMock()
        instance.request = AsyncMock(return_value=mock_resp)
        instance.__aenter__ = AsyncMock(return_value=instance)
        instance.__aexit__ = AsyncMock(return_value=False)
        MockClient.return_value = instance

        result = await github.get_pr("tut-ua/odoo-enterprise", 42)

    assert result["number"] == 42
    call_args = instance.request.call_args
    assert call_args[0][0] == "GET"
    assert "/repos/tut-ua/odoo-enterprise/pulls/42" in call_args[0][1]


@pytest.mark.asyncio
async def test_merge_pr_squash(github: GitHubClient) -> None:
    mock_resp = _mock_response(json_data={"merged": True})
    with patch("httpx.AsyncClient") as MockClient:
        instance = AsyncMock()
        instance.request = AsyncMock(return_value=mock_resp)
        instance.__aenter__ = AsyncMock(return_value=instance)
        instance.__aexit__ = AsyncMock(return_value=False)
        MockClient.return_value = instance

        await github.merge_pr("tut-ua/repo", 10, method="squash")

    call_kwargs = instance.request.call_args[1]
    assert call_kwargs["json"]["merge_method"] == "squash"


@pytest.mark.asyncio
async def test_comment_pr(github: GitHubClient) -> None:
    mock_resp = _mock_response(json_data={"id": 1})
    with patch("httpx.AsyncClient") as MockClient:
        instance = AsyncMock()
        instance.request = AsyncMock(return_value=mock_resp)
        instance.__aenter__ = AsyncMock(return_value=instance)
        instance.__aexit__ = AsyncMock(return_value=False)
        MockClient.return_value = instance

        await github.comment_pr("tut-ua/repo", 10, "LGTM")

    call_args = instance.request.call_args
    assert call_args[0][0] == "POST"
    assert "/issues/10/comments" in call_args[0][1]
    assert call_args[1]["json"]["body"] == "LGTM"


@pytest.mark.asyncio
async def test_create_pr_uses_deploy_pat(github: GitHubClient) -> None:
    mock_resp = _mock_response(json_data={"number": 99, "html_url": ""})
    with patch("httpx.AsyncClient") as MockClient:
        instance = AsyncMock()
        instance.request = AsyncMock(return_value=mock_resp)
        instance.__aenter__ = AsyncMock(return_value=instance)
        instance.__aexit__ = AsyncMock(return_value=False)
        MockClient.return_value = instance

        await github.create_pr("tut-ua/repo", "feat", "main", "Title")

    call_args = instance.request.call_args
    headers = call_args[1]["headers"]
    assert "ghp_deploy" in headers["Authorization"]


def test_headers_default_token(github_no_deploy_pat: GitHubClient) -> None:
    headers = github_no_deploy_pat._headers(use_deploy_pat=True)
    assert "ghp_test" in headers["Authorization"]
