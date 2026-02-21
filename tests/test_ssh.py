"""Tests for worker.ssh — CommandResult and AsyncSSHClient."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from worker.config import ServerConfig
from worker.ssh import AsyncSSHClient, CommandResult, RemoteCommandError


# ── CommandResult ─────────────────────────────────────────


def test_command_result_success() -> None:
    r = CommandResult(stdout="ok", stderr="", exit_code=0)
    assert r.success is True


def test_command_result_failure() -> None:
    r = CommandResult(stdout="", stderr="error", exit_code=1)
    assert r.success is False


def test_command_result_check_raises() -> None:
    r = CommandResult(stdout="", stderr="bad thing", exit_code=1)
    with pytest.raises(RemoteCommandError, match="bad thing"):
        r.check("oops")


def test_command_result_check_success() -> None:
    r = CommandResult(stdout="ok", stderr="", exit_code=0)
    assert r.check("should not raise") is r


# ── RemoteCommandError ────────────────────────────────────


def test_remote_command_error() -> None:
    exc = RemoteCommandError("connection lost")
    assert isinstance(exc, Exception)
    assert str(exc) == "connection lost"


# ── AsyncSSHClient.run_in_repo ────────────────────────────


@pytest.mark.asyncio
async def test_run_in_repo_prepends_cd() -> None:
    server = ServerConfig(host="test.host", ssh_user="deploy", repo_dir="/opt/repo")
    client = AsyncSSHClient()

    with patch.object(client, "run", new_callable=AsyncMock) as mock_run:
        mock_run.return_value = CommandResult(stdout="ok", stderr="", exit_code=0)
        await client.run_in_repo(server, "git status")

    mock_run.assert_awaited_once()
    call_args = mock_run.call_args
    assert call_args[0][0] is server
    assert call_args[0][1] == "cd /opt/repo && git status"
