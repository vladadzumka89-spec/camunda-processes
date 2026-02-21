"""Tests for worker.handlers.clickbot — E2E browser test handler."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from worker.config import AppConfig, ServerConfig
from worker.handlers.clickbot import register_clickbot_handlers
from worker.ssh import CommandResult


def _make_ssh_result(stdout: str = "", stderr: str = "", exit_code: int = 0) -> CommandResult:
    return CommandResult(stdout=stdout, stderr=stderr, exit_code=exit_code)


def _extract_handlers(config: AppConfig, ssh: AsyncMock) -> dict:
    handlers = {}

    def task_decorator(task_type: str, **kwargs):
        def wrapper(fn):
            handlers[task_type] = fn
            return fn
        return wrapper

    worker = MagicMock()
    worker.task = task_decorator
    register_clickbot_handlers(worker, config, ssh)
    return handlers


@pytest.fixture
def clickbot_config() -> AppConfig:
    return AppConfig(
        servers={
            "staging": ServerConfig(
                host="staging.example.com",
                ssh_user="deploy",
                repo_dir="/opt/odoo-enterprise",
                db_name="odoo19",
                container="odoo19",
            ),
        },
    )


@pytest.fixture
def mock_ssh() -> AsyncMock:
    ssh = AsyncMock()
    ssh.run = AsyncMock()
    ssh.run_in_repo = AsyncMock()
    return ssh


@pytest.fixture
def handlers(clickbot_config: AppConfig, mock_ssh: AsyncMock) -> dict:
    return _extract_handlers(clickbot_config, mock_ssh)


# ── clickbot-test ─────────────────────────────────────────


def _setup_clickbot_ssh(mock_ssh: AsyncMock, test_stdout: str, exit_code: int = 0) -> None:
    """Set up SSH mock for a full clickbot run."""
    mock_ssh.run_in_repo.side_effect = [
        _make_ssh_result(),  # cleanup previous
        _make_ssh_result(),  # start clickbot-db
        _make_ssh_result(stdout=test_stdout, exit_code=exit_code),  # run tests
        _make_ssh_result(),  # cleanup finally
    ]
    mock_ssh.run.side_effect = [
        _make_ssh_result(),  # pg_dump
        _make_ssh_result(),  # docker cp
        _make_ssh_result(),  # pg_restore
        _make_ssh_result(stdout="1", exit_code=0),  # verify DB
        _make_ssh_result(),  # prepare SQL
        _make_ssh_result(),  # rm dump finally
    ]


@pytest.mark.asyncio
async def test_clickbot_passed(handlers: dict, mock_ssh: AsyncMock) -> None:
    with patch("worker.handlers.clickbot.asyncio.sleep", new_callable=AsyncMock):
        _setup_clickbot_ssh(mock_ssh, "clickbot test succeeded", exit_code=0)
        result = await handlers["clickbot-test"](server_host="staging")
    assert result["clickbot_passed"] is True


@pytest.mark.asyncio
async def test_clickbot_failed(handlers: dict, mock_ssh: AsyncMock) -> None:
    with patch("worker.handlers.clickbot.asyncio.sleep", new_callable=AsyncMock):
        _setup_clickbot_ssh(
            mock_ssh,
            "FAIL: Subtest clickbot app='sale_management'\n"
            "FAIL: Subtest clickbot app='purchase'",
            exit_code=1,
        )
        result = await handlers["clickbot-test"](server_host="staging")
    assert result["clickbot_passed"] is False
    assert "sale_management" in result["clickbot_report"]


@pytest.mark.asyncio
async def test_clickbot_cleanup_on_success(handlers: dict, mock_ssh: AsyncMock) -> None:
    with patch("worker.handlers.clickbot.asyncio.sleep", new_callable=AsyncMock):
        _setup_clickbot_ssh(mock_ssh, "clickbot test succeeded", exit_code=0)
        await handlers["clickbot-test"](server_host="staging")
    # Verify cleanup docker-compose down was called (last run_in_repo call)
    last_repo_call = mock_ssh.run_in_repo.call_args_list[-1]
    assert "down" in last_repo_call[0][1]


@pytest.mark.asyncio
async def test_clickbot_cleanup_on_error(handlers: dict, mock_ssh: AsyncMock) -> None:
    with patch("worker.handlers.clickbot.asyncio.sleep", new_callable=AsyncMock):
        mock_ssh.run_in_repo.side_effect = [
            _make_ssh_result(),  # cleanup previous
            _make_ssh_result(),  # start clickbot-db
            RuntimeError("SSH connection lost"),  # test fails
            _make_ssh_result(),  # cleanup finally
        ]
        mock_ssh.run.side_effect = [
            _make_ssh_result(),  # pg_dump
            _make_ssh_result(),  # docker cp
            _make_ssh_result(),  # pg_restore
            _make_ssh_result(stdout="1", exit_code=0),  # verify DB
            _make_ssh_result(),  # prepare SQL
            _make_ssh_result(),  # rm dump finally
        ]
        with pytest.raises(RuntimeError, match="SSH connection lost"):
            await handlers["clickbot-test"](server_host="staging")
    # Cleanup should still be called in finally block
    last_repo_call = mock_ssh.run_in_repo.call_args_list[-1]
    assert "down" in last_repo_call[0][1]
