"""Tests for worker.handlers.deploy — deploy process handlers."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from worker.config import AppConfig, ServerConfig
from worker.handlers.deploy import register_deploy_handlers
from worker.ssh import CommandResult


def _make_ssh_result(stdout: str = "", stderr: str = "", exit_code: int = 0) -> CommandResult:
    return CommandResult(stdout=stdout, stderr=stderr, exit_code=exit_code)


def _extract_handlers(config: AppConfig, ssh: AsyncMock) -> dict:
    """Register handlers and capture them from mock worker."""
    handlers = {}

    def task_decorator(task_type: str, **kwargs):
        def wrapper(fn):
            handlers[task_type] = fn
            return fn
        return wrapper

    worker = MagicMock()
    worker.task = task_decorator
    register_deploy_handlers(worker, config, ssh)
    return handlers


@pytest.fixture
def mock_ssh() -> AsyncMock:
    ssh = AsyncMock()
    ssh.run = AsyncMock()
    ssh.run_in_repo = AsyncMock()
    return ssh


@pytest.fixture
def handlers(app_config: AppConfig, mock_ssh: AsyncMock) -> dict:
    return _extract_handlers(app_config, mock_ssh)


# ── git-pull ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_git_pull_has_changes(handlers: dict, mock_ssh: AsyncMock) -> None:
    mock_ssh.run.return_value = _make_ssh_result(stdout="aaa1111\n")
    mock_ssh.run_in_repo.side_effect = [
        _make_ssh_result(),  # git fetch
        _make_ssh_result(),  # git checkout
        _make_ssh_result(stdout="bbb2222\n"),  # git rev-parse HEAD
    ]
    result = await handlers["git-pull"](
        server_host="staging", branch="staging",
    )
    assert result["has_changes"] is True
    assert result["old_commit"] == "aaa1111"
    assert result["new_commit"] == "bbb2222"


@pytest.mark.asyncio
async def test_git_pull_no_changes(handlers: dict, mock_ssh: AsyncMock) -> None:
    mock_ssh.run.return_value = _make_ssh_result(stdout="aaa1111\n")
    mock_ssh.run_in_repo.side_effect = [
        _make_ssh_result(),  # git fetch
        _make_ssh_result(),  # git checkout
        _make_ssh_result(stdout="aaa1111\n"),  # same commit
    ]
    result = await handlers["git-pull"](
        server_host="staging", branch="staging",
    )
    assert result["has_changes"] is False


# ── detect-modules ────────────────────────────────────────


@pytest.mark.asyncio
async def test_detect_modules_first_deploy(handlers: dict) -> None:
    result = await handlers["detect-modules"](
        server_host="staging", old_commit="none", new_commit="abc123",
    )
    assert result["changed_modules"] == "all"
    assert result["docker_build_needed"] is True


@pytest.mark.asyncio
async def test_detect_modules_finds_changed(handlers: dict, mock_ssh: AsyncMock) -> None:
    mock_ssh.run_in_repo.side_effect = [
        _make_ssh_result(stdout="3\n"),  # total file count
        # src/custom diff
        _make_ssh_result(stdout="src/custom/tut_hr/models/hr.py\n"),
        _make_ssh_result(stdout="yes\n"),  # manifest check
        # src/enterprise diff
        _make_ssh_result(stdout=""),
        # src/third-party diff
        _make_ssh_result(stdout=""),
        # community addons diff
        _make_ssh_result(stdout=""),
        # docker diff
        _make_ssh_result(stdout=""),
    ]
    result = await handlers["detect-modules"](
        server_host="staging", old_commit="aaa", new_commit="bbb",
    )
    assert "tut_hr" in result["changed_modules"]
    assert result["docker_build_needed"] is False


# ── docker-build ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_docker_build(handlers: dict, mock_ssh: AsyncMock) -> None:
    mock_ssh.run_in_repo.return_value = _make_ssh_result()
    result = await handlers["docker-build"](server_host="staging")
    assert result == {}
    # Verify docker compose build was called
    call_cmd = mock_ssh.run_in_repo.call_args[0][1]
    assert "docker compose build" in call_cmd


# ── module-update ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_module_update_empty(handlers: dict, mock_ssh: AsyncMock) -> None:
    result = await handlers["module-update"](
        server_host="staging", changed_modules="",
    )
    assert result["modules_updated"] == ""
    mock_ssh.run.assert_not_awaited()


# ── cache-clear ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_cache_clear(handlers: dict, mock_ssh: AsyncMock) -> None:
    mock_ssh.run.return_value = _make_ssh_result()
    mock_ssh.run_in_repo.return_value = _make_ssh_result()
    result = await handlers["cache-clear"](server_host="staging")
    assert result == {}
    # Verify SQL DELETE on assets
    sql_call = mock_ssh.run.call_args[0][1]
    assert "DELETE FROM ir_attachment" in sql_call
    # Verify docker compose up
    up_call = mock_ssh.run_in_repo.call_args[0][1]
    assert "docker compose up -d" in up_call


# ── save-deploy-state ─────────────────────────────────────


@pytest.mark.asyncio
async def test_save_deploy_state(handlers: dict, mock_ssh: AsyncMock) -> None:
    mock_ssh.run.return_value = _make_ssh_result()
    result = await handlers["save-deploy-state"](
        server_host="staging", branch="staging", new_commit="abc123def",
    )
    assert result == {}
    cmd = mock_ssh.run.call_args[0][1]
    assert "mkdir -p" in cmd
    assert "abc123def" in cmd
    assert "deploy_state_staging" in cmd


# ── rollback ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_rollback_with_branch(handlers: dict, mock_ssh: AsyncMock) -> None:
    mock_ssh.run_in_repo.return_value = _make_ssh_result()
    result = await handlers["rollback"](
        server_host="staging", old_commit="abc123", branch="staging",
    )
    assert result == {}
    first_call_cmd = mock_ssh.run_in_repo.call_args_list[0][0][1]
    assert "git checkout -B staging abc123" in first_call_cmd


@pytest.mark.asyncio
async def test_rollback_no_commit(handlers: dict, mock_ssh: AsyncMock) -> None:
    result = await handlers["rollback"](
        server_host="staging", old_commit="none",
    )
    assert result == {}
    mock_ssh.run_in_repo.assert_not_awaited()
