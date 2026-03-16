"""Tests for worker.handlers.deploy — deploy process handlers."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from worker.config import AppConfig, ServerConfig
from worker.handlers.deploy import register_deploy_handlers
from worker.ssh import CommandResult, RemoteCommandError


def _make_ssh_result(stdout: str = "", stderr: str = "", exit_code: int = 0) -> CommandResult:
    return CommandResult(stdout=stdout, stderr=stderr, exit_code=exit_code)


OK = _make_ssh_result
FAIL = lambda msg="error": _make_ssh_result(stderr=msg, exit_code=1)


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


@pytest.fixture
def prod_handlers(app_config_with_production: AppConfig, mock_ssh: AsyncMock) -> dict:
    return _extract_handlers(app_config_with_production, mock_ssh)


# ══════════════════════════════════════════════════════════
# Registration
# ══════════════════════════════════════════════════════════


def test_all_11_handlers_registered(handlers: dict) -> None:
    expected = {
        "git-pull", "detect-modules", "docker-build", "docker-up",
        "module-update", "cache-clear", "smoke-test", "http-verify",
        "save-deploy-state", "rollback", "db-checkpoint",
    }
    assert set(handlers.keys()) == expected


# ══════════════════════════════════════════════════════════
# git-pull
# ══════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_git_pull_has_changes(handlers: dict, mock_ssh: AsyncMock) -> None:
    mock_ssh.run.return_value = _make_ssh_result(stdout="aaa1111\n")
    mock_ssh.run_in_repo.side_effect = [
        OK(),  # git fetch
        OK(),  # git checkout
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
        OK(),  # git fetch
        OK(),  # git checkout
        _make_ssh_result(stdout="aaa1111\n"),  # same commit
    ]
    result = await handlers["git-pull"](
        server_host="staging", branch="staging",
    )
    assert result["has_changes"] is False


@pytest.mark.asyncio
async def test_git_pull_first_deploy_no_state(handlers: dict, mock_ssh: AsyncMock) -> None:
    """When state file doesn't exist, old_commit='none'."""
    mock_ssh.run.return_value = _make_ssh_result(stdout="none\n")
    mock_ssh.run_in_repo.side_effect = [
        OK(),  # git fetch
        OK(),  # git checkout
        _make_ssh_result(stdout="abc1234\n"),  # rev-parse
    ]
    result = await handlers["git-pull"](server_host="staging", branch="main")
    assert result["old_commit"] == "none"
    assert result["new_commit"] == "abc1234"
    assert result["has_changes"] is True


@pytest.mark.asyncio
async def test_git_pull_custom_repo_dir(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Custom repo_dir overrides server default."""
    mock_ssh.run.return_value = _make_ssh_result(stdout="aaa\n")
    mock_ssh.run_in_repo.side_effect = [OK(), OK(), _make_ssh_result(stdout="bbb\n")]

    await handlers["git-pull"](
        server_host="staging", branch="main", repo_dir="/custom/path",
    )
    # State file path should use custom repo dir
    state_cmd = mock_ssh.run.call_args[0][1]
    assert "/custom/path/.deploy-state/" in state_cmd


@pytest.mark.asyncio
async def test_git_pull_retry_on_fetch_failure(handlers: dict, mock_ssh: AsyncMock) -> None:
    """git fetch retries up to 3 times via retry()."""
    mock_ssh.run.return_value = _make_ssh_result(stdout="aaa\n")  # state file
    mock_ssh.run_in_repo.side_effect = [
        # fetch attempt 1 — fail (check=True raises)
        RemoteCommandError("network error"),
        # fetch attempt 2 — fail
        RemoteCommandError("network error"),
        # fetch attempt 3 — success
        OK(),
        # checkout
        OK(),
        # rev-parse
        _make_ssh_result(stdout="bbb\n"),
    ]

    with patch("worker.handlers.deploy._asyncio.sleep", new_callable=AsyncMock):
        result = await handlers["git-pull"](server_host="staging", branch="main")

    assert result["new_commit"] == "bbb"


@pytest.mark.asyncio
async def test_git_pull_all_retries_exhausted(handlers: dict, mock_ssh: AsyncMock) -> None:
    """When all 3 fetch retries fail, the handler raises."""
    mock_ssh.run.return_value = _make_ssh_result(stdout="aaa\n")
    mock_ssh.run_in_repo.side_effect = [
        RemoteCommandError("fail"),
        RemoteCommandError("fail"),
        RemoteCommandError("fail"),
    ]

    with patch("worker.handlers.deploy._asyncio.sleep", new_callable=AsyncMock):
        with pytest.raises(RemoteCommandError):
            await handlers["git-pull"](server_host="staging", branch="main")


# ══════════════════════════════════════════════════════════
# detect-modules
# ══════════════════════════════════════════════════════════


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


@pytest.mark.asyncio
async def test_detect_modules_too_many_files(handlers: dict, mock_ssh: AsyncMock) -> None:
    """When >250 files changed, return 'all' immediately."""
    mock_ssh.run_in_repo.return_value = _make_ssh_result(stdout="300\n")
    result = await handlers["detect-modules"](
        server_host="staging", old_commit="aaa", new_commit="bbb",
    )
    assert result["changed_modules"] == "all"
    assert result["docker_build_needed"] is True
    # Should only have called once (total file count)
    assert mock_ssh.run_in_repo.await_count == 1


@pytest.mark.asyncio
async def test_detect_modules_docker_build_needed(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Docker-related file changes trigger docker_build_needed."""
    mock_ssh.run_in_repo.side_effect = [
        _make_ssh_result(stdout="2\n"),  # total file count
        _make_ssh_result(stdout=""),  # src/custom
        _make_ssh_result(stdout=""),  # src/enterprise
        _make_ssh_result(stdout=""),  # src/third-party
        _make_ssh_result(stdout=""),  # community
        _make_ssh_result(stdout="Dockerfile\n"),  # docker diff — has changes
    ]
    result = await handlers["detect-modules"](
        server_host="staging", old_commit="aaa", new_commit="bbb",
    )
    assert result["changed_modules"] == ""
    assert result["docker_build_needed"] is True


@pytest.mark.asyncio
async def test_detect_modules_community_addons(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Community addons at deeper path are detected."""
    mock_ssh.run_in_repo.side_effect = [
        _make_ssh_result(stdout="2\n"),  # total files
        _make_ssh_result(stdout=""),  # src/custom
        _make_ssh_result(stdout=""),  # src/enterprise
        _make_ssh_result(stdout=""),  # src/third-party
        # community addons diff
        _make_ssh_result(stdout="src/community/odoo/addons/sale_custom/models/sale.py\n"),
        _make_ssh_result(stdout="yes\n"),  # manifest check
        # docker diff
        _make_ssh_result(stdout=""),
    ]
    result = await handlers["detect-modules"](
        server_host="staging", old_commit="aaa", new_commit="bbb",
    )
    assert "sale_custom" in result["changed_modules"]


@pytest.mark.asyncio
async def test_detect_modules_no_manifest_skipped(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Changed dir without __manifest__.py is not a valid module."""
    mock_ssh.run_in_repo.side_effect = [
        _make_ssh_result(stdout="1\n"),  # total files
        _make_ssh_result(stdout="src/custom/not_a_module/readme.txt\n"),
        _make_ssh_result(stdout="no\n"),  # no manifest
        _make_ssh_result(stdout=""),  # enterprise
        _make_ssh_result(stdout=""),  # third-party
        _make_ssh_result(stdout=""),  # community
        _make_ssh_result(stdout=""),  # docker
    ]
    result = await handlers["detect-modules"](
        server_host="staging", old_commit="aaa", new_commit="bbb",
    )
    assert result["changed_modules"] == ""


@pytest.mark.asyncio
async def test_detect_modules_multiple_dirs(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Modules from multiple source dirs are merged and sorted."""
    mock_ssh.run_in_repo.side_effect = [
        _make_ssh_result(stdout="5\n"),  # total files
        # src/custom — 1 module
        _make_ssh_result(stdout="src/custom/tut_hr/models/hr.py\n"),
        _make_ssh_result(stdout="yes\n"),
        # src/enterprise — 1 module
        _make_ssh_result(stdout="src/enterprise/sale_ent/views/sale.xml\n"),
        _make_ssh_result(stdout="yes\n"),
        # src/third-party — nothing
        _make_ssh_result(stdout=""),
        # community — nothing
        _make_ssh_result(stdout=""),
        # docker
        _make_ssh_result(stdout=""),
    ]
    result = await handlers["detect-modules"](
        server_host="staging", old_commit="aaa", new_commit="bbb",
    )
    modules = result["changed_modules"].split(",")
    assert "sale_ent" in modules
    assert "tut_hr" in modules
    assert modules == sorted(modules)  # sorted


@pytest.mark.asyncio
async def test_detect_modules_no_changes(handlers: dict, mock_ssh: AsyncMock) -> None:
    """No file changes in any source dir."""
    mock_ssh.run_in_repo.side_effect = [
        _make_ssh_result(stdout="0\n"),  # total files
        _make_ssh_result(stdout=""),  # custom
        _make_ssh_result(stdout=""),  # enterprise
        _make_ssh_result(stdout=""),  # third-party
        _make_ssh_result(stdout=""),  # community
        _make_ssh_result(stdout=""),  # docker
    ]
    result = await handlers["detect-modules"](
        server_host="staging", old_commit="aaa", new_commit="bbb",
    )
    assert result["changed_modules"] == ""
    assert result["docker_build_needed"] is False


@pytest.mark.asyncio
async def test_detect_modules_deduplicates(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Same module changed in multiple files is counted once."""
    mock_ssh.run_in_repo.side_effect = [
        _make_ssh_result(stdout="3\n"),
        # src/custom — 2 files in same module
        _make_ssh_result(stdout="src/custom/tut_hr/models/hr.py\nsrc/custom/tut_hr/views/hr.xml\n"),
        _make_ssh_result(stdout="yes\n"),  # manifest check (first)
        _make_ssh_result(stdout="yes\n"),  # manifest check (second — same module, dedup'd in set)
        # enterprise
        _make_ssh_result(stdout=""),
        # third-party
        _make_ssh_result(stdout=""),
        # community
        _make_ssh_result(stdout=""),
        # docker
        _make_ssh_result(stdout=""),
    ]
    result = await handlers["detect-modules"](
        server_host="staging", old_commit="aaa", new_commit="bbb",
    )
    assert result["changed_modules"] == "tut_hr"


# ══════════════════════════════════════════════════════════
# docker-build
# ══════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_docker_build(handlers: dict, mock_ssh: AsyncMock) -> None:
    mock_ssh.run_in_repo.return_value = _make_ssh_result()
    result = await handlers["docker-build"](server_host="staging")
    assert result == {}
    # Verify docker compose build was called
    call_cmd = mock_ssh.run_in_repo.call_args[0][1]
    assert "docker compose build" in call_cmd


@pytest.mark.asyncio
async def test_docker_build_retry_on_failure(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Docker build retries on transient failures."""
    mock_ssh.run_in_repo.side_effect = [
        _make_ssh_result(stderr="network timeout", exit_code=1),
        _make_ssh_result(stderr="network timeout", exit_code=1),
        OK(),  # 3rd attempt succeeds
    ]
    with patch("worker.handlers.deploy._asyncio.sleep", new_callable=AsyncMock):
        result = await handlers["docker-build"](server_host="staging")
    assert result == {}


@pytest.mark.asyncio
async def test_docker_build_custom_repo(handlers: dict, mock_ssh: AsyncMock) -> None:
    mock_ssh.run_in_repo.return_value = OK()
    await handlers["docker-build"](server_host="staging", repo_dir="/custom/repo")
    assert mock_ssh.run_in_repo.await_count == 1


# ══════════════════════════════════════════════════════════
# docker-up
# ══════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_docker_up_happy_path(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Container starts and HTTP responds on first check."""
    mock_ssh.run_in_repo.return_value = OK()  # docker compose up
    mock_ssh.run.side_effect = [
        _make_ssh_result(stdout="running\n"),  # container status — running immediately
        _make_ssh_result(exit_code=0),  # curl HTTP check — OK
        OK(),  # nginx restart
    ]
    with patch("worker.handlers.deploy._sleep", new_callable=AsyncMock):
        result = await handlers["docker-up"](server_host="staging")
    assert result == {}


@pytest.mark.asyncio
async def test_docker_up_waits_for_container(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Container takes a few checks to become running."""
    mock_ssh.run_in_repo.return_value = OK()
    mock_ssh.run.side_effect = [
        _make_ssh_result(stdout="created\n"),  # not running yet
        _make_ssh_result(stdout="starting\n"),  # still not
        _make_ssh_result(stdout="running\n"),  # now running
        _make_ssh_result(exit_code=0),  # HTTP OK
        OK(),  # nginx restart
    ]
    with patch("worker.handlers.deploy._sleep", new_callable=AsyncMock):
        result = await handlers["docker-up"](server_host="staging")
    assert result == {}


@pytest.mark.asyncio
async def test_docker_up_container_never_starts(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Container never becomes 'running' — raises after 12 attempts."""
    mock_ssh.run_in_repo.return_value = OK()
    mock_ssh.run.return_value = _make_ssh_result(stdout="created\n")

    with patch("worker.handlers.deploy._sleep", new_callable=AsyncMock):
        with pytest.raises(RuntimeError, match="not running after 60s"):
            await handlers["docker-up"](server_host="staging")


@pytest.mark.asyncio
async def test_docker_up_waits_for_http(handlers: dict, mock_ssh: AsyncMock) -> None:
    """HTTP endpoint takes multiple attempts to respond."""
    mock_ssh.run_in_repo.return_value = OK()
    mock_ssh.run.side_effect = [
        _make_ssh_result(stdout="running\n"),  # container running
        _make_ssh_result(exit_code=7),  # curl fail
        _make_ssh_result(exit_code=7),  # curl fail
        _make_ssh_result(exit_code=0),  # curl OK
        OK(),  # nginx restart
    ]
    with patch("worker.handlers.deploy._sleep", new_callable=AsyncMock):
        result = await handlers["docker-up"](server_host="staging")
    assert result == {}


@pytest.mark.asyncio
async def test_docker_up_http_never_responds(handlers: dict, mock_ssh: AsyncMock) -> None:
    """HTTP never responds — raises RuntimeError after max attempts."""
    mock_ssh.run_in_repo.return_value = OK()
    # container running + 24 failed curl attempts
    mock_ssh.run.side_effect = [
        _make_ssh_result(stdout="running\n"),
    ] + [_make_ssh_result(exit_code=7)] * 24

    with patch("worker.handlers.deploy._sleep", new_callable=AsyncMock):
        with pytest.raises(RuntimeError, match="HTTP service not responding"):
            await handlers["docker-up"](server_host="staging")


@pytest.mark.asyncio
async def test_docker_up_custom_container_and_port(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Custom container/port override server defaults."""
    mock_ssh.run_in_repo.return_value = OK()
    mock_ssh.run.side_effect = [
        _make_ssh_result(stdout="running\n"),
        _make_ssh_result(exit_code=0),  # HTTP OK
        OK(),  # nginx
    ]
    with patch("worker.handlers.deploy._sleep", new_callable=AsyncMock):
        await handlers["docker-up"](
            server_host="staging", container="my-ctr", port=8080,
        )
    # Verify custom container name in docker inspect
    inspect_cmd = mock_ssh.run.call_args_list[0][0][1]
    assert "my-ctr" in inspect_cmd


@pytest.mark.asyncio
async def test_docker_up_nginx_restart_failure_ignored(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Nginx restart failure is not fatal (|| true in command)."""
    mock_ssh.run_in_repo.return_value = OK()
    mock_ssh.run.side_effect = [
        _make_ssh_result(stdout="running\n"),
        _make_ssh_result(exit_code=0),  # HTTP OK
        _make_ssh_result(exit_code=0),  # nginx restart (uses || true)
    ]
    with patch("worker.handlers.deploy._sleep", new_callable=AsyncMock):
        result = await handlers["docker-up"](server_host="staging")
    assert result == {}


@pytest.mark.asyncio
async def test_docker_up_retry_compose_up(handlers: dict, mock_ssh: AsyncMock) -> None:
    """docker compose up retries on failure."""
    mock_ssh.run_in_repo.side_effect = [
        FAIL("compose error"),
        FAIL("compose error"),
        OK(),  # 3rd attempt
    ]
    mock_ssh.run.side_effect = [
        _make_ssh_result(stdout="running\n"),
        _make_ssh_result(exit_code=0),  # HTTP
        OK(),  # nginx
    ]
    with patch("worker.handlers.deploy._sleep", new_callable=AsyncMock):
        with patch("worker.handlers.deploy._asyncio.sleep", new_callable=AsyncMock):
            result = await handlers["docker-up"](server_host="staging")
    assert result == {}


# ══════════════════════════════════════════════════════════
# module-update
# ══════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_module_update_empty(handlers: dict, mock_ssh: AsyncMock) -> None:
    result = await handlers["module-update"](
        server_host="staging", changed_modules="",
    )
    assert result["modules_updated"] == ""
    mock_ssh.run.assert_not_awaited()


@pytest.mark.asyncio
async def test_module_update_all(handlers: dict, mock_ssh: AsyncMock) -> None:
    """'all' modules update — skips installed check."""
    mock_ssh.run.side_effect = [
        _make_ssh_result(stdout="secret123\n"),  # DB password from container
        OK(),  # asset cache clear
    ]
    mock_ssh.run_in_repo.side_effect = [
        OK(),  # find __pycache__
        OK(),  # docker compose stop
        OK(),  # docker compose start db + module update
        OK(),  # docker compose up -d
        OK(),  # finally: docker compose start db
    ]
    result = await handlers["module-update"](
        server_host="staging", changed_modules="all",
    )
    assert result["modules_updated"] == "all"
    # Should contain -u all in the update command
    update_cmd = mock_ssh.run_in_repo.call_args_list[2][0][1]
    assert "-u all" in update_cmd


@pytest.mark.asyncio
async def test_module_update_specific_modules(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Specific modules — filter to installed ones only."""
    mock_ssh.run.side_effect = [
        _make_ssh_result(stdout="dbpass\n"),  # DB password
        # installed modules query
        _make_ssh_result(stdout="tut_hr\nsale\naccount\n"),
        # asset cache clear
        OK(),
    ]
    mock_ssh.run_in_repo.side_effect = [
        OK(),  # find __pycache__
        OK(),  # docker compose stop
        OK(),  # module update
        OK(),  # docker compose up
        OK(),  # finally: docker compose start db
    ]
    result = await handlers["module-update"](
        server_host="staging", changed_modules="tut_hr,tut_core,sale",
    )
    # tut_core is NOT installed, so should only update tut_hr,sale
    updated = result["modules_updated"]
    assert "tut_hr" in updated
    assert "sale" in updated
    assert "tut_core" not in updated


@pytest.mark.asyncio
async def test_module_update_none_installed(handlers: dict, mock_ssh: AsyncMock) -> None:
    """None of the changed modules are installed — skip update."""
    mock_ssh.run.side_effect = [
        _make_ssh_result(stdout="dbpass\n"),  # password
        _make_ssh_result(stdout="base\nweb\n"),  # installed (none match)
    ]
    result = await handlers["module-update"](
        server_host="staging", changed_modules="tut_new_module",
    )
    assert result["modules_updated"] == ""


@pytest.mark.asyncio
async def test_module_update_over_10_switches_to_all(handlers: dict, mock_ssh: AsyncMock) -> None:
    """More than 10 matching modules switches to -u all."""
    many_mods = ",".join(f"mod_{i}" for i in range(15))
    installed_stdout = "\n".join(f"mod_{i}" for i in range(15)) + "\n"

    mock_ssh.run.side_effect = [
        _make_ssh_result(stdout="dbpass\n"),  # password
        _make_ssh_result(stdout=installed_stdout),  # all installed
        OK(),  # asset cache
    ]
    mock_ssh.run_in_repo.side_effect = [
        OK(),  # __pycache__
        OK(),  # stop
        OK(),  # update
        OK(),  # up
        OK(),  # finally: docker compose start db
    ]
    result = await handlers["module-update"](
        server_host="staging", changed_modules=many_mods,
    )
    assert result["modules_updated"] == "all"


@pytest.mark.asyncio
async def test_module_update_db_password_from_env_file(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Falls back to .env file if container env fails."""
    mock_ssh.run.side_effect = [
        _make_ssh_result(stdout="", exit_code=1),  # container env fails
        OK(),  # asset cache
    ]
    mock_ssh.run_in_repo.side_effect = [
        _make_ssh_result(stdout="env_pass\n"),  # .env file fallback
        OK(),  # __pycache__
        OK(),  # stop
        OK(),  # update
        OK(),  # up
        OK(),  # finally: docker compose start db
    ]
    result = await handlers["module-update"](
        server_host="staging", changed_modules="all",
    )
    assert result["modules_updated"] == "all"


@pytest.mark.asyncio
async def test_module_update_no_db_password_raises(handlers: dict, mock_ssh: AsyncMock) -> None:
    """If DB password can't be retrieved, raises RuntimeError."""
    mock_ssh.run.return_value = _make_ssh_result(stdout="", exit_code=1)
    mock_ssh.run_in_repo.return_value = _make_ssh_result(stdout="", exit_code=1)

    with pytest.raises(RuntimeError, match="Cannot retrieve DB password"):
        await handlers["module-update"](
            server_host="staging", changed_modules="all",
        )


@pytest.mark.asyncio
async def test_module_update_clears_pycache(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Verify __pycache__ cleanup is called."""
    mock_ssh.run.side_effect = [
        _make_ssh_result(stdout="pass\n"),  # password
        OK(),  # asset cache
    ]
    mock_ssh.run_in_repo.side_effect = [OK(), OK(), OK(), OK(), OK()]
    await handlers["module-update"](server_host="staging", changed_modules="all")
    pycache_cmd = mock_ssh.run_in_repo.call_args_list[0][0][1]
    assert "__pycache__" in pycache_cmd


@pytest.mark.asyncio
async def test_module_update_restarts_services(handlers: dict, mock_ssh: AsyncMock) -> None:
    """After module update, services are restarted."""
    mock_ssh.run.side_effect = [
        _make_ssh_result(stdout="pass\n"),
        OK(),  # asset cache clear
    ]
    mock_ssh.run_in_repo.side_effect = [OK(), OK(), OK(), OK(), OK()]
    await handlers["module-update"](server_host="staging", changed_modules="all")
    # docker compose up -d is second-to-last; last is finally: docker compose start db
    up_cmd = mock_ssh.run_in_repo.call_args_list[-2][0][1]
    assert "docker compose up -d" in up_cmd
    # finally block always runs docker compose start db
    finally_cmd = mock_ssh.run_in_repo.call_args_list[-1][0][1]
    assert "docker compose start db" in finally_cmd


@pytest.mark.asyncio
async def test_module_update_restarts_db_on_failure(handlers: dict, mock_ssh: AsyncMock) -> None:
    """If module-update command fails, DB should be restarted in finally block."""
    mock_ssh.run.side_effect = [
        _make_ssh_result(stdout="password123"),        # _get_db_password
        _make_ssh_result(stdout="sale_management\n"),  # installed modules query
    ]
    mock_ssh.run_in_repo.side_effect = [
        _make_ssh_result(),                    # find __pycache__
        _make_ssh_result(),                    # docker compose stop
        RemoteCommandError("odoo-bin failed", 1, ""),  # module update command fails
        _make_ssh_result(),                    # finally: docker compose start db
    ]
    with pytest.raises(RemoteCommandError, match="odoo-bin failed"):
        await handlers["module-update"](
            server_host="staging",
            changed_modules="sale_management",
        )
    # Verify the finally block ran: docker compose start db
    last_call = mock_ssh.run_in_repo.call_args_list[-1].args[1]
    assert "docker compose start db" in last_call


# ══════════════════════════════════════════════════════════
# cache-clear
# ══════════════════════════════════════════════════════════


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


@pytest.mark.asyncio
async def test_cache_clear_custom_db(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Custom db_name is used in SQL command."""
    mock_ssh.run.return_value = OK()
    mock_ssh.run_in_repo.return_value = OK()
    await handlers["cache-clear"](server_host="staging", db_name="custom_db")
    sql_cmd = mock_ssh.run.call_args[0][1]
    assert "custom_db" in sql_cmd


@pytest.mark.asyncio
async def test_cache_clear_custom_container(handlers: dict, mock_ssh: AsyncMock) -> None:
    mock_ssh.run.return_value = OK()
    mock_ssh.run_in_repo.return_value = OK()
    await handlers["cache-clear"](server_host="staging", container="odoo_test")
    sql_cmd = mock_ssh.run.call_args[0][1]
    assert "odoo_test-db" in sql_cmd


# ══════════════════════════════════════════════════════════
# smoke-test
# ══════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_smoke_test_passes(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Clean smoke test — no errors."""
    mock_ssh.run.return_value = _make_ssh_result(stdout="dbpass\n")  # password
    mock_ssh.run_in_repo.side_effect = [
        OK(),  # docker compose stop
        OK(),  # start db
        _make_ssh_result(stdout="INFO odoo: Modules loaded.\n"),  # smoke test
        OK(),  # docker compose up
    ]
    with patch("worker.handlers.deploy._sleep", new_callable=AsyncMock):
        result = await handlers["smoke-test"](server_host="staging")
    assert result["smoke_passed"] is True


@pytest.mark.asyncio
async def test_smoke_test_fails_on_error(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Smoke test detects ERROR lines and raises RuntimeError."""
    mock_ssh.run.return_value = _make_ssh_result(stdout="dbpass\n")
    mock_ssh.run_in_repo.side_effect = [
        OK(),  # stop
        OK(),  # start db
        _make_ssh_result(
            stdout="ERROR odoo.modules: Failed to import module tut_hr\nTraceback blah\n",
        ),
        OK(),  # up
    ]
    with patch("worker.handlers.deploy._sleep", new_callable=AsyncMock):
        with pytest.raises(RuntimeError, match="Smoke test failed"):
            await handlers["smoke-test"](server_host="staging")


@pytest.mark.asyncio
async def test_smoke_test_fails_on_exit_code(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Non-zero exit code means failure — raises RuntimeError."""
    mock_ssh.run.return_value = _make_ssh_result(stdout="dbpass\n")
    mock_ssh.run_in_repo.side_effect = [
        OK(),  # stop
        OK(),  # start db
        _make_ssh_result(stdout="", exit_code=1),  # crashed
        OK(),  # up
    ]
    with patch("worker.handlers.deploy._sleep", new_callable=AsyncMock):
        with pytest.raises(RuntimeError, match="Smoke test failed"):
            await handlers["smoke-test"](server_host="staging")


@pytest.mark.asyncio
async def test_smoke_test_ignores_safe_warnings(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Known safe warnings are filtered out."""
    mock_ssh.run.return_value = _make_ssh_result(stdout="dbpass\n")
    mock_ssh.run_in_repo.side_effect = [
        OK(),
        OK(),
        _make_ssh_result(
            stdout="ERROR: Some modules are not loaded, ignored: crm_ext\n"
                   "ERROR: inconsistent states during test\n",
        ),
        OK(),
    ]
    with patch("worker.handlers.deploy._sleep", new_callable=AsyncMock):
        result = await handlers["smoke-test"](server_host="staging")
    assert result["smoke_passed"] is True


@pytest.mark.asyncio
async def test_smoke_test_detects_critical(handlers: dict, mock_ssh: AsyncMock) -> None:
    """CRITICAL level is caught — raises RuntimeError."""
    mock_ssh.run.return_value = _make_ssh_result(stdout="dbpass\n")
    mock_ssh.run_in_repo.side_effect = [
        OK(), OK(),
        _make_ssh_result(stdout="CRITICAL odoo: database connection failed\n"),
        OK(),
    ]
    with patch("worker.handlers.deploy._sleep", new_callable=AsyncMock):
        with pytest.raises(RuntimeError, match="Smoke test failed"):
            await handlers["smoke-test"](server_host="staging")


@pytest.mark.asyncio
async def test_smoke_test_detects_import_error(handlers: dict, mock_ssh: AsyncMock) -> None:
    """ImportError / ModuleNotFoundError detected — raises RuntimeError."""
    mock_ssh.run.return_value = _make_ssh_result(stdout="dbpass\n")
    mock_ssh.run_in_repo.side_effect = [
        OK(), OK(),
        _make_ssh_result(stdout="ImportError: No module named 'missing_dep'\n"),
        OK(),
    ]
    with patch("worker.handlers.deploy._sleep", new_callable=AsyncMock):
        with pytest.raises(RuntimeError, match="Smoke test failed"):
            await handlers["smoke-test"](server_host="staging")


@pytest.mark.asyncio
async def test_smoke_test_detects_syntax_error(handlers: dict, mock_ssh: AsyncMock) -> None:
    """SyntaxError detected — raises RuntimeError."""
    mock_ssh.run.return_value = _make_ssh_result(stdout="dbpass\n")
    mock_ssh.run_in_repo.side_effect = [
        OK(), OK(),
        _make_ssh_result(stdout="SyntaxError: invalid syntax in /opt/odoo/src/custom/tut_hr/models.py\n"),
        OK(),
    ]
    with patch("worker.handlers.deploy._sleep", new_callable=AsyncMock):
        with pytest.raises(RuntimeError, match="Smoke test failed"):
            await handlers["smoke-test"](server_host="staging")


@pytest.mark.asyncio
async def test_smoke_test_raises_on_error(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Smoke test should raise RuntimeError when errors are detected."""
    mock_ssh.run.side_effect = [_make_ssh_result(stdout="password123")]  # _get_db_password
    mock_ssh.run_in_repo.side_effect = [
        OK(),  # docker compose stop
        OK(),  # docker compose start db
        _make_ssh_result(stdout="2026 ERROR something broke\n", exit_code=1),
        OK(),  # docker compose up -d (always runs before raise)
    ]
    with patch("worker.handlers.deploy._sleep", new_callable=AsyncMock):
        with pytest.raises(RuntimeError, match="Smoke test failed"):
            await handlers["smoke-test"](server_host="staging")


@pytest.mark.asyncio
async def test_smoke_test_always_restarts(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Even on failure, docker compose up -d is always called before raising."""
    mock_ssh.run.return_value = _make_ssh_result(stdout="dbpass\n")
    mock_ssh.run_in_repo.side_effect = [
        OK(), OK(),
        _make_ssh_result(stdout="CRITICAL: boom\n", exit_code=1),
        OK(),  # up -d must still be called
    ]
    with patch("worker.handlers.deploy._sleep", new_callable=AsyncMock):
        with pytest.raises(RuntimeError, match="Smoke test failed"):
            await handlers["smoke-test"](server_host="staging")
    # Verify compose up was called (second-to-last or last call before raise)
    all_cmds = [call[0][1] for call in mock_ssh.run_in_repo.call_args_list]
    assert any("docker compose up -d" in cmd for cmd in all_cmds)


@pytest.mark.asyncio
async def test_smoke_test_db_password_fallback(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Password fallback to .env file works for smoke-test."""
    mock_ssh.run.side_effect = [
        _make_ssh_result(stdout="", exit_code=1),  # container env fails
    ]
    mock_ssh.run_in_repo.side_effect = [
        _make_ssh_result(stdout="envpass\n"),  # .env fallback
        OK(),  # stop
        OK(),  # start db
        _make_ssh_result(stdout="OK\n"),  # smoke output
        OK(),  # up
    ]
    with patch("worker.handlers.deploy._sleep", new_callable=AsyncMock):
        result = await handlers["smoke-test"](server_host="staging")
    assert result["smoke_passed"] is True


# ══════════════════════════════════════════════════════════
# http-verify
# ══════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_http_verify_ok(handlers: dict, mock_ssh: AsyncMock) -> None:
    """HTTP responds on first attempt."""
    mock_ssh.run.return_value = _make_ssh_result(exit_code=0)
    with patch("worker.handlers.deploy._sleep", new_callable=AsyncMock):
        result = await handlers["http-verify"](server_host="staging")
    assert result == {}


@pytest.mark.asyncio
async def test_http_verify_retries_then_ok(handlers: dict, mock_ssh: AsyncMock) -> None:
    """HTTP fails a few times then succeeds."""
    mock_ssh.run.side_effect = [
        _make_ssh_result(exit_code=7),  # connection refused
        _make_ssh_result(exit_code=7),
        _make_ssh_result(exit_code=0),  # success
    ]
    with patch("worker.handlers.deploy._sleep", new_callable=AsyncMock):
        result = await handlers["http-verify"](server_host="staging")
    assert result == {}


@pytest.mark.asyncio
async def test_http_verify_all_retries_fail(handlers: dict, mock_ssh: AsyncMock) -> None:
    """HTTP never responds — raises RuntimeError."""
    mock_ssh.run.return_value = _make_ssh_result(exit_code=7)
    with patch("worker.handlers.deploy._sleep", new_callable=AsyncMock):
        with pytest.raises(RuntimeError, match="HTTP service not responding"):
            await handlers["http-verify"](server_host="staging")
    # Should have tried 24 times
    assert mock_ssh.run.await_count == 24


@pytest.mark.asyncio
async def test_http_verify_custom_port(handlers: dict, mock_ssh: AsyncMock) -> None:
    mock_ssh.run.return_value = OK()
    with patch("worker.handlers.deploy._sleep", new_callable=AsyncMock):
        await handlers["http-verify"](server_host="staging", port=8080)
    cmd = mock_ssh.run.call_args[0][1]
    assert "8080" in cmd


# ══════════════════════════════════════════════════════════
# save-deploy-state
# ══════════════════════════════════════════════════════════


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


@pytest.mark.asyncio
async def test_save_deploy_state_branch_slash_sanitized(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Branch names with slashes are sanitized (/ → _)."""
    mock_ssh.run.return_value = OK()
    await handlers["save-deploy-state"](
        server_host="staging", branch="feature/my-branch", new_commit="abc",
    )
    cmd = mock_ssh.run.call_args[0][1]
    assert "deploy_state_feature_my-branch" in cmd
    assert "feature/" not in cmd


@pytest.mark.asyncio
async def test_save_deploy_state_permissions(handlers: dict, mock_ssh: AsyncMock) -> None:
    """State dir and file have restricted permissions."""
    mock_ssh.run.return_value = OK()
    await handlers["save-deploy-state"](
        server_host="staging", branch="main", new_commit="abc",
    )
    cmd = mock_ssh.run.call_args[0][1]
    assert "chmod 700" in cmd
    assert "chmod 600" in cmd


# ══════════════════════════════════════════════════════════
# rollback
# ══════════════════════════════════════════════════════════


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


@pytest.mark.asyncio
async def test_rollback_empty_string_commit(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Empty string old_commit also skips rollback."""
    result = await handlers["rollback"](server_host="staging", old_commit="")
    assert result == {}
    mock_ssh.run_in_repo.assert_not_awaited()


@pytest.mark.asyncio
async def test_rollback_without_branch_detached(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Rollback without branch does detached HEAD checkout."""
    mock_ssh.run_in_repo.return_value = OK()
    await handlers["rollback"](server_host="staging", old_commit="deadbeef")
    checkout_cmd = mock_ssh.run_in_repo.call_args_list[0][0][1]
    assert "git checkout deadbeef" in checkout_cmd
    assert "-B" not in checkout_cmd


@pytest.mark.asyncio
async def test_rollback_force_recreates(handlers: dict, mock_ssh: AsyncMock) -> None:
    """After rollback, docker compose up --force-recreate is called."""
    mock_ssh.run_in_repo.return_value = OK()
    await handlers["rollback"](server_host="staging", old_commit="abc", branch="main")
    up_cmd = mock_ssh.run_in_repo.call_args_list[-1][0][1]
    assert "--force-recreate" in up_cmd


# ══════════════════════════════════════════════════════════
# db-checkpoint
# ══════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_db_checkpoint_default_command(handlers, mock_ssh):
    """db-checkpoint runs pgBackRest command by default."""
    mock_ssh.run.side_effect = [_make_ssh_result()]
    result = await handlers["db-checkpoint"](
        server_host="staging",
    )
    assert result["checkpoint_created"] is True
    cmd = mock_ssh.run.call_args_list[0].args[1]
    assert "pgbackrest" in cmd
    assert "flock" in cmd


@pytest.mark.asyncio
async def test_db_checkpoint_custom_command(handlers, mock_ssh):
    """db-checkpoint uses custom command when provided."""
    mock_ssh.run.side_effect = [_make_ssh_result()]
    result = await handlers["db-checkpoint"](
        server_host="staging",
        db_checkpoint_command="pg_dump -Fc mydb > /tmp/backup.custom",
    )
    assert result["checkpoint_created"] is True
    cmd = mock_ssh.run.call_args_list[0].args[1]
    assert "pg_dump" in cmd


@pytest.mark.asyncio
async def test_db_checkpoint_uses_server_container(handlers, mock_ssh):
    """db-checkpoint constructs command using server.container from config."""
    mock_ssh.run.side_effect = [_make_ssh_result()]
    await handlers["db-checkpoint"](server_host="staging")
    cmd = mock_ssh.run.call_args_list[0].args[1]
    # staging server container from conftest is "odoo19", so expect "odoo19-db"
    assert "-db" in cmd  # container name + "-db" suffix


# ══════════════════════════════════════════════════════════
# Multi-server / production scenarios
# ══════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_git_pull_production_server(prod_handlers: dict, mock_ssh: AsyncMock) -> None:
    """Handlers resolve production server correctly."""
    mock_ssh.run.return_value = _make_ssh_result(stdout="old\n")
    mock_ssh.run_in_repo.side_effect = [OK(), OK(), _make_ssh_result(stdout="new\n")]
    result = await prod_handlers["git-pull"](server_host="production", branch="main")
    assert result["has_changes"] is True


@pytest.mark.asyncio
async def test_unknown_server_raises(handlers: dict) -> None:
    """Requesting an unknown server raises ValueError."""
    with pytest.raises(ValueError, match="No server config"):
        await handlers["git-pull"](server_host="unknown_host", branch="main")


# ══════════════════════════════════════════════════════════
# Full deploy simulation (integration-like)
# ══════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_full_deploy_simulation(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Simulate a full deploy pipeline: pull → detect → build → up → update → smoke → verify → save-state."""

    # 1. git-pull
    mock_ssh.run.return_value = _make_ssh_result(stdout="aaa1111\n")
    mock_ssh.run_in_repo.side_effect = [OK(), OK(), _make_ssh_result(stdout="bbb2222\n")]
    pull_result = await handlers["git-pull"](server_host="staging", branch="staging")
    assert pull_result["has_changes"] is True

    # 2. detect-modules
    mock_ssh.run_in_repo.reset_mock()
    mock_ssh.run_in_repo.side_effect = [
        _make_ssh_result(stdout="2\n"),
        _make_ssh_result(stdout="src/custom/tut_hr/models/hr.py\n"),
        _make_ssh_result(stdout="yes\n"),
        _make_ssh_result(stdout=""), _make_ssh_result(stdout=""),
        _make_ssh_result(stdout=""),
        _make_ssh_result(stdout=""),
    ]
    detect_result = await handlers["detect-modules"](
        server_host="staging",
        old_commit=pull_result["old_commit"],
        new_commit=pull_result["new_commit"],
    )
    assert detect_result["changed_modules"] == "tut_hr"
    assert detect_result["docker_build_needed"] is False

    # 3. docker-build (skip — not needed)

    # 4. docker-up
    mock_ssh.run.reset_mock()
    mock_ssh.run_in_repo.reset_mock()
    mock_ssh.run_in_repo.side_effect = None
    mock_ssh.run_in_repo.return_value = OK()
    mock_ssh.run.side_effect = [
        _make_ssh_result(stdout="running\n"),
        _make_ssh_result(exit_code=0),
        OK(),
    ]
    with patch("worker.handlers.deploy._sleep", new_callable=AsyncMock):
        await handlers["docker-up"](server_host="staging")

    # 5. module-update
    mock_ssh.run.reset_mock()
    mock_ssh.run_in_repo.reset_mock()
    mock_ssh.run.side_effect = None
    mock_ssh.run_in_repo.side_effect = None
    mock_ssh.run.side_effect = [
        _make_ssh_result(stdout="dbpass\n"),
        _make_ssh_result(stdout="tut_hr\nsale\n"),
        OK(),
    ]
    mock_ssh.run_in_repo.side_effect = [OK(), OK(), OK(), OK()]
    update_result = await handlers["module-update"](
        server_host="staging",
        changed_modules=detect_result["changed_modules"],
    )
    assert update_result["modules_updated"] == "tut_hr"

    # 6. smoke-test
    mock_ssh.run.reset_mock()
    mock_ssh.run_in_repo.reset_mock()
    mock_ssh.run.side_effect = None
    mock_ssh.run_in_repo.side_effect = None
    mock_ssh.run.return_value = _make_ssh_result(stdout="dbpass\n")
    mock_ssh.run_in_repo.side_effect = [
        OK(), OK(),
        _make_ssh_result(stdout="INFO odoo: Modules loaded.\n"),
        OK(),
    ]
    with patch("worker.handlers.deploy._sleep", new_callable=AsyncMock):
        smoke_result = await handlers["smoke-test"](server_host="staging")
    assert smoke_result["smoke_passed"] is True  # no errors → returns True

    # 7. http-verify
    mock_ssh.run.reset_mock()
    mock_ssh.run.side_effect = None
    mock_ssh.run.return_value = OK()
    with patch("worker.handlers.deploy._sleep", new_callable=AsyncMock):
        await handlers["http-verify"](server_host="staging")

    # 8. save-deploy-state
    mock_ssh.run.reset_mock()
    mock_ssh.run.side_effect = None
    mock_ssh.run.return_value = OK()
    await handlers["save-deploy-state"](
        server_host="staging", branch="staging",
        new_commit=pull_result["new_commit"],
    )
    cmd = mock_ssh.run.call_args[0][1]
    assert pull_result["new_commit"] in cmd


@pytest.mark.asyncio
async def test_deploy_with_rollback_on_smoke_failure(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Simulate deploy where smoke test fails and rollback is triggered."""

    # 1. git-pull
    mock_ssh.run.return_value = _make_ssh_result(stdout="oldcommit\n")
    mock_ssh.run_in_repo.side_effect = [OK(), OK(), _make_ssh_result(stdout="newcommit\n")]
    pull_result = await handlers["git-pull"](server_host="staging", branch="staging")

    # 2-5. detect + up + update (assume they pass)

    # 6. smoke-test fails
    mock_ssh.run.reset_mock()
    mock_ssh.run_in_repo.reset_mock()
    mock_ssh.run.side_effect = None
    mock_ssh.run_in_repo.side_effect = None
    mock_ssh.run.return_value = _make_ssh_result(stdout="dbpass\n")
    mock_ssh.run_in_repo.side_effect = [
        OK(), OK(),
        _make_ssh_result(stdout="CRITICAL: database migration failed\n", exit_code=1),
        OK(),
    ]
    with patch("worker.handlers.deploy._sleep", new_callable=AsyncMock):
        with pytest.raises(RuntimeError, match="Smoke test failed"):
            await handlers["smoke-test"](server_host="staging")

    # 7. rollback (in real BPMN this is triggered by the Error Event Subprocess)
    mock_ssh.run_in_repo.reset_mock()
    mock_ssh.run_in_repo.side_effect = None
    mock_ssh.run_in_repo.return_value = OK()
    await handlers["rollback"](
        server_host="staging",
        old_commit=pull_result["old_commit"],
        branch="staging",
    )
    checkout_cmd = mock_ssh.run_in_repo.call_args_list[0][0][1]
    assert "oldcommit" in checkout_cmd


@pytest.mark.asyncio
async def test_first_deploy_full_flow(handlers: dict, mock_ssh: AsyncMock) -> None:
    """First deploy — old_commit=none → detect returns 'all' → full build."""

    # git-pull (first deploy)
    mock_ssh.run.return_value = _make_ssh_result(stdout="none\n")
    mock_ssh.run_in_repo.side_effect = [OK(), OK(), _make_ssh_result(stdout="abc123\n")]
    pull = await handlers["git-pull"](server_host="staging", branch="main")
    assert pull["old_commit"] == "none"

    # detect → all
    detect = await handlers["detect-modules"](
        server_host="staging", old_commit="none", new_commit="abc123",
    )
    assert detect["changed_modules"] == "all"
    assert detect["docker_build_needed"] is True

    # docker-build
    mock_ssh.run_in_repo.reset_mock()
    mock_ssh.run_in_repo.side_effect = None
    mock_ssh.run_in_repo.return_value = OK()
    with patch("worker.handlers.deploy._asyncio.sleep", new_callable=AsyncMock):
        await handlers["docker-build"](server_host="staging")

    # Rollback on first deploy — no old commit, should skip
    mock_ssh.run_in_repo.reset_mock()
    mock_ssh.run_in_repo.side_effect = None
    result = await handlers["rollback"](server_host="staging", old_commit="none")
    assert result == {}
    mock_ssh.run_in_repo.assert_not_awaited()
