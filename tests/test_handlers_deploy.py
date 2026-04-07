"""Tests for worker.handlers.deploy — deploy process handlers."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from worker2.config import AppConfig, ServerConfig
from worker2.handlers.deploy import register_deploy_handlers
from worker2.ssh import CommandResult, RemoteCommandError


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
async def test_detect_modules_checksum_finds_changed(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Checksum scan returns changed module names."""
    mock_ssh.run_in_repo.return_value = OK("tut_custom\ndiscuss_folders\n")
    result = await handlers["detect-modules"](server_host="staging")
    assert result["changed_modules"] == "discuss_folders,tut_custom"


@pytest.mark.asyncio
async def test_detect_modules_checksum_no_changes(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Empty output means no modules changed."""
    mock_ssh.run_in_repo.return_value = OK("")
    result = await handlers["detect-modules"](server_host="staging")
    assert result["changed_modules"] == ""


@pytest.mark.asyncio
async def test_detect_modules_checksum_fallback_on_error(handlers: dict, mock_ssh: AsyncMock) -> None:
    """If checksum scan fails, fallback to 'all'."""
    mock_ssh.run_in_repo.return_value = FAIL("container not running")
    result = await handlers["detect-modules"](server_host="staging")
    assert result["changed_modules"] == "all"


@pytest.mark.asyncio
async def test_detect_modules_checksum_deduplicates(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Duplicate module names from multiple dirs are deduplicated."""
    mock_ssh.run_in_repo.return_value = OK("mod_a\nmod_b\nmod_a\n")
    result = await handlers["detect-modules"](server_host="staging")
    assert result["changed_modules"] == "mod_a,mod_b"


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
        OK(),  # docker compose run --rm --no-deps web odoo-bin
    ]
    result = await handlers["module-update"](
        server_host="staging", changed_modules="all",
    )
    assert result["modules_updated"] == "all"
    # Should contain -u all in the update command
    update_cmd = mock_ssh.run_in_repo.call_args_list[1][0][1]
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
        OK(),  # docker compose run --rm --no-deps web odoo-bin
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
async def test_module_update_installs_new_modules(handlers: dict, mock_ssh: AsyncMock) -> None:
    """New modules (not installed) get -i flag instead of being skipped."""
    mock_ssh.run.side_effect = [
        _make_ssh_result(stdout="dbpass\n"),  # password
        _make_ssh_result(stdout="base\nweb\n"),  # installed (none match changed)
        OK(),  # docker exec odoo-bin -i tut_new_module
        OK(),  # psql cache clear
    ]
    mock_ssh.run_in_repo.side_effect = [
        OK(),  # clean __pycache__
        OK(),  # docker compose build web
        OK(),  # docker compose up -d web
    ]
    result = await handlers["module-update"](
        server_host="staging", changed_modules="tut_new_module",
    )
    assert result["modules_updated"] == "tut_new_module"
    # Verify -i flag used
    odoo_cmd = mock_ssh.run.call_args_list[2][0][1]
    assert "-i tut_new_module" in odoo_cmd


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
        OK(),  # docker compose run --rm --no-deps web odoo-bin
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
        OK(),  # docker compose run --rm --no-deps web odoo-bin
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
    mock_ssh.run_in_repo.side_effect = [OK(), OK()]
    await handlers["module-update"](server_host="staging", changed_modules="all")
    pycache_cmd = mock_ssh.run_in_repo.call_args_list[0][0][1]
    assert "__pycache__" in pycache_cmd


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
        _make_ssh_result(stdout="INFO odoo: Modules loaded.\n"),  # docker compose run smoke test
    ]
    result = await handlers["smoke-test"](server_host="staging")
    assert result["smoke_passed"] is True


@pytest.mark.asyncio
async def test_smoke_test_fails_on_error(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Smoke test detects ERROR lines and raises RuntimeError."""
    mock_ssh.run.return_value = _make_ssh_result(stdout="dbpass\n")
    mock_ssh.run_in_repo.side_effect = [
        _make_ssh_result(
            stdout="ERROR odoo.modules: Failed to import module tut_hr\nTraceback blah\n",
        ),
    ]
    with pytest.raises(RuntimeError, match="Smoke test failed"):
        await handlers["smoke-test"](server_host="staging")


@pytest.mark.asyncio
async def test_smoke_test_fails_on_exit_code(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Non-zero exit code means failure — raises RuntimeError."""
    mock_ssh.run.return_value = _make_ssh_result(stdout="dbpass\n")
    mock_ssh.run_in_repo.side_effect = [
        _make_ssh_result(stdout="", exit_code=1),  # crashed
    ]
    with pytest.raises(RuntimeError, match="Smoke test failed"):
        await handlers["smoke-test"](server_host="staging")


@pytest.mark.asyncio
async def test_smoke_test_ignores_safe_warnings(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Known safe warnings are filtered out."""
    mock_ssh.run.return_value = _make_ssh_result(stdout="dbpass\n")
    mock_ssh.run_in_repo.side_effect = [
        _make_ssh_result(
            stdout="ERROR: Some modules are not loaded, ignored: crm_ext\n"
                   "ERROR: inconsistent states during test\n",
        ),
    ]
    result = await handlers["smoke-test"](server_host="staging")
    assert result["smoke_passed"] is True


@pytest.mark.asyncio
async def test_smoke_test_detects_critical(handlers: dict, mock_ssh: AsyncMock) -> None:
    """CRITICAL level is caught — raises RuntimeError."""
    mock_ssh.run.return_value = _make_ssh_result(stdout="dbpass\n")
    mock_ssh.run_in_repo.side_effect = [
        _make_ssh_result(stdout="CRITICAL odoo: database connection failed\n"),
    ]
    with pytest.raises(RuntimeError, match="Smoke test failed"):
        await handlers["smoke-test"](server_host="staging")


@pytest.mark.asyncio
async def test_smoke_test_detects_import_error(handlers: dict, mock_ssh: AsyncMock) -> None:
    """ImportError / ModuleNotFoundError detected — raises RuntimeError."""
    mock_ssh.run.return_value = _make_ssh_result(stdout="dbpass\n")
    mock_ssh.run_in_repo.side_effect = [
        _make_ssh_result(stdout="ImportError: No module named 'missing_dep'\n"),
    ]
    with pytest.raises(RuntimeError, match="Smoke test failed"):
        await handlers["smoke-test"](server_host="staging")


@pytest.mark.asyncio
async def test_smoke_test_detects_syntax_error(handlers: dict, mock_ssh: AsyncMock) -> None:
    """SyntaxError detected — raises RuntimeError."""
    mock_ssh.run.return_value = _make_ssh_result(stdout="dbpass\n")
    mock_ssh.run_in_repo.side_effect = [
        _make_ssh_result(stdout="SyntaxError: invalid syntax in /opt/odoo/src/custom/tut_hr/models.py\n"),
    ]
    with pytest.raises(RuntimeError, match="Smoke test failed"):
        await handlers["smoke-test"](server_host="staging")


@pytest.mark.asyncio
async def test_smoke_test_raises_on_error(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Smoke test should raise RuntimeError when errors are detected."""
    mock_ssh.run.side_effect = [_make_ssh_result(stdout="password123")]  # _get_db_password
    mock_ssh.run_in_repo.side_effect = [
        _make_ssh_result(stdout="2026 ERROR something broke\n", exit_code=1),
    ]
    with pytest.raises(RuntimeError, match="Smoke test failed"):
        await handlers["smoke-test"](server_host="staging")


@pytest.mark.asyncio
async def test_smoke_test_no_restart_on_failure(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Smoke test runs in a separate container — no stop/start/up of main service."""
    mock_ssh.run.return_value = _make_ssh_result(stdout="dbpass\n")
    mock_ssh.run_in_repo.side_effect = [
        _make_ssh_result(stdout="CRITICAL: boom\n", exit_code=1),
    ]
    with pytest.raises(RuntimeError, match="Smoke test failed"):
        await handlers["smoke-test"](server_host="staging")
    # Only 1 run_in_repo call — the docker compose run, no stop/start/up
    assert mock_ssh.run_in_repo.await_count == 1
    cmd = mock_ssh.run_in_repo.call_args[0][1]
    assert "docker compose run" in cmd


@pytest.mark.asyncio
async def test_smoke_test_db_password_fallback(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Password fallback to .env file works for smoke-test."""
    mock_ssh.run.side_effect = [
        _make_ssh_result(stdout="", exit_code=1),  # container env fails
    ]
    mock_ssh.run_in_repo.side_effect = [
        _make_ssh_result(stdout="envpass\n"),  # .env fallback
        _make_ssh_result(stdout="OK\n"),  # docker compose run smoke test
    ]
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
    assert result == {"state_saved": True}
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


@pytest.mark.asyncio
async def test_save_deploy_state_handles_ssh_error(handlers, mock_ssh):
    """save-deploy-state should not raise on SSH failure — returns state_saved=False."""
    from worker2.ssh import RemoteCommandError
    mock_ssh.run.side_effect = RemoteCommandError("SSH failed", 1, "")
    result = await handlers["save-deploy-state"](
        server_host="staging",
        branch="staging",
        new_commit="abc123",
    )
    assert result["state_saved"] is False


# ══════════════════════════════════════════════════════════
# rollback
# ══════════════════════════════════════════════════════════


def _make_rollback_handlers(mock_ssh: AsyncMock, db_checkpoint_base_url: str = "http://danylo:9090") -> dict:
    """Create handlers with db_checkpoint_base_url configured for rollback tests."""
    cfg = AppConfig(
        servers={
            "staging": ServerConfig(host="staging.example.com", ssh_user="deploy"),
            "production": ServerConfig(host="prod.example.com", ssh_user="deploy"),
        },
        db_checkpoint_base_url=db_checkpoint_base_url,
    )
    return _extract_handlers(cfg, mock_ssh)


def _mock_httpx_context():
    """Patch httpx.AsyncClient for rollback HTTP restore call."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status = MagicMock()
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_response)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    return mock_client


@pytest.mark.asyncio
async def test_rollback_no_checkpoint_url(handlers: dict, mock_ssh: AsyncMock) -> None:
    """Rollback returns restored=False when db_checkpoint_base_url is not configured."""
    result = await handlers["rollback"](server_host="staging", branch="staging")
    assert result == {"restored": False}
    mock_ssh.run.assert_not_awaited()


@pytest.mark.asyncio
async def test_rollback_force_push_no_merge_sha(mock_ssh: AsyncMock) -> None:
    """Rollback without merge_sha falls back to force-push after DB restore."""
    h = _make_rollback_handlers(mock_ssh)
    mock_ssh.run.return_value = OK()  # SSH connectivity check
    mock_ssh.run_in_repo.return_value = OK()  # force-push

    mock_client = _mock_httpx_context()
    with patch("worker2.handlers.deploy.httpx") as mock_httpx, \
         patch("worker2.handlers.deploy._sleep", new_callable=AsyncMock):
        mock_httpx.AsyncClient.return_value = mock_client
        result = await h["rollback"](server_host="staging", branch="staging")

    assert result["restored"] is True
    assert result["branch_fixed"] is True
    assert result["method"] == "force-push"
    # Verify force-push was called
    force_push_cmd = mock_ssh.run_in_repo.call_args_list[-1][0][1]
    assert "git push --force origin staging" in force_push_cmd


@pytest.mark.asyncio
async def test_rollback_revert_merge_sha(mock_ssh: AsyncMock) -> None:
    """Rollback with merge_sha reverts the merge commit instead of force-pushing."""
    h = _make_rollback_handlers(mock_ssh)
    mock_ssh.run.return_value = OK()  # SSH connectivity check
    mock_ssh.run_in_repo.return_value = OK()  # all git commands succeed

    mock_client = _mock_httpx_context()
    with patch("worker2.handlers.deploy.httpx") as mock_httpx, \
         patch("worker2.handlers.deploy._sleep", new_callable=AsyncMock):
        mock_httpx.AsyncClient.return_value = mock_client
        result = await h["rollback"](
            server_host="staging", branch="staging", merge_sha="abc123def456",
        )

    assert result["restored"] is True
    assert result["branch_fixed"] is True
    assert result["method"] == "revert"
    # Verify git revert was called (not force-push)
    all_cmds = [call[0][1] for call in mock_ssh.run_in_repo.call_args_list]
    assert any("git revert abc123def456 -m 1 --no-edit" in cmd for cmd in all_cmds)
    assert not any("--force" in cmd for cmd in all_cmds)


@pytest.mark.asyncio
async def test_rollback_revert_fails_falls_back_to_force_push(mock_ssh: AsyncMock) -> None:
    """When git revert fails, rollback falls back to force-push."""
    h = _make_rollback_handlers(mock_ssh)
    mock_ssh.run.return_value = OK()  # SSH connectivity check

    # First calls: fetch+reset OK, revert FAILS, revert --abort OK,
    # then force-push OK
    mock_ssh.run_in_repo.side_effect = [
        OK(),   # git fetch + reset
        FAIL("conflict"),  # git revert fails
        OK(),   # git revert --abort
        OK(),   # git push --force
    ]

    mock_client = _mock_httpx_context()
    with patch("worker2.handlers.deploy.httpx") as mock_httpx, \
         patch("worker2.handlers.deploy._sleep", new_callable=AsyncMock):
        mock_httpx.AsyncClient.return_value = mock_client
        result = await h["rollback"](
            server_host="staging", branch="staging", merge_sha="badmerge123",
        )

    assert result["restored"] is True
    assert result["branch_fixed"] is True
    assert result["method"] == "force-push"


@pytest.mark.asyncio
async def test_rollback_db_restore_url_correct(mock_ssh: AsyncMock) -> None:
    """Rollback calls correct restore URL based on server name."""
    h = _make_rollback_handlers(mock_ssh)
    mock_ssh.run.return_value = OK()
    mock_ssh.run_in_repo.return_value = OK()

    mock_client = _mock_httpx_context()
    with patch("worker2.handlers.deploy.httpx") as mock_httpx, \
         patch("worker2.handlers.deploy._sleep", new_callable=AsyncMock):
        mock_httpx.AsyncClient.return_value = mock_client
        await h["rollback"](server_host="production", branch="main")

    mock_client.post.assert_awaited_once()
    call_args = mock_client.post.call_args
    assert call_args.args[0] == "http://danylo:9090/restore/production"


@pytest.mark.asyncio
async def test_rollback_no_db_url_skips_restore(mock_ssh: AsyncMock) -> None:
    """Rollback skips HTTP restore when db_checkpoint_base_url is empty."""
    h = _make_rollback_handlers(mock_ssh, db_checkpoint_base_url="")

    with patch("worker2.handlers.deploy.httpx") as mock_httpx:
        result = await h["rollback"](server_host="staging", branch="staging")
        mock_httpx.AsyncClient.assert_not_called()

    assert result == {"restored": False}


@pytest.mark.asyncio
async def test_rollback_staging_restore_url(mock_ssh: AsyncMock) -> None:
    """Staging rollback calls HTTP restore with staging URL."""
    h = _make_rollback_handlers(mock_ssh)
    mock_ssh.run.return_value = OK()
    mock_ssh.run_in_repo.return_value = OK()

    mock_client = _mock_httpx_context()
    with patch("worker2.handlers.deploy.httpx") as mock_httpx, \
         patch("worker2.handlers.deploy._sleep", new_callable=AsyncMock):
        mock_httpx.AsyncClient.return_value = mock_client
        await h["rollback"](server_host="staging", branch="staging")

    mock_client.post.assert_awaited_once()
    call_args = mock_client.post.call_args
    assert call_args.args[0] == "http://danylo:9090/restore/staging"


@pytest.mark.asyncio
async def test_rollback_ssh_not_ready_returns_branch_not_fixed(mock_ssh: AsyncMock) -> None:
    """When VM never comes back after restore, branch_fixed is False."""
    h = _make_rollback_handlers(mock_ssh)
    mock_ssh.run.side_effect = Exception("Connection refused")

    mock_client = _mock_httpx_context()
    with patch("worker2.handlers.deploy.httpx") as mock_httpx, \
         patch("worker2.handlers.deploy._sleep", new_callable=AsyncMock):
        mock_httpx.AsyncClient.return_value = mock_client
        result = await h["rollback"](server_host="staging", branch="staging")

    assert result["restored"] is True
    assert result["branch_fixed"] is False


@pytest.mark.asyncio
async def test_rollback_empty_merge_sha_uses_force_push(mock_ssh: AsyncMock) -> None:
    """Empty string merge_sha skips revert and goes straight to force-push."""
    h = _make_rollback_handlers(mock_ssh)
    mock_ssh.run.return_value = OK()
    mock_ssh.run_in_repo.return_value = OK()

    mock_client = _mock_httpx_context()
    with patch("worker2.handlers.deploy.httpx") as mock_httpx, \
         patch("worker2.handlers.deploy._sleep", new_callable=AsyncMock):
        mock_httpx.AsyncClient.return_value = mock_client
        result = await h["rollback"](
            server_host="staging", branch="staging", merge_sha="",
        )

    assert result["method"] == "force-push"
    all_cmds = [call[0][1] for call in mock_ssh.run_in_repo.call_args_list]
    assert not any("git revert" in cmd for cmd in all_cmds)


# ══════════════════════════════════════════════════════════
# db-checkpoint
# ══════════════════════════════════════════════════════════


def _make_handlers_with_config(config: AppConfig, mock_ssh: AsyncMock) -> dict:
    """Extract handlers using a custom AppConfig."""
    return _extract_handlers(config, mock_ssh)


@pytest.mark.asyncio
async def test_db_checkpoint_calls_http(mock_ssh: AsyncMock) -> None:
    """db-checkpoint calls checkpoint URL via HTTP POST with auth token."""
    cfg = AppConfig(
        servers={"staging": ServerConfig(host="staging.example.com", ssh_user="deploy")},
        db_checkpoint_base_url="http://danylo:9090",
        db_checkpoint_token="test-token-123",
    )
    h = _make_handlers_with_config(cfg, mock_ssh)

    with patch("worker.handlers.deploy.httpx") as mock_httpx:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_httpx.AsyncClient.return_value = mock_client

        result = await h["db-checkpoint"](server_host="staging")

    assert result["checkpoint_created"] is True
    mock_client.post.assert_awaited_once()
    call_args = mock_client.post.call_args
    assert call_args.args[0] == "http://danylo:9090/checkpoint/staging"
    assert call_args.kwargs.get("headers", {}).get("X-Auth-Token") == "test-token-123"


@pytest.mark.asyncio
async def test_db_checkpoint_skips_without_url(mock_ssh: AsyncMock) -> None:
    """db-checkpoint skips if no URL configured."""
    cfg = AppConfig(
        servers={"staging": ServerConfig(host="staging.example.com", ssh_user="deploy")},
        db_checkpoint_base_url="",
    )
    h = _make_handlers_with_config(cfg, mock_ssh)
    result = await h["db-checkpoint"](server_host="staging")
    assert result["checkpoint_created"] is False


@pytest.mark.asyncio
async def test_db_checkpoint_no_token_omits_header(mock_ssh: AsyncMock) -> None:
    """db-checkpoint posts without X-Auth-Token header when token is empty."""
    cfg = AppConfig(
        servers={"staging": ServerConfig(host="staging.example.com", ssh_user="deploy")},
        db_checkpoint_base_url="http://danylo:9090",
        db_checkpoint_token="",
    )
    h = _make_handlers_with_config(cfg, mock_ssh)

    with patch("worker.handlers.deploy.httpx") as mock_httpx:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_httpx.AsyncClient.return_value = mock_client

        result = await h["db-checkpoint"](server_host="staging")

    assert result["checkpoint_created"] is True
    call_args = mock_client.post.call_args
    assert "X-Auth-Token" not in call_args.kwargs.get("headers", {})


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
        _make_ssh_result(stdout="INFO odoo: Modules loaded.\n"),
    ]
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
        _make_ssh_result(stdout="CRITICAL: database migration failed\n", exit_code=1),
    ]
    with pytest.raises(RuntimeError, match="Smoke test failed"):
        await handlers["smoke-test"](server_host="staging")

    # 7. rollback (in real BPMN this is triggered by the Error Event Subprocess)
    # Note: handlers fixture has no db_checkpoint_base_url, so rollback skips
    result = await handlers["rollback"](
        server_host="staging",
        branch="staging",
    )
    assert result == {"restored": False}


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

    # Rollback on first deploy — no db_checkpoint_base_url configured, skips
    result = await handlers["rollback"](server_host="staging", branch="main")
    assert result == {"restored": False}


# ══════════════════════════════════════════════════════════
# extract-deployed-prs
# ══════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_extract_deployed_prs_parses_pr_numbers(
    handlers: dict,
    mock_ssh: AsyncMock,
) -> None:
    """extract-deployed-prs parses PR numbers from git log output."""
    mock_ssh.run.return_value = CommandResult(
        stdout=(
            "abc1234 feat: add login page (#123)\n"
            "def5678 fix: broken redirect (#456)\n"
            "ghi9012 chore: update deps\n"
            "jkl3456 refactor: auth module (#789)\n"
        ),
        stderr="",
        exit_code=0,
    )

    handler = handlers["extract-deployed-prs"]
    result = await handler(
        old_commit="aaa0000",
        new_commit="bbb1111",
        server_host="staging",
    )

    assert result["deployed_prs"] == [123, 456, 789]
    mock_ssh.run.assert_awaited_once()
    call_args = mock_ssh.run.call_args
    assert "git log aaa0000..bbb1111" in str(call_args)


@pytest.mark.asyncio
async def test_extract_deployed_prs_empty_log(
    handlers: dict,
    mock_ssh: AsyncMock,
) -> None:
    """extract-deployed-prs returns empty array when no PRs found."""
    mock_ssh.run.return_value = CommandResult(
        stdout="abc1234 chore: no pr reference\n",
        stderr="",
        exit_code=0,
    )

    handler = handlers["extract-deployed-prs"]
    result = await handler(
        old_commit="aaa0000",
        new_commit="bbb1111",
        server_host="staging",
    )

    assert result["deployed_prs"] == []
