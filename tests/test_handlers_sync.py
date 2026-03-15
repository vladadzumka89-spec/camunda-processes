"""Tests for worker.handlers.sync — upstream sync handlers."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from worker.config import AppConfig, ServerConfig
from worker.handlers.sync import register_sync_handlers
from worker.ssh import CommandResult


def _make_ssh_result(stdout: str = "", stderr: str = "", exit_code: int = 0) -> CommandResult:
    return CommandResult(stdout=stdout, stderr=stderr, exit_code=exit_code)


def _extract_handlers(config: AppConfig, ssh: AsyncMock, github: AsyncMock) -> dict:
    """Register handlers and capture them from mock worker."""
    handlers = {}

    def task_decorator(task_type: str, **kwargs):
        def wrapper(fn):
            handlers[task_type] = fn
            return fn
        return wrapper

    worker = MagicMock()
    worker.task = task_decorator
    register_sync_handlers(worker, config, ssh, github)
    return handlers


@pytest.fixture
def kozak_config() -> AppConfig:
    """Config with kozak_demo server for sync handlers."""
    return AppConfig(
        servers={
            "kozak_demo": ServerConfig(
                host="kozak.example.com",
                ssh_user="deploy",
                repo_dir="/opt/odoo-enterprise",
            ),
            "staging": ServerConfig(
                host="staging.example.com",
                ssh_user="deploy",
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
def mock_github() -> AsyncMock:
    github = AsyncMock()
    github.mark_pr_ready = AsyncMock()
    github.get_pr = AsyncMock(return_value={
        "html_url": "https://github.com/tut-ua/odoo-enterprise/pull/99",
        "title": "Sync PR",
        "user": {"login": "bot"},
        "base": {"ref": "staging"},
        "head": {"ref": "sync/upstream-test"},
    })
    return github


@pytest.fixture
def handlers(kozak_config: AppConfig, mock_ssh: AsyncMock, mock_github: AsyncMock) -> dict:
    return _extract_handlers(kozak_config, mock_ssh, mock_github)


# ── fetch-current-version ─────────────────────────────────


@pytest.mark.asyncio
async def test_fetch_current_version(handlers: dict, mock_ssh: AsyncMock) -> None:
    mock_ssh.run.side_effect = [
        _make_ssh_result(stdout="version_info = (19, 0, 0, FINAL, 0, '')\n"),
        _make_ssh_result(stdout='{"community_sha": "aaa", "enterprise_sha": "bbb"}\n'),
    ]
    result = await handlers["fetch-current-version"]()
    assert result["current_version"] == "19.0"
    assert result["current_community_sha"] == "aaa"
    assert result["current_enterprise_sha"] == "bbb"


# ── fetch-runbot ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_fetch_runbot(handlers: dict) -> None:
    runbot_data = {
        "19.0": {
            "commits": [
                {"repo": "odoo", "head": "com_sha_abc"},
                {"repo": "enterprise", "head": "ent_sha_def"},
            ]
        }
    }
    with patch("worker.handlers.sync.httpx.AsyncClient") as MockClient:
        instance = AsyncMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = runbot_data
        mock_resp.raise_for_status = MagicMock()
        instance.get = AsyncMock(return_value=mock_resp)
        instance.__aenter__ = AsyncMock(return_value=instance)
        instance.__aexit__ = AsyncMock(return_value=False)
        MockClient.return_value = instance

        result = await handlers["fetch-runbot"]()

    assert result["runbot_community_sha"] == "com_sha_abc"
    assert result["runbot_enterprise_sha"] == "ent_sha_def"


@pytest.mark.asyncio
async def test_fetch_runbot_incomplete(handlers: dict) -> None:
    runbot_data = {
        "19.0": {
            "commits": [
                {"repo": "odoo", "head": "com_sha"},
            ]
        }
    }
    with patch("worker.handlers.sync.httpx.AsyncClient") as MockClient:
        instance = AsyncMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = runbot_data
        mock_resp.raise_for_status = MagicMock()
        instance.get = AsyncMock(return_value=mock_resp)
        instance.__aenter__ = AsyncMock(return_value=instance)
        instance.__aexit__ = AsyncMock(return_value=False)
        MockClient.return_value = instance

        with pytest.raises(ValueError, match="Incomplete Runbot data"):
            await handlers["fetch-runbot"]()


# ── diff-report ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_diff_report_no_changes(handlers: dict, mock_ssh: AsyncMock) -> None:
    mock_ssh.run.side_effect = [
        _make_ssh_result(),  # git add -N
        _make_ssh_result(stdout="0\n"),  # community check
        _make_ssh_result(stdout="0\n"),  # enterprise check
    ]
    result = await handlers["diff-report"](workspace_dir="/tmp/ws")
    assert result["has_changes"] is False


@pytest.mark.asyncio
async def test_diff_report_with_changes(handlers: dict, mock_ssh: AsyncMock) -> None:
    mock_ssh.run.side_effect = [
        _make_ssh_result(),  # git add -N
        _make_ssh_result(stdout="1\n"),  # community: changed
        _make_ssh_result(stdout="1\n"),  # enterprise: changed
        _make_ssh_result(stdout="5\n"),  # community file count
        _make_ssh_result(stdout="3\n"),  # enterprise file count
        _make_ssh_result(stdout="sale\naccount\n"),  # enterprise module names
        _make_ssh_result(stdout="base\n"),  # community addons
    ]
    result = await handlers["diff-report"](workspace_dir="/tmp/ws")
    assert result["has_changes"] is True
    assert result["enterprise_files"] == 3
    assert "sale" in result["changed_modules"]


# ── impact-analysis ───────────────────────────────────────


@pytest.mark.asyncio
async def test_impact_analysis_empty(handlers: dict) -> None:
    result = await handlers["impact-analysis"](changed_modules="")
    assert result["affected_custom_count"] == 0


@pytest.mark.asyncio
async def test_impact_analysis_finds_affected(handlers: dict, mock_ssh: AsyncMock) -> None:
    mock_ssh.run.side_effect = [
        # find custom modules
        _make_ssh_result(stdout="/tmp/sync-workspace/src/custom/tut_hr\n"),
        # read __manifest__.py
        _make_ssh_result(stdout="{'name': 'TUT HR', 'depends': ['hr', 'sale']}"),
    ]
    result = await handlers["impact-analysis"](changed_modules="sale, account", workspace_dir="/tmp/ws")
    assert result["affected_custom_count"] == 1
    assert "tut_hr" in result["impact_table"]


# ── sync-code-to-demo ─────────────────────────────────────


@pytest.mark.asyncio
async def test_sync_code_to_demo_success(handlers: dict, mock_ssh: AsyncMock) -> None:
    mock_ssh.run.side_effect = [
        _make_ssh_result(),  # git fetch
        _make_ssh_result(),  # git checkout
    ]
    result = await handlers["sync-code-to-demo"](sync_branch="sync/upstream-20260225-120000")
    assert result == {"code_synced": True}

    calls = mock_ssh.run.call_args_list
    assert len(calls) == 2
    # Verify git fetch command
    assert "git fetch origin sync/upstream-20260225-120000" in calls[0].args[1]
    # Verify git checkout command
    assert "git checkout -B sync/upstream-20260225-120000 origin/sync/upstream-20260225-120000" in calls[1].args[1]


# ── merge-feature-to-staging ──────────────────────────────


def _make_mock_job(process_instance_key: int = 12345) -> MagicMock:
    job = MagicMock()
    job.process_instance_key = process_instance_key
    return job


@pytest.mark.asyncio
async def test_merge_feature_to_staging_success(handlers: dict, mock_ssh: AsyncMock) -> None:
    mock_ssh.run.side_effect = [
        _make_ssh_result(),  # git clone
        _make_ssh_result(),  # git fetch
        _make_ssh_result(exit_code=0),  # git merge (success)
        _make_ssh_result(),  # git push
        _make_ssh_result(stdout="deadbeef\n"),  # git rev-parse HEAD
        _make_ssh_result(),  # rm -rf cleanup
    ]
    job = _make_mock_job(99999)
    result = await handlers["merge-feature-to-staging"](
        job=job,
        feature_branch="feat/my-feature",
        server_host="staging",
    )
    assert result["staging_merged"] is True
    assert result["process_instance_key"] == 99999
    assert result["merge_sha"] == "deadbeef"

    calls = mock_ssh.run.call_args_list
    # Verify clone command
    assert "clone" in calls[0].args[1]
    assert "staging" in calls[0].args[1]
    # Verify fetch
    assert "fetch origin feat/my-feature" in calls[1].args[1]
    # Verify merge with -X theirs (feature branch wins on staging)
    assert "merge feat/my-feature" in calls[2].args[1]
    assert "-X theirs" in calls[2].args[1]
    # Verify push
    assert "push" in calls[3].args[1]


@pytest.mark.asyncio
async def test_merge_feature_to_staging_returns_merge_sha(
    handlers: dict, mock_ssh: AsyncMock,
) -> None:
    """merge-feature-to-staging should return merge_sha (HEAD after push)."""
    mock_ssh.run.side_effect = [
        _make_ssh_result(),                    # git clone
        _make_ssh_result(),                    # git fetch
        _make_ssh_result(exit_code=0),         # git merge
        _make_ssh_result(),                    # git push
        _make_ssh_result(stdout="abc123\n"),   # git rev-parse HEAD
        _make_ssh_result(),                    # rm -rf workspace
    ]
    job = _make_mock_job(99999)
    result = await handlers["merge-feature-to-staging"](
        job=job,
        feature_branch="feat/x",
        server_host="staging",
    )
    assert result["staging_merged"] is True
    assert result["merge_sha"] == "abc123"
    # Verify rev-parse was called
    calls = mock_ssh.run.call_args_list
    assert "rev-parse HEAD" in calls[4].args[1]


@pytest.mark.asyncio
async def test_merge_feature_to_staging_conflict(handlers: dict, mock_ssh: AsyncMock) -> None:
    mock_ssh.run.side_effect = [
        _make_ssh_result(),  # git clone
        _make_ssh_result(),  # git fetch
        _make_ssh_result(exit_code=1, stderr="CONFLICT"),  # git merge (conflict)
        _make_ssh_result(),  # git merge --abort
        _make_ssh_result(),  # rm -rf cleanup
    ]
    with pytest.raises(RuntimeError, match="Merge failed"):
        await handlers["merge-feature-to-staging"](
            job=_make_mock_job(),
            feature_branch="feat/conflicting",
            server_host="staging",
        )


@pytest.mark.asyncio
async def test_merge_feature_to_staging_missing_branch(handlers: dict) -> None:
    with pytest.raises(ValueError, match="feature_branch is required"):
        await handlers["merge-feature-to-staging"](job=_make_mock_job(), feature_branch="")


# ── github-pr-ready ───────────────────────────────────────


@pytest.mark.asyncio
async def test_github_pr_ready(
    handlers: dict,
    mock_github: AsyncMock,
    kozak_config: AppConfig,
) -> None:
    result = await handlers["github-pr-ready"](pr_number=99)

    assert result == {}
    mock_github.mark_pr_ready.assert_awaited_once()
