"""Tests for worker.handlers.audit — audit analysis handler."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from worker.config import AppConfig, ServerConfig
from worker.handlers.audit import register_audit_handlers
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
    register_audit_handlers(worker, config, ssh)
    return handlers


@pytest.fixture
def kozak_config() -> AppConfig:
    return AppConfig(
        servers={
            "kozak_demo": ServerConfig(
                host="kozak.example.com",
                ssh_user="deploy",
            ),
        },
    )


@pytest.fixture
def mock_ssh() -> AsyncMock:
    ssh = AsyncMock()
    ssh.run = AsyncMock()
    return ssh


@pytest.fixture
def handlers(kozak_config: AppConfig, mock_ssh: AsyncMock) -> dict:
    return _extract_handlers(kozak_config, mock_ssh)


# ── audit-analysis ────────────────────────────────────────


@pytest.mark.asyncio
async def test_audit_no_changed_modules(handlers: dict) -> None:
    result = await handlers["audit-analysis"](changed_modules="")
    assert result["audit_conflicts"] == 0
    assert result["audit_critical"] == 0
    assert result["audit_warning"] == 0
    assert result["audit_report"] == ""


@pytest.mark.asyncio
async def test_audit_success_with_conflicts(handlers: dict, mock_ssh: AsyncMock) -> None:
    audit_output = json.dumps({
        "conflicts": [
            {"id": 1, "severity": "critical", "type": "python_override",
             "custom_module": "tut_hr", "target": "hr.employee.write",
             "base_file": "src/enterprise/hr/models/hr.py"},
            {"id": 2, "severity": "warning", "type": "js_patch",
             "custom_module": "tut_web", "target": "WebClient",
             "base_module": "web"},
        ],
        "stats": {"total": 2, "critical": 1, "warning": 1, "info": 0},
        "extension_points": 15,
    })
    mock_ssh.run.side_effect = [
        _make_ssh_result(),  # write script
        _make_ssh_result(),  # git add -N
        _make_ssh_result(stdout=audit_output),  # run script
        _make_ssh_result(),  # rm script
    ]
    result = await handlers["audit-analysis"](changed_modules="hr, web")
    assert result["audit_conflicts"] == 2
    assert result["audit_critical"] == 1
    assert result["audit_warning"] == 1
    assert "tut_hr" in result["audit_report"]


@pytest.mark.asyncio
async def test_audit_script_failure(handlers: dict, mock_ssh: AsyncMock) -> None:
    mock_ssh.run.side_effect = [
        _make_ssh_result(),  # write script
        _make_ssh_result(),  # git add -N
        _make_ssh_result(stdout="", exit_code=1),  # script failed
        _make_ssh_result(),  # rm script
    ]
    result = await handlers["audit-analysis"](changed_modules="sale")
    assert result["audit_conflicts"] == 0
    assert result["audit_critical"] == 0


@pytest.mark.asyncio
async def test_audit_invalid_json(handlers: dict, mock_ssh: AsyncMock) -> None:
    mock_ssh.run.side_effect = [
        _make_ssh_result(),  # write script
        _make_ssh_result(),  # git add -N
        _make_ssh_result(stdout="not valid json{"),  # bad output
        _make_ssh_result(),  # rm script
    ]
    result = await handlers["audit-analysis"](changed_modules="sale")
    assert result["audit_conflicts"] == 0
    assert "JSON parse error" in result["audit_report"]


@pytest.mark.asyncio
async def test_audit_cleanup(handlers: dict, mock_ssh: AsyncMock) -> None:
    mock_ssh.run.side_effect = [
        _make_ssh_result(),  # write script
        _make_ssh_result(),  # git add -N
        _make_ssh_result(stdout="", exit_code=1),  # script failed
        _make_ssh_result(),  # rm script
    ]
    await handlers["audit-analysis"](changed_modules="sale")
    # Verify rm -f was called (last ssh.run call)
    last_call = mock_ssh.run.call_args_list[-1]
    assert "rm -f" in last_call[0][1]
