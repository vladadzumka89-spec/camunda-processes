"""Tests for worker2.handlers.staging_sync — staging-nfs-deliver task."""
from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from worker2.config import AppConfig, ServerConfig
from worker2.handlers.staging_sync import register_staging_sync_handlers
from worker2.ssh import CommandResult


def _make_ssh_result(stdout: str = "", stderr: str = "", exit_code: int = 0) -> CommandResult:
    return CommandResult(stdout=stdout, stderr=stderr, exit_code=exit_code)


def _extract_handlers(config: AppConfig, ssh: AsyncMock) -> dict:
    handlers: dict = {}

    def task_decorator(task_type: str, **kwargs):
        def wrapper(fn):
            handlers[task_type] = fn
            return fn
        return wrapper

    worker = MagicMock()
    worker.task = task_decorator
    register_staging_sync_handlers(worker, config, ssh)
    return handlers


@pytest.fixture
def sync_config() -> AppConfig:
    return AppConfig(
        ssh_key_path="/root/.ssh/id_ed25519",
        servers={
            "kozak_demo": ServerConfig(host="kozak.example.com", ssh_user="root"),
            "staging": ServerConfig(host="staging.example.com", ssh_user="root"),
            "production": ServerConfig(host="prod.example.com", ssh_user="root"),
        },
    )


@pytest.fixture
def mock_ssh() -> AsyncMock:
    ssh = AsyncMock()
    ssh.run = AsyncMock(return_value=_make_ssh_result(stdout="dummy"))
    return ssh


@pytest.fixture
def handlers(sync_config: AppConfig, mock_ssh: AsyncMock) -> dict:
    return _extract_handlers(sync_config, mock_ssh)


def _make_nfs_conn_mock() -> MagicMock:
    """Async context manager mock for asyncssh.connect(NFS_HOST)."""
    conn = AsyncMock()
    conn.run = AsyncMock()
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=conn)
    cm.__aexit__ = AsyncMock(return_value=False)
    return cm


@pytest.mark.asyncio
async def test_nfs_deliver_success_returns_empty_dict(
    handlers: dict, mock_ssh: AsyncMock,
) -> None:
    """On success, staging-nfs-deliver returns {}."""
    with patch("worker2.handlers.staging_sync.asyncssh.connect", return_value=_make_nfs_conn_mock()):
        with patch("worker2.handlers.staging_sync._stream_file", new_callable=AsyncMock):
            result = await handlers["staging-nfs-deliver"]()

    assert result == {}


@pytest.mark.asyncio
async def test_nfs_deliver_ssh_failure_returns_empty_dict(
    handlers: dict, mock_ssh: AsyncMock,
) -> None:
    """If SSH dump on kozak fails, staging-nfs-deliver still returns {} (non-critical)."""
    mock_ssh.run.side_effect = RuntimeError("SSH connection refused")

    result = await handlers["staging-nfs-deliver"]()

    assert result == {}


@pytest.mark.asyncio
async def test_nfs_deliver_stream_failure_returns_empty_dict(
    handlers: dict, mock_ssh: AsyncMock,
) -> None:
    """If _stream_file fails, staging-nfs-deliver still returns {} (non-critical)."""
    with patch("worker2.handlers.staging_sync.asyncssh.connect", return_value=_make_nfs_conn_mock()):
        with patch(
            "worker2.handlers.staging_sync._stream_file",
            side_effect=RuntimeError("SFTP write error"),
        ):
            result = await handlers["staging-nfs-deliver"]()

    assert result == {}


@pytest.mark.asyncio
async def test_nfs_deliver_filename_contains_today(
    handlers: dict, mock_ssh: AsyncMock,
) -> None:
    """Destination path includes today's date in odoo_anon_YYYY-MM-DD.sql.zst format."""
    today = datetime.now().strftime("%Y-%m-%d")
    expected_filename = f"odoo_anon_{today}.sql.zst"

    captured: list = []

    async def capture_stream(*args, **kwargs) -> None:
        captured.append(kwargs)

    with patch("worker2.handlers.staging_sync.asyncssh.connect", return_value=_make_nfs_conn_mock()):
        with patch("worker2.handlers.staging_sync._stream_file", side_effect=capture_stream):
            await handlers["staging-nfs-deliver"]()

    assert len(captured) == 1
    dst_path = captured[0]["dst_path"]
    assert expected_filename in dst_path


@pytest.mark.asyncio
async def test_nfs_deliver_streams_from_kozak_to_nfs_host(
    handlers: dict, mock_ssh: AsyncMock,
) -> None:
    """Stream source is kozak_demo host, destination is 10.1.1.99."""
    captured: list = []

    async def capture_stream(*args, **kwargs) -> None:
        captured.append(kwargs)

    with patch("worker2.handlers.staging_sync.asyncssh.connect", return_value=_make_nfs_conn_mock()):
        with patch("worker2.handlers.staging_sync._stream_file", side_effect=capture_stream):
            await handlers["staging-nfs-deliver"]()

    assert len(captured) == 1
    assert captured[0]["src_host"] == "kozak.example.com"
    assert captured[0]["dst_host"] == "10.1.1.99"
    assert captured[0]["dst_path"].startswith("/mnt/borys-nfs-import-db/")
    assert captured[0]["key_path"] == "/root/.ssh/id_ed25519"
