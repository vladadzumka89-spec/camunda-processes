"""Async SSH client for executing commands on remote servers."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Optional

import asyncssh

from .config import ServerConfig

logger = logging.getLogger(__name__)


@dataclass
class CommandResult:
    """Result of a remote command execution."""

    stdout: str
    stderr: str
    exit_code: int

    @property
    def success(self) -> bool:
        return self.exit_code == 0

    def check(self, message: str = 'Command failed') -> CommandResult:
        """Raise if command failed."""
        if not self.success:
            error = self.stderr.strip() or self.stdout[:500]
            raise RemoteCommandError(
                f'{message} (exit code {self.exit_code}): {error}'
            )
        return self


class RemoteCommandError(Exception):
    """Raised when a remote command fails."""


class AsyncSSHClient:
    """SSH client with connection pooling for executing remote commands."""

    def __init__(self, key_path: str = '') -> None:
        self._key_path = key_path
        self._connections: dict[str, asyncssh.SSHClientConnection] = {}
        self._lock = asyncio.Lock()

    async def _get_connection(self, server: ServerConfig) -> asyncssh.SSHClientConnection:
        """Get or create an SSH connection to the server."""
        key = f'{server.ssh_user}@{server.host}:{server.ssh_port}'

        async with self._lock:
            conn = self._connections.get(key)
            if conn is not None:
                try:
                    await conn.run('true', check=True, timeout=5)
                except Exception:
                    self._connections.pop(key, None)
                    conn = None

            if conn is None:
                connect_kwargs: dict = {
                    'host': server.host,
                    'port': server.ssh_port,
                    'username': server.ssh_user,
                    'known_hosts': None,
                }
                if self._key_path:
                    connect_kwargs['client_keys'] = [self._key_path]

                logger.info('Connecting to %s', key)
                conn = await asyncssh.connect(**connect_kwargs)
                self._connections[key] = conn

        return conn

    async def run(
        self,
        server: ServerConfig,
        command: str,
        timeout: int = 120,
        check: bool = False,
        env: Optional[dict[str, str]] = None,
    ) -> CommandResult:
        """Execute a command on the remote server.

        Args:
            server: Target server configuration.
            command: Shell command to execute.
            timeout: Command timeout in seconds.
            check: If True, raise on non-zero exit code.
            env: Additional environment variables.
        """
        conn = await self._get_connection(server)

        if env:
            env_prefix = ' '.join(f'{k}={v}' for k, v in env.items())
            command = f'{env_prefix} {command}'

        logger.debug('SSH %s: %s', server.host, command[:200])

        try:
            result = await asyncio.wait_for(
                conn.run(command, check=False),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            raise RemoteCommandError(
                f'Command timed out after {timeout}s on {server.host}: {command[:100]}'
            )

        cmd_result = CommandResult(
            stdout=result.stdout or '',
            stderr=result.stderr or '',
            exit_code=result.exit_status or 0,
        )

        if check:
            cmd_result.check(f'Failed on {server.host}')

        return cmd_result

    async def run_in_repo(
        self,
        server: ServerConfig,
        command: str,
        timeout: int = 120,
        check: bool = False,
        env: Optional[dict[str, str]] = None,
    ) -> CommandResult:
        """Execute a command inside the repo directory on the remote server."""
        return await self.run(
            server,
            f'cd {server.repo_dir} && {command}',
            timeout=timeout,
            check=check,
            env=env,
        )

    async def close(self) -> None:
        """Close all SSH connections."""
        async with self._lock:
            for key, conn in self._connections.items():
                logger.info('Closing SSH connection: %s', key)
                conn.close()
            self._connections.clear()
