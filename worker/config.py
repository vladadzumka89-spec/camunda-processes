"""Server configuration and environment loading for Camunda workers."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / '.env.camunda')


@dataclass(frozen=True)
class ServerConfig:
    """Configuration for a target server accessed via SSH."""

    host: str
    ssh_user: str
    repo_dir: str = '/opt/odoo-enterprise'
    db_name: str = 'odoo19'
    container: str = 'odoo19'
    port: int = 8069
    ssh_port: int = 22


@dataclass(frozen=True)
class ZeebeConfig:
    """Zeebe connection settings."""

    gateway_address: str = 'zeebe:26500'
    insecure: bool = True
    use_tls: bool = False
    client_id: str = ''
    client_secret: str = ''
    token_url: str = ''
    audience: str = ''


@dataclass(frozen=True)
class GitHubConfig:
    """GitHub API credentials."""

    token: str = ''
    deploy_pat: str = ''
    webhook_secret: str = ''
    repository: str = 'tut-ua/odoo-enterprise'


@dataclass(frozen=True)
class WebhookConfig:
    """Webhook server settings."""

    host: str = '0.0.0.0'
    port: int = 9001
    odoo_webhook_token: str = ''


@dataclass(frozen=True)
class OdooConfig:
    """Odoo webhook connection for task creation."""

    webhook_url: str = ''
    project_id: int = 0
    assignee_id: int = 0


@dataclass(frozen=True)
class AppConfig:
    """Root configuration assembled from environment variables."""

    zeebe: ZeebeConfig = field(default_factory=ZeebeConfig)
    github: GitHubConfig = field(default_factory=GitHubConfig)
    webhook: WebhookConfig = field(default_factory=WebhookConfig)
    odoo: OdooConfig = field(default_factory=OdooConfig)
    ssh_key_path: str = ''
    openrouter_api_key: str = ''
    servers: dict[str, ServerConfig] = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> AppConfig:
        """Build configuration from environment variables."""
        servers = {}
        for name in ('staging', 'production', 'kozak_demo'):
            prefix = name.upper()
            host = os.getenv(f'{prefix}_HOST', '')
            if not host:
                continue
            servers[name] = ServerConfig(
                host=host,
                ssh_user=os.getenv(f'{prefix}_SSH_USER', 'deploy'),
                repo_dir=os.getenv(f'{prefix}_REPO_DIR', '/opt/odoo-enterprise'),
                db_name=os.getenv(f'{prefix}_DB_NAME', 'odoo19'),
                container=os.getenv(f'{prefix}_CONTAINER', name),
                port=int(os.getenv(f'{prefix}_PORT', '8069')),
                ssh_port=int(os.getenv(f'{prefix}_SSH_PORT', '22')),
            )

        return cls(
            zeebe=ZeebeConfig(
                gateway_address=os.getenv('ZEEBE_ADDRESS', 'zeebe:26500'),
                use_tls=os.getenv('ZEEBE_USE_TLS', 'false').lower() == 'true',
                client_id=os.getenv('ZEEBE_CLIENT_ID', ''),
                client_secret=os.getenv('ZEEBE_CLIENT_SECRET', ''),
                token_url=os.getenv('ZEEBE_TOKEN_URL', ''),
                audience=os.getenv('ZEEBE_AUDIENCE', ''),
            ),
            github=GitHubConfig(
                token=os.getenv('GITHUB_TOKEN', ''),
                deploy_pat=os.getenv('DEPLOY_PAT', ''),
                webhook_secret=os.getenv('GITHUB_WEBHOOK_SECRET', ''),
                repository=os.getenv('REPOSITORY', 'tut-ua/odoo-enterprise'),
            ),
            webhook=WebhookConfig(
                host=os.getenv('WEBHOOK_HOST', '0.0.0.0'),
                port=int(os.getenv('WEBHOOK_PORT', '9001')),
                odoo_webhook_token=os.getenv('ODOO_WEBHOOK_TOKEN', ''),
            ),
            odoo=OdooConfig(
                webhook_url=os.getenv('ODOO_WEBHOOK_URL', ''),
                project_id=int(os.getenv('ODOO_PROJECT_ID', '0')),
                assignee_id=int(os.getenv('ODOO_ASSIGNEE_ID', '0')),
            ),
            ssh_key_path=str(Path.home() / '.ssh' / 'id_ed25519'),
            openrouter_api_key=os.getenv('OPENROUTER_API_KEY', ''),
            servers=servers,
        )

    def get_server(self, name: str) -> ServerConfig:
        """Get server config by name, raise if not found."""
        if name not in self.servers:
            raise ValueError(
                f"Server '{name}' not configured. Available: {list(self.servers.keys())}"
            )
        return self.servers[name]

    def resolve_server(self, server_host: str) -> ServerConfig:
        """Resolve server by host or name."""
        # Try by name first
        if server_host in self.servers:
            return self.servers[server_host]
        # Then by host
        for server in self.servers.values():
            if server.host == server_host:
                return server
        raise ValueError(f"No server config for '{server_host}'")
