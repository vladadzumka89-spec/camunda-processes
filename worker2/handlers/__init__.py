"""Handler registration for CI/CD task types."""

from __future__ import annotations

from pyzeebe import ZeebeWorker

from ..config import AppConfig
from ..github_client import GitHubClient
from ..odoo_client import OdooClient
from ..ssh import AsyncSSHClient
from .audit import register_audit_handlers
from .clickbot import register_clickbot_handlers
from .deploy import register_deploy_handlers
from ..http_request_smart import register_http_smart_handlers
from .github import register_github_handlers
from .notify import register_notify_handlers
from .messaging import register_messaging_handlers
from .progress import register_progress_handlers
from .sync import register_sync_handlers
from .staging_sync import register_staging_sync_handlers


def register_all_handlers(
    worker: ZeebeWorker,
    config: AppConfig,
    ssh: AsyncSSHClient,
    github: GitHubClient,
    odoo: OdooClient,
) -> None:
    """Register CI/CD task handlers with the Zeebe worker.

    Task types registered:
        Deploy (10): git-pull, detect-modules, docker-build, docker-up,
                     module-update, cache-clear, smoke-test, http-verify,
                     save-deploy-state, rollback
        GitHub (4):  pr-agent-review, github-merge, github-comment,
                     github-create-pr
        Sync (9):    fetch-current-version, fetch-runbot, clone-upstream,
                     sync-modules, diff-report, impact-analysis,
                     git-commit-push, sync-code-to-demo, github-pr-ready
        Audit (1):   audit-analysis
        Clickbot (1): clickbot-test
        Notify (2):   send-notification, create-odoo-task
        HTTP Smart (1): http-request-smart
        Messaging (1):   publish-message
        Progress (1):    progress
        Staging sync (2): staging-dump, staging-deploy
    Total: 32 task types
    """
    register_http_smart_handlers(worker, config)
    register_deploy_handlers(worker, config, ssh)
    register_github_handlers(worker, config, ssh, github)
    register_sync_handlers(worker, config, ssh, github)
    register_audit_handlers(worker, config, ssh)
    register_clickbot_handlers(worker, config, ssh)
    register_notify_handlers(worker, config)
    register_messaging_handlers(worker, config)
    register_progress_handlers(worker, config)
    register_staging_sync_handlers(worker, config, ssh)
