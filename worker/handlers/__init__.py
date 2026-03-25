"""Handler registration for Camunda task types.

This worker handles ONLY non-CI/CD tasks:
  - http-request-smart (Odoo webhooks, server actions)
  - invoice-data-extractor (OCR)

CI/CD tasks (github, deploy, sync, audit, clickbot, notify)
are handled exclusively by worker2.
"""

from __future__ import annotations

from pyzeebe import ZeebeWorker

from ..config import AppConfig
from ..github_client import GitHubClient
from ..odoo_client import OdooClient
from ..ssh import AsyncSSHClient
from ..http_request_smart import register_http_smart_handlers
from .ocr import register_ocr_handlers


def register_all_handlers(
    worker: ZeebeWorker,
    config: AppConfig,
    ssh: AsyncSSHClient,
    github: GitHubClient,
    odoo: OdooClient,
) -> None:
    """Register non-CI/CD task handlers with the Zeebe worker.

    Task types registered:
        HTTP Smart (1): http-request-smart
        OCR (1):        invoice-data-extractor
    Total: 2 task types

    CI/CD tasks (deploy, github, sync, audit, clickbot, notify)
    are handled by worker2 — do NOT register them here.
    """
    register_http_smart_handlers(worker, config)
    register_ocr_handlers(worker, config)
