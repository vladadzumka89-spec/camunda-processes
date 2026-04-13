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
from ..http_request_smart import register_http_smart_handlers
from .fop_monitor import register_fop_monitor_handlers
from .fop_planner import register_fop_planner_handlers
from .ocr import register_ocr_handlers


def register_all_handlers(
    worker: ZeebeWorker,
    config: AppConfig,
) -> None:
    """Register non-CI/CD task handlers with the Zeebe worker.

    Task types registered:
        HTTP Smart (1): http-request-smart
        OCR (1):        invoice-data-extractor
        FOP Monitor (1): fop-limit-check
        FOP Planner (1): fop-opening-plan
    Total: 4 task types

    CI/CD tasks (deploy, github, sync, audit, clickbot, notify)
    are handled by worker2 — do NOT register them here.
    """
    register_http_smart_handlers(worker, config)
    register_ocr_handlers(worker, config)
    register_fop_monitor_handlers(worker, config)
    register_fop_planner_handlers(worker, config)
