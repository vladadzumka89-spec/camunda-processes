"""Handler registration for business task types (OCR, FOP monitor)."""

from __future__ import annotations

from pyzeebe import ZeebeWorker

from ..config import AppConfig
from ..http_request_smart import register_http_smart_handlers
from .ocr import register_ocr_handlers
from .fop_monitor import register_fop_monitor_handlers
from .messaging import register_messaging_handlers


def register_all_handlers(
    worker: ZeebeWorker,
    config: AppConfig,
) -> None:
    """Register business task handlers with the Zeebe worker.

    Task types registered:
        OCR (1):          invoice-data-extractor
        FOP Monitor (1):  fop-limit-check
        HTTP Smart (1):   http-request-smart
        Messaging (1):    publish-message
    Total: 4 task types
    """
    register_http_smart_handlers(worker, config)
    register_ocr_handlers(worker, config)
    register_fop_monitor_handlers(worker, config)
    register_messaging_handlers(worker, config)
