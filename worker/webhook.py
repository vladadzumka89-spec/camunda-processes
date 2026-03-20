"""Webhook server for business worker — health check and FOP reports.

Endpoints:
    GET  /health              — Liveness probe
    GET  /reports/fop/latest  — Latest FOP limit monitoring JSON report
"""

from __future__ import annotations

import json
import logging

from aiohttp import web

from .config import AppConfig

logger = logging.getLogger(__name__)


class WebhookServer:
    """HTTP server with health check and FOP report endpoint."""

    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._app = web.Application()
        self._app.router.add_get('/health', self._handle_health)
        self._app.router.add_get('/reports/fop/latest', self._handle_fop_report)
        self._runner: web.AppRunner | None = None

    # -- Lifecycle -------------------------------------------------

    async def start(self) -> None:
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(
            self._runner,
            self._config.webhook.host,
            self._config.webhook.port,
        )
        await site.start()
        logger.info(
            "Webhook server listening on %s:%d",
            self._config.webhook.host,
            self._config.webhook.port,
        )
        try:
            while True:
                import asyncio
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()

    async def stop(self) -> None:
        if self._runner:
            await self._runner.cleanup()
            self._runner = None
            logger.info("Webhook server stopped")

    # -- Health check ----------------------------------------------

    async def _handle_health(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "ok"})

    # -- FOP report endpoint ---------------------------------------

    async def _handle_fop_report(self, request: web.Request) -> web.Response:
        """Serve latest FOP limit monitoring JSON report from file."""
        from .handlers.fop_monitor import REPORT_FILE

        if not REPORT_FILE.exists():
            return web.json_response(
                {"error": "Звіт ще не згенеровано"},
                status=404,
            )
        try:
            data = json.loads(REPORT_FILE.read_text())
            return web.json_response(data)
        except Exception as exc:
            logger.error("Failed to read FOP report: %s", exc)
            return web.Response(status=500, text=f"Failed to read report: {exc}")
