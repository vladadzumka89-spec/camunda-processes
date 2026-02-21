"""Webhook server — bridges GitHub/Odoo events to Camunda Zeebe processes.

Endpoints:
    POST /webhook/github  — GitHub PR events (HMAC-SHA256 verified)
    POST /webhook/odoo    — Odoo task closure callback (token auth)
    GET  /health          — Liveness probe

Zeebe messages published:
    msg_pr_event     — PR opened/reopened targeting staging → starts feature-to-production
    msg_pr_updated   — PR synchronize → correlates to running instance by pr_number
    msg_odoo_task_done — Odoo task closed → correlates to upstream-sync by odoo_task_id
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
from typing import Any

from aiohttp import web
from pyzeebe import ZeebeClient

from .auth import ZeebeAuthConfig, create_channel
from .config import AppConfig

logger = logging.getLogger(__name__)


class WebhookServer:
    """HTTP server that receives webhooks and publishes Zeebe messages."""

    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._app = web.Application()
        self._app.router.add_post('/webhook/github', self._handle_github)
        self._app.router.add_post('/webhook/odoo', self._handle_odoo)
        self._app.router.add_get('/health', self._handle_health)
        self._runner: web.AppRunner | None = None

    # ── Lifecycle ─────────────────────────────────────────

    async def start(self) -> None:
        """Start the webhook HTTP server."""
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
        # Block forever (until cancelled by shutdown)
        try:
            while True:
                import asyncio
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Gracefully shutdown the server."""
        if self._runner:
            await self._runner.cleanup()
            self._runner = None
            logger.info("Webhook server stopped")

    # ── Zeebe client (lazy, per-request) ──────────────────

    def _create_zeebe_client(self) -> ZeebeClient:
        """Create a ZeebeClient for publishing messages."""
        auth_config = ZeebeAuthConfig(
            gateway_address=self._config.zeebe.gateway_address,
            client_id=self._config.zeebe.client_id,
            client_secret=self._config.zeebe.client_secret,
            token_url=self._config.zeebe.token_url,
            audience=self._config.zeebe.audience,
            use_tls=self._config.zeebe.use_tls,
        )
        channel = create_channel(auth_config)
        return ZeebeClient(channel)

    # ── Health check ──────────────────────────────────────

    async def _handle_health(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "ok"})

    # ── GitHub webhook ────────────────────────────────────

    async def _handle_github(self, request: web.Request) -> web.Response:
        """Handle GitHub webhook: verify HMAC, route by event type."""
        body = await request.read()

        # 1. HMAC-SHA256 verification
        secret = self._config.github.webhook_secret
        if not secret:
            logger.error("GITHUB_WEBHOOK_SECRET not configured")
            return web.Response(status=500, text="Webhook secret not configured")

        signature = request.headers.get('X-Hub-Signature-256', '')
        if not self._verify_github_signature(body, secret, signature):
            logger.warning("Invalid GitHub webhook signature")
            return web.Response(status=401, text="Invalid signature")

        # 2. Parse event
        event_type = request.headers.get('X-GitHub-Event', '')
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            return web.Response(status=400, text="Invalid JSON")

        delivery_id = request.headers.get('X-GitHub-Delivery', 'unknown')
        logger.info("GitHub webhook: event=%s, delivery=%s", event_type, delivery_id)

        # 3. Route by event type
        if event_type == 'pull_request':
            return await self._route_pr_event(payload)

        # Ignore other events
        return web.json_response({"status": "ignored", "event": event_type})

    async def _route_pr_event(self, payload: dict) -> web.Response:
        """Route pull_request events to appropriate Zeebe messages."""
        action = payload.get('action', '')
        pr = payload.get('pull_request', {})
        base_branch = pr.get('base', {}).get('ref', '')
        pr_number = pr.get('number', 0)

        logger.info(
            "PR #%d action=%s, base=%s",
            pr_number, action, base_branch,
        )

        # Only process PRs targeting staging
        if base_branch != 'staging':
            logger.info("Ignoring PR #%d: targets %s (not staging)", pr_number, base_branch)
            return web.json_response({
                "status": "ignored",
                "reason": f"base_branch={base_branch}",
            })

        if action in ('opened', 'reopened'):
            return await self._publish_pr_event(pr, payload)
        elif action == 'synchronize':
            return await self._publish_pr_updated(pr)
        else:
            logger.info("Ignoring PR #%d action=%s", pr_number, action)
            return web.json_response({"status": "ignored", "action": action})

    async def _publish_pr_event(self, pr: dict, payload: dict) -> web.Response:
        """Publish msg_pr_event — starts a new feature-to-production process instance."""
        pr_number = pr.get('number', 0)
        repo_full = payload.get('repository', {}).get('full_name', self._config.github.repository)

        variables: dict[str, Any] = {
            "pr_number": pr_number,
            "pr_url": pr.get('html_url', ''),
            "pr_title": pr.get('title', ''),
            "pr_author": pr.get('user', {}).get('login', ''),
            "repository": repo_full,
            "base_branch": pr.get('base', {}).get('ref', 'staging'),
            "head_branch": pr.get('head', {}).get('ref', ''),
        }

        # Inject server configs (required by call_deploy_staging/prod BPMN inputs)
        staging = self._config.servers.get('staging')
        if staging:
            variables.update({
                "staging_host": staging.host,
                "staging_ssh_user": staging.ssh_user,
                "staging_repo_dir": staging.repo_dir,
                "staging_db": staging.db_name,
                "staging_container": staging.container,
            })

        production = self._config.servers.get('production')
        if production:
            variables.update({
                "production_host": production.host,
                "production_ssh_user": production.ssh_user,
                "production_repo_dir": production.repo_dir,
                "production_db": production.db_name,
                "production_container": production.container,
            })

        try:
            client = self._create_zeebe_client()
            await client.publish_message(
                name="msg_pr_event",
                correlation_key=variables.get("head_branch", ""),
                variables=variables,
            )
            logger.info(
                "Published msg_pr_event for PR #%d (%s)",
                pr_number, pr.get('title', ''),
            )
            return web.json_response({
                "status": "published",
                "message": "msg_pr_event",
                "pr_number": pr_number,
            })
        except Exception as exc:
            logger.error("Failed to publish msg_pr_event for PR #%d: %s", pr_number, exc)
            return web.Response(status=502, text=f"Zeebe publish failed: {exc}")

    async def _publish_pr_updated(self, pr: dict) -> web.Response:
        """Publish msg_pr_updated — correlates to a running process by pr_number."""
        pr_number = pr.get('number', 0)

        try:
            client = self._create_zeebe_client()
            await client.publish_message(
                name="msg_pr_updated",
                correlation_key=str(pr_number),
                variables={
                    "pr_updated": True,
                    "head_sha": pr.get('head', {}).get('sha', ''),
                },
            )
            logger.info("Published msg_pr_updated for PR #%d", pr_number)
            return web.json_response({
                "status": "published",
                "message": "msg_pr_updated",
                "pr_number": pr_number,
            })
        except Exception as exc:
            logger.error("Failed to publish msg_pr_updated for PR #%d: %s", pr_number, exc)
            return web.Response(status=502, text=f"Zeebe publish failed: {exc}")

    # ── Odoo webhook ──────────────────────────────────────

    async def _handle_odoo(self, request: web.Request) -> web.Response:
        """Handle Odoo task closure callback — simple token auth."""
        # Token verification
        expected_token = self._config.webhook.odoo_webhook_token
        if expected_token:
            auth_header = request.headers.get('Authorization', '')
            token = auth_header.removeprefix('Bearer ').strip()
            if not hmac.compare_digest(token, expected_token):
                logger.warning("Invalid Odoo webhook token")
                return web.Response(status=401, text="Invalid token")

        try:
            payload = await request.json()
        except json.JSONDecodeError:
            return web.Response(status=400, text="Invalid JSON")

        task_id = str(payload.get('task_id', ''))
        if not task_id:
            return web.Response(status=400, text="Missing task_id")

        try:
            client = self._create_zeebe_client()
            await client.publish_message(
                name="msg_odoo_task_done",
                correlation_key=task_id,
                variables={
                    "odoo_task_resolved": True,
                },
            )
            logger.info("Published msg_odoo_task_done for task_id=%s", task_id)
            return web.json_response({
                "status": "published",
                "message": "msg_odoo_task_done",
                "task_id": task_id,
            })
        except Exception as exc:
            logger.error("Failed to publish msg_odoo_task_done for task_id=%s: %s", task_id, exc)
            return web.Response(status=502, text=f"Zeebe publish failed: {exc}")

    # ── Helpers ───────────────────────────────────────────

    @staticmethod
    def _verify_github_signature(body: bytes, secret: str, signature: str) -> bool:
        """Verify GitHub HMAC-SHA256 webhook signature."""
        if not signature.startswith('sha256='):
            return False
        expected = hmac.new(
            secret.encode('utf-8'),
            body,
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(f'sha256={expected}', signature)
