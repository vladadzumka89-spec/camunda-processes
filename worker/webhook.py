"""Webhook server — bridges GitHub/Odoo events to Camunda Zeebe processes.

Endpoints:
    POST /webhook/github      — GitHub PR events (HMAC-SHA256 verified)
    POST /webhook/odoo        — Odoo task closure callback (token auth)
    GET  /health              — Liveness probe
    GET  /reports/fop/latest  — Latest FOP limit monitoring JSON report

Zeebe messages published:
    msg_pr_event       — PR opened/reopened targeting main → starts feature-to-production
    msg_pr_updated     — PR synchronize → correlates to running instance by pr_number
    msg_odoo_task_done — Odoo task closed → correlates to upstream-sync by odoo_task_id
    msg_deploy_trigger — push to staging → starts deploy-process
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import re
from typing import Any

from aiohttp import web
from pyzeebe import ZeebeClient

from .auth import ZeebeAuthConfig, create_channel
from .config import AppConfig

logger = logging.getLogger(__name__)


_INSTALL_RE = re.compile(r'\[install:\s*([^\]]+)\]', re.IGNORECASE)


def _parse_install_modules(payload: dict) -> str:
    """Parse [install: module1, module2] from push commit messages."""
    modules: set[str] = set()
    for commit in payload.get('commits', []):
        msg = commit.get('message', '')
        for match in _INSTALL_RE.finditer(msg):
            for mod in match.group(1).split(','):
                mod = mod.strip()
                if mod:
                    modules.add(mod)
    # Also check head_commit
    head = payload.get('head_commit', {})
    if head:
        msg = head.get('message', '')
        for match in _INSTALL_RE.finditer(msg):
            for mod in match.group(1).split(','):
                mod = mod.strip()
                if mod:
                    modules.add(mod)
    return ",".join(sorted(modules))


class WebhookServer:
    """HTTP server that receives webhooks and publishes Zeebe messages."""

    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._app = web.Application()
        self._app.router.add_post('/webhook/github', self._handle_github)
        self._app.router.add_post('/webhook/odoo', self._handle_odoo)
        self._app.router.add_get('/health', self._handle_health)
        self._app.router.add_get('/reports/fop/latest', self._handle_fop_report)
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

    # ── FOP report endpoint ────────────────────────────────

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
        elif event_type == 'push':
            return await self._route_push_event(payload)

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

        # PR opened/reopened: trigger review for any target branch
        if action in ('opened', 'reopened'):
            return await self._publish_pr_event(pr, payload)
        elif action == 'synchronize':
            return await self._publish_pr_updated(pr)

        logger.info("Ignoring PR #%d action=%s", pr_number, action)
        return web.json_response({"status": "ignored", "action": action})

    async def _route_push_event(self, payload: dict) -> web.Response:
        """Route push events — deploy staging on push to staging branch."""
        ref = payload.get('ref', '')
        after_sha = payload.get('after', '')

        if ref != 'refs/heads/staging':
            logger.info("Ignoring push to %s (not staging)", ref)
            return web.json_response({"status": "ignored", "ref": ref})

        staging = self._config.servers.get('staging')
        if not staging:
            logger.error("No staging server configured for deploy trigger")
            return web.Response(status=500, text="No staging server configured")

        # Parse [install: module1, module2] from commit messages
        install_modules = _parse_install_modules(payload)

        variables: dict[str, Any] = {
            "trigger_sha": after_sha,
            "server_host": staging.host,
            "ssh_user": staging.ssh_user,
            "repo_dir": staging.repo_dir,
            "db_name": staging.db_name,
            "container": staging.container,
            "branch": "staging",
            "run_smoke_test": True,
            "test_mode": "full",
            "odoo_project_id": self._config.odoo.project_id,
        }
        if install_modules:
            variables["install_modules"] = install_modules
            logger.info("Install modules from commit: %s", install_modules)

        try:
            client = self._create_zeebe_client()
            await client.publish_message(
                name="msg_deploy_trigger",
                correlation_key=variables.get("branch", "staging"),
                variables=variables,
                time_to_live_in_milliseconds=3_600_000,
            )
            logger.info(
                "Published msg_deploy_trigger for push to staging (sha=%s)",
                after_sha[:12],
            )
            return web.json_response({
                "status": "published",
                "message": "msg_deploy_trigger",
                "trigger_sha": after_sha,
            })
        except Exception as exc:
            logger.error("Failed to publish msg_deploy_trigger: %s", exc)
            return web.Response(status=502, text=f"Zeebe publish failed: {exc}")

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
            "base_branch": pr.get('base', {}).get('ref', 'main'),
            "head_branch": pr.get('head', {}).get('ref', ''),
            "odoo_project_id": self._config.odoo.project_id,
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
            # msg_pr_review: starts standalone pr-review process (correlation by pr_number)
            await client.publish_message(
                name="msg_pr_review",
                correlation_key=str(pr_number),
                variables=variables,
                time_to_live_in_milliseconds=3_600_000,
            )
            # msg_pr_event: correlates with running feature-to-production (correlation by head_branch)
            await client.publish_message(
                name="msg_pr_event",
                correlation_key=variables.get("head_branch", ""),
                variables=variables,
                time_to_live_in_milliseconds=3_600_000,
            )
            logger.info(
                "Published msg_pr_review + msg_pr_event for PR #%d (%s)",
                pr_number, pr.get('title', ''),
            )
            return web.json_response({
                "status": "published",
                "messages": ["msg_pr_review", "msg_pr_event"],
                "pr_number": pr_number,
            })
        except Exception as exc:
            logger.error("Failed to publish messages for PR #%d: %s", pr_number, exc)
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
                time_to_live_in_milliseconds=3_600_000,
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
        # Token verification (Bearer header or ?token= query param)
        expected_token = self._config.webhook.odoo_webhook_token
        if expected_token:
            auth_header = request.headers.get('Authorization', '')
            token = (
                auth_header.removeprefix('Bearer ').strip()
                or request.query.get('token', '')
            )
            if not hmac.compare_digest(token, expected_token):
                logger.warning("Invalid Odoo webhook token")
                return web.Response(status=401, text="Invalid token")

        try:
            payload = await request.json()
        except json.JSONDecodeError:
            return web.Response(status=400, text="Invalid JSON")

        # Support both task_id and process_instance_key for correlation.
        # Also check x_studio_camunda_process_instance_key (Odoo webhook action field name).
        task_id = str(payload.get('task_id', ''))
        pik = str(
            payload.get('process_instance_key', '')
            or payload.get('x_studio_camunda_process_instance_key', '')
        )
        # action from JSON body or query param (?action=cancel for Odoo webhook actions)
        action = payload.get('action', request.query.get('action', 'done'))
        correlation_key = pik or task_id

        if not correlation_key:
            return web.Response(status=400, text="Missing task_id or process_instance_key")

        logger.info(
            "Odoo webhook: action=%s, task_id=%s, pik=%s, payload_keys=%s",
            action, task_id, pik, list(payload.keys()),
        )

        # Cancel action: terminate the Camunda process instance
        if action == 'cancel' and pik:
            return await self._cancel_process_instance(pik)

        # Complete user task via REST API (for Zeebe-native user tasks)
        user_task_key = str(payload.get('user_task_key', '') or payload.get('x_studio_camunda_user_task_key', ''))

        # If no user_task_key but we have process_instance_key, find active user task automatically
        if not user_task_key and pik and action == 'done':
            found_key = await self._find_active_user_task(pik)
            if found_key:
                user_task_key = found_key

        if user_task_key and action == 'done':
            return await self._complete_user_task(user_task_key, payload)

        try:
            # Pass through all variables from Odoo payload (staging_approved, prod_approved, etc.)
            msg_variables = {"odoo_task_resolved": True}
            for key in ("staging_approved", "prod_approved", "merge_approved",
                        "task_approved", "comment", "rejection_reason"):
                if key in payload:
                    msg_variables[key] = payload[key]

            client = self._create_zeebe_client()
            await client.publish_message(
                name="msg_odoo_task_done",
                correlation_key=correlation_key,
                variables=msg_variables,
            )
            logger.info(
                "Published msg_odoo_task_done correlation_key=%s (task_id=%s, pik=%s)",
                correlation_key, task_id, pik,
            )
            return web.json_response({
                "status": "published",
                "message": "msg_odoo_task_done",
                "correlation_key": correlation_key,
            })
        except Exception as exc:
            logger.error(
                "Failed to publish msg_odoo_task_done correlation_key=%s: %s",
                correlation_key, exc,
            )
            return web.Response(status=502, text=f"Zeebe publish failed: {exc}")

    async def _find_active_user_task(self, process_instance_key: str) -> str | None:
        """Find active user task for a process instance via Camunda REST API."""
        import httpx
        from .http_request_smart import _camunda_rest_request

        try:
            async with httpx.AsyncClient() as client:
                resp = await _camunda_rest_request(
                    client, "POST", "/v2/user-tasks/search",
                    json={"filter": {"processInstanceKey": int(process_instance_key), "state": "CREATED"}},
                )
                if resp.status_code == 200:
                    items = resp.json().get("items", [])
                    if items:
                        key = str(items[0].get("userTaskKey"))
                        logger.info("Found active user task %s for process %s", key, process_instance_key)
                        return key
                    else:
                        logger.warning("No active user tasks for process %s", process_instance_key)
                else:
                    logger.warning("User task search failed: HTTP %d", resp.status_code)
        except Exception as exc:
            logger.error("Failed to find user task for process %s: %s", process_instance_key, exc)

        return None

    async def _complete_user_task(self, user_task_key: str, payload: dict) -> web.Response:
        """Complete a Zeebe-native user task via Camunda REST API."""
        import httpx
        from .http_request_smart import _get_oauth_token

        # Collect x_studio_camunda_* variables from Odoo payload
        variables = {}
        for key, value in payload.items():
            if key.startswith('x_studio_camunda_'):
                variables[key] = value

        logger.info("Completing user task %s with variables: %s", user_task_key, list(variables.keys()))

        gw = self._config.zeebe.gateway_address
        zeebe_host = gw.split(':')[0] if ':' in gw else gw
        rest_url = f"http://{zeebe_host}:8080"

        try:
            async with httpx.AsyncClient() as client:
                token = await _get_oauth_token(client)
                resp = await client.post(
                    f"{rest_url}/v2/user-tasks/{user_task_key}/completion",
                    json={"variables": variables} if variables else {},
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=10.0,
                )

            if resp.status_code in (200, 204):
                logger.info("Completed user task %s", user_task_key)
                return web.json_response({"status": "completed", "user_task_key": user_task_key})
            else:
                logger.error("Failed to complete user task %s: HTTP %d %s", user_task_key, resp.status_code, resp.text)
                return web.Response(status=502, text=f"Complete failed: HTTP {resp.status_code}")
        except Exception as exc:
            logger.error("Failed to complete user task %s: %s", user_task_key, exc)
            return web.Response(status=502, text=f"Complete failed: {exc}")

    async def _cancel_process_instance(self, pik: str) -> web.Response:
        """Cancel a Camunda process instance and rollback sync state if needed."""
        import httpx

        # REST API is on port 8080 inside Docker network (8088 is host-mapped)
        gw = self._config.zeebe.gateway_address  # e.g. "orchestration:26500"
        zeebe_host = gw.split(':')[0] if ':' in gw else gw
        zeebe_rest = f"http://{zeebe_host}:8080"
        auth = ("demo", "demo")

        # 1. Fetch process variables BEFORE cancellation (for rollback)
        old_shas = await self._fetch_sync_shas(zeebe_rest, pik, auth)

        # 2. Cancel the process instance
        url = f"{zeebe_rest}/v2/process-instances/{pik}/cancellation"
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    url, auth=auth,
                    headers={"Content-Type": "application/json"},
                    content="{}", timeout=10,
                )
            if resp.status_code in (200, 204):
                logger.info("Cancelled process instance %s (Odoo task cancelled)", pik)
            elif resp.status_code == 404:
                logger.info("Process %s already terminated (404)", pik)
            else:
                logger.warning(
                    "Failed to cancel process %s: HTTP %d %s",
                    pik, resp.status_code, resp.text,
                )
                return web.Response(
                    status=502,
                    text=f"Cancel failed: HTTP {resp.status_code}",
                )
        except Exception as exc:
            logger.error("Cancel process %s failed: %s", pik, exc)
            return web.Response(status=502, text=f"Cancel failed: {exc}")

        # 3. Rollback SHA state file if this was an upstream-sync process
        if old_shas:
            await self._rollback_sync_state(old_shas)

        return web.json_response({
            "status": "cancelled",
            "process_instance_key": pik,
        })

    async def _fetch_sync_shas(
        self, zeebe_rest: str, pik: str, auth: tuple,
    ) -> dict | None:
        """Fetch original SHA values from process variables (for rollback)."""
        import httpx

        url = f"{zeebe_rest}/v2/variables/search"
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    url, auth=auth,
                    headers={"Content-Type": "application/json"},
                    content=json.dumps({
                        "filter": {"processInstanceKey": pik},
                    }),
                    timeout=10,
                )
            if resp.status_code != 200:
                logger.warning("Variables search failed: HTTP %d", resp.status_code)
                return None
            data = resp.json()
            variables = {}
            for item in data.get("items", []):
                name = item.get("name", "")
                if name in (
                    "current_community_sha", "current_enterprise_sha",
                    "enterprise_version", "server_host",
                ):
                    val = item.get("value", "")
                    # Values come JSON-encoded (e.g. '"abc"')
                    try:
                        variables[name] = json.loads(val)
                    except (json.JSONDecodeError, TypeError):
                        variables[name] = val
            if variables.get("current_enterprise_sha"):
                return variables
        except Exception as exc:
            logger.warning("Failed to fetch variables for %s: %s", pik, exc)
        return None

    async def _rollback_sync_state(self, shas: dict) -> None:
        """Restore SHA state file on the server after cancelled sync."""
        import asyncssh

        community_sha = shas.get("current_community_sha", "")
        enterprise_sha = shas.get("current_enterprise_sha", "")
        upstream_branch = shas.get("enterprise_version", "19.0")

        # Find the server config (default to staging)
        server = self._config.servers.get("staging")
        if not server:
            logger.warning("No staging server config — cannot rollback sync state")
            return

        state = json.dumps({
            "community_sha": community_sha,
            "enterprise_sha": enterprise_sha,
            "synced_at": "rollback",
            "upstream_branch": upstream_branch,
        })
        repo_dir = server.repo_dir
        cmd = (
            f"mkdir -p {repo_dir}/.sync-state && "
            f"echo '{state}' > {repo_dir}/.sync-state/upstream_shas.json"
        )

        try:
            async with asyncssh.connect(
                server.host,
                username=server.ssh_user,
                known_hosts=None,
            ) as conn:
                result = await conn.run(cmd, check=True, timeout=10)
            logger.info(
                "Rolled back sync state: enterprise=%s, community=%s",
                enterprise_sha[:8], community_sha[:8],
            )
        except Exception as exc:
            logger.warning("Failed to rollback sync state: %s", exc)

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
