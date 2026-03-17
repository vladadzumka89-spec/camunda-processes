"""Deploy process handlers — 10 task types.

Source: .github/workflows/deploy.yml
All operations execute via SSH on target servers.
"""

from __future__ import annotations

import logging
import re
from typing import Any

import httpx
from pyzeebe import ZeebeWorker

from ..config import AppConfig
from ..retry import retry
from ..ssh import AsyncSSHClient, CommandResult

logger = logging.getLogger(__name__)


def register_deploy_handlers(
    worker: ZeebeWorker,
    config: AppConfig,
    ssh: AsyncSSHClient,
) -> None:
    """Register all deploy-process task handlers."""

    # ── git-pull ───────────────────────────────────────────────

    @worker.task(task_type="git-pull", timeout_ms=120_000)
    async def git_pull(
        server_host: str,
        branch: str,
        repo_dir: str = "",
        **kwargs: Any,
    ) -> dict:
        """Fetch and checkout branch on remote server."""
        server = config.resolve_server(server_host)
        repo = repo_dir or server.repo_dir

        # Read previous deploy state
        state_file = f"{repo}/.deploy-state/deploy_state_{branch}"
        result = await ssh.run(server, f"cat {state_file} 2>/dev/null || echo none")
        old_commit = result.stdout.strip()

        # Git fetch with retry
        async def _fetch() -> CommandResult:
            return await ssh.run_in_repo(
                server,
                f"git config --global --add safe.directory {repo} 2>/dev/null; "
                f"git fetch origin {branch}",
                check=True,
                timeout=60,
            )

        await retry(_fetch, max_attempts=3, delay=5.0)

        # Checkout
        await ssh.run_in_repo(
            server,
            f"git checkout -B {branch} origin/{branch}",
            check=True,
        )

        # Get new commit
        result = await ssh.run_in_repo(server, "git rev-parse HEAD", check=True)
        new_commit = result.stdout.strip()

        has_changes = old_commit != new_commit
        logger.info(
            "git-pull on %s: %s → %s (changed=%s)",
            server.host, old_commit[:8], new_commit[:8], has_changes,
        )

        return {
            "old_commit": old_commit,
            "new_commit": new_commit,
            "has_changes": has_changes,
        }

    # ── detect-modules ─────────────────────────────────────────

    @worker.task(task_type="detect-modules", timeout_ms=60_000)
    async def detect_modules(
        server_host: str,
        old_commit: str,
        new_commit: str,
        repo_dir: str = "",
        **kwargs: Any,
    ) -> dict:
        """Detect changed Odoo modules and whether Docker build is needed."""
        server = config.resolve_server(server_host)
        repo = repo_dir or server.repo_dir

        if old_commit == "none":
            return {"changed_modules": "all", "docker_build_needed": True}

        # Count total changed files
        result = await ssh.run_in_repo(
            server,
            f"git diff --name-only {old_commit} {new_commit} | wc -l",
            check=True,
        )
        total_files = int(result.stdout.strip())

        if total_files > 250:
            return {"changed_modules": "all", "docker_build_needed": True}

        # Detect module changes in each source dir
        modules: set[str] = set()

        for base_dir, depth in [
            ("src/custom", 3),
            ("src/enterprise", 3),
            ("src/third-party", 3),
        ]:
            result = await ssh.run_in_repo(
                server,
                f"git diff --name-only {old_commit} {new_commit} -- {base_dir}/ 2>/dev/null",
            )
            if not result.stdout.strip():
                continue

            for line in result.stdout.strip().split("\n"):
                parts = line.split("/")
                if len(parts) >= depth:
                    mod_name = parts[depth - 1]
                    # Verify __manifest__.py exists
                    check = await ssh.run_in_repo(
                        server,
                        f"test -f {base_dir}/{mod_name}/__manifest__.py && echo yes || echo no",
                    )
                    if check.stdout.strip() == "yes":
                        modules.add(mod_name)

        # Community addons (deeper path: src/community/odoo/addons/MODULE)
        result = await ssh.run_in_repo(
            server,
            f"git diff --name-only {old_commit} {new_commit} -- src/community/odoo/addons/ 2>/dev/null",
        )
        if result.stdout.strip():
            for line in result.stdout.strip().split("\n"):
                parts = line.split("/")
                if len(parts) >= 5:
                    mod_name = parts[4]
                    check = await ssh.run_in_repo(
                        server,
                        f"test -f src/community/odoo/addons/{mod_name}/__manifest__.py && echo yes || echo no",
                    )
                    if check.stdout.strip() == "yes":
                        modules.add(mod_name)

        # Check if Docker build needed
        docker_result = await ssh.run_in_repo(
            server,
            f"git diff --name-only {old_commit} {new_commit} -- "
            "docker/ Dockerfile docker-compose.yml "
            "src/community/requirements.txt src/custom/requirements.txt",
        )
        docker_build_needed = bool(docker_result.stdout.strip())

        changed_modules = ",".join(sorted(modules)) if modules else ""
        logger.info("detect-modules: %s (docker_build=%s)", changed_modules or "none", docker_build_needed)

        return {
            "changed_modules": changed_modules,
            "docker_build_needed": docker_build_needed,
        }

    # ── docker-build ───────────────────────────────────────────

    @worker.task(task_type="docker-build", timeout_ms=600_000)
    async def docker_build(server_host: str, repo_dir: str = "", **kwargs: Any) -> dict:
        """Build Docker image on remote server."""
        server = config.resolve_server(server_host)
        repo = repo_dir or server.repo_dir

        async def _build() -> CommandResult:
            return await ssh.run_in_repo(
                server,
                "docker compose build --pull web",
                check=True,
                timeout=540,
            )

        await retry(_build, max_attempts=3, delay=5.0)
        logger.info("docker-build completed on %s", server.host)
        return {}

    # ── docker-up ──────────────────────────────────────────────

    @worker.task(task_type="docker-up", timeout_ms=300_000)
    async def docker_up(
        server_host: str,
        repo_dir: str = "",
        container: str = "",
        port: int = 0,
        **kwargs: Any,
    ) -> dict:
        """Start containers and wait for service to be healthy."""
        server = config.resolve_server(server_host)
        repo = repo_dir or server.repo_dir
        ctr = container or server.container
        svc_port = port or server.port

        async def _up() -> CommandResult:
            return await ssh.run_in_repo(server, "docker compose up -d", check=True, timeout=60)

        await retry(_up, max_attempts=3, delay=5.0)

        # Wait for container running (max 60s)
        for _ in range(12):
            result = await ssh.run(
                server,
                f"docker inspect --format='{{{{.State.Status}}}}' {ctr} 2>/dev/null || echo unknown",
            )
            if result.stdout.strip().strip("'") == "running":
                break
            await _sleep(5)
        else:
            raise RuntimeError(f"Container {ctr} not running after 60s")

        # Wait for HTTP service (max 240s)
        await _wait_http(ssh, server, svc_port, max_attempts=24, interval=10)

        # Restart nginx — it caches DNS at startup and fails if odoo wasn't running
        await ssh.run(
            server,
            f"docker restart {ctr}-nginx 2>/dev/null || true",
            timeout=30,
        )

        logger.info("docker-up: service healthy on %s:%d", server.host, svc_port)
        return {}

    # ── module-update ──────────────────────────────────────────

    @worker.task(task_type="module-update", timeout_ms=900_000)
    async def module_update(
        server_host: str,
        changed_modules: str = "",
        db_name: str = "",
        container: str = "",
        repo_dir: str = "",
        **kwargs: Any,
    ) -> dict:
        """Update Odoo modules on remote server."""
        server = config.resolve_server(server_host)
        repo = repo_dir or server.repo_dir
        db = db_name or server.db_name
        ctr = container or server.container

        if not changed_modules:
            return {"modules_updated": ""}

        # Get DB password
        db_password = await _get_db_password(ssh, server, ctr)

        # Determine update strategy
        if changed_modules == "all":
            update_modules = "all"
        else:
            module_list = [m.strip() for m in changed_modules.split(",") if m.strip()]

            # Query installed modules
            result = await ssh.run(
                server,
                f"docker exec {ctr}-db psql -U odoo -d {db} -t -A "
                f"-c \"SELECT name FROM ir_module_module WHERE state = 'installed';\"",
                check=True,
            )
            installed = set(result.stdout.strip().split("\n"))

            update_mods = [m for m in module_list if m in installed]
            # If >10 modules, switch to -u all
            if len(update_mods) > 10:
                update_modules = "all"
            else:
                update_modules = ",".join(update_mods) if update_mods else ""

        if not update_modules:
            return {"modules_updated": ""}

        # Clean __pycache__
        await ssh.run_in_repo(
            server,
            "find src -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true",
        )

        # Run module update in a separate container (production keeps running)
        await ssh.run_in_repo(
            server,
            f"timeout 2000 docker compose run --rm --no-deps web "
            f"odoo-bin -d {db} -u {update_modules} "
            f"--db_password='{db_password}' "
            f"--stop-after-init --no-http --log-level=warn",
            check=True,
            timeout=2100,
        )

        # Clear asset cache
        await ssh.run(
            server,
            f"docker exec {ctr}-db psql -U odoo -d {db} -c "
            "\"DELETE FROM ir_attachment WHERE url LIKE '/web/assets/%' OR name LIKE 'web.assets%';\"",
        )

        logger.info("module-update on %s: %s", server.host, update_modules)
        return {"modules_updated": update_modules}

    # ── cache-clear ────────────────────────────────────────────

    @worker.task(task_type="cache-clear", timeout_ms=60_000)
    async def cache_clear(
        server_host: str,
        db_name: str = "",
        container: str = "",
        repo_dir: str = "",
        **kwargs: Any,
    ) -> dict:
        """Clear Odoo asset cache and restart."""
        server = config.resolve_server(server_host)
        db = db_name or server.db_name
        ctr = container or server.container

        await ssh.run(
            server,
            f"docker exec {ctr}-db psql -U odoo -d {db} -c "
            "\"DELETE FROM ir_attachment WHERE url LIKE '/web/assets/%' OR name LIKE 'web.assets%';\"",
        )
        await ssh.run_in_repo(server, "docker compose up -d", check=True)
        logger.info("cache-clear on %s", server.host)
        return {}

    # ── smoke-test ─────────────────────────────────────────────

    @worker.task(task_type="smoke-test", timeout_ms=300_000)
    async def smoke_test(
        server_host: str,
        db_name: str = "",
        container: str = "",
        repo_dir: str = "",
        **kwargs: Any,
    ) -> dict:
        """Run Odoo smoke test to detect critical errors."""
        server = config.resolve_server(server_host)
        db = db_name or server.db_name
        ctr = container or server.container

        db_password = await _get_db_password(ssh, server, ctr)

        # Run smoke test in a separate container without stopping services.
        # Odoo with --stop-after-init --no-http can run alongside the main instance.
        result = await ssh.run_in_repo(
            server,
            f"timeout 120 docker compose run --rm --no-deps -T web "
            f"odoo-bin -d {db} --db_password='{db_password}' "
            f"--stop-after-init --no-http 2>&1",
            timeout=150,
        )

        # Parse errors (ignore safe warnings)
        ignore_patterns = [
            "Some modules are not loaded",
            "inconsistent states",
            "Importing test framework",
        ]
        error_lines = []
        for line in result.stdout.split("\n"):
            if re.search(r"CRITICAL|ERROR|ImportError|ModuleNotFoundError|SyntaxError|Traceback", line):
                if not any(p in line for p in ignore_patterns):
                    error_lines.append(line.strip())

        smoke_passed = result.exit_code == 0 and not error_lines

        if not smoke_passed:
            error_summary = "; ".join(error_lines[:3]) if error_lines else f"exit code {result.exit_code}"
            raise RuntimeError(f"Smoke test failed on {server.host}: {error_summary}")

        logger.info("smoke-test on %s: passed=True", server.host)
        return {"smoke_passed": True}

    # ── http-verify ────────────────────────────────────────────

    @worker.task(task_type="http-verify", timeout_ms=300_000)
    async def http_verify(
        server_host: str,
        port: int = 0,
        **kwargs: Any,
    ) -> dict:
        """Verify HTTP service is responding."""
        server = config.resolve_server(server_host)
        svc_port = port or server.port

        await _wait_http(ssh, server, svc_port, max_attempts=24, interval=10)
        logger.info("http-verify on %s:%d: OK", server.host, svc_port)
        return {}

    # ── save-deploy-state ──────────────────────────────────────

    @worker.task(task_type="save-deploy-state", timeout_ms=30_000)
    async def save_deploy_state(
        server_host: str,
        branch: str,
        new_commit: str,
        repo_dir: str = "",
        **kwargs: Any,
    ) -> dict:
        """Save deployed commit hash to state file. Best-effort — does not fail deploy."""
        server = config.resolve_server(server_host)
        repo = repo_dir or server.repo_dir

        try:
            safe_branch = branch.replace("/", "_")
            await ssh.run(
                server,
                f"mkdir -p {repo}/.deploy-state && chmod 700 {repo}/.deploy-state && "
                f"echo '{new_commit}' > {repo}/.deploy-state/deploy_state_{safe_branch} && "
                f"chmod 600 {repo}/.deploy-state/deploy_state_{safe_branch}",
                check=True,
            )
            logger.info("save-deploy-state on %s: %s → %s", server.host, branch, new_commit[:8])
            return {"state_saved": True}
        except Exception as exc:
            logger.warning("Failed to save deploy state on %s: %s", server.host, exc)
            return {"state_saved": False}

    # ── rollback ───────────────────────────────────────────────

    @worker.task(task_type="rollback", timeout_ms=600_000)
    async def rollback(
        server_host: str,
        **kwargs: Any,
    ) -> dict:
        """Restore from checkpoint via HTTP API."""
        if not config.db_restore_url:
            logger.warning("rollback: no DB_RESTORE_URL configured, skipping")
            return {"restored": False}

        headers: dict[str, str] = {}
        if config.db_checkpoint_token:
            headers["X-Auth-Token"] = config.db_checkpoint_token

        async with httpx.AsyncClient(timeout=540) as client:
            resp = await client.post(config.db_restore_url, headers=headers, content=b"")
            resp.raise_for_status()

        logger.info("rollback on %s: restored from checkpoint (HTTP %d)", server_host, resp.status_code)
        return {"restored": True}

    # ── db-checkpoint ─────────────────────────────────────────

    @worker.task(task_type="db-checkpoint", timeout_ms=600_000)
    async def db_checkpoint(
        server_host: str,
        **kwargs: Any,
    ) -> dict:
        """Create checkpoint via HTTP API before deploy."""
        checkpoint_url = config.db_checkpoint_url
        checkpoint_token = config.db_checkpoint_token

        if not checkpoint_url:
            logger.warning("db-checkpoint: no DB_CHECKPOINT_URL configured, skipping")
            return {"checkpoint_created": False}

        headers: dict[str, str] = {}
        if checkpoint_token:
            headers["X-Auth-Token"] = checkpoint_token

        async with httpx.AsyncClient(timeout=540) as client:
            resp = await client.post(checkpoint_url, headers=headers, content=b"")
            resp.raise_for_status()

        logger.info("db-checkpoint: completed (HTTP %d)", resp.status_code)
        return {"checkpoint_created": True}


# ── Helpers ────────────────────────────────────────────────────

import asyncio as _asyncio


async def _sleep(seconds: float) -> None:
    await _asyncio.sleep(seconds)


async def _wait_http(
    ssh: AsyncSSHClient,
    server: Any,
    port: int,
    max_attempts: int = 24,
    interval: int = 10,
) -> None:
    """Poll HTTP endpoint until it responds."""
    for attempt in range(1, max_attempts + 1):
        result = await ssh.run(
            server,
            f"curl -sf -o /dev/null --max-time 10 http://localhost:{port}/web/login",
        )
        if result.success:
            return
        if attempt < max_attempts:
            await _sleep(interval)

    raise RuntimeError(
        f"HTTP service not responding on {server.host}:{port} after {max_attempts * interval}s"
    )


async def _get_db_password(ssh: AsyncSSHClient, server: Any, container: str) -> str:
    """Retrieve database password from container or .env file."""
    # Try container env
    result = await ssh.run(server, f"docker exec {container} printenv PASSWORD 2>/dev/null")
    if result.success and result.stdout.strip():
        return result.stdout.strip()

    # Fallback: .env file
    result = await ssh.run_in_repo(
        server,
        "grep -oP 'POSTGRES_PASSWORD=\\K.*' .env 2>/dev/null",
    )
    if result.success and result.stdout.strip():
        return result.stdout.strip()

    raise RuntimeError(f"Cannot retrieve DB password on {server.host}")
