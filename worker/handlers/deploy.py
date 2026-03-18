"""Deploy process handlers — 10 task types.

Source: .github/workflows/deploy.yml
All operations execute via SSH on target servers.
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Any

import httpx
from pyzeebe import ZeebeWorker

from ..config import AppConfig
from ..retry import retry
from ..ssh import AsyncSSHClient, CommandResult

logger = logging.getLogger(__name__)

# One lock per server — serializes deploys to the same server
_deploy_locks: dict[str, asyncio.Lock] = {}


def _get_deploy_lock(server_host: str) -> asyncio.Lock:
    if server_host not in _deploy_locks:
        _deploy_locks[server_host] = asyncio.Lock()
    return _deploy_locks[server_host]


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
        lock = _get_deploy_lock(server_host)
        async with lock:
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

    @worker.task(task_type="detect-modules", timeout_ms=120_000)
    async def detect_modules(
        server_host: str,
        container: str = "",
        db_name: str = "",
        repo_dir: str = "",
        **kwargs: Any,
    ) -> dict:
        """Detect changed modules by comparing manifest version on disk vs installed_version in DB."""
        server = config.resolve_server(server_host)
        ctr = container or server.container
        db = db_name or server.db_name
        repo = repo_dir or server.repo_dir

        scan_script = _build_version_compare_script(ctr, db)

        result = await ssh.run_in_repo(
            server,
            scan_script,
            timeout=120,
        )

        if not result.success:
            logger.warning("detect-modules: version scan failed (%s), falling back to 'all'", result.stderr[:200])
            return {"changed_modules": "all"}

        changed = [m.strip() for m in result.stdout.strip().split("\n") if m.strip()]
        changed_modules = ",".join(sorted(set(changed))) if changed else ""

        logger.info("detect-modules (manifest version): %s", changed_modules or "none")
        return {"changed_modules": changed_modules}

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

    @worker.task(task_type="module-update", timeout_ms=1_200_000)
    async def module_update(
        server_host: str,
        changed_modules: str = "",
        install_modules: str = "",
        db_name: str = "",
        container: str = "",
        repo_dir: str = "",
        **kwargs: Any,
    ) -> dict:
        """Update/install changed Odoo modules, rebuild and restart container."""
        server = config.resolve_server(server_host)
        repo = repo_dir or server.repo_dir
        db = db_name or server.db_name
        ctr = container or server.container

        if not changed_modules and not install_modules:
            return {"modules_updated": ""}

        lock = _get_deploy_lock(server_host)
        async with lock:
            # Get DB password
            db_password = await _get_db_password(ssh, server, ctr)

            # Clean __pycache__
            await ssh.run_in_repo(
                server,
                "find src -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true",
            )

            if changed_modules == "all":
                update_flag = "-u all"
            else:
                # Query installed modules — only update those that are installed
                result = await ssh.run(
                    server,
                    f"docker exec {ctr}-db psql -U odoo -d {db} -t -A "
                    f"-c \"SELECT name FROM ir_module_module WHERE state = 'installed';\"",
                    check=True,
                )
                installed = set(result.stdout.strip().split("\n"))

                to_update = []
                if changed_modules:
                    module_list = [m.strip() for m in changed_modules.split(",") if m.strip()]
                    to_update = [m for m in module_list if m in installed]

                # install_modules from commit message [install: mod1, mod2]
                to_install = []
                if install_modules:
                    to_install = [m.strip() for m in install_modules.split(",") if m.strip() and m.strip() not in installed]

                flags = []
                if to_update:
                    flags.append(f"-u {','.join(to_update)}")
                if to_install:
                    flags.append(f"-i {','.join(to_install)}")
                if not flags:
                    return {"modules_updated": ""}
                update_flag = " ".join(flags)

            # Stop web to avoid concurrent DB access during migration
            await ssh.run_in_repo(server, "docker compose stop web", check=True, timeout=60)

            # Run migration in isolated container (web stopped, no conflicts)
            await ssh.run_in_repo(
                server,
                f"docker compose run --rm --no-deps -T -u odoo web "
                f"odoo-bin -d {db} {update_flag} "
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

            # Rebuild and restart web with new code
            await ssh.run_in_repo(server, "docker compose build web", check=True, timeout=1200)
            await ssh.run_in_repo(server, "docker compose up -d web", check=True, timeout=60)

            logger.info("module-update on %s: %s", server.host, update_flag)
            return {"modules_updated": changed_modules}

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

        # Run smoke test inside the running container
        result = await ssh.run(
            server,
            f"docker exec -u odoo {ctr} odoo-bin -d {db} --db_password='{db_password}' "
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
        if not config.db_checkpoint_base_url:
            logger.warning("rollback: no DB_CHECKPOINT_BASE_URL configured, skipping")
            return {"restored": False}

        server_name = config.resolve_server_name(server_host)
        restore_url = f"{config.db_checkpoint_base_url}/restore/{server_name}"

        headers: dict[str, str] = {}
        if config.db_checkpoint_token:
            headers["X-Auth-Token"] = config.db_checkpoint_token

        async with httpx.AsyncClient(timeout=540) as client:
            resp = await client.post(restore_url, headers=headers, content=b"")
            resp.raise_for_status()

        logger.info("rollback on %s: restored from checkpoint (HTTP %d), waiting 60s...", server_host, resp.status_code)
        await _sleep(60)
        return {"restored": True}

    # ── db-remove ──────────────────────────────────────────────

    @worker.task(task_type="db-remove", timeout_ms=180_000)
    async def db_remove(
        server_host: str,
        **kwargs: Any,
    ) -> dict:
        """Remove old checkpoint via HTTP API before creating a new one."""
        if not config.db_checkpoint_base_url:
            logger.warning("db-remove: no DB_CHECKPOINT_BASE_URL configured, skipping")
            return {"checkpoint_removed": False}

        server_name = config.resolve_server_name(server_host)
        remove_url = f"{config.db_checkpoint_base_url}/remove/{server_name}"

        headers: dict[str, str] = {}
        if config.db_checkpoint_token:
            headers["X-Auth-Token"] = config.db_checkpoint_token

        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(remove_url, headers=headers, content=b"")
            resp.raise_for_status()

        logger.info("db-remove: completed (HTTP %d), waiting 60s...", resp.status_code)
        await _sleep(60)
        return {"checkpoint_removed": True}

    # ── db-checkpoint ─────────────────────────────────────────

    @worker.task(task_type="db-checkpoint", timeout_ms=600_000)
    async def db_checkpoint(
        server_host: str,
        **kwargs: Any,
    ) -> dict:
        """Create checkpoint via HTTP API before deploy."""
        if not config.db_checkpoint_base_url:
            logger.warning("db-checkpoint: no DB_CHECKPOINT_BASE_URL configured, skipping")
            return {"checkpoint_created": False}

        server_name = config.resolve_server_name(server_host)
        checkpoint_url = f"{config.db_checkpoint_base_url}/checkpoint/{server_name}"

        headers: dict[str, str] = {}
        if config.db_checkpoint_token:
            headers["X-Auth-Token"] = config.db_checkpoint_token

        async with httpx.AsyncClient(timeout=540) as client:
            resp = await client.post(checkpoint_url, headers=headers, content=b"")
            resp.raise_for_status()

        logger.info("db-checkpoint: completed (HTTP %d), waiting 60s...", resp.status_code)
        await _sleep(60)
        return {"checkpoint_created": True}


# ── Helpers ────────────────────────────────────────────────────


def _build_version_compare_script(container: str, db_name: str) -> str:
    """Build a shell script that compares manifest version on disk vs installed_version in DB.

    For each module with __manifest__.py on disk, extracts 'version' field
    and compares with installed_version from ir_module_module.
    Outputs module names where versions differ.
    """
    return (
        f'# Get installed versions from DB\n'
        f'INSTALLED=$(docker exec {container}-db psql -U odoo -d {db_name} -t -A -c '
        f'"SELECT name || \'=\' || COALESCE(installed_version, \'\') FROM ir_module_module;" 2>/dev/null)\n'
        f'\n'
        f'for manifest in $(find src/custom src/enterprise src/third-party src/community '
        f'-maxdepth 3 -name "__manifest__.py" 2>/dev/null); do\n'
        f'    mod_dir=$(dirname "$manifest")\n'
        f'    mod_name=$(basename "$mod_dir")\n'
        f'    # Extract version from __manifest__.py\n'
        f'    disk_version=$(python3 -c "\n'
        f'import ast, sys\n'
        f'try:\n'
        f'    m = ast.literal_eval(open(sys.argv[1]).read())\n'
        f'    print(m.get(\'version\', \'0.0\'))\n'
        f'except: print(\'0.0\')\n'
        f'" "$manifest" 2>/dev/null)\n'
        f'    # Find installed version from DB output\n'
        f'    db_version=$(echo "$INSTALLED" | grep "^$mod_name=" | cut -d= -f2)\n'
        f'    # If module not in DB at all — skip (not installed, not our concern)\n'
        f'    [ -z "$db_version" ] && continue\n'
        f'    # Compare versions\n'
        f'    if [ "$disk_version" != "$db_version" ]; then\n'
        f'        echo "$mod_name"\n'
        f'    fi\n'
        f'done'
    )


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
