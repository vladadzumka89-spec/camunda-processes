"""Upstream sync handlers — isolated workspace approach.

Source: .github/workflows/sync-enterprise.yml
All sync operations use an isolated clone in /tmp/sync-workspace-{run_id}
instead of the live server repo, preventing interference with local changes.
Each process instance gets unique directories to prevent concurrent retry conflicts.
"""

from __future__ import annotations

import ast
import json
import logging
import re
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any

import httpx
from pyzeebe import ZeebeWorker
from ..config import AppConfig
from ..github_client import GitHubClient
from ..retry import retry
from ..ssh import AsyncSSHClient

logger = logging.getLogger(__name__)


def register_sync_handlers(
    worker: ZeebeWorker,
    config: AppConfig,
    ssh: AsyncSSHClient,
    github: GitHubClient,
) -> None:
    """Register all upstream-sync task handlers."""

    def _resolve_server(server_host: str = ""):
        """Resolve server config, defaulting to kozak_demo."""
        return config.resolve_server(server_host or "kozak_demo")

    async def _ws_run(server, cmd: str, workspace: str = "", **kwargs):
        """Run a command inside the isolated workspace directory via SSH."""
        ws = workspace
        if not ws:
            raise ValueError("workspace path is required for _ws_run")
        return await ssh.run(server, f"cd {ws} && {cmd}", **kwargs)

    # ── fetch-current-version ──────────────────────────────────

    @worker.task(task_type="fetch-current-version", timeout_ms=30_000)
    async def fetch_current_version(
        server_host: str = "",
        upstream_branch: str = "19.0",
        **kwargs: Any,
    ) -> dict:
        """Read current upstream SHAs from server state file and Odoo version."""
        server = _resolve_server(server_host)
        repo_dir = server.repo_dir

        # Read version from release.py on server
        result = await ssh.run(
            server, f"cat {repo_dir}/src/community/odoo/release.py", check=True,
        )
        vi_match = re.search(r"version_info\s*=\s*\((\d+),\s*(\d+)", result.stdout)
        version = f"{vi_match.group(1)}.{vi_match.group(2)}" if vi_match else upstream_branch

        # Read upstream SHAs from state file (saved after each successful sync)
        state_result = await ssh.run(
            server,
            f"cat {repo_dir}/.sync-state/upstream_shas.json 2>/dev/null || echo '{{}}'",
        )
        community_sha = ""
        enterprise_sha = ""
        try:
            state = json.loads(state_result.stdout.strip())
            community_sha = state.get("community_sha", "")
            enterprise_sha = state.get("enterprise_sha", "")
        except (json.JSONDecodeError, ValueError):
            logger.warning("No sync state found — first sync or state file missing")

        logger.info(
            "Current version: %s (community=%s, enterprise=%s)",
            version, community_sha[:8] or "none", enterprise_sha[:8] or "none",
        )
        return {
            "current_version": version,
            "current_community_sha": community_sha,
            "current_enterprise_sha": enterprise_sha,
        }

    # ── fetch-runbot ───────────────────────────────────────────

    @worker.task(task_type="fetch-runbot", timeout_ms=60_000)
    async def fetch_runbot(
        upstream_branch: str = "19.0",
        **kwargs: Any,
    ) -> dict:
        """Fetch latest verified SHAs from Runbot CI API."""
        url = "https://runbot.odoo.com/runbot/json/last_batches_infos"

        async def _fetch() -> dict:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.get(url)
                resp.raise_for_status()
                return resp.json()

        data = await retry(_fetch, max_attempts=3, delay=5.0)

        branch_data = data.get(upstream_branch, {})
        commits = branch_data.get("commits", [])

        community_sha = ""
        enterprise_sha = ""
        for commit in commits:
            repo_name = commit.get("repo", "")
            if repo_name == "odoo":
                community_sha = commit.get("head", "")
            elif repo_name == "enterprise":
                enterprise_sha = commit.get("head", "")

        if not community_sha or not enterprise_sha:
            raise ValueError(
                f"Incomplete Runbot data for branch {upstream_branch}: "
                f"community={community_sha}, enterprise={enterprise_sha}"
            )

        logger.info(
            "Runbot %s: community=%s, enterprise=%s",
            upstream_branch, community_sha[:8], enterprise_sha[:8],
        )
        return {
            "runbot_community_sha": community_sha,
            "runbot_enterprise_sha": enterprise_sha,
        }

    # ── clone-upstream ─────────────────────────────────────────

    @worker.task(task_type="clone-upstream", timeout_ms=300_000)
    async def clone_upstream(
        runbot_community_sha: str,
        runbot_enterprise_sha: str,
        upstream_branch: str = "19.0",
        server_host: str = "",
        **kwargs: Any,
    ) -> dict:
        """Clone upstream repos at Runbot SHAs + prepare isolated workspace.

        Returns unique directory paths as process variables so downstream tasks
        use the exact dirs this attempt created (no shared-path conflicts).
        """
        server = _resolve_server(server_host)
        deploy_pat = config.github.deploy_pat
        repo = config.github.repository

        # Unique dirs per attempt — prevents concurrent retry conflicts
        run_id = uuid.uuid4().hex[:8]
        community_dir = f"/tmp/upstream-community-{run_id}"
        enterprise_dir = f"/tmp/upstream-enterprise-{run_id}"
        workspace_dir = f"/tmp/sync-workspace-{run_id}"

        try:
            # 1. Clone upstream community (public repo)
            await ssh.run(
                server,
                f"git clone --depth=1 --single-branch --branch {upstream_branch} "
                f"https://github.com/odoo/odoo.git {community_dir} && "
                f"cd {community_dir} && git fetch --depth=1 origin {runbot_community_sha} && "
                "git checkout FETCH_HEAD -q",
                check=True,
                timeout=300,
            )

            # 2. Clone upstream enterprise (private repo, needs PAT)
            await ssh.run(
                server,
                f"git clone --depth=1 --single-branch --branch {upstream_branch} "
                f"https://x-access-token:{deploy_pat}@github.com/odoo/enterprise.git "
                f"{enterprise_dir} && "
                f"cd {enterprise_dir} && git fetch --depth=1 origin {runbot_enterprise_sha} && "
                "git checkout FETCH_HEAD -q",
                check=True,
                timeout=300,
            )

            # 3. Clone our repo to isolated workspace (clean state from main)
            await ssh.run(
                server,
                f"git clone --depth=1 --branch main "
                f"https://x-access-token:{deploy_pat}@github.com/{repo}.git {workspace_dir}",
                check=True,
                timeout=300,
            )
        except Exception:
            # Cleanup unique dirs on failure
            await ssh.run(
                server,
                f"rm -rf {community_dir} {enterprise_dir} {workspace_dir}",
                check=False,
            )
            raise

        logger.info("Prepared isolated workspace at %s (run %s)", workspace_dir, run_id)

        # 4. Get metadata
        com_date_result = await ssh.run(
            server, f"git -C {community_dir} log -1 --format=%ci", check=True,
        )
        ent_date_result = await ssh.run(
            server, f"git -C {enterprise_dir} log -1 --format=%ci", check=True,
        )
        ent_count_result = await ssh.run(
            server,
            f"find {enterprise_dir} -mindepth 1 -maxdepth 1 -type d ! -name '.git' ! -name '.*' | wc -l",
            check=True,
        )

        community_date = com_date_result.stdout.strip().split()[0] if com_date_result.stdout.strip() else ""
        enterprise_date = ent_date_result.stdout.strip().split()[0] if ent_date_result.stdout.strip() else ""
        enterprise_count = int(ent_count_result.stdout.strip() or "0")

        logger.info(
            "Cloned upstream: community=%s (%s), enterprise=%s (%s, %d modules)",
            runbot_community_sha[:8], community_date,
            runbot_enterprise_sha[:8], enterprise_date, enterprise_count,
        )
        return {
            "community_dir": community_dir,
            "enterprise_dir": enterprise_dir,
            "workspace_dir": workspace_dir,
            "community_date": community_date,
            "enterprise_date": enterprise_date,
            "enterprise_count": enterprise_count,
        }

    # ── sync-modules ───────────────────────────────────────────

    @worker.task(task_type="sync-modules", timeout_ms=1_200_000)
    async def sync_modules(
        community_dir: str = "",
        enterprise_dir: str = "",
        workspace_dir: str = "",
        server_host: str = "",
        modules: str = "",
        **kwargs: Any,
    ) -> dict:
        """Sync modules from upstream into isolated workspace via rsync."""
        server = _resolve_server(server_host)
        if not community_dir or not enterprise_dir or not workspace_dir:
            raise ValueError("community_dir, enterprise_dir, workspace_dir are required")

        if modules:
            # Selective mode — sync only specified enterprise modules
            module_list = [m.strip() for m in modules.split(",") if m.strip()]
            synced = 0
            new_modules: list[str] = []

            for mod in module_list:
                # Check if exists in upstream
                check = await ssh.run(
                    server, f"test -d {enterprise_dir}/{mod} && echo yes || echo no",
                )
                if check.stdout.strip() != "yes":
                    logger.warning("Module %s not found in upstream, skipping", mod)
                    continue

                # Check if new (not in workspace)
                check = await ssh.run(
                    server, f"test -d {workspace_dir}/src/enterprise/{mod} && echo yes || echo no",
                )
                if check.stdout.strip() != "yes":
                    new_modules.append(mod)

                await ssh.run(
                    server,
                    f"rsync -a --delete --checksum "
                    f"{enterprise_dir}/{mod}/ {workspace_dir}/src/enterprise/{mod}/",
                    check=True,
                )
                synced += 1

            if synced == 0:
                raise ValueError("No valid modules found in upstream")

            return {
                "sync_mode": "selective",
                "synced_enterprise": synced,
                "new_modules": ", ".join(new_modules),
            }
        else:
            # Full mode — detect new modules first
            new_result = await ssh.run(
                server,
                f"for d in {enterprise_dir}/*/; do "
                f"mod=$(basename \"$d\"); "
                f"[ ! -d \"{workspace_dir}/src/enterprise/$mod\" ] && echo \"$mod\"; "
                f"done 2>/dev/null || true",
            )
            new_modules = [m for m in new_result.stdout.strip().split("\n") if m]

            # Sync community (full replace, exclude .git)
            await ssh.run(
                server,
                f"rsync -a --delete --checksum --exclude='.git' "
                f"{community_dir}/ {workspace_dir}/src/community/",
                check=True,
                timeout=600,
            )

            # Sync enterprise (full replace, exclude .git)
            await ssh.run(
                server,
                f"rsync -a --delete --checksum --exclude='.git' "
                f"{enterprise_dir}/ {workspace_dir}/src/enterprise/",
                check=True,
                timeout=600,
            )

            # Count synced
            count_result = await ssh.run(
                server,
                f"find {enterprise_dir} -mindepth 1 -maxdepth 1 -type d ! -name '.*' | wc -l",
                check=True,
            )
            synced_count = int(count_result.stdout.strip() or "0")

            logger.info("Full sync: %d enterprise modules, %d new", synced_count, len(new_modules))
            return {
                "sync_mode": "full",
                "synced_enterprise": synced_count,
                "new_modules": ", ".join(new_modules),
            }

    # ── diff-report ────────────────────────────────────────────

    @worker.task(task_type="diff-report", timeout_ms=600_000)
    async def diff_report(
        workspace_dir: str = "",
        server_host: str = "",
        **kwargs: Any,
    ) -> dict:
        """Generate diff report after sync (in isolated workspace)."""
        server = _resolve_server(server_host)
        if not workspace_dir:
            raise ValueError("workspace_dir is required")
        ws = workspace_dir

        # Register new files for diff tracking
        await _ws_run(
            server, "git add -N src/community/ src/enterprise/ 2>/dev/null || true",
            workspace=ws,
        )

        # Check community changes
        com_check = await _ws_run(server, "git diff --quiet -- src/community/ 2>/dev/null; echo $?", workspace=ws, timeout=300)
        community_changed = com_check.stdout.strip() != "0"

        # Check enterprise changes
        ent_check = await _ws_run(server, "git diff --quiet -- src/enterprise/ 2>/dev/null; echo $?", workspace=ws, timeout=300)
        enterprise_changed = ent_check.stdout.strip() != "0"

        has_changes = community_changed or enterprise_changed

        community_files = 0
        enterprise_files = 0
        changed_modules: list[str] = []

        if community_changed:
            result = await _ws_run(
                server, "git diff --name-only -- src/community/ | wc -l", check=True, workspace=ws, timeout=300,
            )
            community_files = int(result.stdout.strip())

        if enterprise_changed:
            result = await _ws_run(
                server, "git diff --name-only -- src/enterprise/ | wc -l", check=True, workspace=ws, timeout=300,
            )
            enterprise_files = int(result.stdout.strip())

            # Get changed module names
            result = await _ws_run(
                server,
                "git diff --name-only -- src/enterprise/ | cut -d'/' -f3 | sort -u",
                check=True, workspace=ws, timeout=300,
            )
            changed_modules = [m for m in result.stdout.strip().split("\n") if m]

        # Also check community addons for impact analysis
        if community_changed:
            result = await _ws_run(
                server,
                "git diff --name-only -- src/community/odoo/addons/ 2>/dev/null "
                "| cut -d'/' -f5 | sort -u",
                workspace=ws, timeout=300,
            )
            community_modules = [m for m in result.stdout.strip().split("\n") if m]
            all_modules = sorted(set(changed_modules + community_modules))
        else:
            all_modules = changed_modules

        logger.info(
            "diff-report: changes=%s, community=%d files, enterprise=%d files, modules=%d",
            has_changes, community_files, enterprise_files, len(all_modules),
        )
        return {
            "has_changes": has_changes,
            "changed_modules": ", ".join(all_modules),
            "community_files": community_files,
            "enterprise_files": enterprise_files,
        }

    # ── impact-analysis ────────────────────────────────────────

    @worker.task(task_type="impact-analysis", timeout_ms=120_000)
    async def impact_analysis(
        changed_modules: str = "",
        workspace_dir: str = "",
        server_host: str = "",
        **kwargs: Any,
    ) -> dict:
        """Analyze impact of upstream changes on custom modules (in workspace)."""
        server = _resolve_server(server_host)

        if not changed_modules:
            return {"affected_custom_count": 0, "impact_table": ""}

        if not workspace_dir:
            raise ValueError("workspace_dir is required")

        changed_set = set(m.strip() for m in changed_modules.split(",") if m.strip())

        # List custom modules in workspace
        result = await ssh.run(
            server,
            f"find {workspace_dir}/src/custom -maxdepth 2 -name '__manifest__.py' "
            f"-exec dirname {{}} \\; 2>/dev/null",
        )
        custom_dirs = [d for d in result.stdout.strip().split("\n") if d]

        affected_count = 0
        impact_rows: list[str] = []

        for custom_dir in custom_dirs:
            mod_name = custom_dir.rstrip("/").split("/")[-1]

            # Read __manifest__.py and extract depends
            manifest_result = await ssh.run(
                server, f"cat {custom_dir}/__manifest__.py", check=True,
            )
            try:
                manifest_data = ast.literal_eval(manifest_result.stdout)
                depends = manifest_data.get("depends", [])
            except (ValueError, SyntaxError):
                logger.warning("Cannot parse __manifest__.py for %s", mod_name)
                continue

            matched = [d for d in depends if d in changed_set]
            if matched:
                affected_count += 1
                impact_rows.append(
                    f"| {mod_name} | {', '.join(matched)} |"
                )

        # Build markdown table
        if impact_rows:
            impact_table = (
                "| Custom Module | Affected Dependencies |\n"
                "|---|---|\n"
                + "\n".join(impact_rows)
            )
        else:
            impact_table = ""

        logger.info("impact-analysis: %d custom modules affected", affected_count)
        return {
            "affected_custom_count": affected_count,
            "impact_table": impact_table,
        }

    # ── git-commit-push ────────────────────────────────────────

    @worker.task(task_type="git-commit-push", timeout_ms=120_000)
    async def git_commit_push(
        community_dir: str = "",
        enterprise_dir: str = "",
        workspace_dir: str = "",
        server_host: str = "",
        upstream_branch: str = "19.0",
        sync_mode: str = "full",
        modules: str = "",
        changed_modules: str = "",
        community_date: str = "",
        enterprise_date: str = "",
        synced_enterprise: int = 0,
        affected_custom_count: int = 0,
        impact_table: str = "",
        runbot_community_sha: str = "",
        runbot_enterprise_sha: str = "",
        **kwargs: Any,
    ) -> dict:
        """Create sync branch, commit changes, and push (from isolated workspace)."""
        server = _resolve_server(server_host)
        deploy_pat = config.github.deploy_pat
        repo = config.github.repository

        if not workspace_dir:
            raise ValueError("workspace_dir is required")
        ws = workspace_dir

        timestamp = datetime.now(ZoneInfo("Europe/Kyiv")).strftime("%Y%m%d-%H%M%S")
        branch_name = f"sync/upstream-{timestamp}"

        # Configure git identity in workspace
        await _ws_run(
            server,
            "git config user.name 'github-actions[bot]' && "
            "git config user.email 'github-actions[bot]@users.noreply.github.com'",
            check=True, workspace=ws,
        )

        # Create sync branch
        await _ws_run(server, f"git checkout -b {branch_name}", check=True, workspace=ws)

        # Stage sync changes (community + enterprise only)
        await _ws_run(
            server, "git add src/community/ src/enterprise/", check=True, workspace=ws,
        )

        # Build commit message
        com_short = runbot_community_sha[:8]
        ent_short = runbot_enterprise_sha[:8]

        if sync_mode == "selective":
            commit_msg = f"[sync] Enterprise modules ({modules}) from upstream"
        else:
            commit_msg = (
                f"[sync] Community + Enterprise from Runbot CI\\n\\n"
                f"Community:  {com_short}\\n"
                f"Enterprise: {ent_short}\\n"
                f"Source: Runbot CI (перевірена пара)"
            )

        await _ws_run(
            server,
            f'git commit --no-verify -m $\'{commit_msg}\'',
            check=True, workspace=ws,
        )

        # Push sync branch to GitHub
        push_url = (
            f"https://x-access-token:{deploy_pat}@github.com/{repo}.git"
        )
        await _ws_run(
            server,
            f"git push --no-verify {push_url} {branch_name}",
            check=True, timeout=60, workspace=ws,
        )
        logger.info("Pushed sync branch: %s", branch_name)

        # Cleanup unique tmp dirs (no longer needed after push)
        for d in (community_dir, enterprise_dir, workspace_dir):
            if d:
                await ssh.run(server, f"rm -rf {d}", check=False)

        # Save sync state on server (for next fetch-current-version)
        state_json = json.dumps({
            "community_sha": runbot_community_sha,
            "enterprise_sha": runbot_enterprise_sha,
            "synced_at": timestamp,
            "upstream_branch": upstream_branch,
        })
        repo_dir = server.repo_dir
        await ssh.run(
            server,
            f"mkdir -p {repo_dir}/.sync-state && "
            f"echo '{state_json}' > {repo_dir}/.sync-state/upstream_shas.json",
        )

        # Build PR title and body for github-create-pr
        pr_title = f"[sync] Upstream {upstream_branch} ({com_short}/{ent_short})"
        pr_body_lines = [
            f"## Upstream Sync — {upstream_branch}",
            "",
            "| | SHA | Date |",
            "|---|---|---|",
            f"| Community | `{com_short}` | {community_date} |",
            f"| Enterprise | `{ent_short}` | {enterprise_date} |",
            "",
            f"**Mode:** {sync_mode}",
            f"**Enterprise modules synced:** {synced_enterprise}",
            f"**Changed modules:** {changed_modules}",
            "",
            "### Impact on custom modules",
            f"Affected: **{affected_custom_count}** custom modules",
            "",
            impact_table,
        ]

        return {
            "sync_branch": branch_name,
            "head_branch": branch_name,
            "base_branch": "staging",
            "pr_title": pr_title,
            "pr_body": "\n".join(pr_body_lines),
            "is_draft": True,
        }

    # ── sync-code-to-demo ─────────────────────────────────────

    @worker.task(task_type="sync-code-to-demo", timeout_ms=120_000)
    async def sync_code_to_demo(
        sync_branch: str,
        server_host: str = "",
        **kwargs: Any,
    ) -> dict:
        """Fetch and checkout sync branch on demo server (git only, no deploy).

        Pulls the sync branch onto kozak_demo so the developer can review
        and fix conflict files before the full deploy runs.
        """
        server = _resolve_server(server_host)
        repo_dir = server.repo_dir

        await ssh.run(
            server,
            f"cd {repo_dir} && git fetch origin {sync_branch}",
            check=True,
            timeout=60,
        )
        await ssh.run(
            server,
            f"cd {repo_dir} && git checkout -B {sync_branch} origin/{sync_branch}",
            check=True,
        )

        logger.info("Synced code to %s: branch %s", server.host, sync_branch)
        return {"code_synced": True}

    # ── merge-to-staging ────────────────────────────────────────

    @worker.task(task_type="merge-to-staging", timeout_ms=180_000)
    async def merge_to_staging(
        sync_branch: str = "",
        server_host: str = "",
        repository: str = "",
        **kwargs: Any,
    ) -> dict:
        """Merge sync branch into staging with -X theirs and push.

        Called after deploy to demo + optional conflict resolution.
        The sync branch may contain additional fix commits pushed by the developer.
        """
        server = config.resolve_server(server_host or "staging")
        repo = repository or config.github.repository
        deploy_pat = config.github.deploy_pat

        if not sync_branch:
            raise ValueError("sync_branch is required for merge-to-staging")

        push_url = (
            f"https://x-access-token:{deploy_pat}@github.com/{repo}.git"
        )

        run_id = uuid.uuid4().hex[:8]
        workspace = f"/tmp/merge-workspace-{run_id}"

        try:
            # Merge sync branch into staging with -X theirs
            # (upstream files overwrite staging, custom modules untouched)
            merge_cmd = (
                f"git clone --depth=50 -b staging {push_url} {workspace} && "
                f"cd {workspace} && "
                f"git fetch origin {sync_branch} && "
                f"git merge origin/{sync_branch} -X theirs --no-edit && "
                f"git push --no-verify origin staging"
            )
            await ssh.run(server, merge_cmd, check=True, timeout=120)
            logger.info("Merged %s into staging (workspace %s)", sync_branch, run_id)
        finally:
            await ssh.run(server, f"rm -rf {workspace}", check=False)

        return {"staging_merged": True}

    # ── merge-feature-to-staging ──────────────────────────────

    @worker.task(task_type="merge-feature-to-staging", timeout_ms=180_000)
    async def merge_feature_to_staging(
        feature_branch: str = "",
        server_host: str = "",
        repository: str = "",
        **kwargs: Any,
    ) -> dict:
        """Merge feature branch into staging without -X theirs and push.

        Unlike merge-to-staging (which uses -X theirs for upstream sync),
        this performs a regular merge so conflicts are detected and reported
        back to the developer via BPMN error boundary event.

        Raises RuntimeError on merge conflict (caught as BPMN error).
        """
        server = config.resolve_server(server_host or "staging")
        repo = repository or config.github.repository
        deploy_pat = config.github.deploy_pat

        if not feature_branch:
            raise ValueError("feature_branch is required for merge-feature-to-staging")

        push_url = (
            f"https://x-access-token:{deploy_pat}@github.com/{repo}.git"
        )

        run_id = uuid.uuid4().hex[:8]
        workspace = f"/tmp/merge-feature-{run_id}"

        try:
            # Clone staging branch
            await ssh.run(
                server,
                f"git clone --depth=50 -b staging {push_url} {workspace}",
                check=True, timeout=120,
            )

            # Fetch feature branch
            await ssh.run(
                server,
                f"cd {workspace} && git fetch origin {feature_branch}",
                check=True, timeout=60,
            )

            # Merge feature into staging (no -X theirs — conflicts must be detected)
            merge_result = await ssh.run(
                server,
                f"cd {workspace} && git merge origin/{feature_branch} --no-edit",
                check=False, timeout=60,
            )

            if merge_result.exit_code != 0:
                raise RuntimeError(
                    f"Merge conflict: cannot merge {feature_branch} into staging. "
                    f"Please rebase your branch on main and resolve conflicts."
                )

            # Push merged staging
            await ssh.run(
                server,
                f"cd {workspace} && git push --no-verify origin staging",
                check=True, timeout=60,
            )

            logger.info("Merged %s into staging (workspace %s)", feature_branch, run_id)
        finally:
            # Cleanup workspace
            await ssh.run(server, f"rm -rf {workspace}", check=False)

        return {"staging_merged": True}

    # ── github-pr-ready ────────────────────────────────────────

    @worker.task(task_type="github-pr-ready", timeout_ms=60_000)
    async def github_pr_ready(
        pr_number: int,
        repository: str = "",
        **kwargs: Any,
    ) -> dict:
        """Mark a draft PR as ready for review.

        Called by the feature-to-production process after staging deploy
        and second PR-Agent review pass to undraft the PR targeting main.
        """
        repo = repository or config.github.repository

        await github.mark_pr_ready(repo, pr_number)
        logger.info("Marked PR #%d as ready in %s", pr_number, repo)

        return {}
