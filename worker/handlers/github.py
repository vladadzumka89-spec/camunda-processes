"""GitHub-related handlers — 4 task types.

Source: .github/workflows/pr_agent.yml
Handles PR-Agent review, merge, comment, and PR creation.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from pyzeebe import ZeebeWorker

from ..config import AppConfig
from ..github_client import GitHubClient
from ..ssh import AsyncSSHClient

logger = logging.getLogger(__name__)


def register_github_handlers(
    worker: ZeebeWorker,
    config: AppConfig,
    ssh: AsyncSSHClient,
    github: GitHubClient,
) -> None:
    """Register all GitHub task handlers."""

    # ── pr-agent-review ────────────────────────────────────────

    @worker.task(task_type="pr-agent-review", timeout_ms=600_000)
    async def pr_agent_review(
        pr_number: int,
        pr_url: str,
        repository: str = "",
        **kwargs: Any,
    ) -> dict:
        """Run PR-Agent review and parse the score.

        Launches PR-Agent via Docker, then fetches the review comment
        to extract the score and security assessment.
        """
        repo = repository or config.github.repository

        # Run PR-Agent via Docker on a server (kozak_demo or any available)
        pr_agent_server = None
        for name in ("kozak_demo", "staging"):
            if name in config.servers:
                pr_agent_server = config.servers[name]
                break

        if pr_agent_server:
            # Run PR-Agent container on remote server
            await ssh.run(
                pr_agent_server,
                f"docker run --rm "
                f"-e OPENROUTER__KEY='{config.openrouter_api_key}' "
                f"-e GITHUB_TOKEN='{config.github.token}' "
                f"-e CONFIG.PR_AGENT_CONFIG_PATH='.pr_agent.toml' "
                f"codiumai/pr-agent:latest "
                f"--pr_url={pr_url} review",
                timeout=300,
            )
        else:
            logger.warning("No server available for PR-Agent, skipping review execution")

        # Parse the review comment
        comment = await github.get_bot_review_comment(repo, pr_number)
        if not comment:
            logger.warning("No PR-Agent review comment found for PR #%d", pr_number)
            return {"review_score": 0, "has_critical_issues": False}

        body = comment.get("body", "")
        score = _parse_review_score(body)
        has_critical = _has_critical_security_issues(body)

        logger.info(
            "pr-agent-review PR #%d: score=%d, critical=%s",
            pr_number, score, has_critical,
        )
        return {
            "review_score": score,
            "has_critical_issues": has_critical,
        }

    # ── github-merge ───────────────────────────────────────────

    @worker.task(task_type="github-merge", timeout_ms=60_000)
    async def github_merge(
        pr_number: int,
        repository: str = "",
        pr_title: str = "",
        **kwargs: Any,
    ) -> dict:
        """Squash-merge a PR."""
        repo = repository or config.github.repository
        commit_title = f"{pr_title} (#{pr_number})" if pr_title else None

        await github.merge_pr(
            repo, pr_number, method="squash", commit_title=commit_title,
        )
        logger.info("Merged PR #%d in %s", pr_number, repo)
        return {}

    # ── github-comment ─────────────────────────────────────────

    @worker.task(task_type="github-comment", timeout_ms=30_000)
    async def github_comment(
        pr_number: int,
        comment_text: str,
        repository: str = "",
        **kwargs: Any,
    ) -> dict:
        """Post a comment on a PR."""
        repo = repository or config.github.repository
        await github.comment_pr(repo, pr_number, comment_text)
        logger.info("Commented on PR #%d in %s", pr_number, repo)
        return {}

    # ── github-create-pr ───────────────────────────────────────

    @worker.task(task_type="github-create-pr", timeout_ms=60_000)
    async def github_create_pr(
        head_branch: str,
        base_branch: str,
        pr_title: str,
        repository: str = "",
        pr_body: str = "",
        is_draft: bool = False,
        **kwargs: Any,
    ) -> dict:
        """Create a new pull request."""
        repo = repository or config.github.repository

        result = await github.create_pr(
            repo,
            head=head_branch,
            base=base_branch,
            title=pr_title,
            body=pr_body,
            draft=is_draft,
        )

        pr_url = result.get("html_url", "")
        pr_number = result.get("number", 0)
        logger.info("Created PR #%d: %s", pr_number, pr_url)

        return {"pr_url": pr_url, "pr_number": pr_number}


# ── Score Parsing Helpers ──────────────────────────────────────


def _parse_review_score(body: str) -> int:
    """Extract review score from PR-Agent comment.

    Score formats:
      - HTML table: <strong>Score</strong>: 85
      - Emoji: Score: 85
      - Plain: Score: 8/10
    Normalizes 100-point scale to 10-point.
    """
    # Strip HTML tags for easier parsing
    clean = re.sub(r"<[^>]+>", "", body)

    # Try "Score: NUMBER" pattern
    match = re.search(r"[Ss]core[^0-9]*(\d+)", clean)
    if not match:
        # Try emoji pattern
        match = re.search(r"🏅[^0-9]*(\d+)", clean)

    if not match:
        return 0

    score = int(match.group(1))

    # Normalize: if > 10, assume 100-point scale
    if score > 10:
        score = score // 10

    return score


def _has_critical_security_issues(body: str) -> bool:
    """Check if the review has critical security concerns."""
    if "No security concerns identified" in body:
        return False

    # Extract security section (between lock emoji and </tr> or next section)
    security_match = re.search(r"🔒(.*?)(?:</tr>|$)", body, re.DOTALL)
    if not security_match:
        return False

    security_text = re.sub(r"<[^>]+>", "", security_match.group(1))
    return bool(re.search(r"critical|high severity|блокер|критичн", security_text, re.IGNORECASE))
