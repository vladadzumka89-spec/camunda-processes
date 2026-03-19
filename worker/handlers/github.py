"""GitHub-related handlers — 4 task types.

Source: .github/workflows/pr_agent.yml
Handles PR-Agent review, merge, comment, and PR creation.
"""

import asyncio
import json
import logging
import re
from typing import Any

from pyzeebe import Job, ZeebeWorker

from ..config import AppConfig
from ..github_client import GitHubClient
from ..ssh import AsyncSSHClient

logger = logging.getLogger(__name__)


REVIEW_SYSTEM_PROMPT = """You are a code reviewer for an Odoo 19 Enterprise project.
Analyze the PR diff and provide a structured review.

Scoring guide (0-10):
- 9-10: Excellent, no issues
- 7-8: Good, minor suggestions only
- 5-6: Needs improvement, has notable issues
- 3-4: Significant problems
- 0-2: Critical issues, should not be merged

Mark critical=true ONLY for: security vulnerabilities, data loss risks, or production-breaking bugs.

Review in context of Odoo module development: Python, XML views, security CSV, manifests."""

REVIEW_JSON_SCHEMA = json.dumps({
    "type": "object",
    "properties": {
        "score": {"type": "integer", "minimum": 0, "maximum": 10},
        "critical": {"type": "boolean"},
        "summary": {"type": "string"},
        "issues": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "severity": {"enum": ["critical", "major", "minor", "nit"]},
                    "file": {"type": "string"},
                    "description": {"type": "string"}
                },
                "required": ["severity", "file", "description"]
            }
        }
    },
    "required": ["score", "critical", "summary", "issues"]
})

FALLBACK_RESULT = {
    "score": 0,
    "critical": False,
    "summary": "Review failed — Claude Code unavailable or returned invalid response.",
    "issues": [],
}


async def _run_claude_review(
    diff: str,
    pr_number: int,
    repo: str,
    timeout: int = 300,
) -> dict:
    """Run Claude Code CLI to review a PR diff.

    Returns dict with keys: score, critical, summary, issues.
    On any failure returns FALLBACK_RESULT.
    """
    prompt = (
        f"Review this PR #{pr_number} in {repo}. "
        f"Analyze the diff below and provide your assessment.\n\n"
        f"```diff\n{diff[:80000]}\n```"
    )

    try:
        proc = await asyncio.create_subprocess_exec(
            "claude", "-p", prompt,
            "--output-format", "json",
            "--json-schema", REVIEW_JSON_SCHEMA,
            "--append-system-prompt", REVIEW_SYSTEM_PROMPT,
            "--max-turns", "1",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(), timeout=timeout,
        )
    except asyncio.TimeoutError:
        logger.error("Claude Code timed out after %ds for PR #%d", timeout, pr_number)
        proc.kill()
        await proc.wait()
        return dict(FALLBACK_RESULT)
    except FileNotFoundError:
        logger.error("Claude Code CLI not found — is 'claude' installed?")
        return dict(FALLBACK_RESULT)

    if proc.returncode != 0:
        logger.error(
            "Claude Code exited %d for PR #%d. stderr: %s",
            proc.returncode, pr_number,
            stderr.decode()[-2000:] if stderr else "(empty)",
        )
        return dict(FALLBACK_RESULT)

    try:
        envelope = json.loads(stdout.decode())
        # claude -p --output-format json wraps result in {"result": "...", ...}
        structured = envelope.get("structured_output")
        if structured and isinstance(structured, dict):
            return structured
        # Fallback: parse result field as JSON string
        raw = envelope.get("result", "")
        return json.loads(raw) if isinstance(raw, str) else dict(FALLBACK_RESULT)
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        logger.error("Failed to parse Claude output for PR #%d: %s", pr_number, exc)
        return dict(FALLBACK_RESULT)


_SEVERITY_ICONS = {
    "critical": "\U0001f534",  # red circle
    "major": "\U0001f7e1",     # yellow circle
    "minor": "\U0001f7e2",     # green circle
    "nit": "\U0001f7e2",       # green circle
}


def _format_review_comment(review: dict) -> str:
    """Format review result as a markdown GitHub comment."""
    score = review.get("score", 0)
    summary = review.get("summary", "")
    issues = review.get("issues", [])

    lines = [f"## \U0001f916 Claude Code Review \u2014 Score: {score}/10", ""]
    if summary:
        lines.append(summary)
        lines.append("")

    if not issues:
        lines.append("\u2705 No issues found")
    else:
        lines.append("### Issues")
        lines.append("")
        for issue in issues:
            icon = _SEVERITY_ICONS.get(issue.get("severity", "minor"), "\U0001f7e2")
            sev = issue.get("severity", "minor")
            f = issue.get("file", "")
            desc = issue.get("description", "")
            lines.append(f"{icon} **{sev}** \u00b7 `{f}`")
            lines.append(f"{desc}")
            lines.append("")

    lines.append("---")
    lines.append("*Reviewed by Claude Code (Max subscription)*")
    return "\n".join(lines)


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
        job: Job,
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
            docker_cmd = (
                f"docker run --rm "
                f"-e OPENROUTER.KEY='{config.openrouter_api_key}' "
                f"-e GITHUB.USER_TOKEN='{config.github.token}' "
                f"-e CONFIG.MODEL='openrouter/x-ai/grok-code-fast-1' "
                f"codiumai/pr-agent:latest "
                f"--pr_url={pr_url} review"
            )
            logger.info(
                "Running PR-Agent on %s for PR #%d: %s",
                pr_agent_server.host, pr_number,
                docker_cmd.replace(config.openrouter_api_key, "***")
                          .replace(config.github.token, "***"),
            )
            result = await ssh.run(
                pr_agent_server, docker_cmd, timeout=300,
            )
            if not result.success:
                logger.error(
                    "PR-Agent failed on %s (exit %d) for PR #%d.\nSTDOUT: %s\nSTDERR: %s",
                    pr_agent_server.host, result.exit_code, pr_number,
                    result.stdout[-2000:] if result.stdout else "(empty)",
                    result.stderr[-2000:] if result.stderr else "(empty)",
                )
            else:
                logger.info(
                    "PR-Agent completed on %s for PR #%d (exit 0). Output tail: %s",
                    pr_agent_server.host, pr_number,
                    result.stdout[-500:] if result.stdout else "(empty)",
                )
        else:
            logger.warning("No server available for PR-Agent, skipping review execution")

        # Parse the review comment
        comment = await github.get_bot_review_comment(repo, pr_number)
        if not comment:
            logger.warning(
                "No PR-Agent review comment found for PR #%d in %s. "
                "PR-Agent Docker likely failed — check logs above.",
                pr_number, repo,
            )
            return {
                "review_score": 0,
                "has_critical_issues": False,
                "process_instance_key": job.process_instance_key,
            }

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
            "process_instance_key": job.process_instance_key,
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
