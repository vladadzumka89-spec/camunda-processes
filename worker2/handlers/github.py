"""GitHub-related handlers — 4 task types.

Handles Codex CLI review, merge, comment, and PR creation.
"""

import asyncio
import json
import logging
from pathlib import Path
from typing import Any

from pyzeebe import Job, ZeebeWorker

from ..config import AppConfig
from ..github_client import GitHubClient
from ..ssh import AsyncSSHClient

logger = logging.getLogger(__name__)

# Limit concurrent review processes.
_REVIEW_SEMAPHORE = asyncio.Semaphore(2)

_REVIEW_PROMPT_PATH = Path(__file__).resolve().parent.parent / "review_prompt.txt"

REVIEW_SYSTEM_PROMPT = (
    _REVIEW_PROMPT_PATH.read_text(encoding="utf-8")
    if _REVIEW_PROMPT_PATH.exists()
    else "You are a code reviewer for Odoo 19 Enterprise. Respond in Ukrainian."
)

_REVIEW_SCHEMA_OBJ = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "score": {"type": "integer", "minimum": 0, "maximum": 10},
        "critical": {"type": "boolean"},
        "summary": {"type": "string"},
        "issues": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "severity": {"enum": ["critical", "major", "minor", "nit"]},
                    "file": {"type": "string"},
                    "description": {"type": "string"},
                },
                "required": ["severity", "file", "description"],
            },
        },
    },
    "required": ["score", "critical", "summary", "issues"],
}

# Write schema to a file once — Codex CLI's --output-schema needs a path.
_REVIEW_SCHEMA_PATH = Path("/tmp/codex_review_schema.json")
_REVIEW_SCHEMA_PATH.write_text(json.dumps(_REVIEW_SCHEMA_OBJ), encoding="utf-8")

FALLBACK_RESULT = {
    "score": 0,
    "critical": False,
    "summary": "Review failed — Codex CLI unavailable or returned invalid response.",
    "issues": [],
}


async def _run_review_once(
    diff: str,
    pr_number: int,
    repo: str,
    timeout: int = 500,
) -> dict | None:
    """Single attempt to run Codex CLI review.

    Returns structured result dict on success, None on failure (for retry).
    Acquires _REVIEW_SEMAPHORE to avoid concurrent API rate-limit issues.
    """
    prompt = (
        f"{REVIEW_SYSTEM_PROMPT}\n\n"
        f"---\n\n"
        f"Review this PR #{pr_number} in {repo}. "
        f"Analyze the diff below and provide your assessment.\n"
        f"Return your final response ONLY as JSON matching the required schema "
        f"(fields: score 0-10, critical bool, summary, issues[]).\n\n"
        f"```diff\n{diff[:80000]}\n```"
    )

    async with _REVIEW_SEMAPHORE:
        try:
            # Codex CLI 0.130+ takes prompt as positional arg; no more --prompt / --append-system-prompt.
            # --skip-git-repo-check: worker CWD (/app) is not a git repo.
            # --output-schema: forces the final agent message to be JSON matching the schema.
            proc = await asyncio.create_subprocess_exec(
                "codex", "exec",
                "--json",
                "--skip-git-repo-check",
                "--output-schema", str(_REVIEW_SCHEMA_PATH),
                prompt,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=timeout,
            )
        except asyncio.TimeoutError:
            logger.error("Codex CLI timed out after %ds for PR #%d", timeout, pr_number)
            proc.kill()
            await proc.wait()
            return None
        except FileNotFoundError:
            logger.error("Codex CLI not found — is 'codex' installed?")
            return None

        if proc.returncode != 0:
            logger.error(
                "Codex CLI exited %d for PR #%d. stderr: %s",
                proc.returncode, pr_number,
                stderr.decode()[-2000:] if stderr else "(empty)",
            )
            return None

        # Codex CLI returns a JSONL stream. Walk it in reverse to find the
        # last agent_message and parse its text as JSON matching the schema.
        raw_stdout = stdout.decode()
        last_agent_text: str | None = None
        for line in reversed(raw_stdout.strip().split('\n')):
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get("type") != "item.completed":
                continue
            item = event.get("item", {})
            if item.get("type") != "agent_message":
                continue
            last_agent_text = item.get("text", "")
            break

        if last_agent_text is None:
            logger.error(
                "Codex output for PR #%d had no agent_message. stdout tail: %s",
                pr_number, raw_stdout[-1000:],
            )
            return None

        try:
            structured = json.loads(last_agent_text)
        except json.JSONDecodeError as exc:
            logger.error(
                "Codex agent_message for PR #%d is not valid JSON: %s. Text: %s",
                pr_number, exc, last_agent_text[:1000],
            )
            return None

        issues = structured.get("issues") or []
        return {
            "score": int(structured.get("score", 0)),
            "critical": bool(structured.get("critical", False)),
            "summary": structured.get("summary", ""),
            "issues": issues,
        }


async def _run_review(
    diff: str,
    pr_number: int,
    repo: str,
    timeout: int = 500,
    max_retries: int = 3,
    retry_delay: int = 30,
) -> dict:
    """Run Codex CLI to review a PR diff with retries.

    Retries up to max_retries times with retry_delay seconds between attempts.
    Returns dict with keys: score, critical, summary, issues.
    On all failures returns FALLBACK_RESULT.
    """
    for attempt in range(1, max_retries + 1):
        result = await _run_review_once(diff, pr_number, repo, timeout)
        if result is not None:
            return result
        if attempt < max_retries:
            logger.warning(
                "Codex review attempt %d/%d failed for PR #%d — retrying in %ds",
                attempt, max_retries, pr_number, retry_delay,
            )
            await asyncio.sleep(retry_delay)

    logger.error("All %d Codex review attempts failed for PR #%d", max_retries, pr_number)
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

    lines = [f"## \U0001f916 Codex Code Review \u2014 Score: {score}/10", ""]
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
    lines.append("*Reviewed by Codex CLI (Subscription)*")
    return "\n".join(lines)


def register_github_handlers(
    worker: ZeebeWorker,
    config: AppConfig,
    ssh: AsyncSSHClient,
    github: GitHubClient,
) -> None:
    """Register all GitHub task handlers."""

    # ── codex-review / pr-agent-review ─────────────────────────

    @worker.task(task_type="codex-review", timeout_ms=1_800_000)
    @worker.task(task_type="pr-agent-review", timeout_ms=1_800_000)  # Alias for legacy BPMN
    async def codex_review_handler(
        job: Job,
        pr_number: int,
        pr_url: str,
        repository: str = "",
        **kwargs: Any,
    ) -> dict:
        """Run Codex CLI review on a PR and return score.

        Fetches the PR diff via GitHub API, sends it to Codex CLI
        for structured review, posts a formal GitHub Review,
        and returns score + critical flag to Zeebe.
        """
        repo = repository or config.github.repository

        # 1. Fetch diff
        try:
            diff = await github.get_pr_diff(repo, pr_number)
        except Exception as exc:
            logger.error("Failed to fetch diff for PR #%d: %s", pr_number, exc)
            return {
                "review_score": 0,
                "has_critical_issues": False,
                "process_instance_key": job.process_instance_key,
            }

        # 2. Run Codex CLI review
        review = await _run_review(diff, pr_number, repo)
        score = review.get("score", 0)
        critical = review.get("critical", False)

        logger.info(
            "codex-review PR #%d: score=%d, critical=%s",
            pr_number, score, critical,
        )

        # 3. Post or update review comment on PR (single comment per PR, edited on re-run).
        try:
            comment_body = _format_review_comment(review)
            await github.upsert_comment(
                repo, pr_number, comment_body, marker="Codex Code Review",
            )
            logger.info("Posted/updated Codex review comment on PR #%d in %s", pr_number, repo)
        except Exception as exc:
            logger.warning("Failed to post review comment on PR #%d: %s", pr_number, exc)

        return {
            "review_score": score,
            "has_critical_issues": critical,
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
        ignore_errors: bool = False,
        **kwargs: Any,
    ) -> dict:
        """Post a comment on a PR."""
        repo = repository or config.github.repository
        try:
            await github.comment_pr(repo, pr_number, comment_text)
            logger.info("Commented on PR #%d in %s", pr_number, repo)
        except Exception as exc:
            if ignore_errors:
                logger.warning("Failed to comment on PR #%d (ignored): %s", pr_number, exc)
            else:
                raise
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
