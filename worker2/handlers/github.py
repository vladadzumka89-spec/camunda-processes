"""GitHub-related handlers.

Handles Codex CLI review, merge, comments, deploy status, and PR creation.
"""

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any

from pyzeebe import Job, ZeebeWorker

from ..config import AppConfig
from ..errors import ReviewUnavailableError
from ..github_client import GitHubClient
from ..ssh import AsyncSSHClient

logger = logging.getLogger(__name__)

# Limit concurrent review processes.
_REVIEW_SEMAPHORE = asyncio.Semaphore(2)
MAX_REVIEW_DIFF_KB = int(os.getenv("CODEX_REVIEW_MAX_DIFF_KB", "1024"))
MAX_REVIEW_DIFF_BYTES = MAX_REVIEW_DIFF_KB * 1024

# Claude is used as a fallback reviewer when Codex is unavailable (usage limit, etc.).
CLAUDE_REVIEW_MODEL = os.getenv("CLAUDE_REVIEW_MODEL", "sonnet")

# stderr substrings that indicate Codex hit a usage/rate limit — fall back immediately
# rather than burning through retry delays waiting for a quota that won't refill in time.
_CODEX_USAGE_LIMIT_MARKERS = (
    "usage limit",
    "rate limit",
    "rate_limit",
    "ratelimit",
    "quota",
    "insufficient_quota",
    "too many requests",
    "429",
)


class _CodexUsageLimitError(Exception):
    """Internal signal: Codex CLI hit a usage/rate limit — switch to the fallback reviewer."""

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


def _take_utf8_bytes(text: str, max_bytes: int, *, from_end: bool = False) -> str:
    """Take a UTF-8 byte budget from text without splitting code points."""
    if max_bytes <= 0:
        return ""
    raw = text.encode("utf-8")
    if len(raw) <= max_bytes:
        return text
    if from_end:
        return raw[-max_bytes:].decode("utf-8", errors="ignore")
    return raw[:max_bytes].decode("utf-8", errors="ignore")


def _limit_review_diff(diff: str, max_bytes: int = MAX_REVIEW_DIFF_BYTES) -> tuple[str, int]:
    """Limit review diff size in bytes while preserving beginning and end context."""
    diff_bytes = len(diff.encode("utf-8"))
    if max_bytes <= 0 or diff_bytes <= max_bytes:
        return diff, 0

    marker = (
        "\n\n"
        f"... Diff truncated by review worker: {diff_bytes - max_bytes} bytes omitted ...\n"
        "\n"
    )
    marker_bytes = len(marker.encode("utf-8"))
    keep_bytes = max(max_bytes - marker_bytes, 0)
    head_bytes = int(keep_bytes * 0.7)
    tail_bytes = keep_bytes - head_bytes
    head = _take_utf8_bytes(diff, head_bytes)
    tail = _take_utf8_bytes(diff, tail_bytes, from_end=True)
    omitted = max(diff_bytes - len(head.encode("utf-8")) - len(tail.encode("utf-8")), 0)
    return f"{head}{marker}{tail}", omitted


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
    review_diff, omitted_bytes = _limit_review_diff(diff)
    truncation_note = (
        f"\nThe diff was truncated to {MAX_REVIEW_DIFF_KB} KB; "
        f"{omitted_bytes} bytes were omitted. Mention this limitation in the summary.\n"
        if omitted_bytes
        else ""
    )
    prompt = (
        f"{REVIEW_SYSTEM_PROMPT}\n\n"
        f"---\n\n"
        f"Review this PR #{pr_number} in {repo}. "
        f"Analyze the diff below and provide your assessment.\n"
        f"{truncation_note}"
        f"Return your final response ONLY as JSON matching the required schema "
        f"(fields: score 0-10, critical bool, summary, issues[]).\n\n"
        f"```diff\n{review_diff}\n```"
    )

    async with _REVIEW_SEMAPHORE:
        try:
            # Codex CLI 0.130+ reads prompt from stdin when "-" is passed.
            # --skip-git-repo-check: worker CWD (/app) is not a git repo.
            # --output-schema: forces the final agent message to be JSON matching the schema.
            proc = await asyncio.create_subprocess_exec(
                "codex", "exec",
                "--json",
                "--skip-git-repo-check",
                "--output-schema", str(_REVIEW_SCHEMA_PATH),
                "-",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                stdin=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(prompt.encode()), timeout=timeout,
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
            stderr_text = stderr.decode() if stderr else ""
            logger.error(
                "Codex CLI exited %d for PR #%d. stderr: %s",
                proc.returncode, pr_number,
                stderr_text[-2000:] or "(empty)",
            )
            lowered = stderr_text.lower()
            if any(marker in lowered for marker in _CODEX_USAGE_LIMIT_MARKERS):
                raise _CodexUsageLimitError(
                    f"Codex usage/rate limit for PR #{pr_number}: "
                    f"{stderr_text.strip()[-300:] or 'limit reached'}"
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
            "engine": "codex",
        }


def _extract_json_object(text: str) -> str | None:
    """Extract the first JSON object from free-form text (strips ``` fences)."""
    if not text:
        return None
    stripped = text.strip()
    # Drop a leading ```json / ``` fence and the trailing fence, if present.
    if stripped.startswith("```"):
        stripped = stripped.split("\n", 1)[-1] if "\n" in stripped else stripped
        if stripped.endswith("```"):
            stripped = stripped[: -3]
        stripped = stripped.strip()
        if stripped.startswith("json"):
            stripped = stripped[4:].strip()
    start = stripped.find("{")
    end = stripped.rfind("}")
    if start == -1 or end == -1 or end < start:
        return None
    return stripped[start : end + 1]


async def _run_review_claude_once(
    diff: str,
    pr_number: int,
    repo: str,
    timeout: int = 500,
) -> dict | None:
    """Single fallback review attempt via the Claude Code CLI.

    Used when Codex is unavailable. Returns a structured result dict on success,
    None on failure. Shares _REVIEW_SEMAPHORE to keep total concurrency bounded.
    """
    review_diff, omitted_bytes = _limit_review_diff(diff)
    truncation_note = (
        f"\nThe diff was truncated to {MAX_REVIEW_DIFF_KB} KB; "
        f"{omitted_bytes} bytes were omitted. Mention this limitation in the summary.\n"
        if omitted_bytes
        else ""
    )
    prompt = (
        f"{REVIEW_SYSTEM_PROMPT}\n\n"
        f"---\n\n"
        f"Review this PR #{pr_number} in {repo}. "
        f"Analyze the diff below and provide your assessment.\n"
        f"{truncation_note}"
        f"Return your final response ONLY as a single JSON object with exactly these "
        f"fields: score (integer 0-10), critical (boolean), summary (string), "
        f"issues (array of objects with severity one of "
        f"[critical, major, minor, nit], file, description). "
        f"Do not wrap it in markdown fences or add any text outside the JSON.\n\n"
        f"```diff\n{review_diff}\n```"
    )

    # Claude Code refuses to run as root, so drop to the dedicated claude-runner
    # user (its credentials are mounted at /home/claude-runner/.claude). Outside a
    # root container (e.g. tests) run it directly.
    claude_argv = [
        "claude", "-p",
        "--output-format", "json",
        "--model", CLAUDE_REVIEW_MODEL,
    ]
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        claude_argv = ["sudo", "-H", "-u", "claude-runner", *claude_argv]

    async with _REVIEW_SEMAPHORE:
        try:
            # Print mode (-p) reads the prompt from stdin and emits a JSON envelope.
            proc = await asyncio.create_subprocess_exec(
                *claude_argv,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                stdin=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(prompt.encode()), timeout=timeout,
            )
        except asyncio.TimeoutError:
            logger.error("Claude CLI timed out after %ds for PR #%d", timeout, pr_number)
            proc.kill()
            await proc.wait()
            return None
        except FileNotFoundError:
            logger.error("Claude CLI not found — is '@anthropic-ai/claude-code' installed?")
            return None

        if proc.returncode != 0:
            logger.error(
                "Claude CLI exited %d for PR #%d. stderr: %s",
                proc.returncode, pr_number,
                stderr.decode()[-2000:] if stderr else "(empty)",
            )
            return None

        # Outer envelope: {"type":"result","subtype":"success","result":"<text>",...}
        try:
            envelope = json.loads(stdout.decode())
        except json.JSONDecodeError as exc:
            logger.error(
                "Claude output for PR #%d is not valid JSON: %s. stdout tail: %s",
                pr_number, exc, stdout.decode()[-1000:],
            )
            return None

        if envelope.get("is_error") or envelope.get("subtype") != "success":
            logger.error(
                "Claude review for PR #%d returned error envelope: %s",
                pr_number, str(envelope)[:1000],
            )
            return None

        result_text = envelope.get("result", "")
        json_text = _extract_json_object(result_text)
        if json_text is None:
            logger.error(
                "Claude review for PR #%d had no JSON object. Text: %s",
                pr_number, result_text[:1000],
            )
            return None

        try:
            structured = json.loads(json_text)
        except json.JSONDecodeError as exc:
            logger.error(
                "Claude review for PR #%d is not valid JSON: %s. Text: %s",
                pr_number, exc, json_text[:1000],
            )
            return None

        issues = structured.get("issues") or []
        return {
            "score": int(structured.get("score", 0)),
            "critical": bool(structured.get("critical", False)),
            "summary": structured.get("summary", ""),
            "issues": issues,
            "engine": "claude",
        }


async def _run_review(
    diff: str,
    pr_number: int,
    repo: str,
    timeout: int = 500,
    max_retries: int = 3,
    retry_delay: int = 30,
) -> dict:
    """Run a code review of a PR diff: Codex first, Claude as fallback.

    Codex is retried up to max_retries times. If Codex hits a usage/rate limit,
    or exhausts its retries, the review falls back to the Claude CLI. Returns a
    dict with keys: score, critical, summary, issues, engine.
    Raises ReviewUnavailableError when neither engine produces a valid review.
    """
    codex_unavailable_reason: str | None = None
    for attempt in range(1, max_retries + 1):
        try:
            result = await _run_review_once(diff, pr_number, repo, timeout)
        except _CodexUsageLimitError as exc:
            logger.warning(
                "Codex usage limit for PR #%d — falling back to Claude: %s",
                pr_number, exc,
            )
            codex_unavailable_reason = str(exc)
            break
        if result is not None:
            return result
        if attempt < max_retries:
            logger.warning(
                "Codex review attempt %d/%d failed for PR #%d — retrying in %ds",
                attempt, max_retries, pr_number, retry_delay,
            )
            await asyncio.sleep(retry_delay)

    if codex_unavailable_reason is None:
        logger.error("All %d Codex review attempts failed for PR #%d", max_retries, pr_number)

    # ── Fallback: Claude CLI ──────────────────────────────────────
    logger.warning("Falling back to Claude review for PR #%d", pr_number)
    for attempt in range(1, max_retries + 1):
        result = await _run_review_claude_once(diff, pr_number, repo, timeout)
        if result is not None:
            logger.info("Claude fallback review succeeded for PR #%d", pr_number)
            return result
        if attempt < max_retries:
            logger.warning(
                "Claude review attempt %d/%d failed for PR #%d — retrying in %ds",
                attempt, max_retries, pr_number, retry_delay,
            )
            await asyncio.sleep(retry_delay)

    logger.error(
        "Both Codex and Claude reviews failed for PR #%d", pr_number,
    )
    raise ReviewUnavailableError(
        f"Neither Codex nor Claude produced a valid review for PR #{pr_number}. "
        f"Codex: {codex_unavailable_reason or 'failed after retries'}."
    )


async def _get_pr_diff_with_retries(
    github: GitHubClient,
    repo: str,
    pr_number: int,
    max_retries: int = 3,
    retry_delay: int = 20,
) -> str:
    """Fetch GitHub PR diff, falling back to the files API when .diff fails."""
    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            return await github.get_pr_diff(repo, pr_number)
        except Exception as exc:
            last_exc = exc
            if attempt < max_retries:
                logger.warning(
                    "Failed to fetch diff for PR #%d (attempt %d/%d): %s; retrying in %ds",
                    pr_number, attempt, max_retries, exc, retry_delay,
                )
                await asyncio.sleep(retry_delay)

    logger.warning(
        "Native diff unavailable for PR #%d after %d attempts; trying files API fallback",
        pr_number, max_retries,
    )
    for attempt in range(1, max_retries + 1):
        try:
            fallback_diff = await github.get_pr_diff_from_files(repo, pr_number)
            if fallback_diff:
                logger.info("Built fallback diff for PR #%d from files API", pr_number)
                return fallback_diff
            last_exc = ReviewUnavailableError("GitHub files API returned an empty diff")
        except Exception as exc:
            last_exc = exc
            if attempt < max_retries:
                logger.warning(
                    "Failed to build files API diff for PR #%d (attempt %d/%d): %s; "
                    "retrying in %ds",
                    pr_number, attempt, max_retries, exc, retry_delay,
                )
                await asyncio.sleep(retry_delay)

    raise ReviewUnavailableError(
        f"GitHub diff is unavailable for PR #{pr_number} after native and files API attempts: "
        f"{last_exc}"
    ) from last_exc


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
    engine = review.get("engine", "codex")

    lines = [f"## \U0001f916 Codex Code Review \u2014 Score: {score}/10", ""]
    if engine == "claude":
        lines.append(
            "_\u2139\ufe0f Codex \u0431\u0443\u0432 \u043d\u0435\u0434\u043e\u0441\u0442\u0443\u043f\u043d\u0438\u0439 (\u0432\u0438\u0447\u0435\u0440\u043f\u0430\u043d\u043e \u043b\u0456\u043c\u0456\u0442) \u2014 "
            "\u0440\u0435\u0432'\u044e \u0432\u0438\u043a\u043e\u043d\u0430\u043d\u043e \u0440\u0435\u0437\u0435\u0440\u0432\u043d\u0438\u043c \u0440\u0435\u0432'\u044e\u0432\u0435\u0440\u043e\u043c._"
        )
        lines.append("")
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
    if engine == "claude":
        lines.append("*Reviewed by Claude CLI (fallback)*")
    else:
        lines.append("*Reviewed by Codex CLI (Subscription)*")
    return "\n".join(lines)


def _format_review_unavailable_comment(
    reason: str,
    head_sha: str = "",
    process_instance_key: str | int = "",
    retries_left: int | None = None,
) -> str:
    """Format a status comment for technical review failures without a fake score."""
    safe_reason = (reason or "Unknown technical error").strip()
    safe_reason = safe_reason.replace("```", "` ` `")[:1200]

    lines = [
        "## \U0001f916 Codex Code Review — Review unavailable",
        "",
        "\u26a0\ufe0f Автоматичне ревʼю не виконалось, тому score не оновлено.",
        "Попередня оцінка в цьому PR не є актуальною для поточного commit.",
        "",
        "**Причина:**",
        "",
        "```",
        safe_reason,
        "```",
    ]
    if head_sha:
        lines.extend(["", f"**Head SHA:** `{head_sha[:12]}`"])
    if process_instance_key:
        lines.extend([
            "",
            "[Процес в Operate]"
            f"(http://camunda-demo.a.local:8088/operate/processes/{process_instance_key})",
        ])

    if retries_left is not None:
        lines.append("")
        if retries_left > 0:
            lines.append(f"Camunda повторить ревʼю автоматично. Залишилось retries: {retries_left}.")
        else:
            lines.append(
                "Автоматичні retries вичерпано. Перезапустіть ревʼю після того, "
                "як GitHub знову віддаватиме diff."
            )

    lines.append("---")
    lines.append("*Status reported by Codex review worker*")
    return "\n".join(lines)


async def _mark_review_unavailable(
    github: GitHubClient,
    repo: str,
    pr_number: int,
    reason: str,
    head_sha: str,
    process_instance_key: str | int,
    retries_left: int | None,
) -> None:
    """Replace the mutable Codex review comment with an explicit unavailable state."""
    try:
        comment_body = _format_review_unavailable_comment(
            reason,
            head_sha=head_sha,
            process_instance_key=process_instance_key,
            retries_left=retries_left,
        )
        await github.upsert_comment(
            repo,
            pr_number,
            comment_body,
            marker="Codex Code Review",
            update_note=None,
        )
        logger.info("Posted/updated review unavailable comment on PR #%d in %s", pr_number, repo)
    except Exception as exc:
        logger.warning("Failed to post review unavailable comment on PR #%d: %s", pr_number, exc)


def _format_deploy_status_comment(
    status: str,
    head_branch: str = "",
    merge_sha: str = "",
    process_instance_key: str | int = "",
    error_message: str = "",
    error_traceback: str = "",
) -> str:
    """Format the single mutable staging deploy status comment."""
    normalized = (status or "started").strip().lower()
    if normalized in {"success", "successful", "ok", "done"}:
        title = "✅ **Staging deploy successful**"
        body = "Гілка задеплоєна на staging. Перевірте зміни і зніміть draft з PR коли все ок."
    elif normalized in {"failed", "failure", "error"}:
        title = "❌ **Staging deploy failed**"
        body = error_message or "Deploy failed без деталізованої помилки."
    else:
        title = "🚀 **Staging deploy started**"
        body = (
            "Код уже замержено в `staging`, деплой запущено. "
            "Цей коментар оновиться після завершення або падіння deploy."
        )

    lines = [
        "<!-- Staging Deploy Status -->",
        "## Staging Deploy Status",
        "",
        title,
        "",
        body,
        "",
    ]
    if head_branch:
        lines.append(f"**Branch:** `{head_branch}`")
    if merge_sha:
        lines.append(f"**Staging commit:** `{str(merge_sha)[:12]}`")
    if process_instance_key:
        lines.append(
            "[Процес в Operate]"
            f"(http://camunda-demo.a.local:8088/operate/processes/{process_instance_key})"
        )

    if normalized in {"failed", "failure", "error"}:
        trace = error_traceback or error_message
        if trace:
            lines.extend([
                "",
                "<details><summary>Traceback</summary>",
                "",
                "```",
                trace,
                "```",
                "",
                "</details>",
            ])

    return "\n".join(lines)


def _comment_body_with_marker(body: str, marker: str) -> str:
    """Ensure upserted comments can be found again without changing rendered text."""
    clean_marker = marker.strip()
    if not clean_marker or clean_marker in body:
        return body
    hidden_marker = (
        clean_marker
        if clean_marker.startswith("<!--") and clean_marker.endswith("-->")
        else f"<!-- {clean_marker} -->"
    )
    return f"{hidden_marker}\n{body}"


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
        review_head_sha = str(kwargs.get("head_sha") or "")
        current_retries = int(getattr(job, "retries", 0) or 0)
        retries_left_after_failure = max(current_retries - 1, 0)

        # 1. Pin the review to the PR head seen at review time.
        try:
            pr_data = await github.get_pr(repo, pr_number)
            review_head_sha = str(
                (pr_data.get("head") or {}).get("sha") or review_head_sha
            )
        except Exception as exc:
            logger.warning("Failed to fetch PR #%d head before review: %s", pr_number, exc)

        # 2. Fetch diff
        try:
            diff = await _get_pr_diff_with_retries(github, repo, pr_number)
        except ReviewUnavailableError as exc:
            logger.error("Review unavailable for PR #%d: %s", pr_number, exc)
            await _mark_review_unavailable(
                github,
                repo,
                pr_number,
                str(exc),
                review_head_sha,
                job.process_instance_key,
                retries_left_after_failure,
            )
            raise

        # 3. Run Codex CLI review
        try:
            review = await _run_review(diff, pr_number, repo)
        except ReviewUnavailableError as exc:
            logger.error("Review unavailable for PR #%d: %s", pr_number, exc)
            await _mark_review_unavailable(
                github,
                repo,
                pr_number,
                str(exc),
                review_head_sha,
                job.process_instance_key,
                retries_left_after_failure,
            )
            raise
        score = review.get("score", 0)
        critical = review.get("critical", False)

        logger.info(
            "codex-review PR #%d: score=%d, critical=%s",
            pr_number, score, critical,
        )

        # 4. Post or update review comment on PR (single comment per PR, edited on re-run).
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
            "review_head_sha": review_head_sha,
            "head_sha": review_head_sha,
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
        comment_marker: str = "",
        comment_update_note: str | None = None,
        ignore_errors: bool = False,
        **kwargs: Any,
    ) -> dict:
        """Post a comment on a PR, or update an existing marked comment."""
        repo = repository or config.github.repository
        marker = str(comment_marker or kwargs.get("comment_marker") or "").strip()
        try:
            if marker:
                body = _comment_body_with_marker(comment_text, marker)
                await github.upsert_comment(
                    repo,
                    pr_number,
                    body,
                    marker=marker,
                    update_note=comment_update_note or None,
                )
                logger.info(
                    "Posted/updated marked comment on PR #%d in %s: %s",
                    pr_number,
                    repo,
                    marker,
                )
            else:
                await github.comment_pr(repo, pr_number, comment_text)
                logger.info("Commented on PR #%d in %s", pr_number, repo)
        except Exception as exc:
            if ignore_errors:
                logger.warning("Failed to comment on PR #%d (ignored): %s", pr_number, exc)
            else:
                raise
        return {}

    # ── github-deploy-status-comment ───────────────────────────

    @worker.task(task_type="github-deploy-status-comment", timeout_ms=30_000)
    async def github_deploy_status_comment(
        pr_number: int,
        deploy_status: str = "started",
        repository: str = "",
        head_branch: str = "",
        merge_sha: str = "",
        error_message: str = "",
        error_traceback: str = "",
        process_instance_key: str = "",
        ignore_errors: bool = True,
        **kwargs: Any,
    ) -> dict:
        """Create/update the single staging deploy status comment on a PR."""
        repo = repository or config.github.repository
        body = _format_deploy_status_comment(
            deploy_status,
            head_branch=head_branch,
            merge_sha=merge_sha,
            process_instance_key=process_instance_key or kwargs.get("process_instance_key", ""),
            error_message=error_message or kwargs.get("caught_error_message", ""),
            error_traceback=error_traceback,
        )
        try:
            await github.upsert_comment(
                repo,
                pr_number,
                body,
                marker="Staging Deploy Status",
                update_note=None,
            )
            logger.info(
                "Posted/updated staging deploy status comment on PR #%d in %s: %s",
                pr_number, repo, deploy_status,
            )
        except Exception as exc:
            if ignore_errors:
                logger.warning(
                    "Failed to update deploy status comment on PR #%d (ignored): %s",
                    pr_number, exc,
                )
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

        try:
            result = await github.create_pr(
                repo,
                head=head_branch,
                base=base_branch,
                title=pr_title,
                body=pr_body,
                draft=is_draft,
            )
        except Exception:
            # GitHub can create the PR but still return a transient 5xx. On retry,
            # creating the same head/base returns 422. Treat the existing PR as success.
            result = await github.find_open_pr(repo, head=head_branch, base=base_branch)
            if not result:
                raise

        pr_url = result.get("html_url", "")
        pr_number = result.get("number", 0)
        logger.info("Created PR #%d: %s", pr_number, pr_url)

        return {"pr_url": pr_url, "pr_number": pr_number}
