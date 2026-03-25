"""Universal progress reporting handler.

Creates Odoo subtasks to track process stage completion.
Fires as an execution listener (eventType="end") on service tasks.
Never fails — all errors are caught and logged so the main process continues.
"""

import asyncio
import logging
from typing import Any, Callable

import httpx
from pyzeebe import Job, ZeebeWorker

from ..config import AppConfig

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Stage maps: bpmn_process_id → { element_id → stage config }
# ---------------------------------------------------------------------------

STAGE_MAPS: dict[str, dict[str, dict[str, Any]]] = {
    "deploy-process": {
        "ST_db_checkpoint":      {"step": 1, "total": 9, "icon": "💾", "name": "DB checkpoint створено"},
        "task_git_pull":         {"step": 2, "total": 9, "icon": "📥", "name": "Код завантажено"},
        "task_detect_modules":   {"step": 3, "total": 9, "icon": "🔍", "name": "Змінені модулі визначено"},
        "task_module_update":    {"step": 4, "total": 9, "icon": "🔄", "name": "БД мігровано"},
        "task_smoke_test":       {"step": 5, "total": 9, "icon": "🧪", "name": "Smoke test пройдено"},
        "task_clickbot":         {"step": 6, "total": 9, "icon": "🤖", "name": "Clickbot завершено"},
        "task_cache_clear":      {"step": 7, "total": 9, "icon": "🧹", "name": "Кеш очищено"},
        "task_http_verify":      {"step": 8, "total": 9, "icon": "🌐", "name": "Сайт доступний"},
        "task_save_state":       {"step": 9, "total": 9, "icon": "💾", "name": "Стан деплою збережено"},
        "ST_publish_no_changes": {"step": 3, "total": 3, "icon": "⏭️", "name": "Без змін — деплой завершено"},
    },
}

# ---------------------------------------------------------------------------
# Stage-specific description builders
# ---------------------------------------------------------------------------


def _desc_default(v: dict) -> str:
    """Default: no extra details."""
    return ""


def _desc_git_pull(v: dict) -> str:
    old = v.get("old_commit", "—")
    new = v.get("new_commit", "—")
    return f"<b>Коміти:</b> {old[:8]} → {new[:8]}"


def _desc_detect_modules(v: dict) -> str:
    modules = v.get("changed_modules", "")
    if not modules:
        return "<b>Результат:</b> Змін не знайдено"
    return f"<b>Модулі:</b> {modules}"


def _desc_module_update(v: dict) -> str:
    modules = v.get("modules_updated", "")
    if not modules:
        return ""
    return f"<b>Оновлено:</b> {modules}"


def _desc_smoke_test(v: dict) -> str:
    passed = v.get("smoke_passed", None)
    if passed is None:
        return ""
    return f"<b>Результат:</b> {'✅ PASSED' if passed else '❌ FAILED'}"


def _desc_clickbot(v: dict) -> str:
    passed = v.get("clickbot_passed", None)
    if passed is None:
        return ""
    return f"<b>Результат:</b> {'✅ PASSED' if passed else '❌ FAILED'}"


def _desc_save_state(v: dict) -> str:
    commit = v.get("new_commit", "")
    if not commit:
        return ""
    return f"<b>SHA:</b> {commit[:8]}"


def _desc_no_changes(v: dict) -> str:
    return "<b>Результат:</b> Змін не знайдено, деплой завершено"


DESCRIPTION_BUILDERS: dict[str, Callable[[dict], str]] = {
    "task_git_pull": _desc_git_pull,
    "task_detect_modules": _desc_detect_modules,
    "task_module_update": _desc_module_update,
    "task_smoke_test": _desc_smoke_test,
    "task_clickbot": _desc_clickbot,
    "task_save_state": _desc_save_state,
    "ST_publish_no_changes": _desc_no_changes,
}

# ---------------------------------------------------------------------------
# Subtask name/description formatting
# ---------------------------------------------------------------------------


def build_subtask_name(stage: dict) -> str:
    """Build subtask name like '[3/9] 🔍 Змінені модулі визначено'."""
    return f"[{stage['step']}/{stage['total']}] {stage['icon']} {stage['name']}"


def build_subtask_description(
    element_id: str,
    variables: dict,
    server_host: str,
    branch: str,
) -> str:
    """Build HTML description with server context + stage-specific details."""
    builder = DESCRIPTION_BUILDERS.get(element_id, _desc_default)
    extra = builder(variables)

    parts = [
        f"<b>Сервер:</b> {server_host}",
        f"<b>Гілка:</b> {branch}",
    ]
    if extra:
        parts.append(extra)

    return "<p>" + "<br/>".join(parts) + "</p>"


# ---------------------------------------------------------------------------
# HTTP POST to Odoo webhook (with retry)
# ---------------------------------------------------------------------------

_MAX_ATTEMPTS = 2
_INITIAL_DELAY = 1.0


async def _post_to_odoo(url: str, payload: dict) -> None:
    """POST payload to Odoo webhook with retry on 5xx/network errors.

    Makes up to _MAX_ATTEMPTS attempts (2 total). Delays: 1s, 3s.
    Never raises — all errors are logged and swallowed.
    """
    delay = _INITIAL_DELAY
    async with httpx.AsyncClient() as client:
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                resp = await client.post(
                    url, json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=30.0,
                )
                if resp.status_code >= 500 and attempt < _MAX_ATTEMPTS:
                    logger.warning(
                        "Progress POST %d from %s, retry %d/%d",
                        resp.status_code, url, attempt, _MAX_ATTEMPTS,
                    )
                    await asyncio.sleep(delay)
                    delay *= 3
                    continue
                if resp.status_code >= 400:
                    body = resp.text[:500] if resp.content else ""
                    logger.warning(
                        "Progress POST failed: HTTP %d (attempt %d/%d): %s",
                        resp.status_code, attempt, _MAX_ATTEMPTS, body,
                    )
                    return
                logger.info("Progress subtask created (HTTP %d)", resp.status_code)
                return
            except httpx.RequestError as exc:
                if attempt < _MAX_ATTEMPTS:
                    logger.warning("Progress POST network error: %s, retry", exc)
                    await asyncio.sleep(delay)
                    delay *= 3
                    continue
                logger.warning(
                    "Progress POST failed after %d attempts: %s",
                    _MAX_ATTEMPTS, exc,
                )
                return


# ---------------------------------------------------------------------------
# Handler registration
# ---------------------------------------------------------------------------


def register_progress_handlers(worker: ZeebeWorker, config: AppConfig) -> None:
    """Register the universal progress reporting handler."""

    @worker.task(task_type="progress", timeout_ms=30_000)
    async def handle_progress(job: Job, **kwargs: Any) -> dict:
        """Create an Odoo subtask reporting stage completion.

        Reads job.bpmn_process_id + job.element_id to determine which
        stage just completed. Never raises — all errors are caught.
        """
        process_id = getattr(job, "bpmn_process_id", "") or ""
        element_id = getattr(job, "element_id", "") or ""
        variables = dict(job.variables)

        try:
            # Look up stage config
            process_map = STAGE_MAPS.get(process_id, {})
            stage = process_map.get(element_id)

            if stage is None:
                # Fallback: generic subtask for unmapped elements
                stage = {"step": 0, "total": 0, "icon": "📌", "name": element_id}
                logger.info(
                    "No stage map for %s/%s — using fallback",
                    process_id, element_id,
                )

            # Build subtask content
            server_host = variables.get("server_host", "unknown")
            branch = variables.get("branch", "unknown")
            odoo_url = variables.get("odoo_webhook_url", "")
            parent_key = variables.get("parent_process_instance_key")

            if not odoo_url:
                logger.warning("No odoo_webhook_url — skipping progress report")
                return {}

            name = build_subtask_name(stage)
            description = build_subtask_description(
                element_id, variables, server_host, branch,
            )

            payload: dict[str, Any] = {
                "name": name,
                "description": description,
                "_model": "project.project",
                "process_instance_key": job.process_instance_key,
                "element_instance_key": getattr(job, "element_instance_key", None),
                "bpmn_process_id": process_id,
                "element_id": element_id,
                "job_key": job.key,
            }
            if parent_key:
                payload["parent_process_instance_key"] = parent_key
            # _id is required by Odoo webhook; fallback to 560 matches BPMN default
            payload["_id"] = config.odoo.project_id or 560

            logger.info(
                "[%s] Progress payload: %s",
                job.process_instance_key, payload,
            )

            await _post_to_odoo(odoo_url, payload)

        except Exception:
            logger.exception(
                "Progress handler failed for %s/%s — continuing",
                process_id, element_id,
            )

        # ALWAYS return empty dict — execution listener must not set variables
        return {}
