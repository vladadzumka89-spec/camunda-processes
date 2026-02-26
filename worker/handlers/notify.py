"""Notification handler — creates tasks in Odoo project."""

import html
import logging
from typing import Any

from pyzeebe import Job, ZeebeWorker

from ..config import AppConfig
from ..odoo_client import OdooClient

logger = logging.getLogger(__name__)


def _parse_md_table(md: str) -> list[dict[str, str]]:
    """Parse a markdown pipe-table into a list of dicts (header→value).

    Only lines containing '|' are considered table rows.
    Preamble text (headings, paragraphs) is skipped.
    """
    # Only keep lines that look like table rows (contain |)
    table_lines = [
        l.strip() for l in md.strip().splitlines()
        if l.strip() and "|" in l
    ]
    if len(table_lines) < 2:
        return []

    rows: list[list[str]] = []
    for line in table_lines:
        cells = [c.strip() for c in line.strip("|").split("|")]
        # Skip separator rows like |---|---|
        if all(set(c) <= {"-", ":", " "} for c in cells):
            continue
        rows.append(cells)

    if len(rows) < 2:
        return []

    headers = rows[0]
    return [dict(zip(headers, row)) for row in rows[1:]]


def _impact_to_html(md: str) -> str:
    """Convert impact markdown table to a simple HTML list."""
    rows = _parse_md_table(md)
    if not rows:
        return "<p>Немає зачеплених модулів</p>"

    items = []
    for r in rows:
        mod = html.escape(r.get("Custom Module", ""))
        deps = html.escape(r.get("Affected Dependencies", ""))
        items.append(f"<li><b>{mod}</b> — {deps}</li>")
    return "<ul>" + "".join(items) + "</ul>"


def _audit_to_html(md: str) -> str:
    """Convert audit report markdown to a grouped HTML list by severity."""
    rows = _parse_md_table(md)
    if not rows:
        return "<p>Конфліктів не знайдено</p>"

    critical = []
    warning = []
    info = []
    for r in rows:
        sev = r.get("Severity", "").strip()
        ctype = html.escape(r.get("Type", ""))
        mod = html.escape(r.get("Custom Module", ""))
        target = html.escape(r.get("Target", ""))
        base = html.escape(r.get("Base", ""))
        custom_file = html.escape(r.get("File", ""))
        line_no = html.escape(r.get("Line", ""))
        super_info = html.escape(r.get("Super", ""))

        # Build detailed line
        entry = f"<li><b>{mod}</b> → <code>{target}</code>"
        if ctype == "python_override":
            entry += f" (Python override"
            if super_info:
                label = {"no": "❌ без super()", "cond": "⚠️ super() в умові", "yes": "✅ super()"}.get(super_info, super_info)
                entry += f", {label}"
            entry += ")"
        elif ctype == "js_patch":
            entry += " (JS patch)"
        elif ctype == "xml_xpath":
            entry += " (XML xpath"
            if super_info:
                entry += f": <code>{super_info}</code>"
            entry += ")"
        if custom_file:
            short_file = custom_file.rsplit("/", 1)[-1] if "/" in custom_file else custom_file
            entry += f"<br/><small>📄 {custom_file}"
            if line_no:
                entry += f":{line_no}"
            if base:
                entry += f" ← base: {base}"
            entry += "</small>"
        elif base:
            entry += f" (base: {base})"
        entry += "</li>"

        if "critical" in sev.lower():
            critical.append(entry)
        elif "warning" in sev.lower():
            warning.append(entry)
        else:
            info.append(entry)

    parts = []
    if critical:
        parts.append(
            f'<p style="color:red;font-weight:bold">🔴 Critical ({len(critical)}):</p>'
            "<ul>" + "".join(critical) + "</ul>"
        )
    if warning:
        parts.append(
            f'<p style="color:orange;font-weight:bold">🟡 Warning ({len(warning)}):</p>'
            "<details><summary>Показати warning конфлікти</summary>"
            "<ul>" + "".join(warning) + "</ul></details>"
        )
    if info:
        parts.append(
            f"<p>ℹ️ Info ({len(info)}):</p>"
            "<details><summary>Показати info</summary>"
            "<ul>" + "".join(info) + "</ul></details>"
        )
    return "".join(parts)


def register_notify_handlers(
    worker: ZeebeWorker,
    config: AppConfig,
    odoo: OdooClient,
) -> None:
    """Register notification handler."""

    @worker.task(task_type="send-notification", timeout_ms=30_000)
    async def send_notification(
        job: Job,
        notification_type: str = "info",
        message_body: str = "",
        pr_url: str = "",
        sync_branch: str = "",
        **kwargs: Any,
    ) -> dict:
        """Create a task in Odoo CI/CD project."""
        # Extract date code from branch name for task identification
        branch_code = sync_branch.split("upstream-", 1)[-1] if "upstream-" in sync_branch else ""
        branch_suffix = f" {branch_code}" if branch_code else ""

        pik = job.process_instance_key

        titles = {
            "staging_ready": "[deploy] Staging готовий до перевірки",
            "deploy_failed": "[deploy] Деплой провалився",
            "review_needed": "[review] Потрібна перевірка",
            "sync_conflicts": "[upstream-sync] Перевірити конфлікти з custom модулями",
            "sync_start": f"[upstream-sync{branch_suffix}] Upstream Sync | x_camunda:{pik}",
            "deploy_error": "[deploy] ❌ Помилка деплою",
            "sync_error": f"[upstream-sync{branch_suffix}] ❌ Помилка синхронізації",
            "pipeline_error": "[pipeline] ❌ Помилка пайплайну",
        }
        name = titles.get(notification_type, f"[ci] {notification_type}")

        # Only parent-type notifications create a process container
        is_parent = notification_type in ("sync_start",)

        description = ""
        if sync_branch:
            repo = config.github.repository
            branch_url = f"https://github.com/{repo}/tree/{sync_branch}"
            description += f'<p>🔗 <b>Гілка:</b> <a href="{branch_url}">{sync_branch}</a></p>'
        if message_body:
            description += f"<p>{message_body}</p>"
        if pr_url:
            description += f'<p>PR: <a href="{pr_url}">{pr_url}</a></p>'

        task_id = odoo.create_task(
            name=name,
            description=description,
            process_instance_key=job.process_instance_key,
            element_instance_key=job.element_instance_key,
            bpmn_process_id=job.bpmn_process_id,
            create_process=is_parent,
        )

        logger.info("Created Odoo task #%d [%s] (parent=%s)", task_id, notification_type, is_parent)
        return {"odoo_task_id": task_id}

    @worker.task(task_type="create-odoo-task", timeout_ms=30_000)
    async def create_odoo_task(
        job: Job,
        odoo_task_type: str = "",
        affected_custom_count: int = 0,
        impact_table: str = "",
        audit_report: str = "",
        audit_conflicts: int = 0,
        audit_critical: int = 0,
        audit_warning: int = 0,
        changed_modules: str = "",
        community_files: int = 0,
        enterprise_files: int = 0,
        current_version: str = "",
        community_date: str = "",
        enterprise_date: str = "",
        pr_url: str = "",
        pr_number: int = 0,
        sync_branch: str = "",
        **kwargs: Any,
    ) -> dict:
        """Create a blocking Odoo task and return its ID for message correlation.

        Used with message catch events: process waits until Odoo task is closed,
        then webhook publishes msg_odoo_task_done with correlation key = odoo_task_id.
        """
        modules_count = len(changed_modules.split(", ")) if changed_modules else 0

        # Extract date code from branch name for task identification
        # sync/upstream-20260225-111130 → 20260225-111130
        branch_code = sync_branch.split("upstream-", 1)[-1] if "upstream-" in sync_branch else sync_branch
        repo = config.github.repository
        branch_url = f"https://github.com/{repo}/tree/{sync_branch}" if sync_branch else ""
        branch_link = f'<p>🔗 <b>Гілка:</b> <a href="{branch_url}">{sync_branch}</a></p>' if branch_url else ""

        task_configs = {
            "resolve_conflicts": {
                "name": f"[upstream-sync {branch_code}] Виправити конфлікти ({affected_custom_count} модулів)",
                "description": (
                    branch_link
                    + f"<h3>Upstream Sync — {current_version} ({enterprise_date})</h3>"
                    f"<p><b>Змінено файлів:</b> community {community_files}, enterprise {enterprise_files}</p>"
                    f"<p><b>Audit:</b> {audit_conflicts} конфліктів "
                    f'(<span style="color:red;font-weight:bold">{audit_critical} critical</span>, '
                    f'<span style="color:orange">{audit_warning} warning</span>)</p>'
                    f"<hr/>"
                    f"<h4>Зачеплені custom модулі ({affected_custom_count})</h4>"
                    + _impact_to_html(impact_table)
                    + f"<hr/>"
                    f"<h4>Audit — конфлікти з upstream</h4>"
                    + _audit_to_html(audit_report)
                    + f"<hr/>"
                    f"<h4>Оновлені модулі ({modules_count})</h4>"
                    f"<details><summary>Показати повний список</summary>"
                    f"<p>{'<br/>'.join(html.escape(m.strip()) for m in changed_modules.split(',') if m.strip())}</p>"
                    f"</details>"
                    f"<hr/>"
                    f"<p><b>Що потрібно зробити:</b></p>"
                    f"<ol>"
                    f'<li>Переглянути <b style="color:red">critical</b> конфлікти</li>'
                    f"<li>Виправити зачеплені custom модулі (tut_*)</li>"
                    f"<li>Закомітити виправлення в репозиторій</li>"
                    f"<li>Закрити цю задачу — процес продовжить створення PR</li>"
                    f"</ol>"
                ),
            },
            "review_sync": {
                "name": f"[upstream-sync {branch_code}] Переглянути аналіз оновлення",
                "description": (
                    branch_link
                    + (f'<p>🔗 <b>PR:</b> <a href="{pr_url}">{pr_url}</a></p>' if pr_url else "")
                    + f"<h3>Upstream Sync — {current_version} ({enterprise_date})</h3>"
                    + f"<p><b>Змінено файлів:</b> community {community_files}, enterprise {enterprise_files}</p>"
                    + (
                        f"<p><b>Audit:</b> {audit_conflicts} конфліктів "
                        f'(<span style="color:red;font-weight:bold">{audit_critical} critical</span>, '
                        f'<span style="color:orange">{audit_warning} warning</span>)</p>'
                        if audit_conflicts else "<p><b>Audit:</b> конфліктів не знайдено ✅</p>"
                    )
                    + f"<hr/>"
                    f"<h4>Зачеплені custom модулі ({affected_custom_count})</h4>"
                    + _impact_to_html(impact_table)
                    + f"<hr/>"
                    f"<h4>Audit — аналіз конфліктів з upstream</h4>"
                    + _audit_to_html(audit_report)
                    + f"<hr/>"
                    f"<h4>Оновлені модулі ({modules_count})</h4>"
                    f"<details><summary>Показати повний список</summary>"
                    f"<p>{'<br/>'.join(html.escape(m.strip()) for m in changed_modules.split(',') if m.strip())}</p>"
                    f"</details>"
                    + f"<hr/>"
                    f"<h4>Що потрібно перевірити</h4>"
                    f"<ul>"
                    f"<li>Які модулі оновились та чи всі потрібні</li>"
                    f"<li>Impact на custom модулі (tut_*)</li>"
                    f"<li>Результати audit — critical/warning конфлікти</li>"
                    f"<li>Чи є нові/видалені модулі</li>"
                    f"</ul>"
                    + f"<p><b>Після перевірки закрийте цю задачу</b> — процес продовжить merge в staging та деплой.</p>"
                ),
            },
        }

        cfg = task_configs.get(odoo_task_type, {
            "name": f"[ci] {odoo_task_type}",
            "description": f"<p>Task type: {odoo_task_type}</p>",
        })

        task_id = odoo.create_task(
            name=cfg["name"],
            description=cfg["description"],
            process_instance_key=job.process_instance_key,
            element_instance_key=job.element_instance_key,
            bpmn_process_id=job.bpmn_process_id,
            create_process=False,
        )

        # Use process_instance_key as correlation key — Odoo webhook may not
        # return the task ID, but process_instance_key is always available
        # and stored on the Odoo task for callback matching.
        correlation_id = str(task_id) if task_id else str(job.process_instance_key)
        logger.info(
            "Created blocking Odoo task #%d [%s] — correlation_id=%s",
            task_id, odoo_task_type, correlation_id,
        )
        return {"odoo_task_id": correlation_id}
