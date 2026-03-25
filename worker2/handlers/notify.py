"""Render handler — builds complex HTML for upstream-sync Odoo tasks."""

import html
import logging
from typing import Any

from pyzeebe import Job, ZeebeWorker

from ..config import AppConfig

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
) -> None:
    """Register render-sync-html handler."""

    @worker.task(task_type="render-sync-html", timeout_ms=30_000)
    async def render_sync_html(
        job: Job,
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
        enterprise_date: str = "",
        pr_url: str = "",
        sync_branch: str = "",
        **kwargs: Any,
    ) -> dict:
        """Render complex HTML for upstream-sync Odoo tasks.

        Returns pre-built name+description for conflict and review tasks.
        Does NOT create tasks in Odoo — only renders HTML.
        """
        modules_count = len(changed_modules.split(", ")) if changed_modules else 0
        branch_code = sync_branch.split("upstream-", 1)[-1] if "upstream-" in sync_branch else sync_branch
        repo = config.github.repository
        branch_url = f"https://github.com/{repo}/tree/{sync_branch}" if sync_branch else ""
        branch_link = f'<p>🔗 <b>Гілка:</b> <a href="{branch_url}">{sync_branch}</a></p>' if branch_url else ""

        modules_html = "<br/>".join(
            html.escape(m.strip()) for m in changed_modules.split(",") if m.strip()
        )

        conflict_task_name = f"[upstream-sync {branch_code}] Виправити конфлікти ({affected_custom_count} модулів)"
        conflict_description = (
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
            f"<p>{modules_html}</p>"
            f"</details>"
            f"<hr/>"
            f"<p><b>Що потрібно зробити:</b></p>"
            f"<ol>"
            f'<li>Переглянути <b style="color:red">critical</b> конфлікти</li>'
            f"<li>Виправити зачеплені custom модулі (tut_*)</li>"
            f"<li>Закомітити виправлення в репозиторій</li>"
            f"<li>Закрити цю задачу — процес продовжить створення PR</li>"
            f"</ol>"
        )

        review_task_name = f"[upstream-sync {branch_code}] Переглянути аналіз оновлення"
        review_description = (
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
            f"<p>{modules_html}</p>"
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
        )

        logger.info("Rendered sync HTML for branch %s", sync_branch)
        return {
            "conflict_task_name": conflict_task_name,
            "conflict_description": conflict_description,
            "review_task_name": review_task_name,
            "review_description": review_description,
        }
