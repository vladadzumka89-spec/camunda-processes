"""Notification handler — creates tasks in Odoo project."""

from __future__ import annotations

import logging
from typing import Any

from pyzeebe import ZeebeWorker

from ..config import AppConfig
from ..odoo_client import OdooClient

logger = logging.getLogger(__name__)


def register_notify_handlers(
    worker: ZeebeWorker,
    config: AppConfig,
    odoo: OdooClient,
) -> None:
    """Register notification handler."""

    @worker.task(task_type="send-notification", timeout_ms=30_000)
    async def send_notification(
        notification_type: str = "info",
        message_body: str = "",
        pr_url: str = "",
        **kwargs: Any,
    ) -> dict:
        """Create a task in Odoo CI/CD project."""
        titles = {
            "staging_ready": "[deploy] Staging готовий до перевірки",
            "deploy_failed": "[deploy] Деплой провалився",
            "review_needed": "[review] Потрібна перевірка",
            "sync_conflicts": "[upstream-sync] Перевірити конфлікти з custom модулями",
        }
        name = titles.get(notification_type, f"[ci] {notification_type}")

        description = ""
        if message_body:
            description += f"<p>{message_body}</p>"
        if pr_url:
            description += f'<p>PR: <a href="{pr_url}">{pr_url}</a></p>'

        task_id = odoo.create_task(name=name, description=description)

        logger.info("Created Odoo task #%d [%s]", task_id, notification_type)
        return {"odoo_task_id": task_id}

    @worker.task(task_type="create-odoo-task", timeout_ms=30_000)
    async def create_odoo_task(
        odoo_task_type: str = "",
        affected_custom_count: int = 0,
        impact_table: str = "",
        pr_url: str = "",
        pr_number: int = 0,
        sync_branch: str = "",
        changed_modules: str = "",
        **kwargs: Any,
    ) -> dict:
        """Create a blocking Odoo task and return its ID for message correlation.

        Used with message catch events: process waits until Odoo task is closed,
        then webhook publishes msg_odoo_task_done with correlation key = odoo_task_id.
        """
        task_configs = {
            "resolve_conflicts": {
                "name": f"[upstream-sync] Виправити конфлікти в custom модулях ({affected_custom_count} модулів)",
                "description": (
                    f"<p>Impact analysis виявив конфлікти з <b>{affected_custom_count}</b> "
                    f"кастомними модулями (tut_*).</p>"
                    f"<p><b>Що потрібно зробити:</b></p>"
                    f"<ul>"
                    f"<li>Переглянути impact table нижче</li>"
                    f"<li>Виправити зачеплені custom модулі</li>"
                    f"<li>Закомітити виправлення</li>"
                    f"<li>Закрити цю задачу</li>"
                    f"</ul>"
                    f"<p><b>Impact table:</b></p>"
                    f"<pre>{impact_table}</pre>"
                ),
            },
            "review_sync": {
                "name": "[upstream-sync] Перевірити sync та позначити Ready",
                "description": (
                    f"<p>Upstream sync завершено. Draft PR створено.</p>"
                    f"<p><b>Що потрібно перевірити:</b></p>"
                    f"<ul>"
                    f"<li>Які модулі оновились</li>"
                    f"<li>Impact на custom модулі (tut_*)</li>"
                    f"<li>Чи є нові/видалені модулі</li>"
                    f"</ul>"
                    + (f'<p>PR: <a href="{pr_url}">{pr_url}</a></p>' if pr_url else "")
                    + (f"<p>Гілка: {sync_branch}</p>" if sync_branch else "")
                    + f"<p><b>Після перевірки закрийте цю задачу</b> — PR буде автоматично позначений як Ready.</p>"
                ),
            },
        }

        cfg = task_configs.get(odoo_task_type, {
            "name": f"[ci] {odoo_task_type}",
            "description": f"<p>Task type: {odoo_task_type}</p>",
        })

        task_id = odoo.create_task(name=cfg["name"], description=cfg["description"])

        logger.info(
            "Created blocking Odoo task #%d [%s] — waiting for closure via webhook",
            task_id, odoo_task_type,
        )
        # Return as string for Zeebe message correlation
        return {"odoo_task_id": str(task_id)}
