"""Odoo webhook client for creating project tasks."""

from __future__ import annotations

import logging

import httpx

from .config import OdooConfig

logger = logging.getLogger(__name__)


class OdooClient:
    """Creates tasks in Odoo via webhook HTTP POST."""

    def __init__(self, config: OdooConfig) -> None:
        self._config = config

    def create_task(
        self,
        name: str,
        description: str = '',
        project_id: int | None = None,
        assignee_id: int | None = None,
        tag_name: str = '',
    ) -> int:
        """Create a project.task in Odoo via webhook. Returns task ID."""
        pid = project_id or self._config.project_id
        uid = assignee_id or self._config.assignee_id

        body: dict = {
            'name': name,
            '_model': 'project.project',
            '_id': pid,
        }
        if description:
            body['description'] = description
        if uid:
            body['x_studio_camunda_user_ids'] = uid

        resp = httpx.post(
            self._config.webhook_url,
            json=body,
            headers={'Content-Type': 'application/json'},
            timeout=30,
        )
        resp.raise_for_status()

        data = resp.json()
        task_id = int(data.get('id', data.get('task_id', 0)))
        logger.info('Created Odoo task #%d: %s', task_id, name)
        return task_id
