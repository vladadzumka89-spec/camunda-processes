"""BPMN Error hierarchy for Zeebe worker.

Each exception carries an explicit ``error_code`` that the BPMN process
can catch via Error Boundary Events / Error Event Subprocesses.

Usage::

    from .errors import OdooWebhookError
    raise OdooWebhookError("HTTP 500 from Odoo webhook")
    # → BPMN receives error_code="ODOO_ERROR"
"""

from __future__ import annotations


class BpmnError(Exception):
    """Base exception mapped to a BPMN Error Event.

    Subclasses MUST set ``error_code`` to a SCREAMING_SNAKE_CASE string
    that matches the error code in the BPMN diagram.
    """

    error_code: str = "PROCESS_ERROR"

    def __init__(self, message: str, *, variables: dict | None = None) -> None:
        super().__init__(message)
        self.variables = variables


class OdooWebhookError(BpmnError):
    """Odoo webhook returned an error (HTTP 4xx/5xx)."""

    error_code = "ODOO_ERROR"


class DeployError(BpmnError):
    """Deployment step failed (git pull, docker build, module update, etc.)."""

    error_code = "DEPLOY_FAILED"


class ConfigError(BpmnError):
    """Missing or invalid configuration (server not found, empty token, etc.)."""

    error_code = "CONFIG_ERROR"


class GitHubError(BpmnError):
    """GitHub API operation failed."""

    error_code = "GITHUB_ERROR"


class SyncError(BpmnError):
    """Upstream sync operation failed."""

    error_code = "SYNC_ERROR"
