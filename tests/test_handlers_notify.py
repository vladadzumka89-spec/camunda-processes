"""Tests for worker.handlers.notify — render-sync-html handler."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from worker.config import AppConfig
from worker.handlers.notify import register_notify_handlers


def _make_mock_job() -> MagicMock:
    """Create a mock pyzeebe Job with required attributes."""
    job = MagicMock()
    job.process_instance_key = 2251799813793035
    job.element_instance_key = 2251799813793040
    job.bpmn_process_id = "upstream-sync"
    return job


def _extract_handlers(config: AppConfig) -> dict:
    handlers = {}

    def task_decorator(task_type: str, **kwargs):
        def wrapper(fn):
            handlers[task_type] = fn
            return fn
        return wrapper

    worker = MagicMock()
    worker.task = task_decorator
    register_notify_handlers(worker, config)
    return handlers


@pytest.fixture
def handlers(app_config: AppConfig) -> dict:
    return _extract_handlers(app_config)


# ── render-sync-html ─────────────────────────────────────


@pytest.mark.asyncio
async def test_render_sync_html_returns_all_fields(handlers: dict) -> None:
    job = _make_mock_job()
    result = await handlers["render-sync-html"](
        job=job,
        affected_custom_count=3,
        impact_table="| Custom Module | Affected Dependencies |\n|---|---|\n| tut_core | sale, stock |",
        audit_report="",
        audit_conflicts=2,
        audit_critical=1,
        audit_warning=1,
        changed_modules="sale, stock, account",
        community_files=50,
        enterprise_files=30,
        current_version="19.0",
        enterprise_date="2026-03-01",
        sync_branch="sync/upstream-20260301-120000",
    )
    assert "conflict_task_name" in result
    assert "conflict_description" in result
    assert "review_task_name" in result
    assert "review_description" in result


@pytest.mark.asyncio
async def test_render_sync_html_conflict_name(handlers: dict) -> None:
    job = _make_mock_job()
    result = await handlers["render-sync-html"](
        job=job,
        affected_custom_count=5,
        sync_branch="sync/upstream-20260301-120000",
    )
    assert "5 модулів" in result["conflict_task_name"]
    assert "upstream-sync" in result["conflict_task_name"]
    assert "20260301-120000" in result["conflict_task_name"]


@pytest.mark.asyncio
async def test_render_sync_html_review_name(handlers: dict) -> None:
    job = _make_mock_job()
    result = await handlers["render-sync-html"](
        job=job,
        sync_branch="sync/upstream-20260301-120000",
    )
    assert "Переглянути аналіз" in result["review_task_name"]
    assert "20260301-120000" in result["review_task_name"]


@pytest.mark.asyncio
async def test_render_sync_html_conflict_description_has_audit(handlers: dict) -> None:
    job = _make_mock_job()
    result = await handlers["render-sync-html"](
        job=job,
        audit_conflicts=3,
        audit_critical=2,
        audit_warning=1,
        community_files=100,
        enterprise_files=50,
        current_version="19.0",
        enterprise_date="2026-03-01",
    )
    assert "3 конфліктів" in result["conflict_description"]
    assert "2 critical" in result["conflict_description"]
    assert "community 100" in result["conflict_description"]


@pytest.mark.asyncio
async def test_render_sync_html_review_description_has_pr_link(handlers: dict) -> None:
    job = _make_mock_job()
    result = await handlers["render-sync-html"](
        job=job,
        pr_url="https://github.com/tut-ua/odoo-enterprise/pull/42",
    )
    assert "pull/42" in result["review_description"]
    assert "href" in result["review_description"]


@pytest.mark.asyncio
async def test_render_sync_html_no_conflicts_message(handlers: dict) -> None:
    job = _make_mock_job()
    result = await handlers["render-sync-html"](
        job=job,
        audit_conflicts=0,
    )
    assert "конфліктів не знайдено" in result["review_description"]


@pytest.mark.asyncio
async def test_render_sync_html_modules_list(handlers: dict) -> None:
    job = _make_mock_job()
    result = await handlers["render-sync-html"](
        job=job,
        changed_modules="sale, stock, account",
    )
    assert "sale" in result["conflict_description"]
    assert "stock" in result["conflict_description"]
    assert "account" in result["conflict_description"]


@pytest.mark.asyncio
async def test_render_sync_html_branch_link(handlers: dict) -> None:
    job = _make_mock_job()
    result = await handlers["render-sync-html"](
        job=job,
        sync_branch="sync/upstream-20260301-120000",
    )
    assert "tut-ua/odoo-enterprise" in result["conflict_description"]
    assert "sync/upstream-20260301-120000" in result["conflict_description"]


@pytest.mark.asyncio
async def test_render_sync_html_impact_table(handlers: dict) -> None:
    job = _make_mock_job()
    result = await handlers["render-sync-html"](
        job=job,
        impact_table="| Custom Module | Affected Dependencies |\n|---|---|\n| tut_core | sale, stock |",
    )
    assert "tut_core" in result["conflict_description"]
    assert "sale, stock" in result["conflict_description"]
