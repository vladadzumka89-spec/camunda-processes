"""Tests for the universal progress reporting handler."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from worker2.handlers.progress import (
    STAGE_MAPS,
    build_subtask_name,
    build_subtask_description,
    _post_to_odoo,
    register_progress_handlers,
)


# ---------- build_subtask_name ----------

class TestBuildSubtaskName:
    def test_normal_stage(self):
        stage = {"step": 3, "total": 9, "icon": "🔍", "name": "Змінені модулі визначено"}
        assert build_subtask_name(stage) == "[3/9] 🔍 Змінені модулі визначено"

    def test_fallback_stage(self):
        stage = {"step": 0, "total": 0, "icon": "📌", "name": "unknown_element"}
        assert build_subtask_name(stage) == "[0/0] 📌 unknown_element"

    def test_no_changes_stage(self):
        stage = STAGE_MAPS["deploy-process"]["ST_publish_no_changes"]
        assert build_subtask_name(stage) == "[3/3] ⏭️ Без змін — деплой завершено"


# ---------- build_subtask_description ----------

class TestBuildSubtaskDescription:
    def test_git_pull_with_commits(self):
        desc = build_subtask_description(
            "task_git_pull",
            {"old_commit": "abc12345", "new_commit": "def67890"},
            "staging", "main",
        )
        assert "abc12345" in desc
        assert "def67890" in desc
        assert "Сервер:</b> staging" in desc

    def test_detect_modules_with_list(self):
        desc = build_subtask_description(
            "task_detect_modules",
            {"changed_modules": "sale_custom,hr_fix"},
            "staging", "main",
        )
        assert "sale_custom,hr_fix" in desc

    def test_detect_modules_empty(self):
        desc = build_subtask_description(
            "task_detect_modules", {"changed_modules": ""},
            "staging", "main",
        )
        assert "Змін не знайдено" in desc

    def test_smoke_test_passed(self):
        desc = build_subtask_description(
            "task_smoke_test", {"smoke_passed": True},
            "staging", "main",
        )
        assert "✅ PASSED" in desc

    def test_smoke_test_failed(self):
        desc = build_subtask_description(
            "task_smoke_test", {"smoke_passed": False},
            "staging", "main",
        )
        assert "❌ FAILED" in desc

    def test_module_update_with_modules(self):
        desc = build_subtask_description(
            "task_module_update",
            {"modules_updated": "sale_custom,hr_fix"},
            "staging", "main",
        )
        assert "sale_custom,hr_fix" in desc

    def test_module_update_empty(self):
        desc = build_subtask_description(
            "task_module_update", {"modules_updated": ""},
            "staging", "main",
        )
        assert "Оновлено" not in desc

    def test_save_state_with_commit(self):
        desc = build_subtask_description(
            "task_save_state",
            {"new_commit": "abc12345def67890"},
            "staging", "main",
        )
        assert "abc12345" in desc

    def test_save_state_no_commit(self):
        desc = build_subtask_description(
            "task_save_state", {},
            "staging", "main",
        )
        assert "SHA" not in desc

    def test_clickbot_passed(self):
        desc = build_subtask_description(
            "task_clickbot", {"clickbot_passed": True},
            "staging", "main",
        )
        assert "✅ PASSED" in desc

    def test_no_changes(self):
        desc = build_subtask_description(
            "ST_publish_no_changes", {},
            "staging", "main",
        )
        assert "деплой завершено" in desc

    def test_default_no_extra(self):
        desc = build_subtask_description(
            "ST_db_checkpoint", {},
            "production", "main",
        )
        assert "Сервер:</b> production" in desc
        assert "Гілка:</b> main" in desc

    def test_unknown_element_fallback(self):
        desc = build_subtask_description(
            "totally_unknown_element", {},
            "staging", "main",
        )
        assert "Сервер:</b> staging" in desc


# ---------- _post_to_odoo ----------

class TestPostToOdoo:
    @pytest.mark.asyncio
    async def test_success(self):
        mock_resp = MagicMock(status_code=200)
        mock_client = AsyncMock()
        mock_client.post.return_value = mock_resp
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("worker2.handlers.progress.httpx.AsyncClient", return_value=mock_client):
            await _post_to_odoo("http://odoo/webhook", {"name": "test"})

        mock_client.post.assert_called_once()

    @pytest.mark.asyncio
    async def test_4xx_does_not_raise(self):
        mock_resp = MagicMock(status_code=400)
        mock_client = AsyncMock()
        mock_client.post.return_value = mock_resp
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("worker2.handlers.progress.httpx.AsyncClient", return_value=mock_client):
            await _post_to_odoo("http://odoo/webhook", {"name": "test"})
        # No exception raised

    @pytest.mark.asyncio
    async def test_network_error_does_not_raise(self):
        import httpx as _httpx
        mock_client = AsyncMock()
        mock_client.post.side_effect = _httpx.ConnectError("connection refused")
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("worker2.handlers.progress.httpx.AsyncClient", return_value=mock_client):
            await _post_to_odoo("http://odoo/webhook", {"name": "test"})
        # No exception raised, 2 attempts made
        assert mock_client.post.call_count == 2


# ---------- handle_progress (full handler) ----------

class TestHandleProgress:
    def _make_job(self, process_id="deploy-process", element_id="task_git_pull", variables=None):
        job = MagicMock()
        job.bpmn_process_id = process_id
        job.element_id = element_id
        job.process_instance_key = 12345
        job.variables = variables or {
            "server_host": "staging",
            "branch": "main",
            "odoo_webhook_url": "http://odoo/webhook",
            "parent_process_instance_key": "999",
            "old_commit": "aaa111",
            "new_commit": "bbb222",
        }
        return job

    def _make_config(self):
        config = MagicMock()
        config.odoo.project_id = 560
        return config

    @pytest.mark.asyncio
    async def test_creates_subtask_for_known_stage(self):
        worker = MagicMock()
        captured_handler = None

        def capture_task(**deco_kwargs):
            def decorator(fn):
                nonlocal captured_handler
                captured_handler = fn
                return fn
            return decorator

        worker.task = capture_task
        register_progress_handlers(worker, self._make_config())

        with patch("worker2.handlers.progress._post_to_odoo", new_callable=AsyncMock) as mock_post:
            result = await captured_handler(self._make_job())

        assert result == {}
        mock_post.assert_called_once()
        payload = mock_post.call_args[0][1]
        assert "[2/9]" in payload["name"]
        assert "📥" in payload["name"]
        assert payload["parent_process_instance_key"] == "999"

    @pytest.mark.asyncio
    async def test_fallback_for_unknown_element(self):
        worker = MagicMock()
        captured_handler = None

        def capture_task(**deco_kwargs):
            def decorator(fn):
                nonlocal captured_handler
                captured_handler = fn
                return fn
            return decorator

        worker.task = capture_task
        register_progress_handlers(worker, self._make_config())

        job = self._make_job(element_id="totally_new_task")
        with patch("worker2.handlers.progress._post_to_odoo", new_callable=AsyncMock) as mock_post:
            result = await captured_handler(job)

        assert result == {}
        mock_post.assert_called_once()
        payload = mock_post.call_args[0][1]
        assert "totally_new_task" in payload["name"]

    @pytest.mark.asyncio
    async def test_no_webhook_url_skips(self):
        worker = MagicMock()
        captured_handler = None

        def capture_task(**deco_kwargs):
            def decorator(fn):
                nonlocal captured_handler
                captured_handler = fn
                return fn
            return decorator

        worker.task = capture_task
        register_progress_handlers(worker, self._make_config())

        job = self._make_job(variables={
            "server_host": "staging",
            "branch": "main",
            "odoo_webhook_url": "",
        })
        with patch("worker2.handlers.progress._post_to_odoo", new_callable=AsyncMock) as mock_post:
            result = await captured_handler(job)

        assert result == {}
        mock_post.assert_not_called()

    @pytest.mark.asyncio
    async def test_odoo_failure_does_not_raise(self):
        worker = MagicMock()
        captured_handler = None

        def capture_task(**deco_kwargs):
            def decorator(fn):
                nonlocal captured_handler
                captured_handler = fn
                return fn
            return decorator

        worker.task = capture_task
        register_progress_handlers(worker, self._make_config())

        with patch(
            "worker2.handlers.progress._post_to_odoo",
            new_callable=AsyncMock,
            side_effect=Exception("Odoo exploded"),
        ):
            result = await captured_handler(self._make_job())

        # Handler catches all exceptions
        assert result == {}
