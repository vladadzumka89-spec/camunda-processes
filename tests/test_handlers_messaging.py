"""Tests for publish-message handler."""
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from worker.config import AppConfig


def _extract_handler(app_config):
    from worker.handlers.messaging import register_messaging_handlers
    handlers = {}
    mock_worker = MagicMock()
    def capture(task_type, timeout_ms=30000):
        def decorator(fn):
            handlers[task_type] = fn
            return fn
        return decorator
    mock_worker.task = capture
    register_messaging_handlers(mock_worker, app_config)
    return handlers["publish-message"]


@pytest.mark.asyncio
async def test_publish_message_success(app_config):
    handler = _extract_handler(app_config)
    job = MagicMock()
    job.variables = {"review_score": 8, "has_critical_issues": False}

    with patch("worker.handlers.messaging.create_channel"), \
         patch("worker.handlers.messaging.ZeebeClient") as MockClient:
        mock_instance = AsyncMock()
        MockClient.return_value = mock_instance

        result = await handler(
            job,
            message_name="msg_review_done",
            correlation_key="42",
        )

    assert result["message_published"] is True
    mock_instance.publish_message.assert_awaited_once()
    call_kw = mock_instance.publish_message.call_args[1]
    assert call_kw["name"] == "msg_review_done"
    assert call_kw["correlation_key"] == "42"
    assert call_kw["time_to_live"] == 3_600_000


@pytest.mark.asyncio
async def test_publish_message_missing_name(app_config):
    handler = _extract_handler(app_config)
    job = MagicMock()
    job.variables = {}
    with pytest.raises(ValueError, match="message_name"):
        await handler(job, message_name="", correlation_key="42")


@pytest.mark.asyncio
async def test_publish_message_missing_correlation(app_config):
    handler = _extract_handler(app_config)
    job = MagicMock()
    job.variables = {}
    with pytest.raises(ValueError, match="correlation_key"):
        await handler(job, message_name="test", correlation_key="")
