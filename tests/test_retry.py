"""Tests for worker.retry — async retry with exponential backoff."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from worker.retry import retry


@pytest.mark.asyncio
async def test_success_first_attempt() -> None:
    func = AsyncMock(return_value="ok")
    result = await retry(func)
    assert result == "ok"
    func.assert_awaited_once()


@pytest.mark.asyncio
async def test_retry_then_success() -> None:
    func = AsyncMock(side_effect=[ValueError("fail"), "ok"])
    with patch("worker.retry.asyncio.sleep", new_callable=AsyncMock):
        result = await retry(func, max_attempts=3, delay=1.0)
    assert result == "ok"
    assert func.await_count == 2


@pytest.mark.asyncio
async def test_all_attempts_fail() -> None:
    func = AsyncMock(side_effect=ValueError("boom"))
    with patch("worker.retry.asyncio.sleep", new_callable=AsyncMock):
        with pytest.raises(ValueError, match="boom"):
            await retry(func, max_attempts=3, delay=1.0)
    assert func.await_count == 3


@pytest.mark.asyncio
async def test_exponential_backoff() -> None:
    func = AsyncMock(side_effect=[ValueError("1"), ValueError("2"), "ok"])
    with patch("worker.retry.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        await retry(func, max_attempts=3, delay=1.0, backoff=2.0)
    assert mock_sleep.await_count == 2
    mock_sleep.assert_any_await(1.0)
    mock_sleep.assert_any_await(2.0)


@pytest.mark.asyncio
async def test_custom_params() -> None:
    func = AsyncMock(
        side_effect=[ValueError("1"), ValueError("2"), ValueError("3"), ValueError("4"), "ok"]
    )
    with patch("worker.retry.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        result = await retry(func, max_attempts=5, delay=1.0, backoff=3.0)
    assert result == "ok"
    assert func.await_count == 5
    # Delays: 1.0, 3.0, 9.0, 27.0
    assert mock_sleep.await_count == 4
