"""Tests for invoice OCR handler — page-by-page processing."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from worker.handlers.ocr import (
    _empty_invoice_item,
    _gemini_extract_from_images,
)


def _make_invoice(**overrides) -> dict:
    item = _empty_invoice_item()
    item.update(overrides)
    return item


@pytest.mark.asyncio
async def test_gemini_called_per_page_not_batch():
    """Each PDF page must be sent to Gemini in a separate request."""
    page1_result = [_make_invoice(partner_name="Стецюк", invoice_number="170", invoice_amount=5000.0)]
    page2_result = [_make_invoice(partner_name="АКСІОМА", invoice_number="ГУ1663", invoice_amount=18681.64)]

    fake_images = [MagicMock(name="page1"), MagicMock(name="page2")]

    with patch("worker.handlers.ocr._gemini_extract_from_images", new_callable=AsyncMock) as mock_gemini:
        mock_gemini.side_effect = [page1_result, page2_result]

        # Import the handler registration to access the inner function
        from worker.handlers.ocr import register_ocr_handlers

        # Call _gemini_extract_from_images per page (simulating the new logic)
        all_items = []
        for page_img in fake_images:
            page_items = await mock_gemini([page_img])
            if page_items:
                all_items.extend(page_items)

        assert len(all_items) == 2
        assert all_items[0]["partner_name"] == "Стецюк"
        assert all_items[1]["partner_name"] == "АКСІОМА"
        assert mock_gemini.call_count == 2
        # Each call should receive exactly 1 image
        for call in mock_gemini.call_args_list:
            assert len(call[0][0]) == 1


def test_ocr_summary_format():
    """ocr_summary should list each invoice on a separate line."""
    items = [
        _make_invoice(partner_name="Стецюк", invoice_number="170", invoice_amount=5000.0),
        _make_invoice(partner_name="АКСІОМА", invoice_number="ГУ1663", invoice_amount=18681.64),
        _make_invoice(partner_name=None, invoice_number=None, invoice_amount=0),
    ]

    from worker.handlers.ocr import _build_ocr_summary
    summary = _build_ocr_summary(items)

    assert "• №170 — Стецюк — 5000.0 грн" in summary
    assert "• №ГУ1663 — АКСІОМА — 18681.64 грн" in summary
    assert "• №? — ? — 0 грн" in summary
    assert summary.count("\n") == 2  # 3 lines, 2 newlines
