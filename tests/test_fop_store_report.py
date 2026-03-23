"""Tests for store report enrichment in fop_monitor.py."""

from __future__ import annotations

import sys
from datetime import date
from types import ModuleType
from unittest.mock import patch, MagicMock
from collections import defaultdict

import pytest

# Mock pymssql before importing
if "pymssql" not in sys.modules:
    sys.modules["pymssql"] = ModuleType("pymssql")

with patch.dict("os.environ", {"BAS_DB_PASSWORD": "test_password"}):
    from worker.handlers.fop_monitor import (
        _fetch_monthly_store_income,
        _fetch_terminal_bindings,
        _calc_growth_percent,
        _determine_current_fop,
        LIMITS,
    )


class TestFetchMonthlyStoreIncome:
    """Test monthly store income aggregation from document-based sales."""

    def _make_conn(self, rows):
        conn = MagicMock()
        cursor = MagicMock()
        cursor.__iter__ = lambda self: iter(rows)
        conn.cursor.return_value = cursor
        return conn

    def test_empty_result(self):
        conn = self._make_conn([])
        result = _fetch_monthly_store_income(conn, 2026)
        assert result == {}

    def test_single_store_single_month(self):
        rows = [
            {"store_name": "920 П Буковина Черн.", "income_month": 3, "monthly_total": 150_000.0},
        ]
        conn = self._make_conn(rows)
        result = _fetch_monthly_store_income(conn, 2026)
        assert result == {"920 П Буковина Черн.": {3: 150_000.0}}

    def test_single_store_multiple_months(self):
        rows = [
            {"store_name": "636 Дорошенко Львів", "income_month": 1, "monthly_total": 80_000.0},
            {"store_name": "636 Дорошенко Львів", "income_month": 2, "monthly_total": 95_000.0},
            {"store_name": "636 Дорошенко Львів", "income_month": 3, "monthly_total": 110_000.0},
        ]
        conn = self._make_conn(rows)
        result = _fetch_monthly_store_income(conn, 2026)
        assert result == {
            "636 Дорошенко Львів": {1: 80_000.0, 2: 95_000.0, 3: 110_000.0}
        }

    def test_multiple_stores(self):
        rows = [
            {"store_name": "920 П Буковина Черн.", "income_month": 1, "monthly_total": 120_000.0},
            {"store_name": "636 Дорошенко Львів", "income_month": 1, "monthly_total": 80_000.0},
        ]
        conn = self._make_conn(rows)
        result = _fetch_monthly_store_income(conn, 2026)
        assert "920 П Буковина Черн." in result
        assert "636 Дорошенко Львів" in result

    def test_db_error_returns_empty(self):
        """Graceful degradation: DB error → empty dict, not exception."""
        conn = MagicMock()
        cursor = MagicMock()
        cursor.execute.side_effect = Exception("SQL error")
        conn.cursor.return_value = cursor
        result = _fetch_monthly_store_income(conn, 2026)
        assert result == {}


class TestFetchTerminalBindings:
    """Test terminal binding history fetch."""

    def _make_conn(self, rows):
        conn = MagicMock()
        cursor = MagicMock()
        cursor.__iter__ = lambda self: iter(rows)
        conn.cursor.return_value = cursor
        return conn

    def test_empty_result(self):
        conn = self._make_conn([])
        result = _fetch_terminal_bindings(conn, 2026)
        assert result == {}

    def test_single_binding(self):
        rows = [
            {
                "store_name": "920 П Буковина Черн.",
                "binding_date": "17.03.2026",
                "fop_name": "ФОП Петренко В.В.",
                "responsible": "Муляєво Наталія",
            },
        ]
        conn = self._make_conn(rows)
        result = _fetch_terminal_bindings(conn, 2026)
        assert "920 П Буковина Черн." in result
        assert len(result["920 П Буковина Черн."]) == 1
        assert result["920 П Буковина Черн."][0]["fop_name"] == "ФОП Петренко В.В."

    def test_multiple_bindings_sorted(self):
        rows = [
            {
                "store_name": "920 П Буковина Черн.",
                "binding_date": "16.12.2025",
                "fop_name": "ФОП Іванов І.І.",
                "responsible": "Зборов Олександр",
            },
            {
                "store_name": "920 П Буковина Черн.",
                "binding_date": "06.02.2026",
                "fop_name": "ФОП Петренко В.В.",
                "responsible": "Мікрилевська Інга",
            },
        ]
        conn = self._make_conn(rows)
        result = _fetch_terminal_bindings(conn, 2026)
        bindings = result["920 П Буковина Черн."]
        assert len(bindings) == 2
        assert bindings[0]["date"] == "16.12.2025"
        assert bindings[1]["date"] == "06.02.2026"

    def test_db_error_returns_empty(self):
        """Graceful degradation: DB error → empty dict, not exception."""
        conn = MagicMock()
        cursor = MagicMock()
        cursor.execute.side_effect = Exception("Table not found")
        conn.cursor.return_value = cursor
        result = _fetch_terminal_bindings(conn, 2026)
        assert result == {}


class TestCalcGrowthPercent:
    """Test growth percentage calculation."""

    def test_normal_growth(self):
        result = _calc_growth_percent(prev=100_000, curr=112_000)
        assert result == 12.0

    def test_decline(self):
        result = _calc_growth_percent(prev=100_000, curr=80_000)
        assert result == -20.0

    def test_zero_prev_returns_none(self):
        result = _calc_growth_percent(prev=0, curr=50_000)
        assert result is None

    def test_both_zero_returns_none(self):
        result = _calc_growth_percent(prev=0, curr=0)
        assert result is None

    def test_no_change(self):
        result = _calc_growth_percent(prev=100_000, curr=100_000)
        assert result == 0.0


class TestDetermineCurrentFop:
    """Test current FOP determination from binding history and fallback."""

    def test_from_binding_history(self):
        bindings = [
            {"date": "16.12.2025", "fop_name": "ФОП Іванов", "responsible": "A"},
            {"date": "06.02.2026", "fop_name": "ФОП Петренко", "responsible": "B"},
        ]
        fops_list = [
            {"fop_name": "ФОП Іванов", "fop_edrpou": "111", "income_from_store": 500_000},
        ]
        name, edrpou = _determine_current_fop(bindings, fops_list)
        assert name == "ФОП Петренко"
        assert edrpou == ""

    def test_from_binding_with_edrpou_match(self):
        bindings = [
            {"date": "06.02.2026", "fop_name": "ФОП Петренко", "responsible": "B"},
        ]
        fops_list = [
            {"fop_name": "ФОП Іванов", "fop_edrpou": "111", "income_from_store": 100_000},
            {"fop_name": "ФОП Петренко", "fop_edrpou": "222", "income_from_store": 400_000},
        ]
        name, edrpou = _determine_current_fop(bindings, fops_list)
        assert name == "ФОП Петренко"
        assert edrpou == "222"

    def test_fallback_no_bindings(self):
        fops_list = [
            {"fop_name": "ФОП Іванов", "fop_edrpou": "111", "income_from_store": 100_000},
            {"fop_name": "ФОП Петренко", "fop_edrpou": "222", "income_from_store": 400_000},
        ]
        name, edrpou = _determine_current_fop([], fops_list)
        assert name == "ФОП Петренко"
        assert edrpou == "222"

    def test_empty_both(self):
        name, edrpou = _determine_current_fop([], [])
        assert name == ""
        assert edrpou == ""


class TestEnrichStoresReport:
    """Test that stores_report entries get all new fields."""

    def test_enriched_fields_present(self):
        """Verify all new fields exist in an enriched store entry."""
        store_data = {
            "store_name": "920 П Буковина Черн.",
            "total_income": 500_000.0,
            "source": "terminal",
            "fops": [
                {"fop_name": "ФОП Петренко", "fop_edrpou": "222",
                 "income_from_store": 500_000, "days_to_limit": 30,
                 "organization": "ФАМО"},
            ],
            "fop_count": 1,
            "organization": "ФАМО",
        }
        bindings = [
            {"date": "06.02.2026", "fop_name": "ФОП Петренко", "responsible": "Тест"},
        ]
        monthly = {1: 120_000, 2: 150_000, 3: 170_000}
        current_month = 3

        name, edrpou = _determine_current_fop(bindings, store_data["fops"])
        assert name == "ФОП Петренко"
        assert edrpou == "222"

        fop_match = next(
            (f for f in store_data["fops"] if f["fop_edrpou"] == edrpou), None
        )
        income_from_fop = fop_match["income_from_store"] if fop_match else 0
        limit = LIMITS.get(2, 3_500_000)
        pct = round((income_from_fop / limit) * 100, 1) if limit > 0 else 0

        assert pct == round((500_000 / 3_500_000) * 100, 1)

        prev_month = current_month - 1
        growth = _calc_growth_percent(monthly.get(prev_month, 0), monthly.get(current_month, 0))
        assert growth is not None
        assert growth == round(((170_000 - 150_000) / 150_000) * 100, 1)

    def test_no_bindings_no_monthly(self):
        """Store with no binding history and no monthly data."""
        store_data = {
            "fops": [
                {"fop_name": "ФОП Іванов", "fop_edrpou": "111",
                 "income_from_store": 200_000, "days_to_limit": 60,
                 "organization": ""},
            ],
        }
        name, edrpou = _determine_current_fop([], store_data["fops"])
        assert name == "ФОП Іванов"

        growth = _calc_growth_percent(0, 0)
        assert growth is None
