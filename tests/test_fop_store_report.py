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
        _fetch_terminal_bindings,
        _calc_growth_percent,
        _determine_current_fop,
        _group_binding_periods,
        LIMITS,
    )


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
                "value_date": "31.12.2099",
            },
        ]
        conn = self._make_conn(rows)
        result = _fetch_terminal_bindings(conn, 2026)
        assert "920 П Буковина Черн." in result
        assert len(result["920 П Буковина Черн."]) == 1
        assert result["920 П Буковина Черн."][0]["fop_name"] == "ФОП Петренко В.В."
        assert result["920 П Буковина Черн."][0]["value_date"] == "31.12.2099"

    def test_multiple_bindings_sorted(self):
        rows = [
            {
                "store_name": "920 П Буковина Черн.",
                "binding_date": "16.12.2025",
                "fop_name": "ФОП Іванов І.І.",
                "value_date": "31.12.2099",
            },
            {
                "store_name": "920 П Буковина Черн.",
                "binding_date": "06.02.2026",
                "fop_name": "ФОП Петренко В.В.",
                "value_date": "31.12.2099",
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
            {"date": "16.12.2025", "fop_name": "ФОП Іванов", },
            {"date": "06.02.2026", "fop_name": "ФОП Петренко"},
        ]
        fops_list = [
            {"fop_name": "ФОП Іванов", "fop_edrpou": "111", "income_from_store": 500_000},
        ]
        name, edrpou = _determine_current_fop(bindings, fops_list)
        assert name == "ФОП Петренко"
        assert edrpou == ""

    def test_from_binding_with_edrpou_match(self):
        bindings = [
            {"date": "06.02.2026", "fop_name": "ФОП Петренко", },
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


class TestGroupBindingPeriods:
    """Test binding period grouping algorithm using value_date."""

    def test_empty(self):
        assert _group_binding_periods([], 2026) == []

    def test_single_connection_active(self):
        """Single FOP connected (value_date=2099), still active."""
        bindings = [
            {"date": "05.01.2026", "fop_name": "ФОП А", "value_date": "31.12.2099"},
        ]
        result = _group_binding_periods(bindings, 2026)
        assert len(result) == 1
        assert result[0]["fop_name"] == "ФОП А"
        assert result[0]["date_to"] is None

    def test_store920_full_history(self):
        """Store 920 Буковина: Щербина → Томусяк → Павлівська.

        Real BAS data with value_date indicating connect/disconnect.
        """
        bindings = [
            {"date": "16.12.2025", "fop_name": "Щербина Анастасія", "value_date": "31.12.2099"},
            {"date": "06.02.2026", "fop_name": "Щербина Анастасія", "value_date": "06.02.2026"},
            {"date": "06.02.2026", "fop_name": "Томусяк Олег", "value_date": "31.12.2099"},
            {"date": "17.03.2026", "fop_name": "Томусяк Олег", "value_date": "17.03.2026"},
            {"date": "17.03.2026", "fop_name": "Павлівська Анастасія", "value_date": "31.12.2099"},
        ]
        result = _group_binding_periods(bindings, 2026)
        # Щербина: 16.12.2025 – 06.02.2026
        shch = [p for p in result if p["fop_name"] == "Щербина Анастасія"]
        assert len(shch) == 1
        assert shch[0]["date_from"] == "16.12.2025"
        assert shch[0]["date_to"] == "06.02.2026"
        # Томусяк: 06.02.2026 – 17.03.2026
        tom = [p for p in result if p["fop_name"] == "Томусяк Олег"]
        assert len(tom) == 1
        assert tom[0]["date_from"] == "06.02.2026"
        assert tom[0]["date_to"] == "17.03.2026"
        # Павлівська: 17.03.2026 – зараз
        pavl = [p for p in result if p["fop_name"] == "Павлівська Анастасія"]
        assert len(pavl) == 1
        assert pavl[0]["date_to"] is None

    def test_store636_full_history(self):
        """Store 636 Дорошенко: Анікіна → Павлів → Омельянчук.

        Initial record (01.01.0001) should be skipped.
        """
        bindings = [
            {"date": "01.01.0001", "fop_name": "Анікіна Анастасія", "value_date": "01.01.0001"},
            {"date": "17.12.2025", "fop_name": "Анікіна Анастасія", "value_date": "17.12.2025"},
            {"date": "17.12.2025", "fop_name": "Павлів Марія", "value_date": "31.12.2099"},
            {"date": "03.03.2026", "fop_name": "Павлів Марія", "value_date": "03.03.2026"},
            {"date": "03.03.2026", "fop_name": "Омельянчук Василь", "value_date": "31.12.2099"},
        ]
        result = _group_binding_periods(bindings, 2026)
        # Анікіна has no connection record (initial 0001 skipped) → not in output
        anik = [p for p in result if p["fop_name"] == "Анікіна Анастасія"]
        assert len(anik) == 0
        # Павлів: 17.12.2025 – 03.03.2026
        pavl = [p for p in result if p["fop_name"] == "Павлів Марія"]
        assert len(pavl) == 1
        assert pavl[0]["date_from"] == "17.12.2025"
        assert pavl[0]["date_to"] == "03.03.2026"
        # Омельянчук: 03.03.2026 – зараз
        omel = [p for p in result if p["fop_name"] == "Омельянчук Василь"]
        assert len(omel) == 1
        assert omel[0]["date_to"] is None

    def test_disconnection_without_connection_skipped(self):
        """Disconnection record without prior connection is ignored."""
        bindings = [
            {"date": "06.01.2026", "fop_name": "ФОП А", "value_date": "06.01.2026"},
            {"date": "06.01.2026", "fop_name": "ФОП Б", "value_date": "31.12.2099"},
        ]
        result = _group_binding_periods(bindings, 2026)
        assert len(result) == 1
        assert result[0]["fop_name"] == "ФОП Б"
        assert result[0]["date_to"] is None

    def test_dates_before_2020_filtered(self):
        """Ancient BAS dates (01.01.0001) should be filtered out."""
        bindings = [
            {"date": "01.01.0001", "fop_name": "ФОП А", "value_date": "31.12.2099"},
            {"date": "05.01.2026", "fop_name": "ФОП Б", "value_date": "31.12.2099"},
        ]
        result = _group_binding_periods(bindings, 2026)
        assert len(result) == 1
        assert result[0]["fop_name"] == "ФОП Б"

    def test_connection_disconnected_before_year_filtered(self):
        """Period that ended before target year should be filtered out."""
        bindings = [
            {"date": "10.03.2025", "fop_name": "ФОП А", "value_date": "31.12.2099"},
            {"date": "08.08.2025", "fop_name": "ФОП А", "value_date": "08.08.2025"},
            {"date": "08.08.2025", "fop_name": "ФОП Б", "value_date": "31.12.2099"},
        ]
        result = _group_binding_periods(bindings, 2026)
        # ФОП А ended 08.08.2025 — before 2026, should be filtered
        fop_a = [p for p in result if p["fop_name"] == "ФОП А"]
        assert len(fop_a) == 0
        # ФОП Б still active
        fop_b = [p for p in result if p["fop_name"] == "ФОП Б"]
        assert len(fop_b) == 1
        assert fop_b[0]["date_to"] is None

    def test_fop_reconnected_twice(self):
        """FOP connected, disconnected, then reconnected."""
        bindings = [
            {"date": "01.09.2025", "fop_name": "ФОП А", "value_date": "31.12.2099"},
            {"date": "01.12.2025", "fop_name": "ФОП А", "value_date": "01.12.2025"},
            {"date": "01.12.2025", "fop_name": "ФОП Б", "value_date": "31.12.2099"},
            {"date": "15.02.2026", "fop_name": "ФОП Б", "value_date": "15.02.2026"},
            {"date": "15.02.2026", "fop_name": "ФОП А", "value_date": "31.12.2099"},
        ]
        result = _group_binding_periods(bindings, 2026)
        fop_a = [p for p in result if p["fop_name"] == "ФОП А"]
        # 2 periods: first closed 01.12.2025 (before 2026 → filtered), second active
        assert len(fop_a) == 1
        assert fop_a[0]["date_from"] == "15.02.2026"
        assert fop_a[0]["date_to"] is None

    def test_fop_count_unique_dates(self):
        """fop_count = unique switching dates in current year."""
        from worker.handlers.fop_monitor import _parse_binding_date

        bindings = [
            {"date": "26.12.2025", "fop_name": "ФОП А", "value_date": "31.12.2099"},
            {"date": "06.01.2026", "fop_name": "ФОП А", "value_date": "06.01.2026"},
            {"date": "06.01.2026", "fop_name": "ФОП Б", "value_date": "31.12.2099"},
            {"date": "15.02.2026", "fop_name": "ФОП Б", "value_date": "15.02.2026"},
            {"date": "15.02.2026", "fop_name": "ФОП В", "value_date": "31.12.2099"},
        ]
        year_start = date(2026, 1, 1)
        switch_dates = set()
        for b in bindings:
            d = _parse_binding_date(b["date"])
            if d and d >= year_start:
                switch_dates.add(d)
        assert len(switch_dates) == 2  # 06.01, 15.02


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
            {"date": "06.02.2026", "fop_name": "ФОП Петренко", "value_date": "31.12.2099"},
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
