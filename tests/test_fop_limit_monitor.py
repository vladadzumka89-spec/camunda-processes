"""Tests for fop_limit_monitor.py — core functions."""

from __future__ import annotations

import json
import sys
import tempfile
from datetime import datetime, date, timedelta
from pathlib import Path
from types import ModuleType
from unittest.mock import patch, MagicMock

import pytest

# Mock pymssql before importing fop_limit_monitor (may not be installed in test env)
if "pymssql" not in sys.modules:
    sys.modules["pymssql"] = ModuleType("pymssql")

# Patch BAS_DB_PASSWORD before import
with patch.dict("os.environ", {"BAS_DB_PASSWORD": "test_password"}):
    from fop_limit_monitor import (
        analyze_fop,
        _parse_terminal_name,
        load_state,
        save_state,
        format_currency,
        format_date,
        safe_pct,
        STATE_FILE,
    )


# === safe_pct ===

class TestSafePct:
    def test_normal(self):
        assert safe_pct(3_450_000, 6_900_000) == 50.0

    def test_zero_limit(self):
        assert safe_pct(1_000_000, 0) == 0.0

    def test_zero_income(self):
        assert safe_pct(0, 6_900_000) == 0.0

    def test_over_100_percent(self):
        assert safe_pct(7_000_000, 6_900_000) > 100.0


# === format_currency ===

class TestFormatCurrency:
    def test_basic(self):
        assert format_currency(1234567.89) == "1 234 567.89"

    def test_zero(self):
        assert format_currency(0) == "0.00"

    def test_small(self):
        assert format_currency(42.5) == "42.50"


# === format_date ===

class TestFormatDate:
    def test_none(self):
        assert format_date(None) == "не досягне"

    def test_string(self):
        assert format_date("ПЕРЕВИЩЕНО") == "ПЕРЕВИЩЕНО"

    def test_date(self):
        assert format_date(date(2026, 6, 15)) == "15.06.2026"


# === _parse_terminal_name ===

class TestParseTerminalName:
    def test_standard_format(self):
        result = _parse_terminal_name("cmps: 19 ,19 ,FORUM PINKY Кiльк тр 53шт.")
        assert result == "FORUM PINKY"

    def test_single_number(self):
        result = _parse_terminal_name("cmps: 25 ,Ostrov Pinky Кiльк тр 54шт.")
        assert result == "Ostrov Pinky"

    def test_another_format(self):
        result = _parse_terminal_name("cmps: 36 ,Pinky Nikolskyi Кiльк тр 94шт.")
        assert result == "Pinky Nikolskyi"

    def test_no_match(self):
        assert _parse_terminal_name("Звичайний платіж") is None

    def test_empty_string(self):
        assert _parse_terminal_name("") is None

    def test_too_short_name(self):
        assert _parse_terminal_name("cmps: 1 ,A Кiльк тр 1шт.") is None


# === analyze_fop ===

class TestAnalyzeFop:
    def test_empty_data(self):
        assert analyze_fop([], date(2026, 6, 15), 2026) is None

    def test_basic_analysis(self):
        today = date(2026, 6, 15)
        daily_data = []
        # Generate 30 days of data
        for i in range(30):
            d = today - timedelta(days=30 - i)
            daily_data.append({"date": d, "amount": 10_000.0, "count": 5})

        result = analyze_fop(daily_data, today, 2026)

        assert result is not None
        assert result["total_income"] == 300_000.0
        assert result["days_elapsed"] > 0
        assert result["days_remaining"] > 0
        assert result["active_days"] == 30
        assert result["mean_daily"] == 10_000.0
        assert 2 in result["limit_dates"]
        assert 3 in result["limit_dates"]

    def test_exceeded_limit(self):
        today = date(2026, 11, 1)
        daily_data = []
        # Generate high income that exceeds limit
        for i in range(300):
            d = date(2026, 1, 1) + timedelta(days=i)
            if d > today:
                break
            daily_data.append({"date": d, "amount": 25_000.0, "count": 10})

        result = analyze_fop(daily_data, today, 2026)
        assert result is not None
        # Total income = ~7.5M, should exceed group 2 limit (6.9M)
        assert result["total_income"] > 6_900_000
        assert result["limit_dates"][2]["already_exceeded"] is True

    def test_trend_ratio_no_prior_data(self):
        """When there's only recent data, trend should be flat (1.0), not 1.5."""
        today = date(2026, 6, 15)
        daily_data = []
        # Only 10 days of data (within last 21 days window)
        for i in range(10):
            d = today - timedelta(days=10 - i)
            daily_data.append({"date": d, "amount": 15_000.0, "count": 3})

        result = analyze_fop(daily_data, today, 2026)
        assert result is not None
        assert result["trend_ratio"] == 1.0  # Should be flat, not 1.5

    def test_trend_ratio_with_growth(self):
        """When recent income is higher than prior period, trend > 1."""
        today = date(2026, 6, 15)
        daily_data = []
        # Old period (6-3 weeks ago): low income
        for i in range(21):
            d = today - timedelta(days=42 - i)
            daily_data.append({"date": d, "amount": 5_000.0, "count": 2})
        # Recent period (last 3 weeks): high income
        for i in range(21):
            d = today - timedelta(days=21 - i)
            daily_data.append({"date": d, "amount": 10_000.0, "count": 5})

        result = analyze_fop(daily_data, today, 2026)
        assert result is not None
        assert result["trend_ratio"] > 1.0

    def test_datetime_input_converted(self):
        """Ensure datetime objects in daily_data are handled (converted to date)."""
        today = date(2026, 6, 15)
        daily_data = [
            {"date": datetime(2026, 6, 10, 12, 0), "amount": 10_000.0, "count": 1},
            {"date": datetime(2026, 6, 11, 14, 0), "amount": 20_000.0, "count": 2},
        ]
        result = analyze_fop(daily_data, today, 2026)
        assert result is not None
        assert result["total_income"] == 30_000.0


# === load_state / save_state ===

class TestStateFile:
    def test_load_nonexistent(self, tmp_path):
        fake_state = tmp_path / "nonexistent.json"
        with patch("fop_limit_monitor.STATE_FILE", fake_state):
            result = load_state()
        assert result == {}

    def test_save_and_load(self, tmp_path):
        fake_state = tmp_path / "state.json"
        state = {"ФОП Тест": 12345}
        with patch("fop_limit_monitor.STATE_FILE", fake_state):
            save_state(state)
            loaded = load_state()
        assert loaded == state

    def test_load_corrupted_creates_backup(self, tmp_path):
        fake_state = tmp_path / "state.json"
        fake_state.write_text("{invalid json")
        backup = tmp_path / "state.json.bak"

        with patch("fop_limit_monitor.STATE_FILE", fake_state):
            result = load_state()

        assert result == {}
        assert backup.exists()

    def test_atomic_save(self, tmp_path):
        """save_state uses tmp file + rename for atomicity."""
        fake_state = tmp_path / "state.json"
        state = {"key": 42}
        with patch("fop_limit_monitor.STATE_FILE", fake_state):
            save_state(state)

        assert fake_state.exists()
        assert json.loads(fake_state.read_text()) == state
        # tmp file should be cleaned up (renamed)
        tmp_file = tmp_path / "state.json.tmp"
        assert not tmp_file.exists()
