"""Tests for store employee enrichment in fop_monitor.py."""

from __future__ import annotations

import sys
from types import ModuleType
from unittest.mock import patch, MagicMock

import pytest

# Mock pymssql before importing
if "pymssql" not in sys.modules:
    sys.modules["pymssql"] = ModuleType("pymssql")

with patch.dict("os.environ", {"BAS_DB_PASSWORD": "test_password"}):
    from worker.handlers.fop_monitor import (
        _fetch_store_employees,
        _enrich_store_with_employees,
    )


class TestFetchStoreEmployees:
    """Test _fetch_store_employees SQL fetch and grouping."""

    def _make_conn(self, rows):
        conn = MagicMock()
        cursor = MagicMock()
        cursor.__iter__ = lambda self: iter(rows)
        conn.cursor.return_value = cursor
        return conn

    def test_empty_result(self):
        conn = self._make_conn([])
        result = _fetch_store_employees(conn)
        assert result == {}

    def test_single_employee(self):
        rows = [
            {
                "employee_name": "  Гандзій Тетяна Андріївна  ",
                "employer_fop_name": "  Абаркіна Лариса Миколаївна  ",
                "employer_edrpou": "1234567890",
                "department_name": "  650 Софія Київ  ",
            },
        ]
        conn = self._make_conn(rows)
        result = _fetch_store_employees(conn)

        assert "650 Софія Київ" in result
        assert len(result["650 Софія Київ"]) == 1
        emp = result["650 Софія Київ"][0]
        assert emp["name"] == "Гандзій Тетяна Андріївна"
        assert emp["employer_fop"] == "Абаркіна Лариса Миколаївна"
        assert emp["employer_edrpou"] == "1234567890"

    def test_multiple_employees_same_store(self):
        rows = [
            {
                "employee_name": "Гандзій Тетяна Андріївна",
                "employer_fop_name": "Абаркіна Л.М.",
                "employer_edrpou": "111",
                "department_name": "650 Софія Київ",
            },
            {
                "employee_name": "Грама Вікторія Анатоліївна",
                "employer_fop_name": "Абаркіна Л.М.",
                "employer_edrpou": "111",
                "department_name": "650 Софія Київ",
            },
        ]
        conn = self._make_conn(rows)
        result = _fetch_store_employees(conn)
        assert len(result["650 Софія Київ"]) == 2

    def test_multiple_stores(self):
        rows = [
            {
                "employee_name": "Іванов І.І.",
                "employer_fop_name": "ФОП А",
                "employer_edrpou": "111",
                "department_name": "650 Софія Київ",
            },
            {
                "employee_name": "Петров П.П.",
                "employer_fop_name": "ФОП Б",
                "employer_edrpou": "222",
                "department_name": "507 Глобал",
            },
        ]
        conn = self._make_conn(rows)
        result = _fetch_store_employees(conn)
        assert len(result) == 2
        assert "650 Софія Київ" in result
        assert "507 Глобал" in result

    def test_db_error_returns_empty(self):
        conn = MagicMock()
        cursor = MagicMock()
        cursor.execute.side_effect = Exception("Connection lost")
        conn.cursor.return_value = cursor
        result = _fetch_store_employees(conn)
        assert result == {}

    def test_null_edrpou_becomes_empty_string(self):
        rows = [
            {
                "employee_name": "Іванов І.І.",
                "employer_fop_name": "ФОП А",
                "employer_edrpou": None,
                "department_name": "650 Софія Київ",
            },
        ]
        conn = self._make_conn(rows)
        result = _fetch_store_employees(conn)
        assert result["650 Софія Київ"][0]["employer_edrpou"] == ""


class TestEnrichStoreEmployees:
    """Test enrichment of stores_report with employee data and fop_match."""

    def test_employees_added_to_store(self):
        """Employees matched by exact department name."""
        store_data = {
            "subdivision": "650 Софія Київ",
            "current_fop_edrpou": "111",
        }
        store_employees = {
            "650 Софія Київ": [
                {"name": "Іванов І.І.", "employer_fop": "ФОП А", "employer_edrpou": "111"},
                {"name": "Петров П.П.", "employer_fop": "ФОП Б", "employer_edrpou": "222"},
            ],
        }
        _enrich_store_with_employees(store_data, store_employees)

        assert store_data["employee_count"] == 2
        assert store_data["mismatch_count"] == 1
        assert store_data["employees"][0]["fop_match"] is True
        assert store_data["employees"][1]["fop_match"] is False

    def test_no_employees_found(self):
        """Store with no matching employees."""
        store_data = {
            "subdivision": "650 Софія Київ",
            "current_fop_edrpou": "111",
        }
        _enrich_store_with_employees(store_data, {})

        assert store_data["employee_count"] == 0
        assert store_data["mismatch_count"] == 0
        assert store_data["employees"] == []

    def test_fallback_by_3digit_code(self):
        """Fallback: match by first 3-digit code when exact name not found."""
        store_data = {
            "subdivision": "650 Софія Київ",
            "current_fop_edrpou": "111",
        }
        store_employees = {
            "650 ТРЦ Софія Київ Подільський": [
                {"name": "Іванов І.І.", "employer_fop": "ФОП А", "employer_edrpou": "111"},
            ],
        }
        _enrich_store_with_employees(store_data, store_employees)

        assert store_data["employee_count"] == 1
        assert store_data["employees"][0]["fop_match"] is True

    def test_empty_current_fop_all_mismatch_false(self):
        """If current_fop_edrpou is empty, fop_match is False for everyone."""
        store_data = {
            "subdivision": "650 Софія Київ",
            "current_fop_edrpou": "",
        }
        store_employees = {
            "650 Софія Київ": [
                {"name": "Іванов І.І.", "employer_fop": "ФОП А", "employer_edrpou": "111"},
            ],
        }
        _enrich_store_with_employees(store_data, store_employees)

        assert store_data["mismatch_count"] == 0
        assert store_data["employees"][0]["fop_match"] is False

    def test_employees_text_formatting(self):
        """employees_text is a comma-separated list of names."""
        store_data = {
            "subdivision": "650 Софія Київ",
            "current_fop_edrpou": "111",
        }
        store_employees = {
            "650 Софія Київ": [
                {"name": "Іванов Іван Іванович", "employer_fop": "ФОП А", "employer_edrpou": "111"},
                {"name": "Петров Петро Петрович", "employer_fop": "ФОП А", "employer_edrpou": "111"},
            ],
        }
        _enrich_store_with_employees(store_data, store_employees)

        assert "Іванов Іван Іванович" in store_data["employees_text"]
        assert "Петров Петро Петрович" in store_data["employees_text"]
