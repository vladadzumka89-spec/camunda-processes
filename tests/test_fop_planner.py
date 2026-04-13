"""Tests for fop_planner.py — FOP opening planning functions."""

from __future__ import annotations

import sys
from datetime import date
from types import ModuleType
from unittest.mock import patch

import pytest

# Mock pymssql before importing
if "pymssql" not in sys.modules:
    sys.modules["pymssql"] = ModuleType("pymssql")

with patch.dict("os.environ", {"BAS_DB_PASSWORD": "test"}):
    from worker.handlers.fop_planner import (
        find_reserve_fops,
        calculate_strategic_summary,
        calculate_registration_date,
        check_employee_capacity,
        build_monthly_plan,
    )


# === find_reserve_fops ===


class TestFindReserveFops:
    """Reserve FOPs = status Open + group 2 + income below threshold."""

    def test_finds_reserve_with_zero_income(self):
        all_fops = [
            {"id": b"\x01", "name": "ФОП Петренко", "edrpou": "111"},
            {"id": b"\x02", "name": "ФОП Сидоренко", "edrpou": "222"},
        ]
        fop_statuses = {b"\x01": "Відкрита", b"\x02": "Відкрита"}
        fop_groups = {b"\x01": 2, b"\x02": 2}
        analyses = {
            b"\x01": {"total_income": 2_500_000},
        }
        store_employees = {}

        result = find_reserve_fops(
            all_fops, fop_statuses, fop_groups, analyses, store_employees,
            reserve_threshold=100_000, employee_limit=8,
        )

        assert len(result) == 1
        assert result[0]["fop_name"] == "ФОП Сидоренко"
        assert result[0]["fop_edrpou"] == "222"
        assert result[0]["current_income"] == 0
        assert result[0]["free_employee_slots"] == 8

    def test_excludes_closed_fops(self):
        all_fops = [{"id": b"\x01", "name": "ФОП Закритий", "edrpou": "333"}]
        fop_statuses = {b"\x01": "Закрита"}
        fop_groups = {b"\x01": 2}
        analyses = {}
        store_employees = {}

        result = find_reserve_fops(
            all_fops, fop_statuses, fop_groups, analyses, store_employees,
            reserve_threshold=100_000, employee_limit=8,
        )
        assert len(result) == 0

    def test_excludes_group_3(self):
        all_fops = [{"id": b"\x01", "name": "ФОП Група3", "edrpou": "444"}]
        fop_statuses = {b"\x01": "Відкрита"}
        fop_groups = {b"\x01": 3}
        analyses = {}
        store_employees = {}

        result = find_reserve_fops(
            all_fops, fop_statuses, fop_groups, analyses, store_employees,
            reserve_threshold=100_000, employee_limit=8,
        )
        assert len(result) == 0

    def test_counts_employees_on_reserve_fop(self):
        all_fops = [{"id": b"\x01", "name": "ФОП Резерв", "edrpou": "555"}]
        fop_statuses = {b"\x01": "Відкрита"}
        fop_groups = {b"\x01": 2}
        analyses = {}
        store_employees = {
            "Магазин 639": [
                {"name": "Іванов", "employer_edrpou": "555"},
                {"name": "Петров", "employer_edrpou": "555"},
            ]
        }

        result = find_reserve_fops(
            all_fops, fop_statuses, fop_groups, analyses, store_employees,
            reserve_threshold=100_000, employee_limit=8,
        )
        assert result[0]["current_employees"] == 2
        assert result[0]["free_employee_slots"] == 6

    def test_includes_low_income_fop_below_threshold(self):
        all_fops = [{"id": b"\x01", "name": "ФОП Мало", "edrpou": "666"}]
        fop_statuses = {b"\x01": "Відкрита"}
        fop_groups = {b"\x01": 2}
        analyses = {b"\x01": {"total_income": 50_000}}
        store_employees = {}

        result = find_reserve_fops(
            all_fops, fop_statuses, fop_groups, analyses, store_employees,
            reserve_threshold=100_000, employee_limit=8,
        )
        assert len(result) == 1
        assert result[0]["current_income"] == 50_000


# === calculate_strategic_summary ===


class TestCalculateStrategicSummary:
    """Strategic level: total income forecast per network -> FOPs needed."""

    def test_basic_calculation(self):
        fop_entries = [
            {"fop_name": "ФОП А", "network": "ФАМО",
             "projected_total": 3_200_000, "is_active": True},
            {"fop_name": "ФОП Б", "network": "ФАМО",
             "projected_total": 3_400_000, "is_active": True},
            {"fop_name": "ФОП В", "network": "Технопростір",
             "projected_total": 2_000_000, "is_active": True},
        ]
        reserve_fops = []

        result = calculate_strategic_summary(
            fop_entries, reserve_fops, income_limit=3_500_000,
        )

        assert result["ФАМО"]["projected_annual_income"] == 6_600_000
        assert result["ФАМО"]["fops_needed"] == 2
        assert result["ФАМО"]["fops_active"] == 2
        assert result["ФАМО"]["fops_to_open"] == 0

        assert result["Технопростір"]["fops_needed"] == 1
        assert result["Технопростір"]["fops_active"] == 1

    def test_deficit_with_reserves(self):
        fop_entries = [
            {"fop_name": "ФОП А", "network": "ФАМО",
             "projected_total": 10_000_000, "is_active": True},
        ]
        reserve_fops = [
            {"network": "ФАМО", "fop_name": "ФОП Резерв"},
        ]

        result = calculate_strategic_summary(
            fop_entries, reserve_fops, income_limit=3_500_000,
        )

        assert result["ФАМО"]["fops_needed"] == 3
        assert result["ФАМО"]["fops_active"] == 1
        assert result["ФАМО"]["fops_reserve"] == 1
        assert result["ФАМО"]["fops_to_open"] == 1

    def test_no_deficit(self):
        fop_entries = [
            {"fop_name": "ФОП А", "network": "ФАМО",
             "projected_total": 1_000_000, "is_active": True},
            {"fop_name": "ФОП Б", "network": "ФАМО",
             "projected_total": 500_000, "is_active": True},
        ]

        result = calculate_strategic_summary(
            fop_entries, [], income_limit=3_500_000,
        )

        assert result["ФАМО"]["fops_to_open"] == 0


# === calculate_registration_date ===


class TestCalculateRegistrationDate:
    """Registration must start ~15th of month before the target ready date."""

    def test_limit_date_june_15(self):
        result = calculate_registration_date(
            limit_date=date(2026, 6, 15),
            today=date(2026, 4, 1),
        )
        assert result["ready_date"] == date(2026, 6, 1)
        assert result["registration_start"] == date(2026, 5, 15)
        assert result["urgency"] == "normal"

    def test_limit_date_may_1(self):
        result = calculate_registration_date(
            limit_date=date(2026, 5, 1),
            today=date(2026, 4, 1),
        )
        assert result["ready_date"] == date(2026, 5, 1)
        assert result["registration_start"] == date(2026, 4, 15)
        assert result["urgency"] == "normal"

    def test_urgent_when_less_than_month(self):
        result = calculate_registration_date(
            limit_date=date(2026, 4, 21),
            today=date(2026, 4, 1),
        )
        assert result["urgency"] == "urgent"

    def test_already_exceeded(self):
        result = calculate_registration_date(
            limit_date=date(2026, 3, 15),
            today=date(2026, 4, 1),
        )
        assert result["urgency"] == "overdue"

    def test_limit_date_january(self):
        result = calculate_registration_date(
            limit_date=date(2027, 1, 20),
            today=date(2026, 11, 1),
        )
        assert result["ready_date"] == date(2027, 1, 1)
        assert result["registration_start"] == date(2026, 12, 15)


# === check_employee_capacity ===


class TestCheckEmployeeCapacity:
    """Verify that transferring stores won't exceed 8 employees on target FOP."""

    def test_capacity_ok(self):
        result = check_employee_capacity(
            target_fop_employees=2,
            stores_to_transfer=[
                {"name": "PINKY 911", "employees": 3},
                {"name": "FORUM 639", "employees": 2},
            ],
            employee_limit=8,
        )
        assert result["ok"] is True
        assert result["total_after"] == 7
        assert result["overflow"] == 0

    def test_capacity_exact(self):
        result = check_employee_capacity(
            target_fop_employees=3,
            stores_to_transfer=[
                {"name": "Store A", "employees": 5},
            ],
            employee_limit=8,
        )
        assert result["ok"] is True
        assert result["total_after"] == 8

    def test_capacity_overflow(self):
        result = check_employee_capacity(
            target_fop_employees=5,
            stores_to_transfer=[
                {"name": "Store A", "employees": 3},
                {"name": "Store B", "employees": 2},
            ],
            employee_limit=8,
        )
        assert result["ok"] is False
        assert result["total_after"] == 10
        assert result["overflow"] == 2

    def test_empty_stores(self):
        result = check_employee_capacity(
            target_fop_employees=6,
            stores_to_transfer=[],
            employee_limit=8,
        )
        assert result["ok"] is True
        assert result["total_after"] == 6


# === build_monthly_plan ===


class TestBuildMonthlyPlan:
    """Tactical level: for each FOP approaching limit, plan replacement."""

    def test_single_fop_needs_replacement_no_reserve(self):
        fop_entries = [
            {
                "fop_name": "ФОП Петренко", "fop_edrpou": "111",
                "network": "ФАМО", "ep_group": 2,
                "total_income": 3_200_000, "limit_amount": 3_500_000,
                "income_percent": 91.4, "days_to_limit": 30,
                "projected_date": "2026-05-15",
                "limit_date": date(2026, 5, 15),
                "stores": [
                    {"name": "PINKY 911", "monthly_income": 200_000, "employees": 3},
                ],
                "employee_count": 3,
            },
        ]
        reserve_fops = []
        today = date(2026, 4, 14)

        plan = build_monthly_plan(fop_entries, reserve_fops, today, employee_limit=8)

        assert len(plan) >= 1
        may_actions = [a for m in plan for a in m["actions"] if m["month"] == "2026-05"]
        assert len(may_actions) == 1
        assert may_actions[0]["type"] == "open_new_fop"
        assert may_actions[0]["source_fop"]["name"] == "ФОП Петренко"

    def test_reserve_fop_used_when_available(self):
        fop_entries = [
            {
                "fop_name": "ФОП Петренко", "fop_edrpou": "111",
                "network": "ФАМО", "ep_group": 2,
                "total_income": 3_200_000, "limit_amount": 3_500_000,
                "income_percent": 91.4, "days_to_limit": 45,
                "projected_date": "2026-06-01",
                "limit_date": date(2026, 6, 1),
                "stores": [
                    {"name": "PINKY 911", "monthly_income": 200_000, "employees": 2},
                ],
                "employee_count": 2,
            },
        ]
        reserve_fops = [
            {
                "fop_name": "ФОП Резерв", "fop_edrpou": "999",
                "network": "ФАМО", "current_employees": 1,
                "free_employee_slots": 7,
            },
        ]
        today = date(2026, 4, 14)

        plan = build_monthly_plan(fop_entries, reserve_fops, today, employee_limit=8)

        actions = [a for m in plan for a in m["actions"]]
        assert len(actions) == 1
        assert actions[0]["type"] == "activate_reserve"
        assert actions[0]["reserve_fop"]["name"] == "ФОП Резерв"

    def test_no_action_if_limit_far_away(self):
        fop_entries = [
            {
                "fop_name": "ФОП Норма", "fop_edrpou": "111",
                "network": "ФАМО", "ep_group": 2,
                "total_income": 1_000_000, "limit_amount": 3_500_000,
                "income_percent": 28.6, "days_to_limit": 999,
                "projected_date": None,
                "limit_date": None,
                "stores": [],
                "employee_count": 4,
            },
        ]

        plan = build_monthly_plan(fop_entries, [], date(2026, 4, 14), employee_limit=8)

        actions = [a for m in plan for a in m["actions"]]
        assert len(actions) == 0

    def test_employee_overflow_generates_warning(self):
        fop_entries = [
            {
                "fop_name": "ФОП Багато", "fop_edrpou": "111",
                "network": "ФАМО", "ep_group": 2,
                "total_income": 3_400_000, "limit_amount": 3_500_000,
                "income_percent": 97.1, "days_to_limit": 10,
                "projected_date": "2026-04-24",
                "limit_date": date(2026, 4, 24),
                "stores": [
                    {"name": "Store A", "monthly_income": 100_000, "employees": 5},
                    {"name": "Store B", "monthly_income": 100_000, "employees": 5},
                ],
                "employee_count": 10,
            },
        ]
        reserve_fops = [
            {
                "fop_name": "ФОП Малий", "fop_edrpou": "222",
                "network": "ФАМО", "current_employees": 0,
                "free_employee_slots": 8,
            },
        ]
        today = date(2026, 4, 14)

        plan = build_monthly_plan(fop_entries, reserve_fops, today, employee_limit=8)

        actions = [a for m in plan for a in m["actions"]]
        assert len(actions) >= 1
        overflow_action = actions[0]
        assert overflow_action["employee_capacity_ok"] is False
