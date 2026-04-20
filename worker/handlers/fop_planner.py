"""FOP opening planner — Zeebe worker handler.

Прогнозує коли поточні ФОП вичерпають ліміт і формує помісячний план
відкриття нових ФОП або активації резервних.

Task type: fop-opening-plan
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from pyzeebe import ZeebeWorker

from ..config import AppConfig
from .fop_common import (
    LIMITS,
    _get_connection,
    _fetch_active_fops,
    _fetch_fop_groups,
    _fetch_fop_statuses,
    _fetch_daily_income,
    _fetch_fop_stores,
    _fetch_fop_companies,
    _fetch_seasonal_coefficients,
    _fetch_store_employees,
    _determine_organization,
    _analyze_fop,
    _safe_pct,
)

logger = logging.getLogger(__name__)

PLAN_REPORT_DIR = Path(os.environ.get("FOP_REPORT_DIR", "reports/fop"))
PLAN_REPORT_FILE = PLAN_REPORT_DIR / "plan-latest.json"


# ── Reserve FOP discovery ─────────────────────────────────────────────


def find_reserve_fops(
    all_fops: list[dict],
    fop_statuses: dict[bytes, str],
    fop_groups: dict[bytes, int],
    analyses: dict[bytes, dict],
    store_employees: dict[str, list[dict]],
    *,
    reserve_threshold: float = 100_000,
    employee_limit: int = 8,
) -> list[dict]:
    """Find FOPs that are open but not actively used (reserve pool).

    Criteria: status=Відкрита, group=2, income < reserve_threshold.
    """
    reserve = []
    for fop in all_fops:
        fop_id = bytes(fop["id"])
        status = fop_statuses.get(fop_id, "Відкрита")
        if status != "Відкрита":
            continue
        group = fop_groups.get(fop_id, 2)
        if group != 2:
            continue

        analysis = analyses.get(fop_id)
        income = analysis["total_income"] if analysis else 0.0
        if income >= reserve_threshold:
            continue

        edrpou = (fop.get("edrpou") or "").strip()
        emp_count = 0
        for dept_emps in store_employees.values():
            emp_count += sum(1 for e in dept_emps if e.get("employer_edrpou") == edrpou)

        network = _determine_organization(fop["name"].strip())

        reserve.append({
            "fop_name": fop["name"].strip(),
            "fop_edrpou": edrpou,
            "network": network,
            "ep_group": group,
            "current_income": income,
            "current_employees": emp_count,
            "free_employee_slots": employee_limit - emp_count,
        })

    return reserve


# ── Strategic summary ─────────────────────────────────────────────────


def calculate_strategic_summary(
    fop_entries: list[dict],
    reserve_fops: list[dict],
    *,
    income_limit: float = 6_600_000,
    growth_percent: float = 0.0,
) -> dict[str, dict]:
    """Level A: how many FOPs each network needs for the year.

    Args:
        growth_percent: year-over-year business growth (e.g. 15.0 = +15%).
            Applied to projected income to account for business expansion.

    Returns {network: {projected_annual_income, fops_needed, fops_active,
                       fops_reserve, fops_to_open}}.
    """
    networks: dict[str, dict] = {}

    for entry in fop_entries:
        net = entry["network"]
        if net not in networks:
            networks[net] = {
                "projected_annual_income": 0.0,
                "fops_needed": 0,
                "fops_active": 0,
                "fops_reserve": 0,
                "fops_to_open": 0,
            }
        networks[net]["projected_annual_income"] += entry["projected_total"]
        if entry.get("is_active"):
            networks[net]["fops_active"] += 1

    for rf in reserve_fops:
        net = rf["network"]
        if net not in networks:
            networks[net] = {
                "projected_annual_income": 0.0,
                "fops_needed": 0,
                "fops_active": 0,
                "fops_reserve": 0,
                "fops_to_open": 0,
            }
        networks[net]["fops_reserve"] += 1

    growth_multiplier = 1.0 + growth_percent / 100.0

    for net, data in networks.items():
        data["projected_annual_income"] = round(
            data["projected_annual_income"] * growth_multiplier, 2,
        )
        data["fops_needed"] = max(1, math.ceil(data["projected_annual_income"] / income_limit))
        deficit = data["fops_needed"] - data["fops_active"] - data["fops_reserve"]
        data["fops_to_open"] = max(0, deficit)

    return networks


# ── Registration date calculation ─────────────────────────────────────


def calculate_registration_date(
    limit_date: date,
    today: date,
) -> dict:
    """Calculate when to start FOP registration given a projected limit date.

    Business rule: register ~15th of the month BEFORE the month when FOP
    is needed. FOP becomes ready on the 1st of the target month.

    Returns {registration_start, ready_date, urgency}.
    """
    ready_date = limit_date.replace(day=1)

    if ready_date.month == 1:
        reg_start = date(ready_date.year - 1, 12, 15)
    else:
        reg_start = date(ready_date.year, ready_date.month - 1, 15)

    days_until_limit = (limit_date - today).days

    if days_until_limit < 0:
        urgency = "overdue"
    elif days_until_limit < 30:
        urgency = "urgent"
    else:
        urgency = "normal"

    return {
        "registration_start": reg_start,
        "ready_date": ready_date,
        "urgency": urgency,
    }


# ── Employee capacity ─────────────────────────────────────────────────


def check_employee_capacity(
    target_fop_employees: int,
    stores_to_transfer: list[dict],
    *,
    employee_limit: int = 8,
) -> dict:
    """Check if transferring stores to a FOP stays within employee limit.

    Returns {ok, total_after, overflow}.
    """
    transferring = sum(s.get("employees", 0) for s in stores_to_transfer)
    total_after = target_fop_employees + transferring
    overflow = max(0, total_after - employee_limit)

    return {
        "ok": total_after <= employee_limit,
        "total_after": total_after,
        "overflow": overflow,
    }


# ── Tactical monthly plan ─────────────────────────────────────────────


def build_monthly_plan(
    fop_entries: list[dict],
    reserve_fops: list[dict],
    today: date,
    *,
    employee_limit: int = 8,
) -> list[dict]:
    """Level B: for each FOP approaching limit, plan replacement by month.

    Returns list of {month: "YYYY-MM", actions: [...]}.
    """
    available_reserves = list(reserve_fops)
    month_actions: dict[str, list[dict]] = {}

    approaching = [
        e for e in fop_entries
        if e.get("limit_date") is not None and e["days_to_limit"] < 999
    ]
    approaching.sort(key=lambda e: e["limit_date"])

    for entry in approaching:
        limit_date = entry["limit_date"]
        target_month = f"{limit_date.year}-{limit_date.month:02d}"

        if target_month not in month_actions:
            month_actions[target_month] = []

        reg = calculate_registration_date(limit_date, today)

        stores = entry.get("stores", [])
        employees_to_transfer = sum(s.get("employees", 0) for s in stores)

        source_fop = {
            "name": entry["fop_name"],
            "edrpou": entry["fop_edrpou"],
            "income_percent": entry["income_percent"],
            "projected_limit_date": entry["projected_date"],
            "days_to_limit": entry["days_to_limit"],
        }

        # Try to find a matching reserve FOP (same network, enough employee slots)
        matched_reserve = None
        for i, rf in enumerate(available_reserves):
            if rf["network"] != entry["network"]:
                continue
            cap = check_employee_capacity(
                rf["current_employees"], stores, employee_limit=employee_limit,
            )
            if cap["ok"]:
                matched_reserve = available_reserves.pop(i)
                break

        if matched_reserve:
            cap = check_employee_capacity(
                matched_reserve["current_employees"], stores,
                employee_limit=employee_limit,
            )
            month_actions[target_month].append({
                "type": "activate_reserve",
                "network": entry["network"],
                "urgency": reg["urgency"],
                "reason": f"{entry['fop_name']} вичерпає ліміт ~{entry['projected_date']}",
                "source_fop": source_fop,
                "reserve_fop": {
                    "name": matched_reserve["fop_name"],
                    "edrpou": matched_reserve["fop_edrpou"],
                    "current_employees": matched_reserve["current_employees"],
                    "free_slots": matched_reserve["free_employee_slots"],
                },
                "stores_to_transfer": stores,
                "employees_to_transfer": employees_to_transfer,
                "total_employees_after": cap["total_after"],
                "employee_capacity_ok": cap["ok"],
            })
        else:
            cap = check_employee_capacity(
                0, stores, employee_limit=employee_limit,
            )
            month_actions[target_month].append({
                "type": "open_new_fop",
                "network": entry["network"],
                "urgency": reg["urgency"],
                "reason": f"{entry['fop_name']} вичерпає ліміт ~{entry['projected_date']}",
                "source_fop": source_fop,
                "registration_start": reg["registration_start"].isoformat(),
                "ready_date": reg["ready_date"].isoformat(),
                "stores_to_transfer": stores,
                "employees_to_transfer": employees_to_transfer,
                "employee_capacity_ok": cap["ok"],
            })

    plan = []
    for month in sorted(month_actions.keys()):
        actions = month_actions[month]
        if actions:
            plan.append({"month": month, "actions": actions})

    return plan


# ── Main orchestration ────────────────────────────────────────────────


def _run_fop_plan(
    *,
    horizon_months: int = 12,
    income_limit: float = 6_600_000,
    employee_limit: int = 8,
    reserve_threshold: float = 100_000,
    growth_percent: float = 0.0,
) -> dict:
    """Synchronous: full FOP opening plan (DB -> analysis -> JSON plan).

    Returns summary dict for Camunda process variables.
    """
    import worker.handlers.fop_common as _common

    today = date.today()
    year = today.year
    horizon_date = date(
        today.year + (today.month + horizon_months - 1) // 12,
        (today.month + horizon_months - 1) % 12 + 1,
        28,
    )

    logger.info("fop-opening-plan: підключення до БД...")
    conn = _get_connection()

    try:
        fops = _fetch_active_fops(conn, year)
        logger.info("Знайдено ФОПів: %d", len(fops))

        daily_income = _fetch_daily_income(conn, year)
        fop_groups = _fetch_fop_groups(conn)
        fop_statuses = _fetch_fop_statuses(conn)
        fop_stores_map, _ = _fetch_fop_stores(conn, year)
        _common._fop_company_cache = _fetch_fop_companies(conn)
        seasonal_coefficients, network_coefficients = _fetch_seasonal_coefficients(conn, year)
        store_employees = _fetch_store_employees(conn)
    finally:
        conn.close()

    # Analyze each FOP
    analyses: dict[bytes, dict] = {}
    for fop in fops:
        fop_id = bytes(fop["id"])
        data = daily_income.get(fop_id, [])
        stores = fop_stores_map.get(fop_id, [])
        result = _analyze_fop(
            data, today, year,
            seasonal_coefficients=seasonal_coefficients,
            network_coefficients=network_coefficients,
            fop_stores=stores,
        )
        if result:
            analyses[fop_id] = result

    # Find reserve FOPs
    reserve = find_reserve_fops(
        fops, fop_statuses, fop_groups, analyses, store_employees,
        reserve_threshold=reserve_threshold, employee_limit=employee_limit,
    )
    reserve_edrpous = {r["fop_edrpou"] for r in reserve}
    logger.info("Резервних ФОПів: %d", len(reserve))

    # Build FOP entries for planning (active FOPs only, not reserves)
    fop_entries = []
    for fop in fops:
        fop_id = bytes(fop["id"])
        edrpou = (fop.get("edrpou") or "").strip()
        if edrpou in reserve_edrpous:
            continue
        analysis = analyses.get(fop_id)
        if not analysis:
            continue

        group = fop_groups.get(fop_id, 2)
        limit = LIMITS.get(group, income_limit)
        info = analysis["limit_dates"].get(group, {})

        if info.get("already_exceeded"):
            limit_date_val = today
            projected_date_str = "ПЕРЕВИЩЕНО"
            days_to_limit = 0
        elif info.get("date") is not None:
            ld = info["date"]
            if isinstance(ld, str):
                limit_date_val = None
                projected_date_str = ld
                days_to_limit = 999
            else:
                limit_date_val = ld
                projected_date_str = ld.strftime("%Y-%m-%d")
                days_to_limit = (ld - today).days
        else:
            limit_date_val = None
            projected_date_str = None
            days_to_limit = 999

        network = _determine_organization(fop["name"].strip())
        stores = fop_stores_map.get(fop_id, [])

        # Count employees for this FOP (once, not per store)
        emp_count = 0
        for dept_emps in store_employees.values():
            emp_count += sum(
                1 for e in dept_emps
                if e.get("employer_edrpou") == edrpou
            )

        store_details = []
        for s in stores:
            s_name = s["name"]
            # Count employees for THIS store only (match by 3-digit code)
            code = s_name[:3] if len(s_name) >= 3 and s_name[:3].isdigit() else ""
            s_emps = 0
            if code:
                for dept_name, dept_emps in store_employees.items():
                    if dept_name.startswith(code):
                        s_emps += sum(
                            1 for e in dept_emps
                            if e.get("employer_edrpou") == edrpou
                        )
            store_details.append({
                "name": s_name,
                "monthly_income": round(
                    s.get("total", 0) / max(1, analysis["days_elapsed"]) * 30, 2
                ),
                "employees": s_emps,
            })

        fop_entries.append({
            "fop_name": fop["name"].strip(),
            "fop_edrpou": edrpou,
            "network": network,
            "ep_group": group,
            "total_income": round(analysis["total_income"], 2),
            "limit_amount": limit,
            "income_percent": _safe_pct(analysis["total_income"], limit),
            "days_to_limit": days_to_limit,
            "projected_date": projected_date_str,
            "projected_total": round(analysis["projected_total"], 2),
            "limit_date": limit_date_val,
            "stores": store_details,
            "employee_count": emp_count,
            "is_active": True,
        })

    # Level A: Strategic summary
    strategic = calculate_strategic_summary(
        fop_entries, reserve, income_limit=income_limit,
        growth_percent=growth_percent,
    )
    logger.info(
        "Стратегічний план: %s",
        {k: v["fops_to_open"] for k, v in strategic.items()},
    )

    # Level B: Tactical monthly plan
    monthly_plan = build_monthly_plan(
        fop_entries, reserve, today, employee_limit=employee_limit,
    )

    # Warnings
    warnings = []
    for month_data in monthly_plan:
        for action in month_data["actions"]:
            if action.get("urgency") in ("urgent", "overdue"):
                warnings.append({
                    "type": "urgent_opening",
                    "message": f"ТЕРМІНОВО: {action['reason']} — {action['urgency']}",
                })
            if not action.get("employee_capacity_ok"):
                emp_count = action.get("employees_to_transfer", 0)
                warnings.append({
                    "type": "employee_overflow",
                    "message": (
                        f"Переведення {emp_count} працівників "
                        f"потребує розподілу між кількома ФОП"
                    ),
                })

    total_actions = sum(len(m["actions"]) for m in monthly_plan)

    report = {
        "plan_date": today.isoformat(),
        "horizon": horizon_date.isoformat(),
        "parameters": {
            "income_limit": income_limit,
            "employee_limit": employee_limit,
            "reserve_threshold": reserve_threshold,
            "horizon_months": horizon_months,
            "growth_percent": growth_percent,
        },
        "strategic_summary": strategic,
        "reserve_fops": reserve,
        "monthly_plan": monthly_plan,
        "warnings": warnings,
    }

    _save_plan_json(report)

    return {
        "plan_date": today.isoformat(),
        "total_active_fops": len(fop_entries),
        "reserve_fops_count": len(reserve),
        "actions_count": total_actions,
        "warnings_count": len(warnings),
        "plan_json": report,
    }


def _save_plan_json(report: dict) -> None:
    """Save plan JSON to file (atomic write)."""
    try:
        PLAN_REPORT_DIR.mkdir(parents=True, exist_ok=True)
        tmp = PLAN_REPORT_FILE.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        tmp.rename(PLAN_REPORT_FILE)
        logger.info("План збережено: %s", PLAN_REPORT_FILE)
    except Exception as e:
        logger.error("Помилка збереження плану: %s", e)


# ── Zeebe handler registration ────────────────────────────────────────


def register_fop_planner_handlers(
    worker: ZeebeWorker,
    config: AppConfig,
) -> None:
    """Register FOP opening planner handler."""

    @worker.task(task_type="fop-opening-plan", timeout_ms=300_000)
    async def fop_opening_plan(
        horizon_months: int = 12,
        income_limit: float = 6_600_000,
        employee_limit: int = 8,
        reserve_threshold: float = 100_000,
        growth_percent: float = 0.0,
        **kwargs: Any,
    ) -> dict:
        """Планування відкриття нових ФОП.

        Input variables:
            horizon_months (int): горизонт планування в місяцях (default: 12)
            income_limit (float): річний ліміт доходу ФОП (default: 6600000)
            employee_limit (int): максимум працівників на ФОП (default: 8)
            reserve_threshold (float): поріг доходу для резервного ФОП (default: 100000)
            growth_percent (float): % росту бізнесу рік-до-року (default: 0)

        Output variables:
            plan_date (str): дата плану (ISO)
            total_active_fops (int): кількість активних ФОП
            reserve_fops_count (int): кількість резервних ФОП
            actions_count (int): загальна кількість запланованих дій
            warnings_count (int): кількість попереджень
            plan_json (dict): повний JSON-план
        """
        logger.info(
            "fop-opening-plan (horizon=%d, limit=%s, emp_limit=%d)",
            horizon_months, income_limit, employee_limit,
        )

        result = await asyncio.to_thread(
            _run_fop_plan,
            horizon_months=horizon_months,
            income_limit=income_limit,
            employee_limit=employee_limit,
            reserve_threshold=reserve_threshold,
            growth_percent=growth_percent,
        )

        logger.info(
            "fop-opening-plan done — %d active, %d reserve, %d actions, %d warnings",
            result["total_active_fops"],
            result["reserve_fops_count"],
            result["actions_count"],
            result["warnings_count"],
        )

        return result
