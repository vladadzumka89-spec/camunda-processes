"""FOP limit monitoring — Zeebe worker handler.

Підключається до БД BAS Бухгалтерія, аналізує надходження на рахунки ФОП
та прогнозує дату досягнення ліміту для 2-ї та 3-ї груп ЄП.

Повертає JSON-звіт (всі ФОП для дашборду Odoo, зберігається у файл) та список
критичних ФОП для оркестрації через BPMN (задачі на зміну терміналу).

Task type: fop-limit-check
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime, date
from typing import Any

from pyzeebe import ZeebeWorker

from ..config import AppConfig
from .fop_common import (
    BAS_YEAR_OFFSET,
    EP_GROUP_ENUM,
    LIMITS,
    REPORT_DIR,
    REPORT_FILE,
    _get_db_config,
    _get_connection,
    _fetch_active_fops,
    _fetch_fop_groups,
    _fetch_fop_statuses,
    _fetch_daily_income,
    _fetch_q4_prev_year_income,
    _fetch_fop_stores,
    _fetch_fop_companies,
    _fetch_seasonal_coefficients,
    _fetch_terminal_changes,
    _fetch_monthly_history,
    _fetch_terminal_bindings,
    _fetch_store_employees,
    _fetch_disbanded_subdivision_codes,
    _fetch_active_terminal_bindings_by_org,
    _determine_organization,
    _determine_store_company,
    _enrich_store_with_employees,
    _group_binding_periods,
    _determine_current_fop,
    _calc_growth_percent,
    _safe_pct,
    _analyze_fop,
    _save_report_json,
    _parse_terminal_name,
    _classify_payment,
    _match_terminal_to_subdivision,
    _translit_ukr,
    _normalize_terminal_name,
    _fetch_subdivision_lookup,
    _compute_seasonal_coefficients,
    _compute_terminal_change,
    _parse_binding_date,
)

logger = logging.getLogger(__name__)

CAMUNDA_REST_URL = os.environ.get(
    "CAMUNDA_REST_URL", "http://orchestration:8080"
)
CAMUNDA_TOKEN_URL = os.environ.get(
    "ZEEBE_TOKEN_URL",
    os.environ.get(
        "CAMUNDA_TOKEN_URL",
        "http://keycloak:18080/auth/realms/camunda-platform/protocol/openid-connect/token",
    ),
)
CAMUNDA_CLIENT_ID = os.environ.get(
    "ZEEBE_CLIENT_ID", os.environ.get("CAMUNDA_CLIENT_ID", "orchestration")
)
CAMUNDA_CLIENT_SECRET = os.environ.get(
    "ZEEBE_CLIENT_SECRET", os.environ.get("CAMUNDA_CLIENT_SECRET", "")
)
CAMUNDA_PROCESS_ID = "Process_0iy2u1a"

_token_cache: dict = {"token": None, "expires_at": 0.0}


# ── Camunda REST API (dedup) ──────────────────────────────────────────


def _get_access_token() -> str:
    """Get OAuth2 token from Keycloak (cached until expiry)."""
    import httpx

    now = time.time()
    if _token_cache["token"] and now < _token_cache["expires_at"] - 30:
        return _token_cache["token"]

    resp = httpx.post(
        CAMUNDA_TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": CAMUNDA_CLIENT_ID,
            "client_secret": CAMUNDA_CLIENT_SECRET,
        },
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    _token_cache["token"] = data["access_token"]
    _token_cache["expires_at"] = now + data.get("expires_in", 300)
    return _token_cache["token"]


def _get_active_fop_edrpous() -> set[str]:
    """Query Camunda for active Process_0iy2u1a instances and return their fop_edrpou values."""
    import httpx

    try:
        token = _get_access_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        # 1. Find all active Process_0iy2u1a instances
        resp = httpx.post(
            f"{CAMUNDA_REST_URL}/v2/process-instances/search",
            headers=headers,
            json={
                "filter": {
                    "processDefinitionId": CAMUNDA_PROCESS_ID,
                    "state": "ACTIVE",
                },
                "page": {"limit": 200},
            },
            timeout=15,
        )
        resp.raise_for_status()
        instances = resp.json().get("items", [])

        if not instances:
            return set()

        # 2. Search for fop_edrpou variables in these instances
        instance_keys = [i["processInstanceKey"] for i in instances]
        active_edrpous = set()

        # Query variables in batches (API may limit)
        for key in instance_keys:
            resp = httpx.post(
                f"{CAMUNDA_REST_URL}/v2/variables/search",
                headers=headers,
                json={
                    "filter": {
                        "processInstanceKey": key,
                        "name": "fop_edrpou",
                    },
                },
                timeout=10,
            )
            if resp.status_code == 200:
                for var in resp.json().get("items", []):
                    val = var.get("value")
                    if val:
                        # value may be JSON-encoded string
                        if isinstance(val, str):
                            val = val.strip('"')
                        active_edrpous.add(str(val))

        logger.info(
            "Дедуплікація: %d активних Process_0iy2u1a, ЄДРПОУ: %s",
            len(instances),
            active_edrpous or "немає",
        )
        return active_edrpous

    except Exception as e:
        logger.warning("Не вдалося перевірити активні процеси (дедуплікація пропущена): %s", e)
        return set()


def _run_fop_check(days_ahead: int = 14) -> dict:
    """Synchronous: full FOP limit check (DB → analysis → JSON report).

    Returns summary dict for Camunda process variables.
    """
    today = datetime.now().date()
    year = today.year

    logger.info("Підключення до БД BAS Бухгалтерія...")
    conn = _get_connection()

    try:
        fops = _fetch_active_fops(conn, year)
        logger.info("Знайдено активних ФОПів: %d", len(fops))

        daily_income = _fetch_daily_income(conn, year)
        logger.info("Завантажено дані по %d ФОПах", len(daily_income))

        q4_prev_income = _fetch_q4_prev_year_income(conn, year)
        logger.info("Q4 %d дохід: %d ФОПів", year - 1, len(q4_prev_income))

        fop_stores, monthly_store_income = _fetch_fop_stores(conn, year)
        logger.info("Магазини: зв'язки для %d ФОПів", len(fop_stores))

        fop_groups = _fetch_fop_groups(conn)

        fop_statuses = _fetch_fop_statuses(conn)

        # FOP company mapping from BAS 'Принадлежність'
        import worker.handlers.fop_common as _common
        _common._fop_company_cache = _fetch_fop_companies(conn)

        # Seasonal coefficients from previous year
        seasonal_coefficients, network_coefficients = _fetch_seasonal_coefficients(conn, year)

        # Terminal count changes (this week vs last week)
        terminal_changes = _fetch_terminal_changes(conn, year)

        # Monthly income history (Q4 previous year + current year)
        monthly_history = _fetch_monthly_history(conn, year)
        logger.info("Місячна історія: %d ФОПів", len(monthly_history))

        logger.info("Помісячний дохід: %d магазинів", len(monthly_store_income))

        # Terminal binding history (store ↔ FOP switches)
        terminal_bindings = _fetch_terminal_bindings(conn, year)
        logger.info("Прив'язка терміналів: %d магазинів", len(terminal_bindings))

        # Current employees per store (from employee directory)
        store_employees = _fetch_store_employees(conn)
        logger.info("Працівники магазинів: %d підрозділів", len(store_employees))

        disbanded_subdivision_codes = _fetch_disbanded_subdivision_codes(conn)
    finally:
        conn.close()

    # Analyze all FOPs
    analyses = {}
    for fop in fops:
        fop_id = bytes(fop["id"])
        data = daily_income.get(fop_id, [])
        stores = fop_stores.get(fop_id, [])
        result = _analyze_fop(
            data, today, year,
            seasonal_coefficients=seasonal_coefficients,
            network_coefficients=network_coefficients,
            fop_stores=stores,
        )
        if result:
            analyses[fop_id] = result

    logger.info("Проаналізовано %d ФОПів з %d", len(analyses), len(fops))

    # Get active EDRPOU set for dedup BEFORE building summary
    active_edrpous = _get_active_fop_edrpous()

    # Build summary for ALL analyzed FOPs with status
    all_fops_report = []
    critical_fops = []

    for fop in fops:
        fop_id = bytes(fop["id"])
        analysis = analyses.get(fop_id)
        if not analysis:
            continue

        group = fop_groups.get(fop_id, 2)
        limit = LIMITS[group]
        info = analysis["limit_dates"][group]

        if info["already_exceeded"]:
            days_to_limit = 0
            projected_date = "ПЕРЕВИЩЕНО"
        elif info["date"] is not None:
            days_to_limit = (info["date"] - today).days
            projected_date = info["date"].strftime("%Y-%m-%d")
        else:
            days_to_limit = 999
            projected_date = None

        edrpou = (fop.get("edrpou") or "").strip()
        is_critical = days_to_limit <= days_ahead
        has_active_process = edrpou in active_edrpous

        if is_critical and has_active_process:
            status = "В роботі"
        elif is_critical:
            status = "Критично"
        else:
            status = "Норма"

        stores = fop_stores.get(fop_id, [])
        # Normalize store names: prefer Reference100 canonical names
        _tb_code_names: dict[str, str] = {}
        for tb_name in terminal_bindings:
            m_tb = re.match(r'^(\d{3})\s', tb_name)
            if m_tb and m_tb.group(1) not in _tb_code_names:
                _tb_code_names[m_tb.group(1)] = tb_name
        stores_list = []
        for s in stores:
            sname = s["name"]
            m_sc = re.match(r'^(\d{3})\s', sname)
            if m_sc and m_sc.group(1) in _tb_code_names:
                sname = _tb_code_names[m_sc.group(1)]
            stores_list.append({
                "name": sname, "doc_count": s["doc_count"],
                "total": s["total"], "source": s.get("source", "document"),
                "last_date": s.get("last_date"),
                "recent_income": s.get("recent_income", 0),
            })
        stores_total = round(sum(s["total"] for s in stores), 2)
        income_diff = round(analysis["total_income"] - stores_total, 2)
        stores_lines = [
            f"{s['name']}: {s['total']:,.0f}".replace(",", " ")
            for s in stores
        ]
        stores_lines.append(f"Різниця: {income_diff:,.0f}".replace(",", " "))
        stores_text = "\n".join(stores_lines)

        # Terminal changes
        tc = terminal_changes.get(fop_id, {})

        org_status = fop_statuses.get(fop_id, "Відкрита")
        organization = _determine_organization(fop["name"])
        monthly = monthly_history.get(fop_id, [])

        q4_income = round(q4_prev_income.get(fop_id, 0), 2)
        total_with_q4 = round(analysis["total_income"] + q4_income, 2)

        # Current & previous month income
        current_month = today.month
        prev_month = current_month - 1 if current_month > 1 else 12
        prev_month_year = today.year if current_month > 1 else today.year - 1
        curr_month_key = f"{today.year}-{current_month:02d}"
        prev_month_key = f"{prev_month_year}-{prev_month:02d}"
        monthly_map = {m["month"]: m["total"] for m in monthly}
        fop_income_curr = round(monthly_map.get(curr_month_key, 0), 2)
        fop_income_prev = round(monthly_map.get(prev_month_key, 0), 2)

        fop_entry = {
            "fop_name": fop["name"].strip(),
            "fop_edrpou": edrpou,
            "company": organization,
            "organization": organization,
            "org_status": org_status,
            "x_studio_camunda_org_status": org_status,
            "ep_group": group,
            "total_income": round(analysis["total_income"], 2),
            "limit_amount": limit,
            "income_percent": _safe_pct(analysis["total_income"], limit),
            "days_to_limit": days_to_limit,
            "projected_date": projected_date,
            "projected_total": round(analysis["projected_total"], 2),
            "mean_daily": round(analysis["mean_daily"], 2),
            "active_days": analysis["active_days"],
            "trend_ratio": round(analysis["trend_ratio"], 2),
            "stores": stores_list,
            "stores_text": stores_text,
            "stores_count": len(stores),
            "status": status,
            "terminal_change": tc.get("terminal_change", 0),
            "terminal_change_percent": tc.get("terminal_change_percent", 0.0),
            "seasonal_adjusted": bool(seasonal_coefficients),
            "monthly_income": monthly,
            "income_curr_month": fop_income_curr,
            "income_prev_month": fop_income_prev,
        }

        # ── Employees per FOP ──
        fop_emp_lines = []
        fop_emp_total = 0
        for dept_name, dept_emps in store_employees.items():
            matching = [e for e in dept_emps if e["employer_edrpou"] == edrpou]
            if matching:
                fop_emp_total += len(matching)
                fop_emp_lines.append(f"{dept_name} ({len(matching)}):")
                for e in matching:
                    fop_emp_lines.append(f"  {e['name']}")
        if fop_emp_lines:
            fop_emp_lines.append(f"Всього: {fop_emp_total}")
        fop_entry["employees_text"] = "\n".join(fop_emp_lines)
        fop_entry["employee_count"] = fop_emp_total

        all_fops_report.append(fop_entry)

        # critical_fops for Camunda multi-instance: only NEW (no active process)
        if is_critical and not has_active_process:
            critical_fops.append({
                "fop_name": fop_entry["fop_name"],
                "fop_edrpou": fop_entry["fop_edrpou"],
                "ep_group": group,
                "total_income": fop_entry["total_income"],
                "limit_amount": limit,
                "income_percent": fop_entry["income_percent"],
                "days_to_limit": days_to_limit,
                "projected_date": projected_date,
                "stores": ", ".join(
                    f"{s['name']}: {s['total']:,.0f}".replace(",", " ")
                    for s in stores[:5]
                ),
                "stores_count": len(stores),
                "trend_ratio": fop_entry["trend_ratio"],
                "terminal_change": fop_entry["terminal_change"],
                "terminal_change_percent": fop_entry["terminal_change_percent"],
            })

    critical_all = sum(1 for f in all_fops_report if f["status"] != "Норма")
    critical_in_progress = sum(1 for f in all_fops_report if f["status"] == "В роботі")

    logger.info(
        "Критичних ФОПів: %d всього (%d нових, %d вже в роботі)",
        critical_all, len(critical_fops), critical_in_progress,
    )

    # JSON report — all FOPs sorted by days_to_limit (most urgent first)
    all_fops_report.sort(key=lambda f: f["days_to_limit"])

    # ── Store-level report ────────────────────────────────────────────
    # Aggregate by 3-digit code to avoid duplicates from Reference100 vs Reference116
    # Build code → canonical name from terminal_bindings (uses Reference100 names)
    _r100_names: dict[str, str] = {}
    for tb_name in terminal_bindings:
        m_tb = re.match(r'^(\d{3})\s', tb_name)
        if m_tb and m_tb.group(1) not in _r100_names:
            _r100_names[m_tb.group(1)] = tb_name
    store_agg: dict[str, dict] = {}
    _code_to_key: dict[str, str] = {}  # "613" → canonical store name
    for fop_entry in all_fops_report:
        for s in fop_entry.get("stores", []):
            name = s["name"]
            # Determine aggregation key: use 3-digit code only for terminal sources
            m_code = re.match(r'^(\d{3})\s', name)
            if m_code and s.get("source") == "terminal":
                code = m_code.group(1)
                if code in _code_to_key:
                    agg_key = _code_to_key[code]
                else:
                    canonical = _r100_names.get(code, name)
                    _code_to_key[code] = canonical
                    agg_key = canonical
            else:
                agg_key = name
            if agg_key not in store_agg:
                store_agg[agg_key] = {
                    "subdivision": agg_key,
                    "source": s.get("source", ""),
                    "total_income": 0.0,
                    "fops": [],
                }
            store_agg[agg_key]["total_income"] += s.get("total", 0)
            _total = s.get("total", 0)
            store_agg[agg_key]["fops"].append({
                "fop_name": fop_entry["fop_name"],
                "fop_edrpou": fop_entry["fop_edrpou"],
                "income_from_store": _total,
                "income_from_store_text": f"{_total:,.0f} грн".replace(",", " "),
                "days_to_limit": fop_entry["days_to_limit"],
                "organization": fop_entry.get("organization", ""),
                "last_date": s.get("last_date"),
                "recent_income": s.get("recent_income", 0),
            })

    # Determine current month for growth calculation
    current_month = today.month
    prev_month = current_month - 1  # 0 means January → no prev data

    # Determine organization per store from prefix + enrich with new fields
    stores_report = []
    # Skip non-store subdivisions (offices, warehouses, service: 2xx-4xx)
    _NON_STORE_PREFIXES = {"2", "3", "4"}
    for name, data in sorted(store_agg.items(), key=lambda x: -x[1]["total_income"]):
        m = re.match(r'^(\d{3})\s', name)
        if m and m.group(1)[0] in _NON_STORE_PREFIXES:
            continue
        data["total_income"] = round(data["total_income"], 2)

        # ── Store status (відкритий/закритий) ──
        # Закритий = підрозділ розформовано (_Fld27513 = 0x01 в _Reference100)
        store_code = m.group(1) if m else None
        data["store_status"] = (
            "закритий" if store_code and store_code in disbanded_subdivision_codes
            else "відкритий"
        )

        # ── Current FOP ──
        bindings = terminal_bindings.get(name, [])
        # Check if bindings have any useful records (dates >= 2020)
        _has_useful = any(
            (d := _parse_binding_date(b["date"])) and d.year >= 2020
            for b in bindings
        )
        if not _has_useful:
            bindings = []
        def _has_useful_bindings(bl: list) -> bool:
            return any(
                (d := _parse_binding_date(b["date"])) and d.year >= 2020
                for b in bl
            )

        # Fallback 1: match by subdivision code (first 3 digits)
        if not bindings and m:
            code = m.group(1)
            for tb_name, tb_bindings in terminal_bindings.items():
                if tb_name.startswith(code + " ") and _has_useful_bindings(tb_bindings):
                    bindings = tb_bindings
                    break
        is_disbanded = data["store_status"] == "закритий"
        current_fop_name, current_fop_edrpou = _determine_current_fop(
            bindings, data["fops"]
        )
        if is_disbanded:
            current_fop_name = ""
            current_fop_edrpou = ""
        data["current_fop_name"] = current_fop_name
        data["current_fop_edrpou"] = current_fop_edrpou
        data["company"] = _determine_store_company(name) or _determine_organization(current_fop_name)

        # Find FOP group → limit
        fop_match = next(
            (f for f in data["fops"] if f["fop_edrpou"] == current_fop_edrpou),
            None,
        )
        income_from_fop = fop_match["income_from_store"] if fop_match else 0
        # Look up FOP's group limit from fop_groups data
        current_fop_limit = LIMITS.get(2, 3_500_000)  # default group 2
        # Try to find actual group from all_fops_report
        for fop_entry in all_fops_report:
            if fop_entry["fop_edrpou"] == current_fop_edrpou:
                current_fop_limit = fop_entry.get("limit_amount", LIMITS.get(2, 3_500_000))
                break
        data["current_fop_limit"] = current_fop_limit
        data["income_percent_of_limit"] = (
            round((income_from_fop / current_fop_limit) * 100, 1)
            if current_fop_limit > 0 else 0.0
        )

        # ── Growth ──
        store_monthly = monthly_store_income.get(name, {})
        curr_income = store_monthly.get(current_month, 0)
        prev_income = store_monthly.get(prev_month, 0) if prev_month > 0 else 0
        data["growth_percent"] = _calc_growth_percent(prev_income, curr_income)
        data["income_prev_month"] = round(prev_income, 2)
        data["income_curr_month"] = round(curr_income, 2)

        # ── Binding history (grouped into periods) ──
        data["binding_history"] = _group_binding_periods(
            bindings, year, current_fop_name if not is_disbanded else ""
        )
        # Disbanded: close all open binding periods
        if is_disbanded:
            for period_entry in data["binding_history"]:
                if period_entry.get("date_to") is None:
                    period_entry["date_to"] = "розформовано"

        # ── FOP income text: only FOPs with income in current year, sorted by amount ──
        fops_lines = []
        for f in sorted(data["fops"], key=lambda x: -x["income_from_store"]):
            if f["income_from_store"] > 0:
                income_str = f"{f['income_from_store']:,.0f} грн".replace(",", " ")
                fops_lines.append(f"{f['fop_name']}: {income_str}")
        data["fops_text"] = "\n".join(fops_lines)

        # ── FOP switch count: unique dates with binding events in current year ──
        year_start = date(year, 1, 1)
        switch_dates = set()
        for b in bindings:
            d = _parse_binding_date(b["date"])
            if d and d >= year_start:
                switch_dates.add(d)
        data["fop_count"] = len(switch_dates)

        # ── Employees ──
        _enrich_store_with_employees(data, store_employees)

        stores_report.append(data)

    logger.info("Звіт по магазинах: %d магазинів", len(stores_report))

    # ── Reverse lookup: FOP EDRPOU → currently connected stores ──
    fop_terminal_stores: dict[str, list[str]] = defaultdict(list)
    for store in stores_report:
        edrpou = store.get("current_fop_edrpou", "")
        if edrpou:
            fop_terminal_stores[edrpou].append(store["subdivision"])

    for fop_entry in all_fops_report:
        edrpou = fop_entry.get("fop_edrpou", "")
        terminal_stores = fop_terminal_stores.get(edrpou, [])
        fop_entry["current_terminal_stores"] = "\n".join(terminal_stores)

    period = f"{date(year, 1, 1).strftime('%d.%m.%Y')} - {today.strftime('%d.%m.%Y')}"

    report_json = {
        "report_date": today.isoformat(),
        "period": period,
        "total_fops": len(fops),
        "total_analyzed": len(analyses),
        "critical_count": critical_all,
        "critical_new": len(critical_fops),
        "critical_in_progress": critical_in_progress,
        "limits": {str(k): v for k, v in LIMITS.items()},
        "fops": all_fops_report,
        "stores_report": stores_report,
    }

    # Save report to file for dashboard endpoint
    _save_report_json(report_json)

    return {
        "report_date": today.isoformat(),
        "total_fops": len(fops),
        "total_analyzed": len(analyses),
        "critical_count": 0,  # TODO: повернути len(critical_fops) коли user tasks будуть готові
        "critical_fops": [],  # TODO: повернути critical_fops коли user tasks будуть готові
        "report_json": report_json,
    }


# ── Handler registration ──────────────────────────────────────────────


def register_fop_monitor_handlers(
    worker: ZeebeWorker,
    config: AppConfig,
) -> None:
    """Register FOP limit monitoring handler."""

    @worker.task(task_type="fop-limit-check", timeout_ms=300_000)
    async def fop_limit_check(
        days_ahead: int = 14,
        **kwargs: Any,
    ) -> dict:
        """Перевірка лімітів ФОП — підключення до БД BAS, аналіз, JSON звіт.

        Input variables:
            days_ahead (int): горизонт попередження у днях (default: 14)

        Output variables:
            report_date (str): дата звіту (ISO)
            total_fops (int): загальна кількість активних ФОПів
            total_analyzed (int): кількість проаналізованих
            critical_count (int): кількість нових критичних (без активного процесу)
            critical_fops (list): нові критичні ФОП для multi-instance (задачі на зміну терміналу)
            report_json (dict): повний JSON-звіт по всіх ФОП для дашборду Odoo (також зберігається у файл)

        Side effects:
            Зберігає report_json у reports/fop/latest.json (GET /reports/fop/latest)
        """
        logger.info("fop-limit-check (days_ahead=%d)", days_ahead)

        result = await asyncio.to_thread(_run_fop_check, days_ahead)

        logger.info(
            "fop-limit-check done — %d/%d analyzed, %d critical",
            result["total_analyzed"],
            result["total_fops"],
            result["critical_count"],
        )

        return result
