"""FOP limit monitoring — Zeebe worker handler.

Підключається до БД BAS Бухгалтерія, аналізує надходження на рахунки ФОП
та прогнозує дату досягнення ліміту для 2-ї та 3-ї груп ЄП.

При наближенні до ліміту (≤ days_ahead днів) запускає процес
"Сповіщення про зміну ФОП" (Process_0iy2u1a) у Camunda.

Task type: fop-limit-check
"""

from __future__ import annotations

import asyncio
import fcntl
import json
import logging
import os
import re
import time
from collections import defaultdict
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Any

import pymssql
import requests
from pyzeebe import Job, ZeebeWorker

from ..config import AppConfig

logger = logging.getLogger(__name__)

# === Constants ===

BAS_YEAR_OFFSET = 2000

EP_GROUP_ENUM = {
    bytes.fromhex("80907066F89BCA3447EBEB86FEF433E2"): 1,
    bytes.fromhex("A80C9C2A3B0E352146FAFF2E22E417BC"): 2,
    bytes.fromhex("BD853EFB6C04CB6D42A4D31D78D446DA"): 3,
}

LIMITS = {
    2: float(os.environ.get("FOP_LIMIT_GROUP_2", "6900000")),
    3: float(os.environ.get("FOP_LIMIT_GROUP_3", "9900000")),
}

STATE_FILE = Path(__file__).resolve().parent.parent.parent / ".fop_monitor_state.json"
LOCK_FILE = Path(__file__).resolve().parent.parent.parent / ".fop_monitor.lock"

# Camunda REST API (OAuth2 / Keycloak)
CAMUNDA_REST_URL = os.environ.get("CAMUNDA_REST_URL", "http://camunda-demo.a.local:8088")
CAMUNDA_TOKEN_URL = os.environ.get(
    "CAMUNDA_TOKEN_URL",
    "http://camunda-demo.a.local:18080/auth/realms/camunda-platform/protocol/openid-connect/token",
)
CAMUNDA_CLIENT_ID = os.environ.get("CAMUNDA_CLIENT_ID", "orchestration")
CAMUNDA_CLIENT_SECRET = os.environ.get("CAMUNDA_CLIENT_SECRET", "")
CAMUNDA_PROCESS_ID = "Process_0iy2u1a"

_token_cache: dict = {"access_token": None, "expires_at": 0.0}


# ── DB connection ──────────────────────────────────────────────────────


def _get_db_config() -> dict:
    return {
        "server": os.environ.get("BAS_DB_HOST", "deneb"),
        "port": int(os.environ.get("BAS_DB_PORT", "1433")),
        "user": os.environ.get("BAS_DB_USER", "AI_buh"),
        "password": os.environ["BAS_DB_PASSWORD"],
        "database": os.environ.get("BAS_DB_NAME", "bas_bdu"),
        "login_timeout": 30,
        "timeout": 300,
        "charset": "UTF-8",
    }


def _get_connection(max_retries: int = 3, initial_delay: int = 5):
    db_config = _get_db_config()
    for attempt in range(max_retries):
        try:
            return pymssql.connect(**db_config)
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            delay = initial_delay * (2 ** attempt)
            logger.warning(
                "БД недоступна (спроба %d/%d), повтор через %dс: %s",
                attempt + 1, max_retries, delay, e,
            )
            time.sleep(delay)


# ── Camunda REST API ───────────────────────────────────────────────────


def _get_access_token() -> str:
    now = time.time()
    if _token_cache["access_token"] and now < _token_cache["expires_at"] - 30:
        return _token_cache["access_token"]

    resp = requests.post(
        CAMUNDA_TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": CAMUNDA_CLIENT_ID,
            "client_secret": CAMUNDA_CLIENT_SECRET,
        },
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    _token_cache["access_token"] = data["access_token"]
    _token_cache["expires_at"] = now + data.get("expires_in", 300)
    return data["access_token"]


def _camunda_headers() -> dict:
    token = _get_access_token()
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _is_process_active(process_instance_key: int) -> bool:
    try:
        resp = requests.post(
            f"{CAMUNDA_REST_URL}/v2/process-instances/search",
            headers=_camunda_headers(),
            json={"filter": {"processInstanceKey": process_instance_key}},
            timeout=10,
        )
        if resp.status_code == 200:
            items = resp.json().get("items", [])
            return any(i.get("state") == "ACTIVE" for i in items)
    except requests.RequestException as e:
        logger.warning("Не вдалося перевірити інстанс %s: %s", process_instance_key, e)
    return False


def _start_camunda_process(fop_vars: dict, dry_run: bool = False) -> int | None:
    """Запускає процес 'Сповіщення про зміну ФОП' у Camunda."""
    if dry_run:
        logger.info("DRY RUN: запустив би процес для %s", fop_vars["fop_name"])
        return None

    try:
        resp = requests.post(
            f"{CAMUNDA_REST_URL}/v2/process-instances",
            headers=_camunda_headers(),
            json={
                "processDefinitionId": CAMUNDA_PROCESS_ID,
                "variables": fop_vars,
            },
            timeout=15,
        )
        if resp.status_code in (200, 201):
            key = resp.json().get("processInstanceKey")
            logger.info("Процес запущено: %s → instanceKey=%s", fop_vars["fop_name"], key)
            return key
        else:
            logger.error("Camunda відповів %s: %s", resp.status_code, resp.text[:300])
    except requests.RequestException as e:
        logger.error("Помилка з'єднання з Camunda: %s", e)

    return None


# ── State file (deduplication) ─────────────────────────────────────────


def _load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except json.JSONDecodeError as e:
            logger.warning("State file пошкоджено (%s), створюю бекап: %s", STATE_FILE, e)
            try:
                STATE_FILE.rename(STATE_FILE.with_suffix(".json.bak"))
            except OSError:
                pass
        except OSError as e:
            logger.warning("Не вдалося прочитати state file: %s", e)
    return {}


def _save_state(state: dict) -> None:
    tmp = STATE_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2))
    tmp.rename(STATE_FILE)


# ── Data fetch functions ───────────────────────────────────────────────


def _fetch_active_fops(conn, year: int) -> list:
    sql = """
        SELECT DISTINCT
            o._IDRRef AS id,
            o._Description AS name,
            RTRIM(o._Fld1495) AS full_name,
            o._Fld1494 AS edrpou
        FROM _Document236 d
        JOIN _Reference90 o ON d._Fld6004RRef = o._IDRRef
        WHERE d._Posted = 0x01
            AND d._Marked = 0x00
            AND d._Date_Time >= %s AND d._Date_Time < %s
            AND o._Marked = 0x00
            AND (o._Fld1495 LIKE N'%%ізична особа%%' OR o._Fld1495 LIKE N'%%ФОП%%')
            AND o._Description NOT LIKE N'яяя%%'
        ORDER BY o._Description
    """
    bas_start = f"{year + BAS_YEAR_OFFSET}-01-01"
    bas_end = f"{year + BAS_YEAR_OFFSET + 1}-01-01"
    cursor = conn.cursor(as_dict=True)
    try:
        cursor.execute(sql, (bas_start, bas_end))
        return cursor.fetchall()
    finally:
        cursor.close()


def _fetch_fop_groups(conn) -> dict:
    sql = """
        ;WITH latest AS (
            SELECT _Fld27928RRef AS org_id,
                   _Fld27930RRef AS group_ref,
                   ROW_NUMBER() OVER (PARTITION BY _Fld27928RRef
                                      ORDER BY _Period DESC) AS rn
            FROM _InfoRg27927
        )
        SELECT org_id, group_ref FROM latest WHERE rn = 1
    """
    cursor = conn.cursor(as_dict=True)
    try:
        cursor.execute(sql)
        result = {}
        for row in cursor:
            if row["org_id"] is None or row["group_ref"] is None:
                continue
            org_id = bytes(row["org_id"])
            group_ref = bytes(row["group_ref"])
            result[org_id] = EP_GROUP_ENUM.get(group_ref, 2)
        return result
    finally:
        cursor.close()


def _fetch_daily_income(conn, year: int) -> dict:
    sql = """
        SELECT
            d._Fld6004RRef AS org_id,
            CAST(DATEADD(year, -2000, d._Date_Time) AS date) AS doc_date,
            SUM(d._Fld6010) AS daily_total,
            COUNT(*) AS doc_count
        FROM _Document236 d
        JOIN _Reference90 o ON d._Fld6004RRef = o._IDRRef
        WHERE d._Posted = 0x01
            AND d._Marked = 0x00
            AND d._Date_Time >= %s AND d._Date_Time < %s
            AND o._Marked = 0x00
            AND (o._Fld1495 LIKE N'%%ізична особа%%' OR o._Fld1495 LIKE N'%%ФОП%%')
            AND o._Description NOT LIKE N'яяя%%'
        GROUP BY d._Fld6004RRef, CAST(DATEADD(year, -2000, d._Date_Time) AS date)
        ORDER BY d._Fld6004RRef, doc_date
    """
    bas_start = f"{year + BAS_YEAR_OFFSET}-01-01"
    bas_end = f"{year + BAS_YEAR_OFFSET + 1}-01-01"
    cursor = conn.cursor(as_dict=True)
    try:
        cursor.execute(sql, (bas_start, bas_end))
        result = defaultdict(list)
        for row in cursor:
            org_id = bytes(row["org_id"])
            result[org_id].append({
                "date": row["doc_date"],
                "amount": float(row["daily_total"] or 0),
                "count": row["doc_count"],
            })
        return result
    finally:
        cursor.close()


def _parse_terminal_name(purpose: str) -> str | None:
    m = re.search(r'cmps:.*?,\s*([A-Za-zА-Яа-яІіЇїЄєҐґ\s]+?)\s*Кiльк\s+тр', purpose)
    if m:
        name = m.group(1).strip()
        name = re.sub(r'^[\d\s,]+', '', name).strip()
        name = re.sub(r'^R\s+', '', name).strip()
        if len(name) >= 2:
            return name
    return None


def _fetch_fop_stores(conn, year: int) -> dict:
    bas_start = f"{year + BAS_YEAR_OFFSET}-01-01"
    bas_end = f"{year + BAS_YEAR_OFFSET + 1}-01-01"

    sql_terminals = """
        SELECT
            d._Fld6004RRef AS org_id,
            d._Fld6019 AS purpose,
            d._Fld6010 AS amount
        FROM _Document236 d
        JOIN _Reference90 o ON d._Fld6004RRef = o._IDRRef
        WHERE d._Posted = 0x01 AND d._Marked = 0x00
            AND d._Date_Time >= %s AND d._Date_Time < %s
            AND o._Marked = 0x00
            AND (o._Fld1495 LIKE N'%%ізична особа%%' OR o._Fld1495 LIKE N'%%ФОП%%')
            AND o._Description NOT LIKE N'яяя%%'
            AND d._Fld6019 LIKE N'cmps:%%'
    """
    cursor = conn.cursor(as_dict=True)
    try:
        cursor.execute(sql_terminals, (bas_start, bas_end))

        terminal_data = defaultdict(lambda: defaultdict(lambda: {"count": 0, "total": 0.0}))
        for row in cursor:
            org_id = bytes(row["org_id"])
            name = _parse_terminal_name(row["purpose"] or "")
            if name:
                terminal_data[org_id][name]["count"] += 1
                terminal_data[org_id][name]["total"] += float(row["amount"] or 0)

        sql_docs = """
            ;WITH store_roots AS (
                SELECT _IDRRef FROM _Reference116
                WHERE _Description IN (N'500 Магазини', N'600 Магазини',
                                       N'900 Пінкі', N'900 Пінкі  Сайт')
            ),
            store_tree AS (
                SELECT _IDRRef FROM _Reference116
                WHERE _IDRRef IN (SELECT _IDRRef FROM store_roots)
                UNION ALL
                SELECT c._IDRRef FROM _Reference116 c
                JOIN store_tree t ON c._ParentIDRRef = t._IDRRef
            ),
            fop_filter AS (
                SELECT _IDRRef FROM _Reference90
                WHERE _Marked = 0x00
                    AND (_Fld1495 LIKE N'%%ізична особа%%' OR _Fld1495 LIKE N'%%ФОП%%')
                    AND _Description NOT LIKE N'яяя%%'
            ),
            all_stores AS (
                SELECT d._Fld6686RRef AS org_id, d._Fld6687RRef AS store_id,
                       COUNT(*) AS doc_count, SUM(d._Fld6704) AS total_sum
                FROM _Document247 d
                WHERE d._Posted = 0x01 AND d._Marked = 0x00
                    AND d._Date_Time >= %s AND d._Date_Time < %s
                    AND d._Fld6686RRef IN (SELECT _IDRRef FROM fop_filter)
                    AND d._Fld6687RRef IN (SELECT _IDRRef FROM store_tree)
                GROUP BY d._Fld6686RRef, d._Fld6687RRef

                UNION ALL

                SELECT d._Fld6103RRef, d._Fld6104RRef,
                       COUNT(*), SUM(d._Fld6119)
                FROM _Document238 d
                WHERE d._Posted = 0x01 AND d._Marked = 0x00
                    AND d._Date_Time >= %s AND d._Date_Time < %s
                    AND d._Fld6103RRef IN (SELECT _IDRRef FROM fop_filter)
                    AND d._Fld6104RRef IN (SELECT _IDRRef FROM store_tree)
                GROUP BY d._Fld6103RRef, d._Fld6104RRef

                UNION ALL

                SELECT d._Fld5008RRef, d._Fld5011RRef,
                       COUNT(*), SUM(d._Fld5016)
                FROM _Document213 d
                WHERE d._Posted = 0x01 AND d._Marked = 0x00
                    AND d._Date_Time >= %s AND d._Date_Time < %s
                    AND d._Fld5008RRef IN (SELECT _IDRRef FROM fop_filter)
                    AND d._Fld5011RRef IN (SELECT _IDRRef FROM store_tree)
                GROUP BY d._Fld5008RRef, d._Fld5011RRef
            )
            SELECT a.org_id, s._Description AS store_name,
                   SUM(a.doc_count) AS doc_count, SUM(a.total_sum) AS total_sum
            FROM all_stores a
            JOIN _Reference116 s ON a.store_id = s._IDRRef
            GROUP BY a.org_id, s._Description
            ORDER BY a.org_id, SUM(a.total_sum) DESC
        """
        cursor.execute(sql_docs, (bas_start, bas_end, bas_start, bas_end, bas_start, bas_end))

        doc_data = defaultdict(list)
        for row in cursor:
            org_id = bytes(row["org_id"])
            doc_data[org_id].append({
                "name": row["store_name"].strip(),
                "doc_count": row["doc_count"],
                "total": float(row["total_sum"] or 0),
            })

        result = defaultdict(list)
        all_org_ids = set(terminal_data.keys()) | set(doc_data.keys())

        for org_id in all_org_ids:
            if org_id in terminal_data:
                for name, info in sorted(
                    terminal_data[org_id].items(), key=lambda x: -x[1]["total"]
                ):
                    result[org_id].append({
                        "name": name,
                        "doc_count": info["count"],
                        "total": info["total"],
                        "source": "terminal",
                    })
            if org_id not in terminal_data and org_id in doc_data:
                for item in doc_data[org_id]:
                    item["source"] = "document"
                    result[org_id].append(item)

        return result
    finally:
        cursor.close()


# ── Analysis ───────────────────────────────────────────────────────────


def _safe_pct(income: float, limit: float) -> float:
    return round((income / limit) * 100, 1) if limit > 0 else 0.0


def _analyze_fop(daily_data: list, today: date, year: int) -> dict | None:
    if not daily_data:
        return None

    year_start = date(year, 1, 1)
    year_end = date(year, 12, 31)

    income_by_date = {}
    for entry in daily_data:
        d = entry["date"]
        if isinstance(d, datetime):
            d = d.date()
        income_by_date[d] = entry["amount"]

    total_income = sum(income_by_date.values())
    days_elapsed = (today - year_start).days + 1
    days_remaining = (year_end - today).days

    if days_elapsed <= 0:
        return None

    # Weekday averages
    weekday_totals = defaultdict(float)
    weekday_counts = defaultdict(int)
    for d, amount in income_by_date.items():
        wd = d.weekday()
        weekday_totals[wd] += amount
        weekday_counts[wd] += 1

    weekday_avg = {}
    for wd in range(7):
        if weekday_counts[wd] > 0:
            weekday_avg[wd] = weekday_totals[wd] / weekday_counts[wd]
        else:
            weekday_avg[wd] = 0.0

    # Trend (6-week window)
    six_weeks_ago = today - timedelta(days=42)
    three_weeks_ago = today - timedelta(days=21)

    income_weeks_old = sum(
        amt for d, amt in income_by_date.items()
        if six_weeks_ago <= d < three_weeks_ago
    )
    income_weeks_recent = sum(
        amt for d, amt in income_by_date.items()
        if three_weeks_ago <= d <= today
    )

    if income_weeks_old > 0:
        trend_ratio = income_weeks_recent / income_weeks_old
    elif income_weeks_recent > 0:
        trend_ratio = 1.0
    else:
        trend_ratio = 1.0

    trend_ratio = max(0.5, min(2.0, trend_ratio))

    # Anomalies
    if income_by_date:
        amounts = list(income_by_date.values())
        mean_daily = sum(amounts) / len(amounts)
        if len(amounts) > 1:
            variance = sum((x - mean_daily) ** 2 for x in amounts) / len(amounts)
            std_daily = variance ** 0.5
        else:
            std_daily = 0
    else:
        mean_daily = 0
        std_daily = 0

    # Projection
    projected_remaining = 0.0
    for day_offset in range(1, days_remaining + 1):
        future_date = today + timedelta(days=day_offset)
        wd = future_date.weekday()
        projected_remaining += weekday_avg.get(wd, mean_daily) * trend_ratio

    projected_total = total_income + projected_remaining

    # Limit dates
    limit_dates = {}
    for group, limit in LIMITS.items():
        if total_income >= limit:
            limit_dates[group] = {"date": "ПЕРЕВИЩЕНО", "already_exceeded": True}
            continue

        remaining_to_limit = limit - total_income
        cumulative = 0.0
        hit_date = None
        for day_offset in range(1, days_remaining + 1):
            future_date = today + timedelta(days=day_offset)
            wd = future_date.weekday()
            cumulative += weekday_avg.get(wd, mean_daily) * trend_ratio
            if cumulative >= remaining_to_limit:
                hit_date = future_date
                break

        limit_dates[group] = {
            "date": hit_date,
            "already_exceeded": False,
            "remaining": remaining_to_limit,
        }

    return {
        "total_income": total_income,
        "days_elapsed": days_elapsed,
        "days_remaining": days_remaining,
        "trend_ratio": trend_ratio,
        "mean_daily": mean_daily,
        "projected_total": projected_total,
        "limit_dates": limit_dates,
        "active_days": len(income_by_date),
    }


# ── Main sync check ───────────────────────────────────────────────────


def _run_fop_check(days_ahead: int = 14, dry_run: bool = False) -> dict:
    """Synchronous: full FOP limit check (DB → analysis → start processes).

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

        fop_stores = _fetch_fop_stores(conn, year)
        logger.info("Магазини: зв'язки для %d ФОПів", len(fop_stores))

        fop_groups = _fetch_fop_groups(conn)
    finally:
        conn.close()

    # Analyze all FOPs
    analyses = {}
    for fop in fops:
        fop_id = bytes(fop["id"])
        data = daily_income.get(fop_id, [])
        result = _analyze_fop(data, today, year)
        if result:
            analyses[fop_id] = result

    logger.info("Проаналізовано %d ФОПів з %d", len(analyses), len(fops))

    # Find critical FOPs
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
            if days_to_limit > days_ahead:
                continue
        else:
            continue

        stores = fop_stores.get(fop_id, [])
        store_names = ", ".join(s["name"] for s in stores[:5]) if stores else ""

        critical_fops.append({
            "fop_name": fop["name"].strip(),
            "fop_edrpou": (fop.get("edrpou") or "").strip(),
            "ep_group": group,
            "total_income": round(analysis["total_income"], 2),
            "limit_amount": limit,
            "income_percent": _safe_pct(analysis["total_income"], limit),
            "days_to_limit": days_to_limit,
            "projected_date": projected_date,
            "stores": store_names,
            "stores_count": len(stores),
            "trend_ratio": round(analysis["trend_ratio"], 2),
        })

    logger.info("Критичних ФОПів (≤%d днів до ліміту): %d", days_ahead, len(critical_fops))

    # Start Camunda processes for critical FOPs (with deduplication)
    started_processes = []
    if critical_fops:
        lock_fd = open(LOCK_FILE, "w")
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)

            state = _load_state()
            state_changed = False

            # Cleanup stale entries
            stale_keys = [k for k, v in state.items() if not _is_process_active(v)]
            for k in stale_keys:
                logger.info("Очищено завершений процес зі стану: %s", k)
                del state[k]
                state_changed = True

            for fop_vars in critical_fops:
                state_key = fop_vars["fop_edrpou"] or fop_vars["fop_name"]

                # Dedup: skip if process already active
                existing_key = state.get(state_key)
                if existing_key and _is_process_active(existing_key):
                    logger.info(
                        "Процес вже активний для %s (key=%s), пропускаємо",
                        fop_vars["fop_name"], existing_key,
                    )
                    continue

                instance_key = _start_camunda_process(fop_vars, dry_run=dry_run)
                if instance_key:
                    state[state_key] = instance_key
                    state_changed = True
                    started_processes.append({
                        "fop_name": fop_vars["fop_name"],
                        "fop_edrpou": fop_vars["fop_edrpou"],
                        "process_instance_key": instance_key,
                    })

            if state_changed:
                _save_state(state)

        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            lock_fd.close()

    return {
        "report_date": today.isoformat(),
        "total_fops": len(fops),
        "total_analyzed": len(analyses),
        "critical_count": len(critical_fops),
        "started_count": len(started_processes),
        "critical_fops": json.dumps(critical_fops, ensure_ascii=False),
        "started_processes": json.dumps(started_processes, ensure_ascii=False),
    }


# ── Handler registration ──────────────────────────────────────────────


def register_fop_monitor_handlers(
    worker: ZeebeWorker,
    config: AppConfig,
) -> None:
    """Register FOP limit monitoring handler."""

    @worker.task(task_type="fop-limit-check", timeout_ms=300_000)
    async def fop_limit_check(
        job: Job,
        days_ahead: int = 14,
        dry_run: bool = False,
        **kwargs: Any,
    ) -> dict:
        """Перевірка лімітів ФОП — підключення до БД BAS, аналіз, запуск процесів.

        Input variables:
            days_ahead (int): горизонт попередження у днях (default: 14)
            dry_run (bool): тільки аналіз, без запуску процесів (default: false)

        Output variables:
            report_date (str): дата звіту (ISO)
            total_fops (int): загальна кількість активних ФОПів
            total_analyzed (int): кількість проаналізованих
            critical_count (int): кількість критичних
            started_count (int): кількість запущених процесів
            critical_fops (str): JSON список критичних ФОПів
            started_processes (str): JSON список запущених процесів
        """
        logger.info(
            "Job %s: fop-limit-check (days_ahead=%d, dry_run=%s)",
            job.key, days_ahead, dry_run,
        )

        result = await asyncio.to_thread(_run_fop_check, days_ahead, dry_run)

        logger.info(
            "Job %s: done — %d/%d analyzed, %d critical, %d started",
            job.key,
            result["total_analyzed"],
            result["total_fops"],
            result["critical_count"],
            result["started_count"],
        )

        return result
