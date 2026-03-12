#!/usr/bin/env python3
"""
Моніторинг ліміту доходу ФОП — прогнозування досягнення граничних обсягів.

Підключається до БД BAS Бухгалтерія, аналізує надходження на рахунки ФОП
та прогнозує дату досягнення ліміту для 2-ї та 3-ї груп ЄП.

Згідно ст. 291.4 ПКУ:
  2 група — 5 921 400 грн/рік (2026)
  3 група — 8 285 700 грн/рік (2026)

Дохід ФОП = фактичні надходження на банківський рахунок (ст. 292.1 ПКУ).

Запуск: python3 fop_limit_monitor.py [--group 2|3] [--days-ahead 14] [--top N]

Cron (щоденний запуск о 08:00):
  0 8 * * * cd /opt/camunda/docker-compose-8.8 && python3 fop_limit_monitor.py >> /var/log/fop_monitor.log 2>&1

Env-змінні (опційні, значення за замовчуванням):
  BAS_DB_HOST=deneb  BAS_DB_PORT=1433  BAS_DB_USER=AI_buh  BAS_DB_NAME=bas_bdu
  BAS_DB_PASSWORD (обов'язково)
  FOP_LIMIT_GROUP_2=6900000  FOP_LIMIT_GROUP_3=9900000
  CAMUNDA_REST_URL=http://localhost:8088  CAMUNDA_USER=demo  CAMUNDA_PASSWORD=demo
"""

import argparse
import os
import re
import sys
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path

import fcntl
import json
import logging
import time

import pymssql
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# === Завантаження .env ===
_env_path = Path(__file__).resolve().parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())

# === Підключення до БД ===
DB_CONFIG = {
    "server": os.environ.get("BAS_DB_HOST", "deneb"),
    "port": int(os.environ.get("BAS_DB_PORT", "1433")),
    "user": os.environ.get("BAS_DB_USER", "AI_buh"),
    "password": os.environ["BAS_DB_PASSWORD"],
    "database": os.environ.get("BAS_DB_NAME", "bas_bdu"),
    "login_timeout": 30,
    "timeout": 300,
    "charset": "UTF-8",
}

# === Ліміти ЄП (ст. 291.4 ПКУ) ===
# Офіційні ліміти 2026: 2гр = 7 211 598 грн, 3гр = 10 091 049 грн
# Моніторинг ведеться по безпечній межі (нижче офіційного)
LIMITS = {
    2: float(os.environ.get("FOP_LIMIT_GROUP_2", "6900000")),
    3: float(os.environ.get("FOP_LIMIT_GROUP_3", "9900000")),
}

# === Перерахування ГруппыПлательщиковЕдиногоНалога (Enum372) ===
# Порядок: 0=1гр, 1=2гр, 2=3гр, 3=4гр ...
EP_GROUP_ENUM = {
    bytes.fromhex("80907066F89BCA3447EBEB86FEF433E2"): 1,
    bytes.fromhex("A80C9C2A3B0E352146FAFF2E22E417BC"): 2,
    bytes.fromhex("BD853EFB6C04CB6D42A4D31D78D446DA"): 3,
}

# === Camunda REST API ===
CAMUNDA_REST_URL = os.environ.get("CAMUNDA_REST_URL", "http://localhost:8088")
CAMUNDA_USER = os.environ.get("CAMUNDA_USER", "demo")
CAMUNDA_PASSWORD = os.environ.get("CAMUNDA_PASSWORD", "demo")
CAMUNDA_PROCESS_ID = "Process_0iy2u1a"  # Сповіщення про зміну ФОП

# Файл стану для дедуплікації (зберігає ЄДРПОУ ФОПів з активними процесами)
STATE_FILE = Path(__file__).resolve().parent / ".fop_monitor_state.json"
LOCK_FILE = Path(__file__).resolve().parent / ".fop_monitor.lock"

log = logging.getLogger("fop_limit_monitor")

# === SQL: offset дати в BAS = +2000 років ===
BAS_YEAR_OFFSET = 2000


def get_connection(max_retries: int = 3, initial_delay: int = 5):
    """Підключення до БД з retry та exponential backoff."""
    for attempt in range(max_retries):
        try:
            return pymssql.connect(**DB_CONFIG)
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            delay = initial_delay * (2 ** attempt)
            log.warning("БД недоступна (спроба %d/%d), повтор через %dс: %s",
                        attempt + 1, max_retries, delay, e)
            time.sleep(delay)


# === Camunda REST API ===

def _camunda_auth():
    return (CAMUNDA_USER, CAMUNDA_PASSWORD)


def load_state() -> dict:
    """Завантажує стан попередніх запусків (ЄДРПОУ → process_instance_key)."""
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except json.JSONDecodeError as e:
            log.warning("State file пошкоджено (%s), створюю бекап: %s", STATE_FILE, e)
            backup = STATE_FILE.with_suffix(".json.bak")
            try:
                STATE_FILE.rename(backup)
            except OSError:
                pass
        except OSError as e:
            log.warning("Не вдалося прочитати state file: %s", e)
    return {}


def save_state(state: dict):
    tmp = STATE_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2))
    tmp.rename(STATE_FILE)


def is_process_active(process_instance_key: int) -> bool:
    """Перевіряє чи інстанс ще активний у Camunda."""
    try:
        resp = requests.post(
            f"{CAMUNDA_REST_URL}/v2/process-instances/search",
            auth=_camunda_auth(),
            json={
                "filter": {"processInstanceKey": process_instance_key},
            },
            timeout=10,
        )
        if resp.status_code == 200:
            items = resp.json().get("items", [])
            return any(i.get("state") == "ACTIVE" for i in items)
    except requests.RequestException as e:
        log.warning("Не вдалося перевірити інстанс %s: %s", process_instance_key, e)
    return False


def start_camunda_process(fop: dict, analysis: dict, group: int,
                          stores: list, dry_run: bool = False) -> int | None:
    """Запускає процес 'Сповіщення про зміну ФОП' у Camunda.

    Повертає processInstanceKey або None при помилці.
    """
    fop_name = fop["name"].strip()
    edrpou = fop["edrpou"].strip() if fop.get("edrpou") else ""
    limit = LIMITS[group]
    info = analysis["limit_dates"][group]

    days_to_limit = None
    projected_date = None
    if info["already_exceeded"]:
        days_to_limit = 0
        projected_date = "ПЕРЕВИЩЕНО"
    elif info["date"] is not None:
        today = datetime.now().date()
        days_to_limit = (info["date"] - today).days
        projected_date = info["date"].strftime("%Y-%m-%d")

    store_names = ", ".join(s["name"] for s in stores[:5]) if stores else ""

    variables = {
        "fop_name": fop_name,
        "fop_edrpou": edrpou,
        "ep_group": group,
        "total_income": round(analysis["total_income"], 2),
        "limit_amount": limit,
        "income_percent": safe_pct(analysis["total_income"], limit),
        "days_to_limit": days_to_limit,
        "projected_date": projected_date,
        "stores": store_names,
        "trend_ratio": round(analysis["trend_ratio"], 2),
    }

    if dry_run:
        print(f"  🧪 DRY RUN: запустив би процес для {fop_name} ({edrpou})")
        print(f"     Змінні: {json.dumps(variables, ensure_ascii=False)}")
        return None

    try:
        resp = requests.post(
            f"{CAMUNDA_REST_URL}/v2/process-instances",
            auth=_camunda_auth(),
            json={
                "processDefinitionId": CAMUNDA_PROCESS_ID,
                "variables": variables,
            },
            timeout=15,
        )
        if resp.status_code in (200, 201):
            key = resp.json().get("processInstanceKey")
            print(f"  ✅ Процес запущено: {fop_name} → instanceKey={key}")
            return key
        else:
            log.error("Camunda відповів %s: %s", resp.status_code, resp.text[:300])
            print(f"  ❌ Помилка запуску для {fop_name}: HTTP {resp.status_code}")
    except requests.RequestException as e:
        log.error("Помилка з'єднання з Camunda: %s", e)
        print(f"  ❌ Немає з'єднання з Camunda: {e}")

    return None


def fetch_active_fops(conn, year: int):
    """Повертає список активних ФОПів з їх ID та назвою."""
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


def fetch_fop_groups(conn):
    """Повертає маппінг org_id → група ЄП (2 або 3) з останнього запису регістру."""
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
            group_num = EP_GROUP_ENUM.get(group_ref, 2)  # за замовчуванням 2 група
            result[org_id] = group_num
        return result
    finally:
        cursor.close()


def _parse_terminal_name(purpose: str) -> str | None:
    """Витягує назву терміналу з призначення платежу.

    Формати:
      cmps: 19 ,19 ,FORUM PINKY Кiльк тр 53шт.
      cmps: 25 ,Ostrov Pinky Кiльк тр 54шт.
      cmps: 36 ,Pinky Nikolskyi Кiльк тр 94шт.
    """
    m = re.search(r'cmps:.*?,\s*([A-Za-zА-Яа-яІіЇїЄєҐґ\s]+?)\s*Кiльк\s+тр', purpose)
    if m:
        name = m.group(1).strip()
        # Прибрати залишкові числа та коми на початку (але не літери слів)
        name = re.sub(r'^[\d\s,]+', '', name).strip()
        # Прибрати одиничну "R " на початку (код повернення), але не частину слова
        name = re.sub(r'^R\s+', '', name).strip()
        if len(name) >= 2:
            return name
    return None


def fetch_fop_stores(conn, year: int):
    """Повертає маппінг org_id → список магазинів.

    Джерела:
    1. Призначення платежів банківських документів (назви терміналів)
    2. Документи реалізації / поступлення / роздрібних продажів (складський облік)
    """
    bas_start = f"{year + BAS_YEAR_OFFSET}-01-01"
    bas_end = f"{year + BAS_YEAR_OFFSET + 1}-01-01"

    # --- 1. Термінали з призначень платежів ---
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

        # Агрегація: org_id → terminal_name → {count, total}
        terminal_data = defaultdict(lambda: defaultdict(lambda: {"count": 0, "total": 0.0}))
        for row in cursor:
            org_id = bytes(row["org_id"])
            name = _parse_terminal_name(row["purpose"] or "")
            if name:
                terminal_data[org_id][name]["count"] += 1
                terminal_data[org_id][name]["total"] += float(row["amount"] or 0)

        # --- 2. Складський облік (документи) ---
        # Фільтруємо тільки реальні магазини (500/600/900 ієрархія)
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

        # --- Об'єднання: термінали мають пріоритет, документи — доповнення ---
        result = defaultdict(list)
        all_org_ids = set(terminal_data.keys()) | set(doc_data.keys())

        for org_id in all_org_ids:
            # Термінали (з призначень платежів)
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

            # Документи (склади) — додаємо тільки якщо немає терміналів
            if org_id not in terminal_data and org_id in doc_data:
                for item in doc_data[org_id]:
                    item["source"] = "document"
                    result[org_id].append(item)

        return result
    finally:
        cursor.close()


def fetch_daily_income(conn, year: int):
    """Повертає щоденні суми надходжень по кожному ФОП за рік."""
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


def analyze_fop(daily_data: list, today: datetime.date, year: int) -> dict:
    """
    Аналізує дані ФОП та прогнозує досягнення ліміту.

    Модель прогнозування:
    1. Зважене середнє з урахуванням дня тижня (пн-нд мають різну активність)
    2. Виявлення тренду (зростання/падіння) за останні 4 тижні
    3. Детектування аномальних днів (розпродажі, акції)
    4. Прогноз = базова ставка × тренд-коефіцієнт × кількість днів до кінця року
    """
    if not daily_data:
        return None

    year_start = datetime(year, 1, 1).date()
    year_end = datetime(year, 12, 31).date()

    # Побудова карти дохід-по-днях
    income_by_date = {}
    for entry in daily_data:
        d = entry["date"]
        if isinstance(d, datetime):
            d = d.date()
        income_by_date[d] = entry["amount"]

    # Загальний дохід за рік
    total_income = sum(income_by_date.values())

    # Дні від початку року до сьогодні
    days_elapsed = (today - year_start).days + 1
    days_remaining = (year_end - today).days

    if days_elapsed <= 0:
        return None

    # === Аналіз по днях тижня ===
    weekday_totals = defaultdict(float)
    weekday_counts = defaultdict(int)
    for d, amount in income_by_date.items():
        wd = d.weekday()  # 0=Пн, 6=Нд
        weekday_totals[wd] += amount
        weekday_counts[wd] += 1

    # Середній дохід по кожному дню тижня
    weekday_avg = {}
    for wd in range(7):
        if weekday_counts[wd] > 0:
            weekday_avg[wd] = weekday_totals[wd] / weekday_counts[wd]
        else:
            weekday_avg[wd] = 0.0

    # === Тренд за останні 6 тижнів (42 дні) ===
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

    # Коефіцієнт тренду
    if income_weeks_old > 0:
        trend_ratio = income_weeks_recent / income_weeks_old
    elif income_weeks_recent > 0:
        trend_ratio = 1.0  # Немає попередніх даних для порівняння — плоский прогноз
    else:
        trend_ratio = 1.0

    # Обмежуємо тренд від 0.5 до 2.0
    trend_ratio = max(0.5, min(2.0, trend_ratio))

    # === Детекція аномалій (дні з доходом > 3σ) ===
    if income_by_date:
        amounts = list(income_by_date.values())
        mean_daily = sum(amounts) / len(amounts)
        if len(amounts) > 1:
            variance = sum((x - mean_daily) ** 2 for x in amounts) / len(amounts)
            std_daily = variance ** 0.5
        else:
            std_daily = 0

        anomaly_threshold = mean_daily + 3 * std_daily if std_daily > 0 else mean_daily * 3
        anomaly_days = [
            (d, amt) for d, amt in income_by_date.items()
            if amt > anomaly_threshold and anomaly_threshold > 0
        ]
    else:
        mean_daily = 0
        std_daily = 0
        anomaly_days = []

    # === Прогноз до кінця року ===
    # Метод: прогнозуємо дохід на кожен день, що залишився, за середнім для цього дня тижня,
    # скоригованим на тренд
    projected_remaining = 0.0
    for day_offset in range(1, days_remaining + 1):
        future_date = today + timedelta(days=day_offset)
        wd = future_date.weekday()
        daily_forecast = weekday_avg.get(wd, mean_daily) * trend_ratio
        projected_remaining += daily_forecast

    projected_total = total_income + projected_remaining

    # === Прогнозна дата досягнення кожного ліміту ===
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
            daily_forecast = weekday_avg.get(wd, mean_daily) * trend_ratio
            cumulative += daily_forecast
            if cumulative >= remaining_to_limit:
                hit_date = future_date
                break

        limit_dates[group] = {
            "date": hit_date,
            "already_exceeded": False,
            "remaining": remaining_to_limit,
        }

    # === Останні 7 днів для контексту ===
    last_7_days = []
    for i in range(7, 0, -1):
        d = today - timedelta(days=i)
        amt = income_by_date.get(d, 0)
        last_7_days.append((d, amt))

    # === Кумулятивний дохід по днях (для залишку до ліміту) ===
    sorted_dates = sorted(income_by_date.keys())
    cumulative = 0.0
    cumulative_by_date = {}
    for d in sorted_dates:
        cumulative += income_by_date[d]
        cumulative_by_date[d] = cumulative
    # Для днів без доходу — взяти попередній кумулятив
    if sorted_dates:
        # Знайти останній кумулятив до початку 7-денного вікна
        check_start = today - timedelta(days=7)
        last_cum = 0.0
        for d in sorted_dates:
            if d <= check_start:
                last_cum = cumulative_by_date[d]
            else:
                break
        # Заповнити 7-денне вікно
        for i in range(8):
            d = check_start + timedelta(days=i)
            if d in cumulative_by_date:
                last_cum = cumulative_by_date[d]
            else:
                cumulative_by_date[d] = last_cum

    return {
        "total_income": total_income,
        "days_elapsed": days_elapsed,
        "days_remaining": days_remaining,
        "weekday_avg": weekday_avg,
        "trend_ratio": trend_ratio,
        "mean_daily": mean_daily,
        "std_daily": std_daily,
        "anomaly_days": anomaly_days,
        "projected_total": projected_total,
        "limit_dates": limit_dates,
        "last_7_days": last_7_days,
        "cumulative_by_date": cumulative_by_date,
        "active_days": len(income_by_date),
    }


def safe_pct(income: float, limit: float) -> float:
    """Безпечне обчислення відсотка доходу від ліміту."""
    return round((income / limit) * 100, 1) if limit > 0 else 0.0


def format_currency(amount: float) -> str:
    """Форматує суму у гривнях."""
    return f"{amount:,.2f}".replace(",", " ")


def format_date(d) -> str:
    if d is None:
        return "не досягне"
    if isinstance(d, str):
        return d
    return d.strftime("%d.%m.%Y")


WEEKDAY_NAMES = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Нд"]


def print_report(fops: list, analyses: dict, fop_stores: dict,
                 fop_groups: dict,
                 group_filter: int | None, days_ahead: int, top_n: int | None,
                 dry_run: bool = False):
    today = datetime.now().date()
    year = today.year

    print("=" * 90)
    print(f"  МОНІТОРИНГ ЛІМІТІВ ФОП — {today.strftime('%d.%m.%Y')}")
    print(f"  Рік: {year} | Активних ФОПів: {len(fops)}")
    print(f"  Ліміти: 2 група — {format_currency(LIMITS[2])} грн | "
          f"3 група — {format_currency(LIMITS[3])} грн")
    print("=" * 90)

    # Збір ФОПів, що потребують уваги
    alerts = []
    for fop in fops:
        fop_id = bytes(fop["id"])
        analysis = analyses.get(fop_id)
        if not analysis:
            continue

        alert_reasons = []
        alert_level = 0  # 0=ok, 1=увага, 2=попередження, 3=критично

        # Визначаємо групу ЄП цього ФОП
        group = fop_groups.get(fop_id, 2)
        if group_filter and group != group_filter:
            continue
        limit = LIMITS[group]
        info = analysis["limit_dates"][group]

        if info["already_exceeded"]:
            alert_reasons.append(
                f"⛔ ЛІМІТ {group} ГРУПИ ПЕРЕВИЩЕНО! "
                f"Дохід: {format_currency(analysis['total_income'])} грн"
            )
            alert_level = max(alert_level, 3)
        elif info["date"] is not None:
            days_to_limit = (info["date"] - today).days
            pct = safe_pct(analysis["total_income"], limit)

            if days_to_limit <= days_ahead:
                alert_reasons.append(
                    f"🔴 {group} група: досягне ліміту ~{format_date(info['date'])} "
                    f"(через {days_to_limit} дн.) | "
                    f"Зараз: {format_currency(analysis['total_income'])} грн ({pct:.1f}%)"
                )
                alert_level = max(alert_level, 3)
            elif days_to_limit <= days_ahead * 2:
                alert_reasons.append(
                    f"🟡 {group} група: досягне ліміту ~{format_date(info['date'])} "
                    f"(через {days_to_limit} дн.) | "
                    f"Зараз: {format_currency(analysis['total_income'])} грн ({pct:.1f}%)"
                )
                alert_level = max(alert_level, 2)
            elif pct >= 60:
                alert_reasons.append(
                    f"🟢 {group} група: {pct:.1f}% ліміту | "
                    f"Прогноз: ~{format_date(info['date'])}"
                )
                alert_level = max(alert_level, 1)

        else:
            # Не досягне ліміту до кінця року
            pct = safe_pct(analysis["total_income"], limit)
            if pct >= 50:
                alert_reasons.append(
                    f"ℹ️  {group} група: {pct:.1f}% ліміту, "
                    f"не досягне до кінця року за поточним темпом"
                )
                alert_level = max(alert_level, 1)

        # Тренд-попередження
        if analysis["trend_ratio"] > 1.4:
            alert_reasons.append(
                f"📈 Тренд: дохід за останні 3 тижні на "
                f"{(analysis['trend_ratio'] - 1) * 100:.0f}% вищий за попередні 3 тижні"
            )

        # Аномалії
        if analysis["anomaly_days"]:
            recent_anomalies = [
                (d, a) for d, a in analysis["anomaly_days"]
                if (today - d).days <= 14
            ]
            if recent_anomalies:
                for d, amt in recent_anomalies:
                    alert_reasons.append(
                        f"⚡ Аномальний день {format_date(d)}: "
                        f"{format_currency(amt)} грн (середня: "
                        f"{format_currency(analysis['mean_daily'])} грн)"
                    )

        if alert_reasons:
            alerts.append({
                "fop": fop,
                "analysis": analysis,
                "reasons": alert_reasons,
                "level": alert_level,
            })

    # Сортування: спочатку критичні
    alerts.sort(key=lambda x: (-x["level"], -x["analysis"]["total_income"]))

    if top_n:
        alerts = alerts[:top_n]

    if not alerts:
        print("\n✅ Жоден ФОП не наближається до ліміту. Все під контролем.\n")
        return

    # === Запуск процесу ЗМІНА ФОП у Camunda ===
    critical_fops = [a for a in alerts if a["level"] >= 3]
    if critical_fops:
        print()
        print("\033[41;97m" + "█" * 90 + "\033[0m")
        print("\033[41;97m" + "█" + " " * 88 + "█" + "\033[0m")
        print("\033[41;97m" + "█" + "  🚨  ЗАПУСК ПРОЦЕСУ: ЗМІНА ФОП".center(88) + "█" + "\033[0m")
        print("\033[41;97m" + "█" + " " * 88 + "█" + "\033[0m")

        # File lock для захисту від race condition при одночасних запусках
        lock_fd = open(LOCK_FILE, "w")
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)

            state = load_state()
            state_changed = False

            # Очистка: видаляємо записи завершених процесів
            stale_keys = [k for k, v in state.items() if not is_process_active(v)]
            for k in stale_keys:
                log.info("Очищено завершений процес зі стану: %s", k)
                del state[k]
                state_changed = True

            for cf in critical_fops:
                fop_name = cf["fop"]["name"].strip()
                fop_id = bytes(cf["fop"]["id"])
                edrpou = (cf["fop"].get("edrpou") or "").strip()
                cf_group = fop_groups.get(fop_id, 2)
                cf_limit = LIMITS[cf_group]
                income = format_currency(cf["analysis"]["total_income"])
                pct = safe_pct(cf["analysis"]["total_income"], cf_limit)
                stores = fop_stores.get(fop_id, [])
                store_names = ", ".join(s["name"] for s in stores[:3]) if stores else "—"

                line = f"  ➤  {fop_name} ({cf_group}гр) — {income} грн ({pct:.1f}% від межі)"
                print("\033[41;97m" + "█" + line.ljust(88) + "█" + "\033[0m")
                store_line = f"       🏪 {store_names}"
                print("\033[41;97m" + "█" + store_line.ljust(88) + "█" + "\033[0m")

                # Дедуплікація: перевіряємо чи вже є активний процес для цього ФОП
                state_key = edrpou or fop_name
                existing_key = state.get(state_key)
                if existing_key and is_process_active(existing_key):
                    skip_line = f"       ⏭️  Процес вже активний (key={existing_key}), пропускаємо"
                    print("\033[41;97m" + "█" + skip_line.ljust(88) + "█" + "\033[0m")
                    continue

                # Запуск нового процесу
                instance_key = start_camunda_process(
                    cf["fop"], cf["analysis"], cf_group, stores, dry_run=dry_run,
                )
                if instance_key:
                    state[state_key] = instance_key
                    state_changed = True

            if state_changed:
                save_state(state)

        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            lock_fd.close()

        print("\033[41;97m" + "█" + " " * 88 + "█" + "\033[0m")
        print("\033[41;97m" + "█" + "  Необхідно розпочати перехід на іншу юридичну особу!".ljust(88) + "█" + "\033[0m")
        print("\033[41;97m" + "█" + " " * 88 + "█" + "\033[0m")
        print("\033[41;97m" + "█" * 90 + "\033[0m")
        print()

    # === Вивід алертів ===
    print(f"\n⚠️  ФОПи, що потребують уваги: {len(alerts)}\n")

    for i, alert in enumerate(alerts, 1):
        fop = alert["fop"]
        a = alert["analysis"]
        level_icon = {0: "✅", 1: "🟢", 2: "🟡", 3: "🔴"}[alert["level"]]

        print(f"{'─' * 90}")
        print(f"  {level_icon} {i}. {fop['name'].strip()}")
        fop_id_alert = bytes(fop["id"])
        fop_group_alert = fop_groups.get(fop_id_alert, 2)
        print(f"     ІПН: {fop['edrpou']} | {fop_group_alert} група ЄП | "
              f"Дохід {datetime.now().year}: {format_currency(a['total_income'])} грн | "
              f"Активних днів: {a['active_days']}")

        for reason in alert["reasons"]:
            print(f"     {reason}")

        # Профіль по днях тижня
        wd_str = "     Профіль (дн.тижня): "
        for wd in range(7):
            avg = a["weekday_avg"].get(wd, 0)
            wd_str += f"{WEEKDAY_NAMES[wd]}={format_currency(avg)}  "
        print(wd_str)

        # Останні 7 днів
        last7_str = "     Останні 7 днів: "
        for d, amt in a["last_7_days"]:
            last7_str += f"{d.strftime('%d.%m')}={format_currency(amt)}  "
        print(last7_str)

        # Залишок до ліміту по днях
        fop_id = bytes(fop["id"])
        fop_group = fop_groups.get(fop_id, 2)
        fop_limit = LIMITS[fop_group]
        cum_by_date = a.get("cumulative_by_date", {})
        if cum_by_date:
            rem_str = f"     Залишок {fop_group}гр:    "
            for d, _amt in a["last_7_days"]:
                cum = cum_by_date.get(d, 0)
                remaining = fop_limit - cum
                if remaining > 0:
                    rem_str += f"{d.strftime('%d.%m')}={format_currency(remaining)}  "
                else:
                    rem_str += f"{d.strftime('%d.%m')}=⛔ ПЕРЕВИЩ  "
            print(rem_str)

        # Магазини
        fop_id = bytes(fop["id"])
        stores = fop_stores.get(fop_id, [])
        if stores:
            store_parts = []
            for s in stores:
                store_parts.append(f"{s['name']} ({format_currency(s['total'])} грн, {s['doc_count']} док.)")
            print(f"     🏪 Магазини: {', '.join(store_parts)}")
        else:
            print(f"     🏪 Магазини: не визначено")

        print()

    # === Зведена таблиця ===
    print(f"{'═' * 90}")
    print(f"  ЗВЕДЕНА ТАБЛИЦЯ (топ за доходом {year})")
    print(f"{'═' * 90}")
    print(f"  {'ФОП':<35} {'Гр':>3} {'Дохід, грн':>16} {'% ліміту':>9} "
          f"{'Залишок, грн':>16} {'Прогноз':>14} {'Дн.':>5}")
    print(f"  {'─' * 35} {'─' * 3} {'─' * 16} {'─' * 9} "
          f"{'─' * 16} {'─' * 14} {'─' * 5}")

    # Всі ФОПи, відсортовані за доходом
    all_fops_sorted = []
    for fop in fops:
        fop_id = bytes(fop["id"])
        analysis = analyses.get(fop_id)
        if analysis:
            all_fops_sorted.append((fop, analysis))
    all_fops_sorted.sort(key=lambda x: -x[1]["total_income"])

    display_count = top_n if top_n else min(30, len(all_fops_sorted))
    for fop, a in all_fops_sorted[:display_count]:
        fop_id = bytes(fop["id"])
        name = fop["name"].strip()[:34]
        group = fop_groups.get(fop_id, 2)
        limit = LIMITS[group]
        pct = safe_pct(a["total_income"], limit)
        remaining = limit - a["total_income"]

        info = a["limit_dates"][group]
        if info["already_exceeded"]:
            date_str = "ПЕРЕВИЩЕНО"
            days_str = "⛔"
        elif info["date"] is None:
            date_str = "не досягне"
            days_str = "—"
        else:
            date_str = format_date(info["date"])
            days_str = str((info["date"] - today).days)

        # Магазини ФОП
        stores = fop_stores.get(fop_id, [])
        store_names = ", ".join(s["name"] for s in stores[:3]) if stores else "—"

        print(f"  {name:<35} {group:>3} {format_currency(a['total_income']):>16} "
              f"{pct:>8.1f}% {format_currency(remaining):>16} {date_str:>14} {days_str:>5}")
        print(f"  {'':35}     🏪 {store_names}")

    print(f"\n{'═' * 90}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Моніторинг ліміту доходу ФОП (ст. 291.4 ПКУ)"
    )
    parser.add_argument(
        "--group", type=int, choices=[2, 3], default=None,
        help="Фільтрувати по групі ЄП (2 або 3). За замовчуванням — обидві."
    )
    parser.add_argument(
        "--days-ahead", type=int, default=14,
        help="Горизонт попередження у днях (за замовчуванням 14)"
    )
    parser.add_argument(
        "--top", type=int, default=None,
        help="Показати лише топ-N ФОПів"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Тільки звіт, без запуску процесів у Camunda"
    )
    parser.add_argument(
        "--camunda-url", type=str, default=None,
        help="URL Camunda REST API (за замовчуванням http://localhost:8088)"
    )
    args = parser.parse_args()

    if args.camunda_url:
        global CAMUNDA_REST_URL
        CAMUNDA_REST_URL = args.camunda_url.rstrip("/")

    today = datetime.now().date()
    year = today.year

    print(f"\n🔌 Підключення до БД BAS Бухгалтерія (deneb:1433/bas_bdu)...")

    try:
        conn = get_connection()
    except Exception as e:
        print(f"❌ Помилка підключення: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"📊 Завантаження даних за {year} рік...")

    fops = fetch_active_fops(conn, year)
    print(f"   Знайдено активних ФОПів: {len(fops)}")

    daily_income = fetch_daily_income(conn, year)
    print(f"   Завантажено дані по {len(daily_income)} ФОПах")

    fop_stores = fetch_fop_stores(conn, year)
    print(f"   Магазини: зв'язки для {len(fop_stores)} ФОПів")

    fop_groups = fetch_fop_groups(conn)
    g2 = sum(1 for g in fop_groups.values() if g == 2)
    g3 = sum(1 for g in fop_groups.values() if g == 3)
    print(f"   Групи ЄП: 2 група — {g2}, 3 група — {g3}")

    conn.close()

    print(f"🔮 Аналіз та прогнозування...\n")

    analyses = {}
    for fop in fops:
        fop_id = bytes(fop["id"])
        data = daily_income.get(fop_id, [])
        result = analyze_fop(data, today, year)
        if result:
            analyses[fop_id] = result

    print_report(fops, analyses, fop_stores, fop_groups,
                 args.group, args.days_ahead, args.top, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
