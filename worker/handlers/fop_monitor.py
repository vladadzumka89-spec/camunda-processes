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
from collections import defaultdict
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Any

from pyzeebe import ZeebeWorker

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

REPORT_DIR = Path(os.environ.get("FOP_REPORT_DIR", "reports/fop"))
REPORT_FILE = REPORT_DIR / "latest.json"

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


# ── DB connection ──────────────────────────────────────────────────────


def _get_db_config() -> dict:
    return {
        "server": os.environ.get("BAS_DB_HOST", "deneb"),
        "port": int(os.environ.get("BAS_DB_PORT", "1433")),
        "user": os.environ.get("BAS_DB_USER", "AI_buh"),
        "password": os.environ.get("BAS_DB_PASSWORD", ""),
        "database": os.environ.get("BAS_DB_NAME", "bas_bdu"),
        "login_timeout": 30,
        "timeout": 300,
        "charset": "UTF-8",
    }


def _get_connection(max_retries: int = 3, initial_delay: int = 5):
    import pymssql  # lazy import — may not be needed if handler never called

    db_config = _get_db_config()
    if not db_config["password"]:
        raise RuntimeError("BAS_DB_PASSWORD env variable is required for fop-limit-check")

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


# ── Terminal → Subdivision mapping ─────────────────────────────────────

_TRANSLIT = {
    'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e',
    'є': 'ie', 'ж': 'zh', 'з': 'z', 'и': 'y', 'і': 'i', 'ї': 'i',
    'й': 'i', 'к': 'k', 'л': 'l', 'м': 'm', 'н': 'n', 'о': 'o',
    'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u', 'ф': 'f',
    'х': 'kh', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'shch',
    'ь': '', 'ю': 'iu', 'я': 'ia', 'ґ': 'g', "'": '', '\u2019': '',
}


def _translit_ukr(text: str) -> str:
    """Transliterate Ukrainian to Latin (simplified passport-style)."""
    return ''.join(_TRANSLIT.get(c, c) for c in text.lower())


def _fetch_subdivision_lookup(conn) -> dict[str, str]:
    """Build {translit_word: full_description} lookup from _Reference116 store tree."""
    sql = """
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
        )
        SELECT _Description FROM _Reference116
        WHERE _IDRRef IN (SELECT _IDRRef FROM store_tree)
            AND _Description NOT IN (N'500 Магазини', N'600 Магазини',
                                     N'900 Пінкі', N'900 Пінкі  Сайт')
    """
    cursor = conn.cursor(as_dict=True)
    try:
        cursor.execute(sql)
        # Build lookup: each significant word (translit) → full description
        # Also store full translit → description for multi-word matching
        lookup = {}  # translit_key -> description
        all_subs = []  # [(description, set_of_translit_words)]
        code_lookup = {}  # "601" -> "601 Квартал Хмель"

        for row in cursor:
            desc = row["_Description"].strip()
            # Only use entries with 3-digit number prefix: "601 Квартал Хмель"
            # These are actual store subdivisions, not intermediate folders
            code_m = re.match(r'^(\d{3})\s', desc)
            if not code_m:
                continue

            code_lookup[code_m.group(1)] = desc

            # "601 Квартал Хмель" → name_part = "Квартал Хмель"
            name_part = re.sub(r'^\d+\s*', '', desc).strip()
            if not name_part:
                continue

            words = [w for w in name_part.split() if len(w) >= 2]
            translit_words = set()
            for w in words:
                tw = _translit_ukr(w)
                if len(tw) >= 2:
                    translit_words.add(tw)

            all_subs.append((desc, translit_words))

            # Map each unique word ≥4 chars to this subdivision
            for tw in translit_words:
                if len(tw) >= 4:
                    lookup[tw] = desc

            # Map full translit name
            full_translit = _translit_ukr(name_part).replace(' ', '')
            if full_translit:
                lookup[full_translit] = desc

        return {"word_lookup": lookup, "subdivisions": all_subs, "code_lookup": code_lookup}
    finally:
        cursor.close()


# Manual fallback for terminal names that can't be auto-transliterated
_TERMINAL_ALIASES = {
    "ocean": "okean",
    "cum": "tsum",
    "golivud": "gollivud",
    "hollywood": "gollivud",
    "city center": "siti tsentr",
    "city centre": "siti tsentr",
    "small": "smol",
    "smart": "smol",
    "schasluvuy": "shchaslyvyi",
    "happy": "shchaslyvyi",
    "uzhhorod": "uzhgorod",
}

# Direct terminal name → subdivision for names that can't be transliterated
_TERMINAL_DIRECT = {
    "пiрамiда": "651 Піраміда Київ",  # Latin 'i' in source
}


def _match_terminal_to_subdivision(
    terminal_name: str,
    sub_data: dict,
) -> str | None:
    """Match English terminal name to Ukrainian subdivision from _Reference116."""
    tn = terminal_name.lower().strip()
    word_lookup = sub_data["word_lookup"]
    subdivisions = sub_data["subdivisions"]

    # 0a) Check direct mapping table
    if tn in _TERMINAL_DIRECT:
        return _TERMINAL_DIRECT[tn]

    # 0b) Apply aliases for known mismatches
    tn_aliased = tn
    for eng, ukr_translit in _TERMINAL_ALIASES.items():
        tn_aliased = tn_aliased.replace(eng, ukr_translit)
    if tn_aliased != tn:
        tn = tn_aliased

    # 1) Direct full-name match (no spaces)
    tn_nospace = tn.replace(' ', '')
    if tn_nospace in word_lookup:
        return word_lookup[tn_nospace]

    # 2) Word-set matching: find subdivision with most overlapping words
    # Filter out noise words that appear in many terminal names
    _NOISE_WORDS = {"pinky", "pinki", "famo", "mag"}
    terminal_words = [w for w in tn.split() if len(w) >= 2 and w not in _NOISE_WORDS]
    if not terminal_words:
        return None

    best_match = None
    best_score = 0
    best_sub_size = 999  # prefer smaller subdivisions (more specific)

    for desc, sub_words in subdivisions:
        if not sub_words:
            continue
        score = 0
        for tw in terminal_words:
            for sw in sub_words:
                # Match: first 4 chars equal, containment, or y/i equivalence
                tw_norm = tw.replace('y', 'i')
                sw_norm = sw.replace('y', 'i')
                if (len(tw) >= 4 and len(sw) >= 4 and
                    (tw[:4] == sw[:4] or tw_norm[:4] == sw_norm[:4])) or \
                   (len(tw) >= 3 and len(sw) >= 3 and
                    (tw in sw or sw in tw or tw_norm in sw_norm or sw_norm in tw_norm)):
                    score += 1
                    break
        # Prefer: higher score → fewer unmatched sub_words (more specific)
        sub_size = len(sub_words)
        if score > best_score or (score == best_score and score > 0
                                   and sub_size < best_sub_size):
            best_score = score
            best_match = desc
            best_sub_size = sub_size

    return best_match if best_score > 0 else None


def _parse_terminal_name(purpose: str) -> tuple[str | None, str | None]:
    """Parse terminal name and optional subdivision code from payment purpose.

    Returns (terminal_name, subdivision_code) where subdivision_code is a
    3-digit string like '911' if found in the text.
    """
    # Use greedy match for cmps prefix to handle double-cmps patterns
    m = re.search(r'cmps:.*,\s*(.*?)\s*Кiльк\s+тр', purpose)
    if not m:
        return None, None
    raw = m.group(1).strip().rstrip(',').strip()
    # Also strip any remaining cmps: prefix
    raw = re.sub(r'^cmps:\s*', '', raw).strip()
    # Look for 3-digit subdivision code (5xx, 6xx, 9xx) in the raw text
    code_match = re.search(r'\b(\d{3})\s*(?=[A-Za-zА-Яа-яІіЇїЄєҐґ])', raw)
    sub_code = None
    if code_match:
        candidate = code_match.group(1)
        if candidate[0] in ('5', '6', '9'):
            sub_code = candidate
    # Extract terminal name (strip numbers, commas, leading noise)
    name = re.sub(r'^[\d\s,]+', '', raw).strip()
    name = re.sub(r'^\d{3}\s*', '', name).strip()  # strip embedded code
    name = re.sub(r'^R\s+', '', name).strip()
    if len(name) >= 2:
        return name, sub_code
    return None, sub_code


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

        terminal_data = defaultdict(lambda: defaultdict(lambda: {"count": 0, "total": 0.0, "code": None}))
        for row in cursor:
            org_id = bytes(row["org_id"])
            name, sub_code = _parse_terminal_name(row["purpose"] or "")
            if name:
                terminal_data[org_id][name]["count"] += 1
                terminal_data[org_id][name]["total"] += float(row["amount"] or 0)
                if sub_code:
                    terminal_data[org_id][name]["code"] = sub_code

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

        # Build terminal → subdivision mapping from _Reference116
        sub_data = _fetch_subdivision_lookup(conn)
        logger.info(
            "Підрозділи: %d записів у lookup, %d підрозділів",
            len(sub_data["word_lookup"]),
            len(sub_data["subdivisions"]),
        )

        result = defaultdict(list)
        all_org_ids = set(terminal_data.keys()) | set(doc_data.keys())
        mapped_count = 0
        unmapped_names = set()

        for org_id in all_org_ids:
            if org_id in doc_data:
                for item in doc_data[org_id]:
                    item["source"] = "document"
                    result[org_id].append(item)
            elif org_id in terminal_data:
                code_lookup = sub_data["code_lookup"]
                for name, info in sorted(
                    terminal_data[org_id].items(), key=lambda x: -x[1]["total"]
                ):
                    subdivision = None
                    # 1) Try direct code lookup from payment text
                    if info["code"] and info["code"] in code_lookup:
                        subdivision = code_lookup[info["code"]]
                    # 2) Fallback: transliteration matching
                    if not subdivision:
                        subdivision = _match_terminal_to_subdivision(name, sub_data)
                    if subdivision:
                        mapped_count += 1
                    else:
                        unmapped_names.add(name)
                    result[org_id].append({
                        "name": subdivision or name,
                        "doc_count": info["count"],
                        "total": info["total"],
                        "source": "terminal",
                    })

        if unmapped_names:
            logger.warning(
                "Не вдалось зіставити %d терміналів: %s",
                len(unmapped_names),
                ", ".join(sorted(unmapped_names)[:10]),
            )
        logger.info("Термінал→підрозділ: %d зіставлено", mapped_count)

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


# ── Report file ────────────────────────────────────────────────────────


def _save_report_json(report: dict) -> None:
    """Save JSON report to file (atomic write)."""
    try:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        tmp = REPORT_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(report, ensure_ascii=False, indent=2))
        tmp.replace(REPORT_FILE)
        logger.info("Звіт збережено: %s", REPORT_FILE)
    except Exception as e:
        logger.warning("Не вдалося зберегти звіт у файл: %s", e)


# ── Main sync check ───────────────────────────────────────────────────


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
            status = "in_progress"
        elif is_critical:
            status = "new"
        else:
            status = "ok"

        stores = fop_stores.get(fop_id, [])
        stores_list = [
            {"name": s["name"], "doc_count": s["doc_count"],
             "total": s["total"], "source": s.get("source", "document")}
            for s in stores
        ]
        stores_text = "\n".join(
            f"{s['name']}: {s['total']:,.0f}".replace(",", " ")
            for s in stores
        )

        fop_entry = {
            "fop_name": fop["name"].strip(),
            "fop_edrpou": edrpou,
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
            "status": status,
        }

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
            })

    critical_all = sum(1 for f in all_fops_report if f["status"] != "ok")
    critical_in_progress = sum(1 for f in all_fops_report if f["status"] == "in_progress")

    logger.info(
        "Критичних ФОПів: %d всього (%d нових, %d вже в роботі)",
        critical_all, len(critical_fops), critical_in_progress,
    )

    # JSON report — all FOPs sorted by days_to_limit (most urgent first)
    all_fops_report.sort(key=lambda f: f["days_to_limit"])

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
    }

    # Save report to file for dashboard endpoint
    _save_report_json(report_json)

    return {
        "report_date": today.isoformat(),
        "total_fops": len(fops),
        "total_analyzed": len(analyses),
        "critical_count": len(critical_fops),
        "critical_fops": critical_fops,  # only NEW — for BPMN multi-instance
        "report_json": report_json,      # all FOPs — for Odoo dashboard
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
