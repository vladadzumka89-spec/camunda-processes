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
    2: float(os.environ.get("FOP_LIMIT_GROUP_2", "3500000")),
    3: float(os.environ.get("FOP_LIMIT_GROUP_3", "3500000")),
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
        JOIN _Document236_VT6023 vt ON vt._Document236_IDRRef = d._IDRRef
        JOIN _Reference129 r129 ON r129._IDRRef = vt._Fld6037RRef
        WHERE d._Posted = 0x01
            AND d._Marked = 0x00
            AND d._Date_Time >= %s AND d._Date_Time < %s
            AND o._Marked = 0x00
            AND (o._Fld1495 LIKE N'%%ізична особа%%' OR o._Fld1495 LIKE N'%%ФОП%%')
            AND o._Description NOT LIKE N'яяя%%'
            AND r129._Description = N'Стоимость проданных товаров (работ, услуг)'
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


# Status property ID and known values in _Reference90_VT1523
_STATUS_PROP_ID = bytes.fromhex("85d7ec0d9a794f5211ed6f042b93621a")
_STATUS_CLOSED_ID = bytes.fromhex("85d7ec0d9a794f5211ed6f042b93621b")


def _fetch_fop_statuses(conn) -> dict:
    """Fetch org status (Відкрита/Закрита) from additional properties."""
    sql = """
        SELECT vt._Reference90_IDRRef AS org_id,
               vt._Fld1526_RRRef AS status_val
        FROM _Reference90_VT1523 vt
        WHERE vt._Fld1525RRef = %s
    """
    cursor = conn.cursor(as_dict=True)
    try:
        cursor.execute(sql, (_STATUS_PROP_ID,))
        result = {}
        for row in cursor:
            org_id = bytes(row["org_id"])
            is_closed = bytes(row["status_val"]) == _STATUS_CLOSED_ID
            result[org_id] = "Закрита" if is_closed else "Відкрита"
        return result
    finally:
        cursor.close()


def _fetch_daily_income(conn, year: int) -> dict:
    """Fetch daily FOP income from accumulation register _AccumRg10618.

    This register is the authoritative source for Книга Доходів (Income Book).
    It already includes bank commission (GROSS), cash receipts, and returns
    (negative amounts) — matching the official tax income report exactly.
    """
    sql = """
        ;WITH fop_filter AS (
            SELECT _IDRRef FROM _Reference90
            WHERE _Marked = 0x00
                AND (_Fld1495 LIKE N'%%ізична особа%%' OR _Fld1495 LIKE N'%%ФОП%%')
                AND _Description NOT LIKE N'яяя%%'
        )
        SELECT r._Fld10619RRef AS org_id,
               CAST(DATEADD(year, -2000, r._Period) AS date) AS doc_date,
               SUM(r._Fld10621) AS daily_total,
               COUNT(*) AS doc_count
        FROM _AccumRg10618 r
        WHERE r._Period >= %s AND r._Period < %s
          AND r._Active = 0x01
          AND r._Fld10619RRef IN (SELECT _IDRRef FROM fop_filter)
        GROUP BY r._Fld10619RRef, CAST(DATEADD(year, -2000, r._Period) AS date)
        ORDER BY r._Fld10619RRef, doc_date
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
    _NOISE_WORDS = {"famo", "mag"}
    is_pinky = "pinky" in tn or "pinki" in tn
    terminal_words = [w for w in tn.split() if len(w) >= 2 and w not in _NOISE_WORDS]
    # For Пінкі terminals, remove "pinky"/"pinki" from matching words
    # but use it as a filter to prefer 9xx subdivisions
    if is_pinky:
        # Determine if PINKY is prefix (terminal brand) or suffix (Пінкі store)
        # "PINKY Obolon" → prefix → match all subdivisions (regular store)
        # "FORUM PINKY", "Ostrov Pinky" → suffix → restrict to 9xx Пінкі
        raw_words = tn.split()
        pinky_is_prefix = raw_words and raw_words[0] in ("pinky", "pinki")
        terminal_words = [w for w in terminal_words if w not in ("pinky", "pinki")]
    if not terminal_words:
        return None

    best_match = None
    best_score = 0
    best_sub_size = 999  # prefer smaller subdivisions (more specific)

    for desc, sub_words in subdivisions:
        if not sub_words:
            continue
        # Suffix Пінкі (e.g. "Forum PINKY") → ONLY match 9xx Пінкі subdivisions
        # Prefix Пінкі (e.g. "PINKY Obolon") → match ALL (terminal brand, not store)
        if is_pinky and not pinky_is_prefix:
            desc_lower = desc.lower()
            if not (re.match(r'^9\d{2}\s', desc) or 'пінкі' in desc_lower):
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


def _classify_payment(purpose: str) -> str:
    """Classify payment by purpose text.

    Returns: 'cmps', 'mono', 'liqpay', 'novapay', 'other'
    """
    p = (purpose or "").strip()
    if p.startswith("cmps:") or ",cmps:" in p:
        return "cmps"
    p_low = p.lower()
    if "еквайринг" in p_low or "універсал банк" in p_low:
        return "mono"
    if "liqpay" in p_low:
        return "liqpay"
    if not p:
        return "novapay"
    return "other"


def _parse_terminal_name(purpose: str) -> tuple[str | None, str | None, str | None]:
    """Parse terminal name, subdivision code, and terminal code from payment purpose.

    Returns (terminal_name, subdivision_code, terminal_code) where:
    - subdivision_code is a 3-digit string like '911' if found in the text
    - terminal_code is the cmps merchant code (e.g. '75' from 'cmps: 75')
    """
    # Extract terminal code (first number after cmps:)
    tc_match = re.search(r'cmps:\s*(\d+)', purpose)
    terminal_code = tc_match.group(1).strip() if tc_match else None

    # Use greedy match for cmps prefix to handle double-cmps patterns
    m = re.search(r'cmps:.*,\s*(.*?)\s*Кiльк\s+тр', purpose)
    if not m:
        return None, None, terminal_code
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
        return name, sub_code, terminal_code
    return None, sub_code, terminal_code


def _fetch_fop_stores(conn, year: int) -> dict:
    bas_start = f"{year + BAS_YEAR_OFFSET}-01-01"
    bas_end = f"{year + BAS_YEAR_OFFSET + 1}-01-01"

    sql_payments = """
        SELECT
            d._Fld6004RRef AS org_id,
            d._Fld6019 AS purpose,
            d._Fld6010 AS amount,
            MONTH(DATEADD(year, -2000, d._Date_Time)) AS mn
        FROM _Document236 d
        JOIN _Reference90 o ON d._Fld6004RRef = o._IDRRef
        JOIN _Document236_VT6023 vt ON vt._Document236_IDRRef = d._IDRRef
        JOIN _Reference129 r129 ON r129._IDRRef = vt._Fld6037RRef
        WHERE d._Posted = 0x01 AND d._Marked = 0x00
            AND d._Date_Time >= %s AND d._Date_Time < %s
            AND o._Marked = 0x00
            AND (o._Fld1495 LIKE N'%%ізична особа%%' OR o._Fld1495 LIKE N'%%ФОП%%')
            AND o._Description NOT LIKE N'яяя%%'
            AND r129._Description = N'Стоимость проданных товаров (работ, услуг)'
    """
    cursor = conn.cursor(as_dict=True)
    try:
        cursor.execute(sql_payments, (bas_start, bas_end))

        _CAT_LABELS = {
            "mono": "101 Інтернет-магазин (Моно)",
            "liqpay": "101 Інтернет-магазин (LiqPay)",
            "novapay": "101 Інтернет-магазин (НоваПей)",
            "other": "Інші надходження",
        }

        # Group cmps by (org_id, terminal_code) to merge entries like
        # "PINKY" and "920 BUKOVYNA PINKY" that come from the same terminal
        terminal_data = defaultdict(lambda: defaultdict(lambda: {"count": 0, "total": 0.0, "code": None}))
        other_income = defaultdict(lambda: defaultdict(lambda: {"count": 0, "total": 0.0}))
        # Track terminal_code → sub_code globally for resolving generic PINKY
        tc_sub_global = defaultdict(set)  # terminal_code → {sub_codes}
        tc_sub_per_fop = defaultdict(lambda: defaultdict(set))  # org → tc → {sub_codes}
        pinky_tc_map = defaultdict(lambda: defaultdict(set))  # org → name → {terminal_codes}
        # Monthly income tracking per raw terminal name and per category label
        monthly_cmps = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))  # org→name→month→total
        monthly_other = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))  # org→label→month→total

        for row in cursor:
            org_id = bytes(row["org_id"])
            purpose = row["purpose"] or ""
            amount = float(row["amount"] or 0)
            month = row["mn"]
            cat = _classify_payment(purpose)

            if cat == "cmps":
                name, sub_code, tc = _parse_terminal_name(purpose)
                if name:
                    terminal_data[org_id][name]["count"] += 1
                    terminal_data[org_id][name]["total"] += amount
                    monthly_cmps[org_id][name][month] += amount
                    if sub_code:
                        terminal_data[org_id][name]["code"] = sub_code
                    if tc:
                        if sub_code:
                            tc_sub_global[tc].add(sub_code)
                            tc_sub_per_fop[org_id][tc].add(sub_code)
                        # Track terminal codes for generic PINKY entries
                        if name.lower().strip() in ("pinky", "pinki"):
                            pinky_tc_map[org_id][name].add(tc)
            else:
                label = _CAT_LABELS[cat]
                other_income[org_id][label]["count"] += 1
                other_income[org_id][label]["total"] += amount
                monthly_other[org_id][label][month] += amount

        # Cash receipts (ПКО — _Document243)
        sql_cash = """
            SELECT d._Fld6492RRef AS org_id,
                   SUM(d._Fld6493) AS total,
                   COUNT(*) AS cnt
            FROM _Document243 d
            JOIN _Reference90 o ON d._Fld6492RRef = o._IDRRef
            WHERE d._Posted = 0x01 AND d._Marked = 0x00
                AND d._Date_Time >= %s AND d._Date_Time < %s
                AND o._Marked = 0x00
                AND (o._Fld1495 LIKE N'%%ізична особа%%' OR o._Fld1495 LIKE N'%%ФОП%%')
                AND o._Description NOT LIKE N'яяя%%'
            GROUP BY d._Fld6492RRef
        """
        cash_data = {}
        cursor.execute(sql_cash, (bas_start, bas_end))
        for row in cursor:
            org_id = bytes(row["org_id"])
            cash_data[org_id] = {
                "count": row["cnt"],
                "total": float(row["total"] or 0),
            }

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
        all_org_ids = set(terminal_data.keys()) | set(doc_data.keys()) | set(other_income.keys()) | set(cash_data.keys())
        mapped_count = 0
        unmapped_names = set()

        for org_id in all_org_ids:
            # 1) Document-based stores (from _Document247/238/213)
            if org_id in doc_data:
                for item in doc_data[org_id]:
                    item["source"] = "document"
                    result[org_id].append(item)

            # 2) cmps: terminal payments (PrivatBank)
            if org_id in terminal_data:
                code_lookup = sub_data["code_lookup"]
                for name, info in sorted(
                    terminal_data[org_id].items(), key=lambda x: -x[1]["total"]
                ):
                    subdivision = None
                    # 2a) Try direct code lookup from payment text
                    if info["code"] and info["code"] in code_lookup:
                        subdivision = code_lookup[info["code"]]
                    # 2b) Fallback: transliteration matching
                    if not subdivision:
                        subdivision = _match_terminal_to_subdivision(name, sub_data)
                    # 2c) For generic PINKY — resolve via terminal code mapping
                    if not subdivision and name.lower().strip() in ("pinky", "pinki"):
                        for tc in pinky_tc_map.get(org_id, {}).get(name, set()):
                            # Per-FOP: same FOP+tc has sub_code from other payments
                            pf = tc_sub_per_fop.get(org_id, {}).get(tc, set())
                            if len(pf) == 1:
                                sc = next(iter(pf))
                                if sc in code_lookup:
                                    subdivision = code_lookup[sc]
                                    break
                            # Global: all FOPs with this tc agree on sub_code
                            gl = tc_sub_global.get(tc, set())
                            if len(gl) == 1:
                                sc = next(iter(gl))
                                if sc in code_lookup:
                                    subdivision = code_lookup[sc]
                                    break
                    if subdivision:
                        mapped_count += 1
                    else:
                        unmapped_names.add(name)
                    resolved_name = subdivision or name
                    result[org_id].append({
                        "name": resolved_name,
                        "doc_count": info["count"],
                        "total": info["total"],
                        "source": "terminal",
                    })

            # 3) Non-cmps payments (Mono, LiqPay, NovaPay, other)
            if org_id in other_income:
                for cat_name, info in sorted(
                    other_income[org_id].items(), key=lambda x: -x[1]["total"]
                ):
                    if info["total"] > 0:
                        result[org_id].append({
                            "name": cat_name,
                            "doc_count": info["count"],
                            "total": info["total"],
                            "source": "payment",
                        })

            # 4) Cash receipts (ПКО — _Document243)
            if org_id in cash_data:
                info = cash_data[org_id]
                if info["total"] > 0:
                    result[org_id].append({
                        "name": "Каса (готівка)",
                        "doc_count": info["count"],
                        "total": info["total"],
                        "source": "cash",
                    })

        if unmapped_names:
            logger.warning(
                "Не вдалось зіставити %d терміналів: %s",
                len(unmapped_names),
                ", ".join(sorted(unmapped_names)[:10]),
            )
        logger.info("Термінал→підрозділ: %d зіставлено", mapped_count)

        # Build raw→resolved name mapping for monthly data
        name_map = {}  # (org_id, raw_name) → resolved_name
        for org_id in all_org_ids:
            if org_id in terminal_data:
                code_lookup_m = sub_data["code_lookup"]
                for raw_name in terminal_data[org_id]:
                    info = terminal_data[org_id][raw_name]
                    resolved = None
                    if info["code"] and info["code"] in code_lookup_m:
                        resolved = code_lookup_m[info["code"]]
                    if not resolved:
                        resolved = _match_terminal_to_subdivision(raw_name, sub_data)
                    if not resolved and raw_name.lower().strip() in ("pinky", "pinki"):
                        for tc in pinky_tc_map.get(org_id, {}).get(raw_name, set()):
                            pf = tc_sub_per_fop.get(org_id, {}).get(tc, set())
                            if len(pf) == 1:
                                sc = next(iter(pf))
                                if sc in code_lookup_m:
                                    resolved = code_lookup_m[sc]
                                    break
                            gl = tc_sub_global.get(tc, set())
                            if len(gl) == 1:
                                sc = next(iter(gl))
                                if sc in code_lookup_m:
                                    resolved = code_lookup_m[sc]
                                    break
                    name_map[(org_id, raw_name)] = resolved or raw_name

        # Build store-level monthly income (aggregated across all FOPs)
        store_monthly: dict[str, dict[int, float]] = defaultdict(lambda: defaultdict(float))
        for org_id in all_org_ids:
            # cmps monthly → resolved names
            if org_id in monthly_cmps:
                for raw_name, months in monthly_cmps[org_id].items():
                    resolved = name_map.get((org_id, raw_name), raw_name)
                    for mn, amt in months.items():
                        store_monthly[resolved][mn] += amt
            # non-cmps monthly (labels already match)
            if org_id in monthly_other:
                for label, months in monthly_other[org_id].items():
                    for mn, amt in months.items():
                        store_monthly[label][mn] += amt

        # Merge duplicate subdivision names per FOP
        merged = defaultdict(list)
        for org_id, stores in result.items():
            seen = {}
            for s in stores:
                name = s["name"]
                if name in seen:
                    seen[name]["doc_count"] += s["doc_count"]
                    seen[name]["total"] += s["total"]
                else:
                    entry = dict(s)
                    seen[name] = entry
                    merged[org_id].append(entry)
            # Re-sort by total descending
            merged[org_id].sort(key=lambda x: -x["total"])

        return merged, {k: dict(v) for k, v in store_monthly.items()}
    finally:
        cursor.close()


# ── Organization classification ────────────────────────────────────────


def _determine_organization(stores: list) -> str:
    """Classify FOP as ФАМО or Технопростір based on its store prefixes.

    500 → Технопростір, 600/900 → ФАМО.
    """
    famo = 0.0
    techno = 0.0
    for s in stores:
        name = s.get("name", "")
        m = re.match(r'^(\d{3})\s', name)
        if not m:
            continue
        prefix = int(m.group(1))
        total = abs(s.get("total", 0))
        if 500 <= prefix < 600:
            techno += total
        elif 600 <= prefix < 700 or 900 <= prefix < 1000:
            famo += total
    if famo > 0 and techno > 0:
        return "ФАМО / Технопростір"
    if famo > 0:
        return "ФАМО"
    if techno > 0:
        return "Технопростір"
    return "Невизначено"


# ── Monthly income history ─────────────────────────────────────────────


def _fetch_monthly_history(conn, year: int) -> dict:
    """Fetch monthly income per FOP from Q4 of previous year to current date.

    Returns:
        {org_id_bytes: [{"month": "2025-10", "total": 123456.78}, ...]}
    """
    prev_year = year - 1
    bas_start = f"{prev_year + BAS_YEAR_OFFSET}-10-01"
    bas_end = f"{year + BAS_YEAR_OFFSET + 1}-01-01"

    sql = """
        ;WITH fop_filter AS (
            SELECT _IDRRef FROM _Reference90
            WHERE _Marked = 0x00
                AND (_Fld1495 LIKE N'%%ізична особа%%' OR _Fld1495 LIKE N'%%ФОП%%')
                AND _Description NOT LIKE N'яяя%%'
        )
        SELECT r._Fld10619RRef AS org_id,
               YEAR(DATEADD(year, -2000, r._Period)) AS yr,
               MONTH(DATEADD(year, -2000, r._Period)) AS mn,
               SUM(r._Fld10621) AS monthly_total
        FROM _AccumRg10618 r
        WHERE r._Period >= %s AND r._Period < %s
          AND r._Active = 0x01
          AND r._Fld10619RRef IN (SELECT _IDRRef FROM fop_filter)
        GROUP BY r._Fld10619RRef,
                 YEAR(DATEADD(year, -2000, r._Period)),
                 MONTH(DATEADD(year, -2000, r._Period))
        ORDER BY r._Fld10619RRef, yr, mn
    """
    cursor = conn.cursor(as_dict=True)
    try:
        cursor.execute(sql, (bas_start, bas_end))
        result = defaultdict(list)
        for row in cursor:
            org_id = bytes(row["org_id"])
            result[org_id].append({
                "month": f"{row['yr']}-{row['mn']:02d}",
                "total": round(float(row["monthly_total"] or 0), 2),
            })
        return result
    finally:
        cursor.close()


# ── Seasonal coefficients ──────────────────────────────────────────────


def _compute_seasonal_coefficients(
    monthly_data: dict[str, dict[int, float]],
) -> dict[str, dict[int, float]]:
    """Compute seasonal coefficients per store per month.

    Args:
        monthly_data: {store_name: {month: avg_daily_income}}

    Returns:
        {store_name: {month: coefficient}} where coefficient = month_avg / year_avg.
        Months without data get coefficient 1.0.
    """
    coefficients = {}
    for store, months in monthly_data.items():
        if not months:
            continue
        year_avg = sum(months.values()) / len(months)
        if year_avg <= 0:
            coefficients[store] = {m: 1.0 for m in range(1, 13)}
            continue
        store_coeffs = {}
        for m in range(1, 13):
            if m in months and months[m] > 0:
                store_coeffs[m] = months[m] / year_avg
            else:
                store_coeffs[m] = 1.0
        coefficients[store] = store_coeffs
    return coefficients


def _fetch_seasonal_coefficients(conn, year: int) -> tuple[dict, dict]:
    """Fetch store-level and network-level seasonal coefficients from previous year.

    Returns:
        (store_coefficients, network_coefficients):
        - store_coefficients: {store_name: {month: coefficient}}
        - network_coefficients: {month: coefficient} — fallback for unknown stores
    """
    prev_year = year - 1
    bas_start = f"{prev_year + BAS_YEAR_OFFSET}-01-01"
    bas_end = f"{prev_year + BAS_YEAR_OFFSET + 1}-01-01"

    sql = """
        SELECT
            MONTH(DATEADD(year, -2000, d._Date_Time)) AS month_num,
            d._Fld6019 AS purpose,
            d._Fld6010 AS amount
        FROM _Document236 d
        JOIN _Reference90 o ON d._Fld6004RRef = o._IDRRef
        JOIN _Document236_VT6023 vt ON vt._Document236_IDRRef = d._IDRRef
        JOIN _Reference129 r129 ON r129._IDRRef = vt._Fld6037RRef
        WHERE d._Posted = 0x01 AND d._Marked = 0x00
            AND d._Date_Time >= %s AND d._Date_Time < %s
            AND o._Marked = 0x00
            AND (o._Fld1495 LIKE N'%%ізична особа%%' OR o._Fld1495 LIKE N'%%ФОП%%')
            AND o._Description NOT LIKE N'яяя%%'
            AND r129._Description = N'Стоимость проданных товаров (работ, услуг)'
    """
    cursor = conn.cursor(as_dict=True)
    try:
        cursor.execute(sql, (bas_start, bas_end))

        store_monthly_totals = defaultdict(lambda: defaultdict(float))
        store_monthly_counts = defaultdict(lambda: defaultdict(int))
        network_monthly_totals = defaultdict(float)
        network_monthly_counts = defaultdict(int)

        for row in cursor:
            purpose = row["purpose"] or ""
            amount = float(row["amount"] or 0)
            month = row["month_num"]
            cat = _classify_payment(purpose)

            if cat == "cmps":
                name, _, _ = _parse_terminal_name(purpose)
                if name:
                    store_monthly_totals[name][month] += amount
                    store_monthly_counts[name][month] += 1

            network_monthly_totals[month] += amount
            network_monthly_counts[month] += 1
    finally:
        cursor.close()

    # Convert totals to daily averages per month
    store_monthly_avg = {}
    for store, months in store_monthly_totals.items():
        store_monthly_avg[store] = {}
        for m, total in months.items():
            count = store_monthly_counts[store][m]
            store_monthly_avg[store][m] = total / count if count > 0 else 0

    network_monthly_avg = {}
    for m, total in network_monthly_totals.items():
        count = network_monthly_counts[m]
        network_monthly_avg[m] = total / count if count > 0 else 0

    store_coefficients = _compute_seasonal_coefficients(store_monthly_avg)
    network_coefficients = _compute_seasonal_coefficients(
        {"_network": network_monthly_avg}
    )
    network_coefficients = network_coefficients.get(
        "_network", {m: 1.0 for m in range(1, 13)}
    )

    logger.info(
        "Сезонні коефіцієнти: %d магазинів з даних %d року",
        len(store_coefficients), prev_year,
    )

    return store_coefficients, network_coefficients


# ── Terminal changes ──────────────────────────────────────────────────


def _compute_terminal_change(current: int, previous: int) -> dict:
    """Compute terminal count change between two periods."""
    change = current - previous
    if previous > 0:
        change_pct = round((change / previous) * 100, 1)
    else:
        change_pct = 0.0
    return {
        "terminal_change": change,
        "terminal_change_percent": change_pct,
    }


def _fetch_terminal_changes(conn, year: int) -> dict:
    """Fetch terminal count changes per FOP: this week vs last week.

    Returns:
        {org_id_bytes: {"current": int, "previous": int,
                        "terminal_change": int, "terminal_change_percent": float}}
    """
    today = datetime.now().date()
    one_week_ago = today - timedelta(days=7)
    two_weeks_ago = today - timedelta(days=14)

    bas_one_week = f"{one_week_ago.year + BAS_YEAR_OFFSET}-{one_week_ago.month:02d}-{one_week_ago.day:02d}"
    bas_two_weeks = f"{two_weeks_ago.year + BAS_YEAR_OFFSET}-{two_weeks_ago.month:02d}-{two_weeks_ago.day:02d}"
    bas_today = f"{today.year + BAS_YEAR_OFFSET}-{today.month:02d}-{today.day:02d}"

    sql = """
        SELECT
            d._Fld6004RRef AS org_id,
            CASE WHEN CAST(d._Date_Time AS date) >= %s THEN 'current' ELSE 'previous' END AS period,
            d._Fld6019 AS purpose
        FROM _Document236 d
        JOIN _Reference90 o ON d._Fld6004RRef = o._IDRRef
        JOIN _Document236_VT6023 vt ON vt._Document236_IDRRef = d._IDRRef
        JOIN _Reference129 r129 ON r129._IDRRef = vt._Fld6037RRef
        WHERE d._Posted = 0x01 AND d._Marked = 0x00
            AND d._Date_Time >= %s AND d._Date_Time < %s
            AND o._Marked = 0x00
            AND (o._Fld1495 LIKE N'%%ізична особа%%' OR o._Fld1495 LIKE N'%%ФОП%%')
            AND o._Description NOT LIKE N'яяя%%'
            AND r129._Description = N'Стоимость проданных товаров (работ, услуг)'
            AND d._Fld6019 LIKE N'%%cmps%%'
    """
    cursor = conn.cursor(as_dict=True)
    try:
        cursor.execute(sql, (bas_one_week, bas_two_weeks, bas_today))

        terminals = defaultdict(lambda: {"current": set(), "previous": set()})

        for row in cursor:
            org_id = bytes(row["org_id"])
            period = row["period"]
            purpose = row["purpose"] or ""
            _, _, tc = _parse_terminal_name(purpose)
            if tc:
                terminals[org_id][period].add(tc)
    finally:
        cursor.close()

    result = {}
    for org_id, periods in terminals.items():
        current_count = len(periods["current"])
        previous_count = len(periods["previous"])
        change_info = _compute_terminal_change(current_count, previous_count)
        result[org_id] = {
            "current": current_count,
            "previous": previous_count,
            **change_info,
        }

    logger.info("Зміни терміналів: %d ФОПів з даними", len(result))
    return result



def _fetch_terminal_bindings(conn, year: int) -> dict[str, list[dict]]:
    """Fetch terminal binding history from BAS.

    Source: РегистрСведений.ТУТ_ДополнительныеДанныеДляОтчетов
    SQL table: _InfoRg28391, filtered by _Fld28393 = 'Терминал'.

    Fields:
        _Fld28392RRef  → _Reference90 (Організація / ФОП)
        _Fld28393      → Свойство (filter: 'Терминал')
        _Fld28396RRef  → _Reference100 (Підрозділ / магазин)
        _Period        → дата прив'язки (BAS offset +2000)
        _Fld28394_T    → Значення (datetime): 31.12.2099 = підключений,
                         інша дата = відключений

    Returns store_name → list of binding records sorted by date.
    Each record: {date, fop_name, value_date} where value_date indicates
    connection (year >= 2090) or disconnection (specific date).
    Graceful degradation: returns {} on any DB error.
    """
    sql = """
        SELECT
            r100._Description AS store_name,
            CONVERT(varchar, DATEADD(year, -2000, t._Period), 104) AS binding_date,
            r90._Description AS fop_name,
            CONVERT(varchar, DATEADD(year, -2000, t._Fld28394_T), 104) AS value_date
        FROM _InfoRg28391 t
        JOIN _Reference100 r100 ON r100._IDRRef = t._Fld28396RRef
        JOIN _Reference90 r90 ON r90._IDRRef = t._Fld28392RRef
        WHERE t._Fld28393 = N'Терминал'
        ORDER BY r100._Description, t._Period
    """
    cursor = conn.cursor(as_dict=True)
    try:
        cursor.execute(sql)
        result: dict[str, list[dict]] = defaultdict(list)
        for row in cursor:
            name = row["store_name"].strip()
            result[name].append({
                "date": row["binding_date"],
                "fop_name": row["fop_name"].strip() if row["fop_name"] else "",
                "value_date": row["value_date"] or "",
            })
        return dict(result)
    except Exception as e:
        logger.warning(
            "Не вдалося завантажити історію прив'язки терміналів: %s", e
        )
        return {}
    finally:
        cursor.close()


def _parse_binding_date(d: str) -> date | None:
    """Parse dd.mm.yyyy binding date string."""
    try:
        parts = d.split(".")
        return date(int(parts[2]), int(parts[1]), int(parts[0]))
    except (ValueError, IndexError):
        return None


def _group_binding_periods(
    bindings: list[dict], year: int = 2026, current_fop_name: str = ""
) -> list[dict]:
    """Build FOP periods from binding records using value_date.

    Each record from BAS has:
    - date: коли відбулась подія (Period)
    - fop_name: ФОП
    - value_date: "31.12.2099" = підключений, інша дата = відключений

    Algorithm: pair connection records (value_date >= 2090) with their
    corresponding disconnection records for the same FOP, sorted by date.

    Result: [{fop_name, date_from, date_to}, ...] where date_to=None means
    the FOP is currently active.
    """
    if not bindings:
        return []

    # Separate into connections and disconnections using value_date
    connections: list[dict] = []   # records where value_date year >= 2090
    disconnections: dict[str, list[str]] = defaultdict(list)  # fop → [dates]

    for b in bindings:
        bd = _parse_binding_date(b["date"])
        vd = _parse_binding_date(b.get("value_date", ""))
        if not bd or bd.year < 2020:
            continue
        if vd and vd.year >= 2090:
            connections.append(b)
        elif vd and vd.year >= 2020:
            disconnections[b["fop_name"]].append(b["date"])

    if not connections:
        return []

    # Sort connections by date
    connections.sort(key=lambda b: _parse_binding_date(b["date"]) or date.min)

    # Sort disconnection dates per FOP
    for fop in disconnections:
        disconnections[fop].sort(
            key=lambda d: _parse_binding_date(d) or date.min
        )

    # Build periods: match each connection with its next disconnection
    disc_idx: dict[str, int] = defaultdict(int)  # fop → next disconnection index
    periods: list[dict] = []

    for c in connections:
        fop = c["fop_name"]
        date_from = c["date"]
        date_to = None

        disc_dates = disconnections.get(fop, [])
        idx = disc_idx[fop]
        if idx < len(disc_dates):
            date_to = disc_dates[idx]
            disc_idx[fop] = idx + 1

        periods.append({
            "fop_name": fop,
            "date_from": date_from,
            "date_to": date_to,
        })

    # Sort by date_from
    periods.sort(key=lambda p: _parse_binding_date(p["date_from"]) or date.min)

    # Filter: keep only periods active in the target year, skip same-day
    year_start = date(year, 1, 1)
    filtered = []
    for p in periods:
        dt_from = _parse_binding_date(p["date_from"])
        dt_to = _parse_binding_date(p["date_to"]) if p["date_to"] else None
        if dt_to:
            if dt_from and dt_from == dt_to:
                continue
            if dt_to >= year_start:
                filtered.append(p)
        else:
            filtered.append(p)

    return filtered


def _calc_growth_percent(prev: float, curr: float) -> float | None:
    """Calculate month-over-month growth percentage.

    Returns None if previous month income is 0 (no baseline).
    """
    if prev <= 0:
        return None
    return round(((curr - prev) / prev) * 100, 1)


def _determine_current_fop(
    bindings: list[dict],
    fops_list: list[dict],
) -> tuple[str, str]:
    """Determine current FOP for a store.

    Priority:
    1. Last entry in binding_history (most accurate)
    2. Fallback: FOP with highest income_from_store

    Returns: (fop_name, fop_edrpou)
    """
    if bindings:
        current_name = bindings[-1]["fop_name"]
        # Try to find edrpou from fops_list
        for f in fops_list:
            if f["fop_name"] == current_name:
                return current_name, f["fop_edrpou"]
        return current_name, ""

    if fops_list:
        best = max(fops_list, key=lambda f: f.get("income_from_store", 0))
        return best["fop_name"], best["fop_edrpou"]

    return "", ""


# ── Analysis ───────────────────────────────────────────────────────────


def _safe_pct(income: float, limit: float) -> float:
    return round((income / limit) * 100, 1) if limit > 0 else 0.0


def _analyze_fop(
    daily_data: list,
    today: date,
    year: int,
    *,
    seasonal_coefficients: dict | None = None,
    network_coefficients: dict | None = None,
    fop_stores: list | None = None,
) -> dict | None:
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

    # Build per-month seasonal multiplier for this FOP
    seasonal_mult = {m: 1.0 for m in range(1, 13)}
    if seasonal_coefficients and fop_stores:
        total_store_income = sum(s.get("total", 0) for s in fop_stores)
        if total_store_income > 0:
            for m in range(1, 13):
                weighted = 0.0
                for s in fop_stores:
                    store_name = s["name"]
                    weight = s.get("total", 0) / total_store_income
                    if store_name in seasonal_coefficients:
                        weighted += seasonal_coefficients[store_name].get(m, 1.0) * weight
                    elif network_coefficients:
                        weighted += network_coefficients.get(m, 1.0) * weight
                    else:
                        weighted += 1.0 * weight
                seasonal_mult[m] = weighted
    elif network_coefficients:
        seasonal_mult = {m: network_coefficients.get(m, 1.0) for m in range(1, 13)}

    # Projection with seasonality
    projected_remaining = 0.0
    for day_offset in range(1, days_remaining + 1):
        future_date = today + timedelta(days=day_offset)
        wd = future_date.weekday()
        m = future_date.month
        projected_remaining += weekday_avg.get(wd, mean_daily) * trend_ratio * seasonal_mult[m]

    projected_total = total_income + projected_remaining

    # Limit dates with seasonality
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
            m = future_date.month
            cumulative += weekday_avg.get(wd, mean_daily) * trend_ratio * seasonal_mult[m]
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

        fop_stores, monthly_store_income = _fetch_fop_stores(conn, year)
        logger.info("Магазини: зв'язки для %d ФОПів", len(fop_stores))

        fop_groups = _fetch_fop_groups(conn)

        fop_statuses = _fetch_fop_statuses(conn)

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

        # Terminal changes
        tc = terminal_changes.get(fop_id, {})

        org_status = fop_statuses.get(fop_id, "Відкрита")
        organization = _determine_organization(stores)
        monthly = monthly_history.get(fop_id, [])

        fop_entry = {
            "fop_name": fop["name"].strip(),
            "fop_edrpou": edrpou,
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
                "terminal_change": fop_entry["terminal_change"],
                "terminal_change_percent": fop_entry["terminal_change_percent"],
            })

    critical_all = sum(1 for f in all_fops_report if f["status"] != "ok")
    critical_in_progress = sum(1 for f in all_fops_report if f["status"] == "in_progress")

    logger.info(
        "Критичних ФОПів: %d всього (%d нових, %d вже в роботі)",
        critical_all, len(critical_fops), critical_in_progress,
    )

    # JSON report — all FOPs sorted by days_to_limit (most urgent first)
    all_fops_report.sort(key=lambda f: f["days_to_limit"])

    # ── Store-level report ────────────────────────────────────────────
    store_agg: dict[str, dict] = {}
    for fop_entry in all_fops_report:
        for s in fop_entry.get("stores", []):
            name = s["name"]
            if name not in store_agg:
                store_agg[name] = {
                    "subdivision": name,
                    "source": s.get("source", ""),
                    "total_income": 0.0,
                    "fops": [],
                }
            store_agg[name]["total_income"] += s.get("total", 0)
            store_agg[name]["fops"].append({
                "fop_name": fop_entry["fop_name"],
                "fop_edrpou": fop_entry["fop_edrpou"],
                "income_from_store": s.get("total", 0),
                "days_to_limit": fop_entry["days_to_limit"],
                "organization": fop_entry.get("organization", ""),
            })

    # Determine current month for growth calculation
    current_month = today.month
    prev_month = current_month - 1  # 0 means January → no prev data

    # Determine organization per store from prefix + enrich with new fields
    stores_report = []
    for name, data in sorted(store_agg.items(), key=lambda x: -x[1]["total_income"]):
        m = re.match(r'^(\d{3})\s', name)
        if m:
            pfx = int(m.group(1))
            if 500 <= pfx < 600:
                org = "Технопростір"
            elif 600 <= pfx < 700 or 900 <= pfx < 1000:
                org = "ФАМО"
            else:
                org = ""
        else:
            org = ""
        data["company"] = org
        data["total_income"] = round(data["total_income"], 2)

        # ── Current FOP ──
        bindings = terminal_bindings.get(name, [])
        # Fallback: match by subdivision code (first 3 digits) if exact name miss
        if not bindings and m:
            code = m.group(1)
            for tb_name, tb_bindings in terminal_bindings.items():
                if tb_name.startswith(code + " "):
                    bindings = tb_bindings
                    break
        current_fop_name, current_fop_edrpou = _determine_current_fop(
            bindings, data["fops"]
        )
        data["current_fop_name"] = current_fop_name
        data["current_fop_edrpou"] = current_fop_edrpou

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
            bindings, year, current_fop_name
        )

        # ── FOP switch count: unique dates with binding events in current year ──
        year_start = date(year, 1, 1)
        switch_dates = set()
        for b in bindings:
            d = _parse_binding_date(b["date"])
            if d and d >= year_start:
                switch_dates.add(d)
        data["fop_count"] = len(switch_dates)

        stores_report.append(data)

    logger.info("Звіт по магазинах: %d магазинів", len(stores_report))

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
        "critical_count": len(critical_fops),
        "critical_fops": critical_fops,
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
