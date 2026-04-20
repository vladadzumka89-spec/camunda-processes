"""Shared FOP data access — constants, DB connection, fetch & analysis functions.

Used by fop_monitor (daily limit check) and fop_planner (opening schedule).
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from collections import defaultdict
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

BAS_YEAR_OFFSET = 2000

EP_GROUP_ENUM = {
    bytes.fromhex("80907066F89BCA3447EBEB86FEF433E2"): 1,
    bytes.fromhex("A80C9C2A3B0E352146FAFF2E22E417BC"): 2,
    bytes.fromhex("BD853EFB6C04CB6D42A4D31D78D446DA"): 3,
}

LIMITS = {
    2: float(os.environ.get("FOP_LIMIT_GROUP_2", "6600000")),
    3: float(os.environ.get("FOP_LIMIT_GROUP_3", "6600000")),
}

REPORT_DIR = Path(os.environ.get("FOP_REPORT_DIR", "reports/fop"))
REPORT_FILE = REPORT_DIR / "latest.json"

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

        UNION

        SELECT
            o._IDRRef AS id,
            o._Description AS name,
            RTRIM(o._Fld1495) AS full_name,
            o._Fld1494 AS edrpou
        FROM _Reference90 o
        JOIN _Reference90_VT1523 vt ON vt._Reference90_IDRRef = o._IDRRef
        JOIN _Reference59 r59 ON r59._IDRRef = vt._Fld1526_RRRef
        WHERE vt._Fld1526_RTRef = 0x0000003b
            AND RTRIM(r59._Description) IN (N'ТП', N'ФАМО')
            AND o._Marked = 0x00
            AND o._Description NOT LIKE N'яяя%%'
            AND o._Description NOT LIKE N'%%ТОВ%%'
            AND o._Description NOT LIKE N'%%Техно Простір%%'
            AND o._Description NOT LIKE N'%%Плюс Технопростір%%'
            AND o._Description NOT LIKE N'%%ПКцентр%%'
            AND NOT EXISTS (
                SELECT 1 FROM _Reference90_VT1523 vt_st
                WHERE vt_st._Reference90_IDRRef = o._IDRRef
                  AND vt_st._Fld1525RRef = 0x85d7ec0d9a794f5211ed6f042b93621a
                  AND vt_st._Fld1526_RRRef = 0x85d7ec0d9a794f5211ed6f042b93621b
            )

        ORDER BY name
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
        SELECT r._Fld10619RRef AS org_id,
               CAST(DATEADD(year, -2000, r._Period) AS date) AS doc_date,
               SUM(r._Fld10621) AS daily_total,
               COUNT(*) AS doc_count
        FROM _AccumRg10618 r
        JOIN _Reference90 o ON o._IDRRef = r._Fld10619RRef
        WHERE r._Period >= %s AND r._Period < %s
          AND r._Active = 0x01
          AND o._Marked = 0x00
          AND o._Description NOT LIKE N'яяя%%'
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


def _fetch_q4_prev_year_income(conn, year: int) -> dict[bytes, float]:
    """Fetch total FOP income for Q4 of previous year (Oct-Dec).

    Uses the same _AccumRg10618 register as _fetch_daily_income.
    Returns: {org_id_bytes: total_q4_income}
    """
    prev_year = year - 1
    bas_q4_start = f"{prev_year + BAS_YEAR_OFFSET}-10-01"
    bas_q4_end = f"{prev_year + BAS_YEAR_OFFSET + 1}-01-01"
    sql = """
        SELECT r._Fld10619RRef AS org_id,
               SUM(r._Fld10621) AS total
        FROM _AccumRg10618 r
        WHERE r._Period >= %s AND r._Period < %s
          AND r._Active = 0x01
        GROUP BY r._Fld10619RRef
    """
    cursor = conn.cursor(as_dict=True)
    try:
        cursor.execute(sql, (bas_q4_start, bas_q4_end))
        result = {}
        for row in cursor:
            org_id = bytes(row["org_id"])
            total = float(row["total"] or 0)
            if total != 0:
                result[org_id] = total
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


# English terminal names → Ukrainian equivalents for matching
_EN_TO_UKR = {
    "city": "сіті", "center": "центр", "centre": "центр",
    "forum": "форум", "global": "глобал", "island": "острів",
    "depot": "депот", "dream": "дрім", "town": "таун",
    "park": "парк", "plaza": "плаза", "mall": "мол",
    "smart": "смарт", "novus": "новус", "quarter": "квартал",
    "happy": "щасливий", "republic": "республіка",
}


def _normalize_terminal_name(name: str) -> str:
    """Normalize terminal name: replace English words with Ukrainian equivalents."""
    words = name.lower().split()
    return " ".join(_EN_TO_UKR.get(w, w) for w in words)


def _fetch_subdivision_lookup(conn) -> dict[str, str]:
    """Build {translit_word: full_description} lookup from _Reference100
    (Підрозділи організацій) — the catalog referenced by terminal codes
    in bank statement payment purposes. Falls back to _Reference116
    (Структурні одиниці) for entries not found in _Reference100.
    """
    # Primary source: _Reference100 (Підрозділи організацій)
    # This is the catalog that terminal codes in bank statements refer to
    sql_r100 = """
        SELECT DISTINCT _Description FROM _Reference100
        WHERE _Marked = 0x00
          AND _Description LIKE N'[0-9][0-9][0-9] %'
    """
    # Secondary source: _Reference116 (Структурні одиниці)
    sql_r116 = """
        ;WITH store_roots AS (
            SELECT _IDRRef FROM _Reference116
            WHERE _Description IN (N'500 Магазини', N'600 Магазини',
                                   N'900 Пінкі', N'900 Пінкі  Сайт',
                                   N'Фамо обладнання')
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
                                     N'900 Пінкі', N'900 Пінкі  Сайт',
                                     N'Фамо обладнання')
    """
    cursor = conn.cursor(as_dict=True)
    try:
        lookup = {}  # translit_key -> description
        all_subs = []  # [(description, set_of_translit_words)]
        code_lookup = {}  # "601" -> "601 Квартал Хмель"

        seen_descs = set()  # avoid exact duplicate descriptions

        def _process_row(desc: str) -> None:
            if desc in seen_descs:
                return
            seen_descs.add(desc)

            code_m = re.match(r'^(\d{3})\s', desc)
            if not code_m:
                return
            code = code_m.group(1)
            # First entry wins for direct code lookup
            if code not in code_lookup:
                code_lookup[code] = desc

            # Always add to word matching (multiple names per code is OK)
            name_part = re.sub(r'^\d+\s*', '', desc).strip()
            if not name_part:
                return
            words = [w for w in name_part.split() if len(w) >= 2]
            translit_words = set()
            for w in words:
                tw = _translit_ukr(w)
                if len(tw) >= 2:
                    translit_words.add(tw)
            all_subs.append((desc, translit_words))
            for tw in translit_words:
                if len(tw) >= 4:
                    lookup[tw] = desc
            full_translit = _translit_ukr(name_part).replace(' ', '')
            if full_translit:
                lookup[full_translit] = desc

        # 1) Primary: _Reference100 (terminal codes reference this catalog)
        cursor.execute(sql_r100)
        r100_count = 0
        for row in cursor:
            desc = row["_Description"].strip()
            _process_row(desc)
            r100_count += 1
        logger.info("_Reference100: %d записів, %d з кодами", r100_count, len(code_lookup))

        # 2) Secondary: _Reference116 (fill gaps not covered by _Reference100)
        r116_before = len(code_lookup)
        cursor.execute(sql_r116)
        for row in cursor:
            desc = row["_Description"].strip()
            _process_row(desc)
        logger.info(
            "_Reference116: додано %d нових кодів (було %d, стало %d)",
            len(code_lookup) - r116_before, r116_before, len(code_lookup),
        )

        return {"word_lookup": lookup, "subdivisions": all_subs, "code_lookup": code_lookup}
    finally:
        cursor.close()


def _fetch_disbanded_subdivision_codes(conn) -> set[str]:
    """Fetch 3-digit codes of disbanded subdivisions from _Reference100.

    A code is considered disbanded if >= 90% of its entries are either
    flagged as disbanded (_Fld27513 = 0x01) or under "Неактуальні" parent.
    This handles cases where a few old employee entries lack the flag.
    """
    cursor = conn.cursor(as_dict=True)
    try:
        cursor.execute("""
            SELECT
                SUBSTRING(d._Description, 1, 3) AS code,
                CASE WHEN d._Fld27513 = 0x01 THEN 1
                     WHEN EXISTS (
                         SELECT 1 FROM _Reference100 p
                         WHERE p._IDRRef = d._ParentIDRRef
                           AND p._Description LIKE N'Неактуальні%'
                     ) THEN 1
                     ELSE 0
                END AS is_disbanded
            FROM _Reference100 d
            WHERE d._Description LIKE N'[0-9][0-9][0-9] %'
              AND d._Marked = 0x00
        """)
        from collections import defaultdict as _dd
        code_stats: dict[str, list[int]] = _dd(lambda: [0, 0])  # [disbanded, active]
        for row in cursor:
            code = row["code"].strip()
            if row["is_disbanded"] == 1:
                code_stats[code][0] += 1
            else:
                code_stats[code][1] += 1
        # Code is disbanded if >= 90% entries have the flag/inactive parent
        codes = set()
        for code, (d, a) in code_stats.items():
            total = d + a
            if total > 0 and d / total >= 0.9:
                codes.add(code)
        logger.info(
            "Розформовані підрозділи: %d (%s)",
            len(codes), ", ".join(sorted(codes)) or "немає",
        )
        return codes
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
    preferred_prefix: str | None = None,
) -> str | None:
    """Match English terminal name to Ukrainian subdivision from _Reference116.

    Args:
        preferred_prefix: '5' for ТП, '6' for ФАМО — used to disambiguate
            when the same terminal name matches multiple subdivisions.
    """
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
        wl_match = word_lookup[tn_nospace]
        # If no preference or match already has the right prefix, use it
        if not preferred_prefix or wl_match[:1] == preferred_prefix:
            return wl_match
        # Otherwise fall through to scoring which respects preferred_prefix

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
    best_pref = False  # tracks if best match has preferred prefix

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
                tw_norm = tw.replace('y', 'i')
                sw_norm = sw.replace('y', 'i')
                # Exact or containment match → 2 points
                if (len(tw) >= 3 and len(sw) >= 3 and
                    (tw in sw or sw in tw or tw_norm in sw_norm or sw_norm in tw_norm)):
                    score += 2
                    break
                # Prefix match (first 4 chars) → 1 point
                if (len(tw) >= 4 and len(sw) >= 4 and
                    (tw[:4] == sw[:4] or tw_norm[:4] == sw_norm[:4])):
                    score += 1
                    break
        if score <= 0:
            continue
        # Check if this subdivision matches the preferred company prefix
        has_pref = bool(preferred_prefix and desc[:1] == preferred_prefix)
        sub_size = len(sub_words)
        # Pick this match if: better score, or same score + preferred prefix wins,
        # or same score + same pref + more specific (fewer words)
        if (score > best_score
            or (score == best_score and has_pref and not best_pref)
            or (score == best_score and has_pref == best_pref
                and sub_size < best_sub_size)):
            best_score = score
            best_match = desc
            best_sub_size = sub_size
            best_pref = has_pref

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
    recent_cutoff = date.today() - timedelta(days=14)

    sql_payments = """
        SELECT
            d._Fld6004RRef AS org_id,
            d._Fld6019 AS purpose,
            d._Fld6010 AS amount,
            MONTH(DATEADD(year, -2000, d._Date_Time)) AS mn,
            DATEADD(year, -2000, d._Date_Time) AS pay_date
        FROM _Document236 d
        JOIN _Reference90 o ON d._Fld6004RRef = o._IDRRef
        JOIN _Document236_VT6023 vt ON vt._Document236_IDRRef = d._IDRRef
        JOIN _Reference129 r129 ON r129._IDRRef = vt._Fld6037RRef
        WHERE d._Posted = 0x01 AND d._Marked = 0x00
            AND d._Date_Time >= %s AND d._Date_Time < %s
            AND o._Marked = 0x00
            AND o._Description NOT LIKE N'яяя%%'
            AND r129._Description = N'Стоимость проданных товаров (работ, услуг)'
    """
    cursor = conn.cursor(as_dict=True)
    try:
        import time as _time
        _t0 = _time.time()
        cursor.execute(sql_payments, (bas_start, bas_end))
        logger.info("sql_payments виконано за %.1f сек", _time.time() - _t0)

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
            pay_date = row["pay_date"]
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
                cur_last = other_income[org_id][label].get("last_date")
                if pay_date and (cur_last is None or pay_date > cur_last):
                    other_income[org_id][label]["last_date"] = pay_date
                if pay_date and hasattr(pay_date, 'date') and pay_date.date() >= recent_cutoff:
                    other_income[org_id][label]["recent_income"] = (
                        other_income[org_id][label].get("recent_income", 0) + amount
                    )
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
                AND o._Description NOT LIKE N'яяя%%'
            GROUP BY d._Fld6492RRef
        """
        cash_data = {}
        _t1 = _time.time()
        logger.info("Початок sql_cash...")
        cursor.execute(sql_cash, (bas_start, bas_end))
        for row in cursor:
            org_id = bytes(row["org_id"])
            cash_data[org_id] = {
                "count": row["cnt"],
                "total": float(row["total"] or 0),
            }

        logger.info("sql_cash виконано за %.1f сек", _time.time() - _t1)

        _t2 = _time.time()
        logger.info("Початок sql_docs...")
        sql_docs = """
            ;WITH store_roots AS (
                SELECT _IDRRef FROM _Reference116
                WHERE _Description IN (N'500 Магазини', N'600 Магазини',
                                       N'900 Пінкі', N'900 Пінкі  Сайт',
                                       N'Фамо обладнання')
            ),
            store_tree AS (
                SELECT _IDRRef FROM _Reference116
                WHERE _IDRRef IN (SELECT _IDRRef FROM store_roots)
                UNION ALL
                SELECT c._IDRRef FROM _Reference116 c
                JOIN store_tree t ON c._ParentIDRRef = t._IDRRef
            ),
            known_stores AS (
                SELECT _IDRRef FROM store_tree
                UNION
                SELECT _IDRRef FROM _Reference100
                WHERE _Description LIKE N'[0-9][0-9][0-9] %' AND _Marked = 0x00
            ),
            fop_filter AS (
                SELECT _IDRRef FROM _Reference90
                WHERE _Marked = 0x00
                    AND _Description NOT LIKE N'яяя%%'
            ),
            all_stores AS (
                SELECT d._Fld6686RRef AS org_id, d._Fld6687RRef AS store_id,
                       COUNT(*) AS doc_count, SUM(d._Fld6704) AS total_sum
                FROM _Document247 d
                WHERE d._Posted = 0x01 AND d._Marked = 0x00
                    AND d._Date_Time >= %s AND d._Date_Time < %s
                    AND d._Fld6686RRef IN (SELECT _IDRRef FROM fop_filter)
                    AND d._Fld6687RRef IN (SELECT _IDRRef FROM known_stores)
                GROUP BY d._Fld6686RRef, d._Fld6687RRef

                UNION ALL

                SELECT d._Fld6103RRef, d._Fld6104RRef,
                       COUNT(*), SUM(d._Fld6119)
                FROM _Document238 d
                WHERE d._Posted = 0x01 AND d._Marked = 0x00
                    AND d._Date_Time >= %s AND d._Date_Time < %s
                    AND d._Fld6103RRef IN (SELECT _IDRRef FROM fop_filter)
                    AND d._Fld6104RRef IN (SELECT _IDRRef FROM known_stores)
                GROUP BY d._Fld6103RRef, d._Fld6104RRef

                UNION ALL

                SELECT d._Fld5008RRef, d._Fld5011RRef,
                       COUNT(*), SUM(d._Fld5016)
                FROM _Document213 d
                WHERE d._Posted = 0x01 AND d._Marked = 0x00
                    AND d._Date_Time >= %s AND d._Date_Time < %s
                    AND d._Fld5008RRef IN (SELECT _IDRRef FROM fop_filter)
                    AND d._Fld5011RRef IN (SELECT _IDRRef FROM known_stores)
                GROUP BY d._Fld5008RRef, d._Fld5011RRef
            )
            ,resolved AS (
                SELECT a.org_id, a.doc_count, a.total_sum,
                       COALESCE(
                           (SELECT TOP 1 MIN(x._Description) FROM _Reference100 x
                            WHERE x._IDRRef = a.store_id
                              AND x._Description LIKE N'[0-9][0-9][0-9] %'
                              AND x._Marked = 0x00),
                           r116._Description
                       ) AS store_name
                FROM all_stores a
                LEFT JOIN _Reference116 r116 ON a.store_id = r116._IDRRef
            )
            SELECT org_id, store_name,
                   SUM(doc_count) AS doc_count, SUM(total_sum) AS total_sum
            FROM resolved
            WHERE store_name IS NOT NULL
            GROUP BY org_id, store_name
            ORDER BY org_id, SUM(total_sum) DESC
        """
        cursor.execute(sql_docs, (bas_start, bas_end, bas_start, bas_end, bas_start, bas_end))

        doc_data = defaultdict(list)
        doc_row_count = 0
        for row in cursor:
            org_id = bytes(row["org_id"])
            doc_data[org_id].append({
                "name": row["store_name"].strip(),
                "doc_count": row["doc_count"],
                "total": float(row["total_sum"] or 0),
            })
            doc_row_count += 1
        logger.info(
            "sql_docs виконано за %.1f сек: %d записів, %d ФОП",
            _time.time() - _t2, doc_row_count, len(doc_data),
        )

        # Build terminal → subdivision mapping
        _t3 = _time.time()
        sub_data = _fetch_subdivision_lookup(conn)
        logger.info(
            "subdivision_lookup за %.1f сек: %d записів, %d підрозділів, коди: %s",
            _time.time() - _t3,
            len(sub_data["word_lookup"]),
            len(sub_data["subdivisions"]),
            sorted(sub_data["code_lookup"].keys()),
        )

        # Fetch active terminal bindings: FOP → stores (for code-based matching)
        active_bindings = _fetch_active_terminal_bindings_by_org(conn)
        logger.info("Активні прив'язки терміналів: %d ФОПів", len(active_bindings))

        # Fetch FOP company mapping (ТП/ФАМО) for subdivision disambiguation
        fop_companies = _fetch_fop_companies(conn)
        _COMPANY_PREFIX = {"Технопростір": "5", "ФАМО": "6"}
        # Build org_id → preferred_prefix: quick lookup of FOP names
        _org_prefix: dict[bytes, str | None] = {}
        cursor2 = conn.cursor(as_dict=True)
        cursor2.execute("SELECT _IDRRef AS id, RTRIM(_Description) AS name FROM _Reference90")
        for _r in cursor2:
            _name = _r["name"]
            _comp = fop_companies.get(_name)
            if _comp:
                _org_prefix[bytes(_r["id"])] = _COMPANY_PREFIX.get(_comp)
        cursor2.close()

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
                pref = _org_prefix.get(org_id)
                for name, info in sorted(
                    terminal_data[org_id].items(), key=lambda x: -x[1]["total"]
                ):
                    subdivision = None
                    _bound_set = set(active_bindings.get(org_id, []))
                    # 2a) Try direct code lookup from payment text
                    if info["code"] and info["code"] in code_lookup:
                        candidate = code_lookup[info["code"]]
                        # If FOP has bindings, validate code against them
                        if _bound_set and candidate not in _bound_set:
                            pass  # Code doesn't match binding — skip, let binding handle
                        else:
                            subdivision = candidate
                    # 2a2) Try binding register: FOP → stores (PRIORITY over code in name)
                    if not subdivision and _bound_set:
                        bound_stores = active_bindings[org_id]
                        if len(bound_stores) == 1:
                            subdivision = bound_stores[0]
                        elif len(bound_stores) > 1:
                            # Constrained matching against bound stores only
                            bound_codes = {}
                            for bs in bound_stores:
                                m = re.match(r'^(\d{3})\s', bs)
                                if m:
                                    bound_codes[m.group(1)] = bs
                            if info["code"] and info["code"] in bound_codes:
                                subdivision = bound_codes[info["code"]]
                            else:
                                # Score each bound store: count matching words
                                name_t = _translit_ukr(name).lower()
                                name_norm = _translit_ukr(_normalize_terminal_name(name))
                                # Split on spaces AND digits to handle concatenated names like "FAMO604Oazis"
                                name_variants = [w for nt in (name_t, name_norm, name.lower()) for w in nt.split() if len(w) >= 3]
                                for _src in (name, name_t, name_norm):
                                    for _part in re.split(r'[\d]+', _src):
                                        _tw = _translit_ukr(_part).lower().strip()
                                        if len(_tw) >= 3 and _tw not in name_variants:
                                            name_variants.append(_tw)
                                is_pinky = any(w in ("pinky", "pinki", "пінкі") for w in name.lower().split())
                                best_score = 0
                                best_store = None
                                for bs in bound_stores:
                                    # Skip non-standard entries (e.g., "[Фамо]")
                                    if not re.match(r'^\d{3}\s', bs):
                                        continue
                                    bs_clean = re.sub(r'[().]', '', bs)
                                    bs_t = _translit_ukr(bs_clean).lower()
                                    bs_words = [w for w in bs_t.split() if len(w) >= 3]
                                    score = 0
                                    for nw in name_variants:
                                        for sw in bs_words:
                                            if sw.startswith(nw[:3]) or nw.startswith(sw[:3]):
                                                score += 1
                                    # Bonus: if terminal name contains Pinky, prefer Пінкі store (starts with "NNN П ")
                                    if is_pinky and re.match(r'^\d{3} П ', bs):
                                        score += 5
                                    # Tie-breaker: prefer longer name (more specific store)
                                    if score > best_score or (score == best_score and score > 0 and len(bs) > len(best_store or "")):
                                        best_score = score
                                        best_store = bs
                                if best_store:
                                    subdivision = best_store
                    # 2a1) Try extracting 3-digit code from terminal name itself
                    if not subdivision:
                        _code_in_name = re.search(r'(\d{3})', name)
                        if _code_in_name and _code_in_name.group(1) in code_lookup:
                            subdivision = code_lookup[_code_in_name.group(1)]
                    # 2a3) If FOP has bindings but no match — try unmatched bound stores
                    if not subdivision and org_id in active_bindings:
                        already_resolved = {s["name"] for s in result.get(org_id, []) if s.get("source") == "terminal"}
                        unmatched_bound = [
                            bs for bs in active_bindings[org_id]
                            if bs not in already_resolved
                        ]
                        if len(unmatched_bound) == 1:
                            subdivision = unmatched_bound[0]
                    # 2b) Fallback: transliteration matching against all subdivisions
                    if not subdivision:
                        subdivision = _match_terminal_to_subdivision(name, sub_data, preferred_prefix=pref)
                    # 2b2) PINKY with binding: if FOP has Пінкі stores bound,
                    # exclude those already matched to other terminals for this FOP
                    if not subdivision and name.lower().strip() in ("pinky", "pinki") and org_id in active_bindings:
                        already_resolved = {s["name"] for s in result.get(org_id, []) if s.get("source") == "terminal"}
                        pinky_bound = [
                            bs for bs in active_bindings[org_id]
                            if re.match(r'^\d{3} П ', bs) and bs not in already_resolved
                        ]
                        if len(pinky_bound) == 1:
                            subdivision = pinky_bound[0]
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

            # 2-pass: re-resolve terminals matched to stores OUTSIDE bindings
            if org_id in active_bindings:
                bound_set = set(active_bindings[org_id])
                correctly_resolved = {s["name"] for s in result[org_id] if s.get("source") == "terminal" and s["name"] in bound_set}
                wrong_resolved = [s for s in result[org_id] if s.get("source") == "terminal" and re.match(r'^\d{3}\s', s["name"]) and s["name"] not in bound_set]
                unmatched_bound = [bs for bs in bound_set if bs not in correctly_resolved]
                if len(unmatched_bound) == 1 and len(wrong_resolved) == 1:
                    wrong_resolved[0]["name"] = unmatched_bound[0]

            # 3) Non-cmps payments (Mono, LiqPay, NovaPay, other)
            if org_id in other_income:
                for cat_name, info in sorted(
                    other_income[org_id].items(), key=lambda x: -x[1]["total"]
                ):
                    if info["total"] > 0:
                        _ld = info.get("last_date")
                        result[org_id].append({
                            "name": cat_name,
                            "doc_count": info["count"],
                            "total": info["total"],
                            "source": "payment",
                            "last_date": _ld.isoformat() if _ld else None,
                            "recent_income": info.get("recent_income", 0),
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
                    _bound_set_m = set(active_bindings.get(org_id, []))
                    if info["code"] and info["code"] in code_lookup_m:
                        candidate = code_lookup_m[info["code"]]
                        if _bound_set_m and candidate not in _bound_set_m:
                            pass  # Code doesn't match binding — skip
                        else:
                            resolved = candidate
                    # Binding register lookup (PRIORITY over code in name)
                    if not resolved and org_id in active_bindings:
                        bound_stores = active_bindings[org_id]
                        if len(bound_stores) == 1:
                            resolved = bound_stores[0]
                        elif len(bound_stores) > 1:
                            bound_codes = {}
                            for bs in bound_stores:
                                bm = re.match(r'^(\d{3})\s', bs)
                                if bm:
                                    bound_codes[bm.group(1)] = bs
                            if info["code"] and info["code"] in bound_codes:
                                resolved = bound_codes[info["code"]]
                            else:
                                name_t = _translit_ukr(raw_name).lower()
                                name_norm = _translit_ukr(_normalize_terminal_name(raw_name))
                                name_variants = [w for nt in (name_t, name_norm, raw_name.lower()) for w in nt.split() if len(w) >= 3]
                                for _src in (raw_name, name_t, name_norm):
                                    for _part in re.split(r'[\d]+', _src):
                                        _tw = _translit_ukr(_part).lower().strip()
                                        if len(_tw) >= 3 and _tw not in name_variants:
                                            name_variants.append(_tw)
                                is_pinky = any(w in ("pinky", "pinki", "пінкі") for w in raw_name.lower().split())
                                best_score = 0
                                best_store = None
                                for bs in bound_stores:
                                    bs_clean = re.sub(r'[().]', '', bs)
                                    bs_t = _translit_ukr(bs_clean).lower()
                                    bs_words = [w for w in bs_t.split() if len(w) >= 3]
                                    score = 0
                                    for nw in name_variants:
                                        for sw in bs_words:
                                            if sw.startswith(nw[:3]) or nw.startswith(sw[:3]):
                                                score += 1
                                    if is_pinky and re.match(r'^\d{3} П ', bs):
                                        score += 5
                                    if score > best_score:
                                        best_score = score
                                        best_store = bs
                                if best_store:
                                    resolved = best_store
                    if not resolved:
                        resolved = _match_terminal_to_subdivision(raw_name, sub_data, preferred_prefix=_org_prefix.get(org_id))
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


_fop_company_cache: dict[str, str] = {}


def _fetch_fop_companies(conn) -> dict[str, str]:
    """Fetch FOP → company mapping from BAS 'Принадлежність' attribute.

    Source: _Reference90_VT1523 (Додаткові реквізити організацій)
    Values: _Reference59 ('ТП' → Технопростір, 'ФАМО' → ФАМО)

    Returns: {fop_name: 'Технопростір'|'ФАМО'}
    """
    sql = """
        SELECT
            RTRIM(o._Description) AS fop_name,
            RTRIM(r59._Description) AS company
        FROM _Reference90 o
        JOIN _Reference90_VT1523 vt ON vt._Reference90_IDRRef = o._IDRRef
        JOIN _Reference59 r59 ON r59._IDRRef = vt._Fld1526_RRRef
        WHERE vt._Fld1526_RTRef = 0x0000003b
          AND RTRIM(r59._Description) IN (N'ТП', N'ФАМО')
          AND o._Marked = 0x00
    """
    _COMPANY_MAP = {"ТП": "Технопростір", "ФАМО": "ФАМО"}
    cursor = conn.cursor(as_dict=True)
    try:
        cursor.execute(sql)
        result = {}
        for row in cursor:
            name = row["fop_name"].strip()
            raw = row["company"].strip()
            result[name] = _COMPANY_MAP.get(raw, raw)
        logger.info(
            "Принадлежність ФОП з БАС: %d ТП, %d ФАМО",
            sum(1 for v in result.values() if v == "Технопростір"),
            sum(1 for v in result.values() if v == "ФАМО"),
        )
        return result
    except Exception as e:
        logger.warning("Не вдалося завантажити Принадлежність ФОП: %s", e)
        return {}
    finally:
        cursor.close()


def _determine_organization(fop_name: str) -> str:
    """Classify FOP as ФАМО or Технопростір using BAS data."""
    name = fop_name.strip()
    if name in _fop_company_cache:
        return _fop_company_cache[name]
    return "ФАМО"


def _determine_store_company(subdivision_name: str) -> str:
    """Determine company by subdivision code prefix.

    500-series → Технопростір
    600-series, 900-series → ФАМО
    No code → empty string (cannot determine)
    """
    m = re.match(r'^(\d)', subdivision_name)
    if not m:
        return ""
    first_digit = m.group(1)
    if first_digit == "5":
        return "Технопростір"
    if first_digit in ("6", "9"):
        return "ФАМО"
    return ""


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


def _fetch_active_terminal_bindings_by_org(conn) -> dict[bytes, list[str]]:
    """Fetch currently active terminal bindings: FOP org_id → list of store names.

    Uses the same register as _fetch_terminal_bindings (_InfoRg28391),
    but returns {org_id_bytes: [store_name, ...]} for active bindings only
    (value_date year >= 2090 = connected).

    This allows matching terminal codes to stores via the FOP binding,
    instead of relying on fuzzy name matching.
    """
    sql = """
        ;WITH latest AS (
            SELECT t._Fld28392RRef AS org_id,
                   t._Fld28396RRef AS store_id,
                   t._Fld28394_T AS value_dt,
                   ROW_NUMBER() OVER (
                       PARTITION BY t._Fld28392RRef, t._Fld28396RRef
                       ORDER BY t._Period DESC
                   ) AS rn
            FROM _InfoRg28391 t
            WHERE t._Fld28393 = N'Терминал'
        )
        SELECT l.org_id,
               r100._Description AS store_name
        FROM latest l
        JOIN _Reference100 r100 ON r100._IDRRef = l.store_id
        WHERE l.rn = 1
          AND DATEADD(year, -2000, l.value_dt) >= '2090-01-01'
    """
    cursor = conn.cursor(as_dict=True)
    try:
        cursor.execute(sql)
        result: dict[bytes, list[str]] = defaultdict(list)
        for row in cursor:
            org_id = bytes(row["org_id"])
            store = (row["store_name"] or "").strip()
            if store and store not in result[org_id]:
                result[org_id].append(store)
        return dict(result)
    except Exception as e:
        logger.warning("Не вдалося завантажити активні прив'язки терміналів: %s", e)
        return {}
    finally:
        cursor.close()


def _fetch_store_employees(conn) -> dict[str, list[dict]]:
    """Fetch current employees per store from BAS.

    Source: РегістрВідомостей.ТекущіКадровіДаніСпівробітників (_InfoRg23178).
    Filter: ДатаЗвільнення < '2002-01-01' (empty = currently employed).

    Fields:
        _Fld23180RRef → _Reference120 (Співробітник)
        _Fld23182RRef → _Reference90  (Поточна Організація / ФОП)
        _Fld23183     → ДатаПрийому
        _Fld23184     → ДатаЗвільнення (0001-01-01 = працює)
        _Fld23188RRef → _Reference100 (Поточний Підрозділ / магазин)

    Returns:
        dict: department_name → list of {name, employer_fop, employer_edrpou}
    """
    sql = """
        SELECT
            emp._Description     AS employee_name,
            org._Description     AS employer_fop_name,
            org._Fld1494         AS employer_edrpou,
            dept._Description    AS department_name
        FROM _InfoRg23178 r
        JOIN _Reference120 emp  ON r._Fld23180RRef = emp._IDRRef
        JOIN _Reference90  org  ON r._Fld23182RRef = org._IDRRef
        JOIN _Reference100 dept ON r._Fld23188RRef = dept._IDRRef
        WHERE DATEADD(year, -2000, r._Fld23184) < '2002-01-01'
    """
    cursor = conn.cursor(as_dict=True)
    try:
        cursor.execute(sql)
        result: dict[str, list[dict]] = defaultdict(list)
        for row in cursor:
            dept = (row["department_name"] or "").strip()
            if not dept:
                continue
            result[dept].append({
                "name": (row["employee_name"] or "").strip(),
                "employer_fop": (row["employer_fop_name"] or "").strip(),
                "employer_edrpou": (row["employer_edrpou"] or "").strip(),
            })
        return dict(result)
    except Exception as e:
        logger.warning(
            "Не вдалося завантажити працівників магазинів: %s", e
        )
        return {}
    finally:
        cursor.close()


def _enrich_store_with_employees(
    store_data: dict,
    store_employees: dict[str, list[dict]],
) -> None:
    """Enrich a single store_data dict with employee info and fop_match flags.

    Matching: exact department name first, fallback by 3-digit code prefix.
    Mutates store_data in place: adds employees, employee_count,
    mismatch_count, employees_text.
    """
    name = store_data["subdivision"]
    current_edrpou = store_data.get("current_fop_edrpou", "")

    # Exact match
    emps = store_employees.get(name, [])

    # Fallback: match by first 3-digit code
    if not emps:
        m = re.match(r'^(\d{3})\s', name)
        if m:
            code = m.group(1)
            for dept_name, dept_emps in store_employees.items():
                if dept_name.startswith(code + " "):
                    emps = dept_emps
                    break

    # Group employees by FOP
    fop_groups: dict[str, list[str]] = defaultdict(list)
    for e in emps:
        fop_groups[e["employer_fop"]].append(e["name"])

    lines = []
    for fop_name, emp_names in fop_groups.items():
        lines.append(f"ФОП {fop_name}:")
        for n in emp_names:
            lines.append(f"  {n}")
    store_data["employees"] = "\n".join(lines)


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
    - value_date: interpretation depends on comparison with period:
        * year >= 2090 (31.12.2099) = підключений безстроково
        * value_date > period = тимчасове підключення (від period до value_date)
        * value_date <= period = відключений на цю дату

    Result: [{fop_name, date_from, date_to}, ...] where date_to=None means
    the FOP is currently active.
    """
    if not bindings:
        return []

    # Classify records into connections (permanent + temporary) and disconnections
    connections: list[dict] = []   # permanent (31.12.2099) connections
    temp_connections: list[dict] = []  # temporary (value > period) connections
    disconnections: dict[str, list[str]] = defaultdict(list)  # fop → [dates]

    for b in bindings:
        bd = _parse_binding_date(b["date"])
        vd = _parse_binding_date(b.get("value_date", ""))
        if not bd or bd.year < 2020:
            continue
        if vd and vd.year >= 2090:
            # Permanent connection (31.12.2099)
            connections.append(b)
        elif vd and bd and vd > bd:
            # Temporary connection: value_date is AFTER period = connected until value_date
            temp_connections.append(b)
        elif vd and vd.year >= 2020:
            # Disconnection: value_date <= period
            disconnections[b["fop_name"]].append(b["date"])

    # Build periods from temporary connections (already have both dates)
    periods: list[dict] = []
    for tc in temp_connections:
        periods.append({
            "fop_name": tc["fop_name"],
            "date_from": tc["date"],
            "date_to": tc.get("value_date"),
        })

    if not connections and not periods:
        return []

    # Sort permanent connections by date
    connections.sort(key=lambda b: _parse_binding_date(b["date"]) or date.min)

    # Sort disconnection dates per FOP
    for fop in disconnections:
        disconnections[fop].sort(
            key=lambda d: _parse_binding_date(d) or date.min
        )

    # Build periods from permanent connections: match with disconnections
    disc_idx: dict[str, int] = defaultdict(int)

    for c in connections:
        fop = c["fop_name"]
        date_from = c["date"]
        date_to = None

        disc_dates = disconnections.get(fop, [])
        idx = disc_idx[fop]
        bd = _parse_binding_date(date_from)
        # Find next disconnection that is on or after the connection date
        while idx < len(disc_dates):
            dd = _parse_binding_date(disc_dates[idx])
            if dd and bd and dd >= bd:
                date_to = disc_dates[idx]
                disc_idx[fop] = idx + 1
                break
            idx += 1
            disc_idx[fop] = idx

        periods.append({
            "fop_name": fop,
            "date_from": date_from,
            "date_to": date_to,
        })

    # Sort by date_from
    periods.sort(key=lambda p: _parse_binding_date(p["date_from"]) or date.min)

    # Close stale open periods: if the same FOP has multiple open periods
    # on the same store, keep only the latest one open
    for i in range(len(periods) - 1):
        if periods[i]["date_to"] is None:
            fop_i = periods[i]["fop_name"]
            dt_i = _parse_binding_date(periods[i]["date_from"])
            # Find next connection for the SAME FOP
            for j in range(i + 1, len(periods)):
                if periods[j]["fop_name"] == fop_i:
                    dt_j = _parse_binding_date(periods[j]["date_from"])
                    if dt_j and dt_i and dt_j > dt_i:
                        periods[i]["date_to"] = periods[j]["date_from"]
                    break

    # Filter: keep periods from prev year onwards, skip same-day
    prev_year_start = date(year - 1, 1, 1)
    filtered = []
    for p in periods:
        dt_from = _parse_binding_date(p["date_from"])
        dt_to = _parse_binding_date(p["date_to"]) if p["date_to"] else None
        if dt_to:
            if dt_from and dt_from == dt_to:
                continue
            if dt_from and dt_from >= prev_year_start:
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
    1. Active period from binding history (date_to is None = still connected)
    2. Last connection record (value_date year >= 2090)
    3. Fallback: FOP with highest income_from_store

    Returns: (fop_name, fop_edrpou)
    """
    # Legal entities to exclude when determining current FOP
    _LEGAL_ENTITIES = {"Плюс Технопростір", "Техно Простір", "Технопростір"}

    def _is_fop(name: str) -> bool:
        return name not in _LEGAL_ENTITIES and "ТОВ" not in name

    if bindings:
        # Use _group_binding_periods to properly pair connections/disconnections
        periods = _group_binding_periods(bindings)
        # Find active period (date_to is None = FOP still connected)
        active = [p for p in periods if p["date_to"] is None]
        if active:
            # Take the most recently connected active FOP (skip legal entities)
            active.sort(
                key=lambda p: _parse_binding_date(p["date_from"]) or date.min
            )
            fop_active = [p for p in active if _is_fop(p["fop_name"])]
            current_name = (fop_active or active)[-1]["fop_name"]
        else:
            # No active period — fall back to last connection record
            conn_records = [
                b for b in bindings
                if (vd := _parse_binding_date(b.get("value_date", "")))
                and vd.year >= 2090
            ]
            if conn_records:
                conn_records.sort(
                    key=lambda b: _parse_binding_date(b["date"]) or date.min
                )
                fop_conn = [b for b in conn_records if _is_fop(b["fop_name"])]
                current_name = (fop_conn or conn_records)[-1]["fop_name"]
            else:
                current_name = bindings[-1]["fop_name"]

        for f in fops_list:
            if f["fop_name"] == current_name:
                return current_name, f["fop_edrpou"]
        return current_name, ""

    if fops_list:
        # For payment-type stores (Mono, LiqPay etc.) prefer FOP with
        # the highest recent income (last 14 days) — this correctly picks
        # the currently assigned FOP after a switch.
        fops_with_recent = [f for f in fops_list if f.get("recent_income", 0) > 0]
        if fops_with_recent:
            best = max(fops_with_recent, key=lambda f: f["recent_income"])
        else:
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
        # FOPs with no income still need an analysis entry
        days_elapsed = (today - date(year, 1, 1)).days + 1
        days_remaining = (date(year, 12, 31) - today).days
        limit_dates = {}
        for grp, lim in LIMITS.items():
            limit_dates[grp] = {
                "date": None,
                "already_exceeded": False,
            }
        return {
            "total_income": 0.0,
            "days_elapsed": days_elapsed,
            "days_remaining": days_remaining,
            "trend_ratio": 0.0,
            "mean_daily": 0.0,
            "projected_total": 0.0,
            "limit_dates": limit_dates,
            "active_days": 0,
        }

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


