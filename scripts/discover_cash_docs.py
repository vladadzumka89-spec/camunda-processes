#!/usr/bin/env python3
"""Discover which _Document table contains ПКО (Прихідний касовий ордер) in BAS.

READ-ONLY diagnostic script. Does not modify any data.

Usage:
    python scripts/discover_cash_docs.py

Environment variables (from .env.camunda):
    BAS_DB_HOST (default: deneb)
    BAS_DB_PORT (default: 1433)
    BAS_DB_USER (default: AI_buh)
    BAS_DB_PASSWORD (required)
    BAS_DB_NAME (default: bas_bdu)
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

# Load env from project root
load_dotenv(Path(__file__).resolve().parent.parent / ".env.camunda")

import pymssql


# ── DB connection (same pattern as fop_monitor.py) ─────────────────────


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
    db_config = _get_db_config()
    if not db_config["password"]:
        print("ERROR: BAS_DB_PASSWORD is required", file=sys.stderr)
        sys.exit(1)

    for attempt in range(max_retries):
        try:
            return pymssql.connect(**db_config)
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            delay = initial_delay * (2**attempt)
            print(f"  БД недоступна (спроба {attempt+1}/{max_retries}), повтор через {delay}с: {e}")
            time.sleep(delay)


# ── Phase 1: List all document tables ──────────────────────────────────


def list_document_tables(conn) -> list[dict]:
    """List all _Document tables (via INFORMATION_SCHEMA — no sys permissions needed)."""
    sql = """
        SELECT TABLE_NAME AS name
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
          AND TABLE_NAME LIKE '_Document[0-9]%'
          AND TABLE_NAME NOT LIKE '%VT%'
          AND TABLE_NAME NOT LIKE '%[_]Chrono'
          AND TABLE_NAME NOT LIKE '%[_]Ext%'
        ORDER BY TABLE_NAME
    """
    cursor = conn.cursor(as_dict=True)
    try:
        cursor.execute(sql)
        return list(cursor)
    finally:
        cursor.close()


# ── Phase 2: Get columns for a table ──────────────────────────────────


def get_table_columns(conn, table_name: str) -> list[dict]:
    sql = """
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
    """
    cursor = conn.cursor(as_dict=True)
    try:
        cursor.execute(sql, (table_name,))
        return list(cursor)
    finally:
        cursor.close()


def get_column_names(conn, table_name: str) -> set[str]:
    return {c["COLUMN_NAME"] for c in get_table_columns(conn, table_name)}


# ── Phase 3: Find "Каса" reference tables ─────────────────────────────


def find_kasa_references(conn) -> list[dict]:
    """Find reference tables that contain 'Каса' entries."""
    # Get all _Reference tables with _Description column
    sql_refs = """
        SELECT c.TABLE_NAME AS name
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_NAME LIKE '_Reference[0-9]%'
          AND c.TABLE_NAME NOT LIKE '%VT%'
          AND c.COLUMN_NAME = '_Description'
    """
    cursor = conn.cursor(as_dict=True)
    results = []
    try:
        cursor.execute(sql_refs)
        ref_tables = [r["name"] for r in cursor]

        for ref_table in ref_tables:
            try:
                cursor.execute(
                    f"SELECT TOP 5 _IDRRef, _Description FROM [{ref_table}] "
                    f"WHERE _Description LIKE N'%%Каса%%' OR _Description LIKE N'%%каса%%'"
                )
                rows = list(cursor)
                if rows:
                    results.append({
                        "table": ref_table,
                        "entries": [
                            {"description": r["_Description"].strip()}
                            for r in rows
                        ],
                    })
            except Exception:
                pass  # skip tables with incompatible schema
    finally:
        cursor.close()
    return results


# ── Phase 4: Score candidates ─────────────────────────────────────────


KNOWN_DOC236_FIELDS = {
    "_IDRRef", "_Date_Time", "_Posted", "_Marked", "_Number",
    "_Fld6004RRef",  # organization
    "_Fld6010",       # amount
    "_Fld6019",       # purpose text
}

# Standard document fields present in almost all _Document tables
STANDARD_FIELDS = {"_IDRRef", "_Date_Time", "_Posted", "_Marked", "_Number"}


def score_candidate(cols: set[str]) -> dict:
    """Score how likely a table is to be a financial document (like ПКО)."""
    has_standard = STANDARD_FIELDS.issubset(cols)
    # Count RRef fields (references to other tables)
    rref_cols = {c for c in cols if c.endswith("RRef") and c != "_IDRRef"}
    # Count numeric fields (potential amount fields)
    # We can't tell types from names alone, so just count non-standard fields
    extra_cols = cols - STANDARD_FIELDS

    return {
        "has_standard_fields": has_standard,
        "ref_count": len(rref_cols),
        "rref_cols": rref_cols,
        "total_cols": len(cols),
    }


# ── Phase 5: Detect org reference and amount fields ──────────────────


def detect_org_and_amount(conn, table_name: str, cols: set[str]) -> dict:
    """Try to detect which column is the organization ref and which is the amount."""
    rref_cols = sorted(c for c in cols if c.endswith("RRef") and c != "_IDRRef")

    # Try each RRef column — join to _Reference90 (Organizations)
    cursor = conn.cursor(as_dict=True)
    org_col = None
    try:
        for col in rref_cols:
            try:
                cursor.execute(
                    f"SELECT TOP 1 o._Description "
                    f"FROM [{table_name}] d "
                    f"JOIN _Reference90 o ON d.[{col}] = o._IDRRef "
                    f"WHERE d._Posted = 0x01"
                )
                row = cursor.fetchone()
                if row:
                    org_col = col
                    break
            except Exception:
                pass
    finally:
        cursor.close()

    return {"org_column": org_col}


# ── Phase 6: Check if a column references a "Каса" table ─────────────


def check_kasa_column(conn, table_name: str, rref_cols: set[str], kasa_tables: list[dict]) -> str | None:
    """Check if any RRef column in the table references a Каса reference table."""
    if not kasa_tables:
        return None

    cursor = conn.cursor(as_dict=True)
    try:
        for col in sorted(rref_cols):
            for kt in kasa_tables:
                kasa_table = kt["table"]
                try:
                    cursor.execute(
                        f"SELECT TOP 1 r._Description "
                        f"FROM [{table_name}] d "
                        f"JOIN [{kasa_table}] r ON d.[{col}] = r._IDRRef "
                        f"WHERE d._Posted = 0x01"
                    )
                    row = cursor.fetchone()
                    if row:
                        return f"{col} → {kasa_table} ('{row['_Description'].strip()}')"
                except Exception:
                    pass
    finally:
        cursor.close()
    return None


# ── Phase 7: Sample rows from candidate ──────────────────────────────


def sample_rows(conn, table_name: str, org_col: str | None, limit: int = 5) -> list[dict]:
    """Get sample rows with org name if possible."""
    cursor = conn.cursor(as_dict=True)
    try:
        if org_col:
            # Get all numeric columns (potential amounts)
            cols_info = get_table_columns(conn, table_name)
            numeric_cols = [
                c["COLUMN_NAME"] for c in cols_info
                if c["DATA_TYPE"] in ("numeric", "decimal", "float", "money", "real")
                and not c["COLUMN_NAME"].startswith("_")
                or (c["DATA_TYPE"] in ("numeric", "decimal", "float", "money", "real")
                    and c["COLUMN_NAME"].startswith("_Fld"))
            ]

            select_parts = [
                f"d._Date_Time",
                f"o._Description AS org_name",
            ]
            for nc in numeric_cols[:5]:  # max 5 numeric cols
                select_parts.append(f"d.[{nc}]")

            # Check for text field (purpose/comment)
            text_cols = [
                c["COLUMN_NAME"] for c in cols_info
                if c["DATA_TYPE"] in ("nvarchar", "varchar", "ntext", "text")
                and c["COLUMN_NAME"].startswith("_Fld")
                and c.get("CHARACTER_MAXIMUM_LENGTH") and (
                    c["CHARACTER_MAXIMUM_LENGTH"] == -1
                    or c["CHARACTER_MAXIMUM_LENGTH"] > 50
                )
            ]
            for tc in text_cols[:2]:
                select_parts.append(f"d.[{tc}] AS [{tc}_text]")

            sql = (
                f"SELECT TOP {limit} {', '.join(select_parts)} "
                f"FROM [{table_name}] d "
                f"JOIN _Reference90 o ON d.[{org_col}] = o._IDRRef "
                f"WHERE d._Posted = 0x01 AND d._Marked = 0x00 "
                f"AND (o._Fld1495 LIKE N'%%ізична особа%%' OR o._Fld1495 LIKE N'%%ФОП%%') "
                f"AND o._Description NOT LIKE N'яяя%%' "
                f"ORDER BY d._Date_Time DESC"
            )
            cursor.execute(sql)
        else:
            cursor.execute(
                f"SELECT TOP {limit} * FROM [{table_name}] "
                f"WHERE _Posted = 0x01 ORDER BY _Date_Time DESC"
            )
        return list(cursor)
    except Exception as e:
        return [{"error": str(e)}]
    finally:
        cursor.close()


# ── Main ─────────────────────────────────────────────────────────────


def main():
    print("=" * 70)
    print("  Пошук касових документів (ПКО) в БД BAS")
    print("=" * 70)

    cfg = _get_db_config()
    print(f"\nПідключення: {cfg['server']}:{cfg['port']} / {cfg['database']}")
    conn = _get_connection()
    print("✓ Підключено\n")

    # Phase 1: List all document tables
    print("=" * 70)
    print("  Phase 1: Усі таблиці _Document (з даними)")
    print("=" * 70)
    doc_tables = list_document_tables(conn)
    for dt in doc_tables:
        print(f"  {dt['name']}")
    print(f"\n  Всього таблиць з даними: {len(doc_tables)}\n")

    # Phase 2: Get _Document236 columns as baseline
    print("=" * 70)
    print("  Phase 2: Колонки _Document236 (еталон — банківські платіжки)")
    print("=" * 70)
    cols_236 = get_column_names(conn, "_Document236")
    print(f"  Кількість колонок: {len(cols_236)}")
    rref_236 = sorted(c for c in cols_236 if c.endswith("RRef") and c != "_IDRRef")
    print(f"  Посилання (RRef): {', '.join(rref_236)}")
    print()

    # Phase 3: Find Каса reference tables
    print("=" * 70)
    print("  Phase 3: Пошук довідника 'Каса' в _Reference таблицях")
    print("=" * 70)
    kasa_refs = find_kasa_references(conn)
    if kasa_refs:
        for kr in kasa_refs:
            entries = ", ".join(e["description"] for e in kr["entries"])
            print(f"  {kr['table']}: {entries}")
    else:
        print("  Не знайдено (спробуємо іншим способом)")
    print()

    # Phase 4: Analyze candidate tables
    # Focus on tables with >100 rows, similar structure to _Document236
    print("=" * 70)
    print("  Phase 4: Аналіз кандидатів (таблиці з >100 рядками)")
    print("=" * 70)
    candidates = []
    known_tables = {"_Document236", "_Document247", "_Document238", "_Document213", "_Document12438"}

    total = len(doc_tables)
    for i, dt in enumerate(doc_tables):
        if dt["name"] in known_tables:
            continue

        if i % 20 == 0:
            print(f"  ... перевірка {i}/{total} ...", flush=True)

        cols = get_column_names(conn, dt["name"])
        score = score_candidate(cols)

        if not score["has_standard_fields"]:
            continue

        # Check if it has an organization reference (join to _Reference90)
        org_info = detect_org_and_amount(conn, dt["name"], cols)

        # Check for Каса reference
        kasa_link = check_kasa_column(conn, dt["name"], score["rref_cols"], kasa_refs)

        candidate = {
            "name": dt["name"],
            "cols": cols,
            "score": score,
            "org_col": org_info["org_column"],
            "kasa_link": kasa_link,
            # Similarity to _Document236
            "shared_with_236": len(cols & cols_236),
            "only_in_candidate": len(cols - cols_236),
        }
        candidates.append(candidate)

        marker = ""
        if kasa_link:
            marker = "  ★ КАСА!"
        if org_info["org_column"]:
            marker += "  [має org_ref]"

        print(
            f"  {dt['name']:25s}  "
            f"cols={len(cols):>3}  "
            f"refs={score['ref_count']:>2}  "
            f"shared_236={candidate['shared_with_236']:>2}"
            f"{marker}"
        )
        sys.stdout.flush()

    print()

    # Phase 5: Show details for candidates with Каса reference
    kasa_candidates = [c for c in candidates if c["kasa_link"]]
    # Also check candidates with org ref but no kasa link — might use different naming
    org_candidates = [c for c in candidates if c["org_col"] and not c["kasa_link"]]

    if kasa_candidates:
        print("=" * 70)
        print("  Phase 5: Кандидати з посиланням на КАСУ")
        print("=" * 70)
        for c in kasa_candidates:
            print(f"\n  ★ {c['name']}")
            print(f"    Каса: {c['kasa_link']}")
            print(f"    Організація: {c['org_col']}")
            print(f"    Кількість колонок: {len(c['cols'])}")

            # Show RRef columns
            rref = sorted(r for r in c["score"]["rref_cols"])
            print(f"    Посилання: {', '.join(rref)}")

            # Sample rows
            print(f"\n    --- Зразки даних (ФОП) ---")
            rows = sample_rows(conn, c["name"], c["org_col"])
            for i, row in enumerate(rows, 1):
                print(f"    [{i}]", end="")
                for k, v in row.items():
                    val = v
                    if isinstance(v, bytes):
                        val = v.hex()[:16] + "..."
                    elif isinstance(v, str):
                        val = v.strip()[:60]
                    elif isinstance(v, float):
                        val = f"{v:,.2f}"
                    print(f"  {k}={val}", end="")
                print()
    else:
        print("  Кандидатів з прямим посиланням на Касу не знайдено.")
        print("  Перевіряємо кандидатів з org_ref...")
        print()

    # Phase 6: Show top org_ref candidates if no kasa link found
    if not kasa_candidates and org_candidates:
        # Sort by similarity to _Document236 and show top 5
        org_candidates.sort(key=lambda c: c["shared_with_236"], reverse=True)
        print("=" * 70)
        print("  Phase 6: Топ кандидатів з org_ref (без прямого зв'язку з Касою)")
        print("=" * 70)
        for c in org_candidates[:10]:
            print(f"\n  {c['name']} (shared_236={c['shared_with_236']})")
            print(f"    Організація: {c['org_col']}")

            rows = sample_rows(conn, c["name"], c["org_col"], limit=3)
            for i, row in enumerate(rows, 1):
                print(f"    [{i}]", end="")
                for k, v in row.items():
                    val = v
                    if isinstance(v, bytes):
                        val = v.hex()[:16] + "..."
                    elif isinstance(v, str):
                        val = v.strip()[:60]
                    elif isinstance(v, float):
                        val = f"{v:,.2f}"
                    print(f"  {k}={val}", end="")
                print()

    # Summary
    print()
    print("=" * 70)
    print("  ВИСНОВОК")
    print("=" * 70)
    if kasa_candidates:
        best = kasa_candidates[0]
        print(f"  Найімовірніша таблиця ПКО: {best['name']}")
        print(f"  Каса: {best['kasa_link']}")
        print(f"  Організація: {best['org_col']}")
        print(f"  Впевненість: ВИСОКА")
    else:
        print("  Таблицю ПКО не вдалось визначити автоматично.")
        print("  Потрібна ручна перевірка кандидатів вище.")
        print("  Впевненість: НИЗЬКА")

    conn.close()
    print("\n✓ Готово")


if __name__ == "__main__":
    main()
