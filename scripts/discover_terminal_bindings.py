#!/usr/bin/env python3
"""Discover the table for "Прив'язка платіжних терміналів" in BAS.

READ-ONLY diagnostic script. Does not modify any data.

Strategy:
1. Search _InfoRg* tables (information registers) that reference BOTH:
   - _Reference116 (stores/subdivisions)
   - _Reference90 (counterparties/FOPs)
2. Also search _Document* tables with same pattern
3. For each candidate, show sample data to identify the right one

Usage:
    python scripts/discover_terminal_bindings.py

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

load_dotenv(Path(__file__).resolve().parent.parent / ".env.camunda")

import pymssql


def _get_connection():
    cfg = {
        "server": os.environ.get("BAS_DB_HOST", "deneb"),
        "port": int(os.environ.get("BAS_DB_PORT", "1433")),
        "user": os.environ.get("BAS_DB_USER", "AI_buh"),
        "password": os.environ.get("BAS_DB_PASSWORD", ""),
        "database": os.environ.get("BAS_DB_NAME", "bas_bdu"),
        "login_timeout": 30,
        "timeout": 300,
        "charset": "UTF-8",
    }
    if not cfg["password"]:
        print("ERROR: BAS_DB_PASSWORD is required", file=sys.stderr)
        sys.exit(1)
    return pymssql.connect(**cfg)


def get_column_names(conn, table_name: str) -> set[str]:
    cursor = conn.cursor(as_dict=True)
    try:
        cursor.execute(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = %s",
            (table_name,),
        )
        return {r["COLUMN_NAME"] for r in cursor}
    finally:
        cursor.close()


def list_info_register_tables(conn) -> list[str]:
    """List all _InfoRg* tables (information registers in 1C)."""
    sql = """
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
          AND TABLE_NAME LIKE '_InfoRg[0-9]%'
          AND TABLE_NAME NOT LIKE '%VT%'
        ORDER BY TABLE_NAME
    """
    cursor = conn.cursor(as_dict=True)
    try:
        cursor.execute(sql)
        return [r["TABLE_NAME"] for r in cursor]
    finally:
        cursor.close()


def list_document_tables(conn) -> list[str]:
    """List all _Document* tables."""
    sql = """
        SELECT TABLE_NAME
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
        return [r["TABLE_NAME"] for r in cursor]
    finally:
        cursor.close()


def check_references(conn, table_name: str, rref_cols: list[str]) -> dict[str, str | None]:
    """Check which RRef columns reference _Reference116 (stores) and _Reference90 (FOPs)."""
    cursor = conn.cursor(as_dict=True)
    result = {"store_col": None, "fop_col": None, "user_col": None}
    try:
        for col in rref_cols:
            # Check _Reference116 (stores)
            if not result["store_col"]:
                try:
                    cursor.execute(
                        f"SELECT TOP 1 s._Description "
                        f"FROM [{table_name}] t "
                        f"JOIN _Reference116 s ON t.[{col}] = s._IDRRef"
                    )
                    row = cursor.fetchone()
                    if row:
                        result["store_col"] = col
                        continue
                except Exception:
                    pass

            # Check _Reference90 (counterparties/FOPs)
            if not result["fop_col"]:
                try:
                    cursor.execute(
                        f"SELECT TOP 1 o._Description "
                        f"FROM [{table_name}] t "
                        f"JOIN _Reference90 o ON t.[{col}] = o._IDRRef"
                    )
                    row = cursor.fetchone()
                    if row:
                        result["fop_col"] = col
                        continue
                except Exception:
                    pass

            # Check _Reference84 (users) — for "responsible" field
            if not result["user_col"]:
                try:
                    cursor.execute(
                        f"SELECT TOP 1 u._Description "
                        f"FROM [{table_name}] t "
                        f"JOIN _Reference84 u ON t.[{col}] = u._IDRRef"
                    )
                    row = cursor.fetchone()
                    if row:
                        result["user_col"] = col
                        continue
                except Exception:
                    pass
    finally:
        cursor.close()
    return result


def sample_terminal_data(conn, table_name: str, refs: dict, limit: int = 10) -> list[dict]:
    """Get sample rows with store + FOP names."""
    cursor = conn.cursor(as_dict=True)
    try:
        parts = ["t._Period"]
        joins = []

        if refs["store_col"]:
            parts.append("s._Description AS store_name")
            joins.append(f"JOIN _Reference116 s ON t.[{refs['store_col']}] = s._IDRRef")

        if refs["fop_col"]:
            parts.append("o._Description AS fop_name")
            joins.append(f"JOIN _Reference90 o ON t.[{refs['fop_col']}] = o._IDRRef")

        if refs["user_col"]:
            parts.append("u._Description AS responsible")
            joins.append(f"LEFT JOIN _Reference84 u ON t.[{refs['user_col']}] = u._IDRRef")

        sql = (
            f"SELECT TOP {limit} {', '.join(parts)} "
            f"FROM [{table_name}] t "
            f"{' '.join(joins)} "
            f"ORDER BY t._Period DESC"
        )
        cursor.execute(sql)
        return list(cursor)
    except Exception as e:
        return [{"error": str(e)}]
    finally:
        cursor.close()


def get_row_count(conn, table_name: str) -> int:
    cursor = conn.cursor(as_dict=True)
    try:
        cursor.execute(f"SELECT COUNT(*) AS cnt FROM [{table_name}]")
        row = cursor.fetchone()
        return row["cnt"] if row else 0
    except Exception:
        return 0
    finally:
        cursor.close()


def main():
    print("=" * 70)
    print("  Пошук таблиці 'Прив'язка платіжних терміналів' в БД BAS")
    print("=" * 70)

    conn = _get_connection()
    print("✓ Підключено\n")

    # Phase 1: Search _InfoRg tables
    print("=" * 70)
    print("  Phase 1: Пошук серед _InfoRg (інформаційні регістри)")
    print("=" * 70)
    info_tables = list_info_register_tables(conn)
    print(f"  Знайдено _InfoRg таблиць: {len(info_tables)}\n")

    candidates = []

    for i, table_name in enumerate(info_tables):
        if i % 20 == 0:
            print(f"  ... перевірка {i}/{len(info_tables)} ...", flush=True)

        cols = get_column_names(conn, table_name)
        rref_cols = sorted(c for c in cols if c.endswith("RRef") and c != "_IDRRef")

        if len(rref_cols) < 2:
            continue  # Need at least store + FOP references

        refs = check_references(conn, table_name, rref_cols)

        if refs["store_col"] and refs["fop_col"]:
            row_count = get_row_count(conn, table_name)
            candidates.append({
                "name": table_name,
                "type": "InfoRg",
                "cols": cols,
                "rref_cols": rref_cols,
                "refs": refs,
                "row_count": row_count,
            })
            print(
                f"  ★ {table_name:25s}  "
                f"rows={row_count:>6}  "
                f"cols={len(cols):>3}  "
                f"store={refs['store_col']}  "
                f"fop={refs['fop_col']}  "
                f"user={refs['user_col'] or '-'}"
            )

    # Phase 2: Also search _Document tables (less likely but possible)
    print(f"\n{'=' * 70}")
    print("  Phase 2: Пошук серед _Document (документи) — менш ймовірно")
    print("=" * 70)
    doc_tables = list_document_tables(conn)
    known_skip = {
        "_Document236", "_Document247", "_Document238", "_Document213",
        "_Document12438", "_Document243",
    }
    print(f"  Знайдено _Document таблиць: {len(doc_tables)} (пропускаємо відомі)\n")

    for i, table_name in enumerate(doc_tables):
        if table_name in known_skip:
            continue
        if i % 20 == 0:
            print(f"  ... перевірка {i}/{len(doc_tables)} ...", flush=True)

        cols = get_column_names(conn, table_name)
        rref_cols = sorted(c for c in cols if c.endswith("RRef") and c != "_IDRRef")

        if len(rref_cols) < 2:
            continue

        refs = check_references(conn, table_name, rref_cols)

        if refs["store_col"] and refs["fop_col"]:
            row_count = get_row_count(conn, table_name)
            if row_count > 0:
                candidates.append({
                    "name": table_name,
                    "type": "Document",
                    "cols": cols,
                    "rref_cols": rref_cols,
                    "refs": refs,
                    "row_count": row_count,
                })
                print(
                    f"  ★ {table_name:25s}  "
                    f"rows={row_count:>6}  "
                    f"cols={len(cols):>3}  "
                    f"store={refs['store_col']}  "
                    f"fop={refs['fop_col']}  "
                    f"user={refs['user_col'] or '-'}"
                )

    # Phase 3: Show details and sample data for all candidates
    print(f"\n{'=' * 70}")
    print(f"  Phase 3: Деталі кандидатів ({len(candidates)} знайдено)")
    print("=" * 70)

    if not candidates:
        print("  Кандидатів не знайдено.")
        conn.close()
        return

    # Sort by row count (terminal bindings should have moderate count, not millions)
    candidates.sort(key=lambda c: c["row_count"])

    for c in candidates:
        print(f"\n  {'★' * 3} {c['name']} ({c['type']}) — {c['row_count']} рядків")
        print(f"    Всі колонки: {', '.join(sorted(c['cols']))}")
        print(f"    RRef колонки: {', '.join(c['rref_cols'])}")
        print(f"    store_col: {c['refs']['store_col']}")
        print(f"    fop_col:   {c['refs']['fop_col']}")
        print(f"    user_col:  {c['refs']['user_col'] or 'не знайдено'}")

        # Has _Period column? (typical for _InfoRg)
        has_period = "_Period" in c["cols"]
        print(f"    _Period:   {'Так' if has_period else 'Ні'}")

        print(f"\n    --- Зразки даних ---")
        rows = sample_terminal_data(conn, c["name"], c["refs"])
        for j, row in enumerate(rows, 1):
            parts = []
            for k, v in row.items():
                if isinstance(v, bytes):
                    v = v.hex()[:16] + "..."
                elif isinstance(v, str):
                    v = v.strip()[:50]
                parts.append(f"{k}={v}")
            print(f"    [{j}] {', '.join(parts)}")

    # Summary
    print(f"\n{'=' * 70}")
    print("  ВИСНОВОК")
    print("=" * 70)
    # Best candidate: _InfoRg with _Period, moderate row count, store+fop+user refs
    best = None
    for c in candidates:
        if c["type"] == "InfoRg" and "_Period" in c["cols"] and c["refs"]["user_col"]:
            best = c
            break
    if not best:
        for c in candidates:
            if "_Period" in c["cols"]:
                best = c
                break
    if not best and candidates:
        best = candidates[0]

    if best:
        print(f"  Найімовірніша таблиця: {best['name']}")
        print(f"  Тип: {best['type']}")
        print(f"  Рядків: {best['row_count']}")
        print(f"  store_col: {best['refs']['store_col']}")
        print(f"  fop_col:   {best['refs']['fop_col']}")
        print(f"  user_col:  {best['refs']['user_col'] or 'не знайдено'}")
        print()
        print("  SQL для _fetch_terminal_bindings():")
        print(f"    FROM {best['name']} t")
        print(f"    JOIN _Reference116 s ON t.[{best['refs']['store_col']}] = s._IDRRef")
        print(f"    JOIN _Reference90 o ON t.[{best['refs']['fop_col']}] = o._IDRRef")
        if best["refs"]["user_col"]:
            print(f"    LEFT JOIN _Reference84 u ON t.[{best['refs']['user_col']}] = u._IDRRef")
    else:
        print("  Таблицю не вдалось визначити автоматично.")

    conn.close()
    print("\n✓ Готово")


if __name__ == "__main__":
    main()
