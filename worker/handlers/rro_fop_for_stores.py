"""Вибірка активних ФН ПРРО ФОП для магазинів ТРЦ за заданий місяць.

Призначення: основа для Camunda external task worker у бізнес-процесі
"Подача звітів ТО для ТРЦ". Витягує всі фіскальні номери ПРРО ФОП,
які були прив'язані до конкретних магазинів у вказаному місяці.

База: famo
- _Reference140         — Справочник.Склады (магазини)
- _InfoRg19031          — РегистрСведений.ПривязкаПлатежныхТерминалов
    _Fld19032RRef       → Склад
    _Fld21898           → ФискальныйНомерРРО_ФОП
    _Fld23144           → Отключен (b'\\x00' = активний)
    _Period             → момент створення/зміни прив'язки (зміщення +2000 років)

Логіка "ФН активний у місяці M":
кожен запис у регістрі діє від свого _Period до _Period наступного запису
по тому ж складу. ФН вважається активним у місяці M, якщо інтервал його
дії перетинається з [start_of_M, end_of_M].
"""
from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, asdict
from datetime import date, datetime, timedelta

from .fop_common import _get_famo_connection

YEAR_OFFSET = 2000  # SQL дати зміщені на +2000 років від реальних


@dataclass
class RroBinding:
    store_code: str
    store_name: str
    store_prefix: str            # "607", "902", ...
    fn_fop: str
    active_from: str             # реальна дата (ISO), момент прив'язки
    active_to: str | None        # реальна дата (ISO) або None, якщо досі діє


def previous_month(today: date | None = None) -> tuple[int, int]:
    """Повертає (рік, місяць) попереднього місяця відносно сьогодні."""
    today = today or date.today()
    first_of_this = today.replace(day=1)
    last_of_prev = first_of_this - timedelta(days=1)
    return last_of_prev.year, last_of_prev.month


def _month_bounds_sql(year: int, month: int) -> tuple[datetime, datetime]:
    """Повертає (start_inclusive, end_exclusive) у SQL-просторі (+2000 років).

    end_exclusive — 00:00 першого дня наступного місяця.
    """
    start = datetime(year + YEAR_OFFSET, month, 1)
    if month == 12:
        end = datetime(year + YEAR_OFFSET + 1, 1, 1)
    else:
        end = datetime(year + YEAR_OFFSET, month + 1, 1)
    return start, end


def _real_iso(sql_dt: datetime | None) -> str | None:
    if sql_dt is None:
        return None
    return sql_dt.replace(year=sql_dt.year - YEAR_OFFSET).isoformat()


_PREFIX_RX = re.compile(r"^\s*(\d{2,4})\b")


def _extract_prefix(token: str) -> str | None:
    """Витягує числовий префікс з рядка типу '607 Форум' або просто '607'."""
    m = _PREFIX_RX.match(token)
    return m.group(1) if m else None


def fetch_active_fop_rro(
    store_prefixes: list[str],
    year: int | None = None,
    month: int | None = None,
    *,
    db_name: str = "famo",
) -> list[dict]:
    """Повертає ФН ПРРО ФОП, активні у вказаному місяці для заданих магазинів.

    Args:
        store_prefixes: список префіксів магазинів, напр. ["607", "609", "902"].
            Допускається передавати "607 Форум" — префікс буде витягнуто.
        year, month: цільовий місяць. Якщо обидва None — попередній місяць.
        db_name: ім'я бази (default 'famo').

    Returns:
        Список словників (JSON-serializable) з полями RroBinding.
        Без дублікатів (store_code, fn_fop).
    """
    if (year is None) ^ (month is None):
        raise ValueError("year і month мають бути або обидва задані, або обидва None")
    if year is None:
        year, month = previous_month()

    prefixes = sorted({p for p in (_extract_prefix(s) for s in store_prefixes) if p})
    if not prefixes:
        return []

    start_sql, end_sql = _month_bounds_sql(year, month)

    # pymssql використовує %s як placeholder (не ? як pyodbc).
    like_clauses = " OR ".join(["s._Description LIKE %s"] * len(prefixes))
    like_params = [f"{p} %" for p in prefixes]

    # Кроки:
    #   history    — кожен запис регістру з інтервалом дії [active_from, active_to)
    #   in_month   — лише ті інтервали, що перетинаються з цільовим місяцем
    #   collapse   — згортка кількох інтервалів того самого ФН на тому ж складі
    #                в один рядок: MIN(від), MAX(до) (NULL=досі поглинає інші)
    sql = f"""
    WITH history AS (
        SELECT
            i._Fld19032RRef       AS store_ref,
            s._Code               AS store_code,
            s._Description        AS store_name,
            i._Fld21898           AS fn_fop,
            i._Period             AS active_from,
            LEAD(i._Period) OVER (
                PARTITION BY i._Fld19032RRef ORDER BY i._Period
            )                     AS active_to,
            i._Fld23144           AS disabled
        FROM _InfoRg19031 i
        INNER JOIN _Reference140 s ON s._IDRRef = i._Fld19032RRef
        WHERE ({like_clauses})
    ),
    in_month AS (
        SELECT *
        FROM history
        WHERE fn_fop <> N''
          AND disabled = 0x00
          AND active_from < %s
          AND (active_to IS NULL OR active_to >= %s)
    )
    SELECT
        store_code,
        store_name,
        fn_fop,
        MIN(active_from)                                              AS active_from,
        CASE WHEN MAX(CASE WHEN active_to IS NULL THEN 1 ELSE 0 END) = 1
             THEN NULL ELSE MAX(active_to) END                        AS active_to
    FROM in_month
    GROUP BY store_code, store_name, fn_fop
    ORDER BY store_code, fn_fop;
    """

    params = (*like_params, end_sql, start_sql)

    with _get_famo_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()

    result: list[dict] = []
    for store_code, store_name, fn_fop, active_from, active_to in rows:
        prefix = _extract_prefix(store_name) or ""
        result.append(
            asdict(
                RroBinding(
                    store_code=store_code.strip(),
                    store_name=store_name.strip(),
                    store_prefix=prefix,
                    fn_fop=fn_fop.strip(),
                    active_from=_real_iso(active_from),
                    active_to=_real_iso(active_to),
                )
            )
        )
    return result


def _cli() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--stores",
        required=True,
        help="Список магазинів через кому: '607,609,902' або '607 Форум,902 Пінкі'",
    )
    p.add_argument("--year", type=int, default=None)
    p.add_argument("--month", type=int, default=None)
    p.add_argument("--db", default="famo")
    p.add_argument("--pretty", action="store_true", help="Вивести у вигляді таблиці")
    args = p.parse_args()

    stores = [s for s in args.stores.split(",") if s.strip()]
    data = fetch_active_fop_rro(stores, args.year, args.month, db_name=args.db)

    if not args.pretty:
        print(json.dumps(data, ensure_ascii=False, indent=2))
        return

    year, month = (args.year, args.month) if args.year else previous_month()
    print(f"ФН ПРРО ФОП, активні у {year}-{month:02d}  ({len(data)} записів)")
    print(f"{'Код':<10} {'Магазин':<35} {'ФН ФОП':<14} {'від':<19} {'до':<19}")
    for row in data:
        print(
            f"{row['store_code']:<10} {row['store_name'][:34]:<35} "
            f"{row['fn_fop']:<14} {row['active_from']:<19} "
            f"{row['active_to'] or '— досі':<19}"
        )


if __name__ == "__main__":
    _cli()
