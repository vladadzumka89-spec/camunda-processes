"""Розклад запуску бізнес-процесу подачі Z-звітів за магазинами ТРЦ.

Для кожного магазину з Google Sheet обчислює:
- "Дата подання" (правило з договору)
- Фактичний дедлайн (з урахуванням переносу з вихідного на попередній робочий)
- Дата запуску user task у Camunda (буфер -N робочих днів від дедлайну)

Правила інтерпретації:
- "До N числа"            → календарний N-й день місяця
- "До N робочого дня"     → N-й робочий день від 1 числа (включно)
- "До N робочих днів"     → те саме
- Робочий день = Пн-Пт (свята зараз не враховуються — за рішенням замовника)
- Якщо дедлайн потрапляє на вихідний → переноситься на попередній робочий день
- Запуск БП = дедлайн мінус BUFFER_WORKDAYS робочих днів (default: 2)
"""
from __future__ import annotations

import argparse
import csv
import io
import re
from dataclasses import dataclass, asdict
from datetime import date, timedelta
from pathlib import Path
from urllib.request import Request, urlopen

DEFAULT_SHEET_ID = "1MkL55mV7jU-SUDwqoyHhQEXCLn-4BEbYMAXEDKlJrzM"
DEFAULT_SHEET_GID = "0"
DEFAULT_BUFFER_WORKDAYS = 2

_PREFIX_RX = re.compile(r"^\s*(\d{2,4})\b")
_NUM_RX = re.compile(r"(\d+)")
UA_WEEKDAY = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Нд"]

def parse_submission_features(text: str) -> tuple[bool, bool]:
    """З колонки "Особливості подання" Google Sheet визначає спецумови подачі.

    Повертає (needs_upload_site, needs_original):
      - needs_upload_site — текст містить "портал" або "сайт"
        (фраза фінансиста: "Загрузити на портал")
      - needs_original   — текст містить "оригінал"
        (фраза фінансиста: "Відправити оригінал")
    """
    t = (text or "").lower()
    needs_upload_site = "портал" in t or "сайт" in t
    needs_original = "оригінал" in t
    return needs_upload_site, needs_original


@dataclass
class StoreSchedule:
    prefix: str
    store_label: str          # назва з Sheet
    rule: str                 # сирий текст правила
    deadline: str             # ISO yyyy-mm-dd, скоригований
    trigger_date: str         # ISO yyyy-mm-dd, день створення user task
    fact_submission: str      # "Так"/"Ні"
    monthly_required: str     # "Передбачено"/"Не передбачено"
    on_demand: str            # "Передбачено"/"Не передбачено"


# ---------------------------- календар ----------------------------

def is_workday(d: date) -> bool:
    return d.weekday() < 5


def nth_workday_of_month(year: int, month: int, n: int) -> date:
    d = date(year, month, 1)
    count = 0
    while True:
        if is_workday(d):
            count += 1
            if count == n:
                return d
        d += timedelta(days=1)


def shift_workdays(d: date, delta: int) -> date:
    """Зсуває дату на delta робочих днів (delta<0 — назад)."""
    step = -1 if delta < 0 else 1
    rem = abs(delta)
    while rem > 0:
        d += timedelta(days=step)
        if is_workday(d):
            rem -= 1
    return d


def adjust_to_previous_workday(d: date) -> date:
    while not is_workday(d):
        d -= timedelta(days=1)
    return d


# ---------------------------- правила ----------------------------

def parse_rule(rule: str) -> tuple[str, int] | None:
    """Повертає ('calendar'|'workday', n) або None, якщо не вдалось розпарсити."""
    txt = (rule or "").lower().strip()
    m = _NUM_RX.search(txt)
    if not m:
        return None
    n = int(m.group(1))
    kind = "workday" if "робоч" in txt else "calendar"
    return kind, n


def compute_deadline(rule: str, year: int, month: int) -> date | None:
    parsed = parse_rule(rule)
    if not parsed:
        return None
    kind, n = parsed
    raw = nth_workday_of_month(year, month, n) if kind == "workday" else date(year, month, n)
    return adjust_to_previous_workday(raw)


# ---------------------------- Sheet ----------------------------

def fetch_sheet_rows(sheet_id: str, gid: str, *, timeout: int = 20) -> list[dict]:
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    req = Request(url, headers={"User-Agent": "ai_1c/schedule"})
    with urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8")

    reader = csv.reader(io.StringIO(body))
    rows = list(reader)
    if not rows:
        return []
    # заголовок: 'Підрозділ ', 'Факт подачі Z-звіту', 'Вимога ... щомісяця',
    #            'Вимога ... в будь-який час', 'Дата подання ', 'Коментар'
    out: list[dict] = []
    seen: set[str] = set()
    for r in rows[1:]:
        if not r or not r[0].strip():
            continue
        label = r[0].strip()
        m = _PREFIX_RX.match(label)
        if not m:
            continue
        prefix = m.group(1)
        if prefix in seen:
            continue
        seen.add(prefix)
        out.append({
            "prefix": prefix,
            "label": label,
            "fact": (r[1] if len(r) > 1 else "").strip(),
            "monthly": (r[2] if len(r) > 2 else "").strip(),
            "on_demand": (r[3] if len(r) > 3 else "").strip(),
            "rule": (r[4] if len(r) > 4 else "").strip(),
            "features": (r[5] if len(r) > 5 else "").strip(),
        })
    return out


# ---------------------------- розклад ----------------------------

def build_schedule(
    year: int,
    month: int,
    *,
    sheet_id: str = DEFAULT_SHEET_ID,
    sheet_gid: str = DEFAULT_SHEET_GID,
    buffer_workdays: int = DEFAULT_BUFFER_WORKDAYS,
) -> list[dict]:
    rows = fetch_sheet_rows(sheet_id, sheet_gid)
    result: list[StoreSchedule] = []
    for row in rows:
        dl = compute_deadline(row["rule"], year, month)
        if dl is None:
            continue
        trig = shift_workdays(dl, -buffer_workdays)
        result.append(StoreSchedule(
            prefix=row["prefix"],
            store_label=row["label"],
            rule=row["rule"],
            deadline=dl.isoformat(),
            trigger_date=trig.isoformat(),
            fact_submission=row["fact"],
            monthly_required=row["monthly"],
            on_demand=row["on_demand"],
        ))
    result.sort(key=lambda r: (r.trigger_date, r.deadline, r.prefix))
    return [asdict(r) for r in result]


def next_month(today: date | None = None) -> tuple[int, int]:
    today = today or date.today()
    if today.month == 12:
        return today.year + 1, 1
    return today.year, today.month + 1


def first_workday_of_month(year: int, month: int) -> date:
    """Перший Пн-Пт у місяці (без врахування свят)."""
    d = date(year, month, 1)
    while not is_workday(d):
        d += timedelta(days=1)
    return d


def build_deadline_groups(
    year: int,
    month: int,
    *,
    sheet_id: str = DEFAULT_SHEET_ID,
    sheet_gid: str = DEFAULT_SHEET_GID,
) -> dict:
    """Повертає магазини, згруповані за дедлайном і відсортовані за датою зростання.

    Призначення: джерело даних для інтеграційного воркера, який створює
    завдання у зовнішній системі документообігу.

    Структура виходу (JSON-serializable):
        {
            "year": 2026,
            "month": 6,
            "start_date": "2026-06-01",
            "start_weekday": "Пн",
            "total_stores": 51,
            "deadline_groups": [
                {
                    "deadline": "2026-06-03",
                    "weekday": "Ср",
                    "is_urgent": false,     # дедлайн <= start_date
                    "stores": [
                        {"prefix": "616", "label": "...", "rule": "..."},
                        ...
                    ]
                },
                ...
            ]
        }
    """
    rows = fetch_sheet_rows(sheet_id, sheet_gid)
    start = first_workday_of_month(year, month)

    by_dl: dict[date, list[dict]] = {}
    for row in rows:
        dl = compute_deadline(row["rule"], year, month)
        if dl is None:
            continue
        needs_upload_site, needs_original = parse_submission_features(row["features"])
        by_dl.setdefault(dl, []).append({
            "prefix": row["prefix"],
            "label": row["label"],
            "rule": row["rule"],
            "needs_upload_site": needs_upload_site,
            "needs_original": needs_original,
        })

    groups = []
    for dl in sorted(by_dl.keys()):
        stores = sorted(by_dl[dl], key=lambda s: s["prefix"])
        groups.append({
            "deadline": dl.isoformat(),
            "weekday": UA_WEEKDAY[dl.weekday()],
            "is_urgent": dl <= start,
            "stores": stores,
        })

    # готові підсписки для окремих user tasks (Загрузити на сайт / Передати оригінали)
    all_stores = [s for g in groups for s in g["stores"]]
    upload_site_stores = sorted(
        (s for s in all_stores if s["needs_upload_site"]), key=lambda s: s["prefix"])
    original_stores = sorted(
        (s for s in all_stores if s["needs_original"]), key=lambda s: s["prefix"])

    return {
        "year": year,
        "month": month,
        "start_date": start.isoformat(),
        "start_weekday": UA_WEEKDAY[start.weekday()],
        "total_stores": len(all_stores),
        "deadline_groups": groups,
        "upload_site_stores": upload_site_stores,
        "original_stores": original_stores,
    }


# ---------------------------- CLI ----------------------------

def _cli() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--year", type=int, default=None)
    p.add_argument("--month", type=int, default=None)
    p.add_argument("--buffer", type=int, default=DEFAULT_BUFFER_WORKDAYS,
                   help="кількість робочих днів буфера до дедлайну")
    p.add_argument("--sheet-id", default=DEFAULT_SHEET_ID)
    p.add_argument("--sheet-gid", default=DEFAULT_SHEET_GID)
    p.add_argument("--groups", action="store_true",
                   help="вивести магазини, ЗГРУПОВАНІ за дедлайнами (JSON для інтеграції)")
    args = p.parse_args()

    if (args.year is None) ^ (args.month is None):
        p.error("--year і --month разом, або обидва None")

    y, m = (args.year, args.month) if args.year else next_month()

    if args.groups:
        import json
        data = build_deadline_groups(y, m, sheet_id=args.sheet_id, sheet_gid=args.sheet_gid)
        print(json.dumps(data, ensure_ascii=False, indent=2))
        return

    data = build_schedule(y, m, buffer_workdays=args.buffer,
                          sheet_id=args.sheet_id, sheet_gid=args.sheet_gid)

    out_dir = Path("results")
    out_dir.mkdir(exist_ok=True)
    csv_path = out_dir / f"submission_schedule_{y}-{m:02d}.csv"
    with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["№", "Магазин", "Правило", "Дедлайн", "День тиж.",
                    "Запуск БП", "День тиж.", "Z-звіт", "Щомісяця", "За запитом"])
        for i, r in enumerate(data, 1):
            d_dl = date.fromisoformat(r["deadline"])
            d_tr = date.fromisoformat(r["trigger_date"])
            w.writerow([i, r["store_label"], r["rule"],
                        r["deadline"], UA_WEEKDAY[d_dl.weekday()],
                        r["trigger_date"], UA_WEEKDAY[d_tr.weekday()],
                        r["fact_submission"], r["monthly_required"], r["on_demand"]])

    print(f"Розклад на {y}-{m:02d}, буфер {args.buffer} робочих днів  "
          f"({len(data)} магазинів)\n")
    print(f"{'№':<3} {'Магазин':<35} {'Правило':<22} {'Дедлайн':<14} {'Запуск':<14}")
    print("-" * 92)
    for i, r in enumerate(data, 1):
        d_dl = date.fromisoformat(r["deadline"])
        d_tr = date.fromisoformat(r["trigger_date"])
        print(f"{i:<3} {r['store_label']:<35} {r['rule']:<22} "
              f"{r['deadline']} {UA_WEEKDAY[d_dl.weekday()]:<4} "
              f"{r['trigger_date']} {UA_WEEKDAY[d_tr.weekday()]}")

    from collections import Counter
    by_trig = Counter(r["trigger_date"] for r in data)
    print("\n— ЗВЕДЕННЯ ПО ДНЯХ ЗАПУСКУ БП —")
    for d_str, cnt in sorted(by_trig.items()):
        d = date.fromisoformat(d_str)
        print(f"  {d_str} ({UA_WEEKDAY[d.weekday()]}): {cnt} магазинів")
    print(f"\nЗбережено: {csv_path}")


if __name__ == "__main__":
    _cli()
