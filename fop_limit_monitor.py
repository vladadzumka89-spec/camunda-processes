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
"""

import argparse
import os
import sys
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path

import pymssql

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
    "server": "deneb",
    "port": 1433,
    "user": "AI_buh",
    "password": os.environ["BAS_DB_PASSWORD"],
    "database": "bas_bdu",
    "login_timeout": 30,
    "timeout": 300,
    "charset": "UTF-8",
}

# === Ліміти ЄП на 2026 рік (ст. 291.4 ПКУ, мінімалка 8 647 грн) ===
# 2 група: 834 × 8 647 = 7 211 598 грн
# 3 група: 1167 × 8 647 = 10 091 049 грн
LIMITS = {
    2: 7_211_598.00,
    3: 10_091_049.00,
}

# === SQL: offset дати в BAS = +2000 років ===
BAS_YEAR_OFFSET = 2000


def get_connection():
    return pymssql.connect(**DB_CONFIG)


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
    cursor.execute(sql, (bas_start, bas_end))
    return cursor.fetchall()


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
    cursor.execute(sql, (bas_start, bas_end))

    result = defaultdict(list)
    for row in cursor:
        org_id = bytes(row["org_id"])
        result[org_id].append({
            "date": row["doc_date"],
            "amount": float(row["daily_total"]),
            "count": row["doc_count"],
        })
    return result


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

    # === Тренд за останні 4 тижні ===
    four_weeks_ago = today - timedelta(days=28)
    two_weeks_ago = today - timedelta(days=14)

    income_weeks_3_4 = sum(
        amt for d, amt in income_by_date.items()
        if four_weeks_ago <= d < two_weeks_ago
    )
    income_weeks_1_2 = sum(
        amt for d, amt in income_by_date.items()
        if two_weeks_ago <= d <= today
    )

    # Коефіцієнт тренду
    if income_weeks_3_4 > 0:
        trend_ratio = income_weeks_1_2 / income_weeks_3_4
    elif income_weeks_1_2 > 0:
        trend_ratio = 1.5  # Є нові дані, але не було старих — вважаємо зростання
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
        "active_days": len(income_by_date),
    }


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


def print_report(fops: list, analyses: dict, group_filter: int | None,
                 days_ahead: int, top_n: int | None):
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

        for group, limit in sorted(LIMITS.items()):
            if group_filter and group != group_filter:
                continue

            info = analysis["limit_dates"][group]

            if info["already_exceeded"]:
                alert_reasons.append(
                    f"⛔ ЛІМІТ {group} ГРУПИ ПЕРЕВИЩЕНО! "
                    f"Дохід: {format_currency(analysis['total_income'])} грн"
                )
                alert_level = max(alert_level, 3)
            elif info["date"] is not None:
                days_to_limit = (info["date"] - today).days
                pct = (analysis["total_income"] / limit) * 100

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
                pct = (analysis["total_income"] / limit) * 100
                if pct >= 50:
                    alert_reasons.append(
                        f"ℹ️  {group} група: {pct:.1f}% ліміту, "
                        f"не досягне до кінця року за поточним темпом"
                    )
                    alert_level = max(alert_level, 1)

        # Тренд-попередження
        if analysis["trend_ratio"] > 1.4:
            alert_reasons.append(
                f"📈 Тренд: дохід за останні 2 тижні на "
                f"{(analysis['trend_ratio'] - 1) * 100:.0f}% вищий за попередні 2 тижні"
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

    # === Вивід алертів ===
    print(f"\n⚠️  ФОПи, що потребують уваги: {len(alerts)}\n")

    for i, alert in enumerate(alerts, 1):
        fop = alert["fop"]
        a = alert["analysis"]
        level_icon = {0: "✅", 1: "🟢", 2: "🟡", 3: "🔴"}[alert["level"]]

        print(f"{'─' * 90}")
        print(f"  {level_icon} {i}. {fop['name'].strip()}")
        print(f"     ІПН: {fop['edrpou']} | "
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

        print()

    # === Зведена таблиця ===
    print(f"{'═' * 90}")
    print(f"  ЗВЕДЕНА ТАБЛИЦЯ (топ за доходом {year})")
    print(f"{'═' * 90}")
    print(f"  {'ФОП':<35} {'Дохід, грн':>16} {'%лім.2гр':>9} {'%лім.3гр':>9} "
          f"{'Прогноз 2гр':>14} {'Прогноз 3гр':>14}")
    print(f"  {'─' * 35} {'─' * 16} {'─' * 9} {'─' * 9} {'─' * 14} {'─' * 14}")

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
        name = fop["name"].strip()[:34]
        pct2 = (a["total_income"] / LIMITS[2]) * 100
        pct3 = (a["total_income"] / LIMITS[3]) * 100

        info2 = a["limit_dates"][2]
        info3 = a["limit_dates"][3]

        date2 = "ПЕРЕВИЩЕНО" if info2["already_exceeded"] else format_date(info2["date"])
        date3 = "ПЕРЕВИЩЕНО" if info3["already_exceeded"] else format_date(info3["date"])

        print(f"  {name:<35} {format_currency(a['total_income']):>16} "
              f"{pct2:>8.1f}% {pct3:>8.1f}% {date2:>14} {date3:>14}")

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
    args = parser.parse_args()

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

    conn.close()

    print(f"🔮 Аналіз та прогнозування...\n")

    analyses = {}
    for fop in fops:
        fop_id = bytes(fop["id"])
        data = daily_income.get(fop_id, [])
        result = analyze_fop(data, today, year)
        if result:
            analyses[fop_id] = result

    print_report(fops, analyses, args.group, args.days_ahead, args.top)


if __name__ == "__main__":
    main()
