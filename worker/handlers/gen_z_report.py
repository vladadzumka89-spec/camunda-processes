"""Generate a periodic Z-report PDF from a WebCheck ПРРО SQLite database.

Логіка:
- Зчитує `.db` базу ПРРО конкретного ФН (експорт з WebCheck).
- Парсить XML кожного Z-звіту (DocType=80) у заданому періоді.
- Агрегує: картка/готівка прихід+повернення, податки.
- Малює PDF-чек 80мм у форматі періодичного звіту (як у WebCheck UI).

Використання як модуль (handler):
    from worker.handlers.gen_z_report import generate_z_report
    pdf_path = generate_z_report('/tmp/4000088915.db', '2026-04-01', '2026-04-30')

Використання як CLI (для дебагу):
    python -m worker.handlers.gen_z_report 4000088915.db 2026-04-01 2026-04-30
"""
from __future__ import annotations

import logging
import os
import sqlite3
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas

logger = logging.getLogger(__name__)


# ── Font registration ────────────────────────────────────────────────

_FONT_CANDIDATES = [
    # Windows Courier (оригінальне середовище WebCheck)
    ('C:/Windows/Fonts/cour.ttf', 'C:/Windows/Fonts/courbd.ttf'),
    # Linux DejaVu (наше docker-середовище)
    ('/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf',
     '/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf'),
]
_FONTS_REGISTERED = False


def _register_fonts() -> None:
    """Реєструє моноширинні шрифти один раз (idempotent)."""
    global _FONTS_REGISTERED
    if _FONTS_REGISTERED:
        return
    for reg, bold in _FONT_CANDIDATES:
        if os.path.isfile(reg) and os.path.isfile(bold):
            pdfmetrics.registerFont(TTFont('Courier-Cyr', reg))
            pdfmetrics.registerFont(TTFont('Courier-Cyr-Bold', bold))
            _FONTS_REGISTERED = True
            return
    raise RuntimeError(
        'Не знайдено моноширинного шрифту (Courier на Windows / DejaVuSansMono на Linux)'
    )


def _decode(b):
    """Декодує bytes у str, пробуючи UTF-8 та cp1251 (legacy WebCheck бази)."""
    if isinstance(b, bytes):
        for enc in ('utf-8', 'cp1251'):
            try:
                return b.decode(enc)
            except Exception:
                pass
        return b.decode('utf-8', errors='replace')
    return b


# ── Aggregation from .db ─────────────────────────────────────────────

@dataclass
class ZReportTotals:
    """Зведений результат за період по одному ФН."""
    fn_prro: str
    tin: str
    pointname: str
    orgname: str
    pointaddr: str
    z_count: int
    first_z_no: int
    last_z_no: int
    first_z_date: str
    last_z_date: str
    card_in: float
    card_out: float
    cash_in: float
    cash_out: float
    tax_in: float
    tax_out: float

    @property
    def total_in(self) -> float:
        return self.card_in + self.cash_in

    @property
    def total_out(self) -> float:
        return self.card_out + self.cash_out

    @property
    def rro_turnover(self) -> float:
        """РРО оберт = чисті продажі (прихід - повернення)."""
        return self.total_in - self.total_out


def _aggregate_z_reports(db_path: str, date_from: str, date_to: str) -> ZReportTotals | None:
    """Читає .db базу і повертає зведений ZReportTotals або None якщо нема Z-звітів."""
    conn = sqlite3.connect(db_path)
    conn.text_factory = bytes
    cur = conn.cursor()

    # Реквізити точки
    cur.execute('SELECT FN, TIN, POINTNAME, ORGNAME, POINTADDR FROM TAXOBJECTS')
    row = cur.fetchone()
    fn_prro = _decode(row[0])
    tin = _decode(row[1])
    pointname = _decode(row[2])
    orgname = _decode(row[3])
    pointaddr = _decode(row[4])

    # Z-звіти за період
    cur.execute("""
        SELECT checkxml, dt FROM ksef
        WHERE DocType=80 AND dt BETWEEN ? AND ?
        ORDER BY dt
    """, (date_from + ' 00:00:00', date_to + ' 23:59:59'))
    z_rows = cur.fetchall()
    conn.close()

    if not z_rows:
        return None

    card_in = cash_in = card_out = cash_out = tax_in = tax_out = 0.0
    first_z_no: Optional[int] = None
    last_z_no: Optional[int] = None
    first_z_date: Optional[str] = None
    last_z_date: Optional[str] = None

    for xml_bytes, dt in z_rows:
        try:
            root = ET.fromstring(_decode(xml_bytes))
        except ET.ParseError:
            continue

        z_el = root.find('.//Z')
        if z_el is None:
            continue

        z_no = int(z_el.get('NO', 0))
        z_date = _decode(dt)[:10]
        if first_z_no is None:
            first_z_no, first_z_date = z_no, z_date
        last_z_no, last_z_date = z_no, z_date

        # Платіжні методи: T=2 → картка, T=0 → готівка
        for m in z_el.findall('M'):
            t = int(m.get('T') or -1)
            smi = float(m.get('SMI') or 0)
            smo = float(m.get('SMO') or 0)
            if t == 2:
                card_in += smi
                card_out += smo
            elif t == 0:
                cash_in += smi
                cash_out += smo
            else:
                nm = _decode(m.get('NM') or b'').upper()
                if 'КАРТ' in nm or 'CART' in nm:
                    card_in += smi
                    card_out += smo
                else:
                    cash_in += smi
                    cash_out += smo

        for tx in z_el.findall('TXS'):
            tax_in += float(tx.get('TXI') or 0)
            tax_out += float(tx.get('TXO') or 0)

    return ZReportTotals(
        fn_prro=fn_prro, tin=tin, pointname=pointname,
        orgname=orgname, pointaddr=pointaddr,
        z_count=len(z_rows),
        first_z_no=first_z_no or 0, last_z_no=last_z_no or 0,
        first_z_date=first_z_date or '', last_z_date=last_z_date or '',
        card_in=card_in, card_out=card_out,
        cash_in=cash_in, cash_out=cash_out,
        tax_in=tax_in, tax_out=tax_out,
    )


# ── PDF rendering ────────────────────────────────────────────────────

def _fmt_money(v: float) -> str:
    return f'{v:,.2f}'.replace(',', ' ')


def _fmt_date(d: str) -> str:
    """'2026-04-01' → '01.04.2026'."""
    return datetime.strptime(d, '%Y-%m-%d').strftime('%d.%m.%Y')


def _render_pdf(totals: ZReportTotals, date_from: str, date_to: str, output_pdf: str) -> None:
    _register_fonts()

    PAGE_W = 80 * mm
    LINE_H = 4.5 * mm
    MARGIN = 4 * mm
    x_left = MARGIN
    x_right = PAGE_W - MARGIN

    lines: list = []

    def sep():
        lines.append(('sep', ''))

    def center(txt, bold=False):
        lines.append(('center', txt, bold))

    def row2(left, right, bold=False):
        lines.append(('row2', left, right, bold))

    # Шапка
    center(totals.orgname, bold=True)
    center(totals.pointname)
    addr = totals.pointaddr
    while len(addr) > 28:
        idx = addr[:28].rfind(' ')
        if idx < 0:
            idx = 28
        center(addr[:idx])
        addr = addr[idx:].strip()
    center(addr)
    center(f'ІД {totals.tin}')
    center('ПЕРІОДИЧНИЙ ЗВІТ')
    center(f'від {_fmt_date(date_from)} до {_fmt_date(date_to)}')
    row2(f'З № {totals.first_z_no}', _fmt_date(totals.first_z_date))
    row2(f'ДО № {totals.last_z_no}', _fmt_date(totals.last_z_date))
    row2('ВСЬОГО Z ЗВІТІВ', str(totals.z_count))
    sep()

    # Sales
    if totals.card_in > 0:
        row2('КАРТКА', _fmt_money(totals.card_in))
        sep()
    if totals.cash_in > 0:
        row2('ГОТІВКА', _fmt_money(totals.cash_in))
        sep()
    row2('ОБIГ Б', _fmt_money(totals.total_in))
    row2('ОБIГ', _fmt_money(totals.total_in))
    row2('ПДВ Б=0.00%', _fmt_money(0))
    row2('ПОДАТОК', _fmt_money(totals.tax_in))
    row2('ЗАГ. СУМА', _fmt_money(totals.total_in))
    sep()

    # Returns
    center('ПОВЕРНЕНI')
    if totals.card_out > 0:
        row2('КАРТКА', _fmt_money(totals.card_out))
        sep()
    if totals.cash_out > 0:
        row2('ГОТІВКА', _fmt_money(totals.cash_out))
        sep()
    row2('ОБIГ Б', _fmt_money(totals.total_out))
    row2('ОБIГ', _fmt_money(totals.total_out))
    row2('ПДВ Б=0.00%', _fmt_money(0))
    row2('ПОДАТОК', _fmt_money(totals.tax_out))
    row2('ЗАГ. СУМА', _fmt_money(totals.total_out))
    sep()

    now = datetime.now()
    row2(now.strftime('%d.%m.%Y'), now.strftime('%H-%M-%S'))
    row2('ФН ПРРО', str(totals.fn_prro))
    center('СЛУЖБОВИЙ ЧЕК')
    center("ПРРО 'ВебЧек'")

    # PDF
    page_h = (len(lines) + 4) * LINE_H + 20 * mm
    c_pdf = canvas.Canvas(output_pdf, pagesize=(PAGE_W, page_h))
    y = page_h - 8 * mm
    FONT_SZ = 7

    for item in lines:
        tag = item[0]
        if tag == 'sep':
            c_pdf.setFont('Courier-Cyr', FONT_SZ)
            c_pdf.drawString(x_left, y, '-' * 29)
            y -= LINE_H
        elif tag == 'center':
            txt = item[1]
            bold = item[2] if len(item) > 2 else False
            fn = 'Courier-Cyr-Bold' if bold else 'Courier-Cyr'
            c_pdf.setFont(fn, FONT_SZ)
            tw = c_pdf.stringWidth(txt, fn, FONT_SZ)
            c_pdf.drawString((PAGE_W - tw) / 2, y, txt)
            y -= LINE_H
        elif tag == 'row2':
            left = item[1]
            right = item[2]
            bold = item[3] if len(item) > 3 else False
            fn = 'Courier-Cyr-Bold' if bold else 'Courier-Cyr'
            c_pdf.setFont(fn, FONT_SZ)
            c_pdf.drawString(x_left, y, left)
            rw = c_pdf.stringWidth(right, fn, FONT_SZ)
            c_pdf.drawString(x_right - rw, y, right)
            y -= LINE_H

    c_pdf.save()


# ── Public API ───────────────────────────────────────────────────────

def generate_z_report(
    db_path: str,
    date_from: str,
    date_to: str,
    output_pdf: str | None = None,
) -> str | None:
    """Генерує PDF періодичного Z-звіту з .db бази ПРРО.

    Args:
        db_path: шлях до .db файлу.
        date_from, date_to: межі періоду у форматі YYYY-MM-DD.
        output_pdf: куди зберегти PDF. Default — поруч з .db: `{db_dir}/{db_stem}_Z_{YYYY-MM}.pdf`.

    Returns:
        Шлях до згенерованого PDF, або None якщо за період немає Z-звітів.

    Raises:
        FileNotFoundError, sqlite3.DatabaseError, RuntimeError (шрифти).
    """
    if not os.path.isfile(db_path):
        raise FileNotFoundError(f'Базу не знайдено: {db_path}')

    for name, val in [('date_from', date_from), ('date_to', date_to)]:
        try:
            datetime.strptime(val, '%Y-%m-%d')
        except ValueError as e:
            raise ValueError(f'{name} має бути у форматі РРРР-ММ-ДД, отримано {val!r}') from e

    if output_pdf is None:
        base = os.path.splitext(db_path)[0]
        output_pdf = f'{base}_Z_{date_from[:7]}.pdf'

    totals = _aggregate_z_reports(db_path, date_from, date_to)
    if totals is None:
        logger.info('Z-reports не знайдено для %s за %s..%s', db_path, date_from, date_to)
        return None

    _render_pdf(totals, date_from, date_to, output_pdf)
    logger.info(
        'PDF згенеровано: %s (Z %d-%d, %d звітів, оберт=%.2f, повернень=%.2f)',
        output_pdf, totals.first_z_no, totals.last_z_no, totals.z_count,
        totals.total_in, totals.total_out,
    )
    return output_pdf


# ── CLI ──────────────────────────────────────────────────────────────

def _cli() -> None:
    import argparse
    p = argparse.ArgumentParser(
        description='Формування PDF Z-звіту за період з бази ПРРО',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='Приклад:\n  python -m worker.handlers.gen_z_report 4000088915.db 2026-04-01 2026-04-30',
    )
    p.add_argument('db', help='Шлях до файлу бази .db')
    p.add_argument('date_from', help='Дата початку (РРРР-ММ-ДД)')
    p.add_argument('date_to', help='Дата кінця (РРРР-ММ-ДД)')
    p.add_argument('-o', '--output', help='Шлях до вихідного PDF')
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    pdf = generate_z_report(args.db, args.date_from, args.date_to, args.output)
    if pdf is None:
        print('No Z-reports found for period', file=sys.stderr)
        sys.exit(1)
    print(f'PDF saved: {pdf}')


if __name__ == '__main__':
    _cli()
