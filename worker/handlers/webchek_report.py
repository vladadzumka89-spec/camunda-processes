r"""Zeebe handler для топіка `generate-webchek-reports` (БП "Подача звітів ТО для ТРЦ").

Замінює ручну роботу аналітика у `UserTask_WebchekReports`:
1. Тягне `.db` бази ПРРО ФН зі SMB-шари (`\\ralf\WebCheck\DB`).
2. Для кожного унікального ФН генерує PDF періодичного Z-звіту через
   `gen_z_report.generate_z_report()`.
3. Зберігає PDF у shared дирекорію (`WEBCHEK_OUTPUT_DIR`).
4. Повертає метадані у Zeebe variables — Odoo Camunda-модуль використовує їх
   щоб створити записи `x.to.webchek.line` з прикріпленими PDF.

Залежності (worker/requirements.txt):
    reportlab, smbprotocol

Env vars (.env.camunda):
    WEBCHEK_SMB_HOST       — сервер шари (default ralf.a.local)
    WEBCHEK_SMB_SHARE      — назва шари (default WebCheck)
    WEBCHEK_SMB_USER       — користувач
    WEBCHEK_SMB_PASSWORD   — пароль
    WEBCHEK_DB_SUBDIR      — підпапка з .db файлами (default DB)
    WEBCHEK_OUTPUT_DIR     — куди класти згенеровані PDF (default /tmp/webchek-reports)
"""
from __future__ import annotations

import asyncio
import calendar
import logging
import os
import shutil
import tempfile
from datetime import date
from typing import Any

from pyzeebe import ZeebeWorker

from ..config import AppConfig
from .gen_z_report import generate_z_report

logger = logging.getLogger(__name__)


# ── SMB helpers ──────────────────────────────────────────────────────


def _get_smb_config() -> dict:
    """Налаштування SMB-шари WebCheck."""
    return {
        'host':     os.environ.get('WEBCHEK_SMB_HOST', 'ralf.a.local'),
        'share':    os.environ.get('WEBCHEK_SMB_SHARE', 'WebCheck'),
        'user':     os.environ.get('WEBCHEK_SMB_USER', ''),
        'password': os.environ.get('WEBCHEK_SMB_PASSWORD', ''),
        'subdir':   os.environ.get('WEBCHEK_DB_SUBDIR', 'DB'),
    }


def _register_smb_session() -> tuple[str, str]:
    """Реєструє SMB-сесію (idempotent). Повертає (host, db_unc_prefix)."""
    from smbclient import register_session

    cfg = _get_smb_config()
    if not cfg['user'] or not cfg['password']:
        raise RuntimeError(
            'WEBCHEK_SMB_USER / WEBCHEK_SMB_PASSWORD env vars обов\'язкові'
        )
    register_session(cfg['host'], username=cfg['user'], password=cfg['password'])
    unc_prefix = rf'\\{cfg["host"]}\{cfg["share"]}\{cfg["subdir"]}'
    return cfg['host'], unc_prefix


def _download_db(fn_fop: str, local_path: str) -> int:
    """Завантажує .db файл з SMB-шари у local_path. Повертає розмір файла."""
    from smbclient import open_file

    _, unc_prefix = _register_smb_session()
    remote = rf'{unc_prefix}\{fn_fop}.db'

    with open_file(remote, mode='rb') as src, open(local_path, 'wb') as dst:
        while chunk := src.read(1024 * 1024):
            dst.write(chunk)
    return os.path.getsize(local_path)


# ── Public handler ───────────────────────────────────────────────────


def _generate_one_report(
    fn_fop: str,
    store_prefix: str,
    store_label: str,
    date_from: str,
    date_to: str,
    output_dir: str,
) -> dict | None:
    """Завантажує .db, генерує PDF, повертає метадані. None при невдачі."""
    tmpfile = tempfile.NamedTemporaryFile(
        prefix=f'webchek_{fn_fop}_', suffix='.db', delete=False,
    )
    tmpfile.close()
    try:
        size = _download_db(fn_fop, tmpfile.name)
        logger.info('downloaded %s.db (%d MB)', fn_fop, size // (1024 * 1024))

        pdf_path = os.path.join(output_dir, f'{fn_fop}.pdf')
        result = generate_z_report(tmpfile.name, date_from, date_to, pdf_path)
        if result is None:
            logger.warning('Z-звітів немає для %s за %s..%s', fn_fop, date_from, date_to)
            return None

        return {
            'fn_fop':       fn_fop,
            'store_prefix': store_prefix,
            'store_label':  store_label,
            'pdf_path':     pdf_path,
            'pdf_size':     os.path.getsize(pdf_path),
        }
    except FileNotFoundError as e:
        logger.error('.db не знайдено на шарі для %s: %s', fn_fop, e)
        return None
    except Exception as e:
        logger.exception('Помилка генерації звіту для %s: %s', fn_fop, e)
        return None
    finally:
        try:
            os.unlink(tmpfile.name)
        except OSError:
            pass


def _month_period(year: int, month: int) -> tuple[str, str]:
    """Повертає (YYYY-MM-01, YYYY-MM-DD останній день) для заданого місяця."""
    last_day = calendar.monthrange(year, month)[1]
    return f'{year:04d}-{month:02d}-01', f'{year:04d}-{month:02d}-{last_day:02d}'


def register_webchek_handlers(
    worker: ZeebeWorker,
    config: AppConfig,
) -> None:
    """Реєструє топік generate-webchek-reports для БП "Подача звітів ТО для ТРЦ"."""

    @worker.task(task_type='generate-webchek-reports', timeout_ms=900_000)
    async def generate_webchek_reports(
        rroData: list | None = None,
        year: int | None = None,
        month: int | None = None,
        **kwargs: Any,
    ) -> dict:
        """Для кожного унікального ФН з rroData тягне .db і генерує PDF.

        Input variables:
            rroData: вихід fetch-rro-fop — список dict з полями
                store_prefix, store_label (або store_name), fn_fop, active_from/to.
            year, month: звітний місяць. Default — попередній місяць.

        Output variables:
            webchekReports: [{fn_fop, store_prefix, store_label, pdf_path, pdf_size}].
            webchekReportsCount: к-сть успішно згенерованих.
            webchekReportsFailed: список fn_fop які не вдалося згенерувати.
            webchekReportsDir: директорія з PDF.
        """
        if not rroData:
            raise ValueError("Очікується змінна процесу 'rroData' (вихід fetch-rro-fop)")

        # Період = звітний місяць
        if year is None or month is None:
            from .rro_fop_for_stores import previous_month
            year, month = previous_month()
        date_from, date_to = _month_period(year, month)

        # Папка для виходу
        cfg_dir = os.environ.get('WEBCHEK_OUTPUT_DIR', '/tmp/webchek-reports')
        output_dir = os.path.join(cfg_dir, f'{year:04d}-{month:02d}')
        os.makedirs(output_dir, exist_ok=True)

        # Унікальні ФН (один .db = один ФН; кілька рядків rroData для одного ФН зайві)
        seen: set[str] = set()
        unique_rows: list[dict] = []
        for r in rroData:
            fn = r.get('fn_fop')
            if not fn or fn in seen:
                continue
            seen.add(fn)
            unique_rows.append(r)

        logger.info(
            'generate-webchek-reports: %s..%s, %d унікальних ФН, output=%s',
            date_from, date_to, len(unique_rows), output_dir,
        )

        # Генерація — послідовно (SMB transfer-bound, паралельність не дасть приросту)
        reports: list[dict] = []
        failed: list[str] = []
        for r in unique_rows:
            store_name = r.get('store_label') or r.get('store_name', '').strip()
            result = await asyncio.to_thread(
                _generate_one_report,
                r['fn_fop'], r.get('store_prefix', ''), store_name,
                date_from, date_to, output_dir,
            )
            if result is None:
                failed.append(r['fn_fop'])
            else:
                reports.append(result)

        logger.info(
            'generate-webchek-reports done: %d ОК, %d failed (%s)',
            len(reports), len(failed), failed[:5],
        )

        return {
            'webchekReports':       reports,
            'webchekReportsCount':  len(reports),
            'webchekReportsFailed': failed,
            'webchekReportsDir':    output_dir,
        }
