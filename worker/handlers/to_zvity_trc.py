"""Zeebe handlers для БП «Подача звітів ТО для ТРЦ» (Camunda 8).

Реєструє 3 task types з bpmn/Подача звітів ТО для ТРЦ/to_zvity_trc.bpmn:
    load-store-schedule          → читання графіка магазинів з Google Sheet
    fetch-rro-fop                → витяг ФН ПРРО ФОП з 1С famo
    check-delivery-confirmation  → no-op нагадування (boundary timer R/P3D
                                   на UserTask_ReceiveConfirmation)

Бізнес-логіка — у build_submission_schedule.py та rro_fop_for_stores.py.
Підключення до famo (1С) — _get_famo_connection() у fop_common.py
(env: FAMO_DB_HOST / FAMO_DB_PORT / FAMO_DB_USER / FAMO_DB_PASSWORD / FAMO_DB_NAME).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date
from typing import Any

from pyzeebe import ZeebeWorker

from ..config import AppConfig
from .build_submission_schedule import build_deadline_groups
from .rro_fop_for_stores import fetch_active_fop_rro, previous_month

logger = logging.getLogger(__name__)


def register_to_zvity_trc_handlers(
    worker: ZeebeWorker,
    config: AppConfig,
) -> None:
    """Реєструє handler-и БП "Подача звітів ТО для ТРЦ"."""

    @worker.task(task_type="load-store-schedule", timeout_ms=60_000)
    async def load_store_schedule(
        year: int | None = None,
        month: int | None = None,
        **kwargs: Any,
    ) -> dict:
        """Графік магазинів, згрупований за дедлайнами.

        Input variables (опц.):
            year, month — ручне перевизначення (default: поточний місяць).

        Output variables:
            stores, deadline_groups, upload_site_stores, original_stores,
            start_date, total_stores.
        """
        if year is None or month is None:
            today = date.today()
            year, month = today.year, today.month

        data = await asyncio.to_thread(build_deadline_groups, year, month)
        logger.info(
            "load-store-schedule %s-%02d: %d магазинів, %d груп дедлайнів",
            year, month, data["total_stores"], len(data["deadline_groups"]),
        )
        return {
            "stores": [s for g in data["deadline_groups"] for s in g["stores"]],
            "deadline_groups": data["deadline_groups"],
            "upload_site_stores": data["upload_site_stores"],
            "original_stores": data["original_stores"],
            "start_date": data["start_date"],
            "total_stores": data["total_stores"],
        }

    @worker.task(task_type="fetch-rro-fop", timeout_ms=120_000)
    async def fetch_rro_fop(
        stores: list | None = None,
        year: int | None = None,
        month: int | None = None,
        **kwargs: Any,
    ) -> dict:
        """Активні ФН ПРРО ФОП з 1С famo для заданих магазинів.

        Input variables:
            stores: список магазинів зі змінної процесу
                (елементи — словники з полем prefix або рядки-префікси).
            year, month: опц. перевизначення (default: попередній місяць).

        Output variables:
            rroData: список словників з активними прив'язками ФН ПРРО ФОП.
        """
        if not stores:
            raise ValueError("Очікується змінна процесу 'stores' (список магазинів)")
        if year is None or month is None:
            year, month = previous_month()

        prefixes = [
            s["prefix"] if isinstance(s, dict) else str(s)
            for s in stores
        ]
        rows = await asyncio.to_thread(
            fetch_active_fop_rro, prefixes, year, month,
        )
        logger.info(
            "fetch-rro-fop %s-%02d: %d рядків ФН ПРРО ФОП",
            year, month, len(rows),
        )
        return {"rroData": rows}

    @worker.task(task_type="check-delivery-confirmation", timeout_ms=10_000)
    async def check_delivery_confirmation(**kwargs: Any) -> dict:
        """No-op нагадування.

        Спрацьовує boundary timer R/P3D на UserTask_ReceiveConfirmation
        (non-interrupting). Виконання реальної логіки не потрібне — лише
        запис у лог. Якщо в майбутньому знадобиться авто-перевірка статусу
        TPL-доставки — реалізувати тут.
        """
        logger.info("check-delivery-confirmation: нагадування спрацювало (no-op)")
        return {}
