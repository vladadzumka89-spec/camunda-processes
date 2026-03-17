"""BDU handlers — перевірка штатного розкладу в БАС Бухгалтерія.

Task types:
    - bdu-check-position   — чи є посада в штатному розкладі
    - bdu-check-units      — чи достатньо вакантних одиниць

Camunda variables (з Odoo):
    x_studio_camunda_position_id      — назва посади ("Продавець-консультант")
    x_studio_camunda_pidrozdil_name   — назва підрозділу ("639 ТРЦ Київ Полтава")
    x_studio_camunda_organization_name — назва організації ("Кременюк Аліна Юріївна")
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any

from pyzeebe import ZeebeWorker

logger = logging.getLogger(__name__)


# ── DB connection ────────────────────────────────────────────────────


def _get_db_config() -> dict:
    return {
        "server": os.environ.get("BAS_DB_HOST", "deneb"),
        "port": int(os.environ.get("BAS_DB_PORT", "1433")),
        "user": os.environ.get("BAS_DB_USER", "AI_buh"),
        "password": os.environ.get("BAS_DB_PASSWORD", ""),
        "database": os.environ.get("BAS_DB_NAME", "bas_bdu"),
        "login_timeout": 30,
        "timeout": 120,
        "charset": "UTF-8",
    }


def _get_connection(max_retries: int = 3, initial_delay: int = 5):
    import pymssql

    db_config = _get_db_config()
    if not db_config["password"]:
        raise RuntimeError("BAS_DB_PASSWORD is required for BDU worker")

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


# ── Helpers ──────────────────────────────────────────────────────────


def _extract_department_number(department_name: str) -> str:
    """'639 ТРЦ Київ Полтава' → '639'."""
    number = ""
    for ch in department_name:
        if ch.isdigit():
            number += ch
        else:
            break
    return number if number else department_name


# ── 1. Перевірка посади ──────────────────────────────────────────────


def _check_position_exists(
    position_name: str,
    department_number: str,
    org_name: str,
) -> dict:
    """Перевірити чи є посада в штатному розкладі.

    Таблиці:
        _Reference12429 — Позиції штатного розкладу
        _Reference12325 — Посади (Fld27124RRef)
        _Reference100   — Підрозділи (Fld27123RRef)
        _Reference90    — Організації (OwnerIDRRef)
        _Fld27127       — Кількість одиниць
    """
    conn = _get_connection()
    try:
        cursor = conn.cursor(as_dict=True)
        try:
            cursor.execute("""
                SELECT TOP 1
                    p._Fld27127 AS units,
                    pos._Description AS pos_name,
                    dept._Description AS dept_name,
                    org._Description AS org_name
                FROM _Reference12429 p
                JOIN _Reference12325 pos ON p._Fld27124RRef = pos._IDRRef
                JOIN _Reference100 dept ON p._Fld27123RRef = dept._IDRRef
                JOIN _Reference90 org ON p._OwnerIDRRef = org._IDRRef
                WHERE pos._Description = %s
                  AND dept._Description LIKE %s
                  AND org._Description = %s
                  AND p._Marked = 0x00
                  AND p._Fld27127 > 0
                ORDER BY p._Fld27129 DESC
            """, (position_name, f"{department_number}%", org_name))

            row = cursor.fetchone()
            position_exists = row is not None
            units = float(row["units"]) if row else 0

            logger.info(
                "bdu-check-position: '%s' в '%s' (орг '%s') → %s (одиниць: %s)",
                position_name, department_number, org_name,
                "знайдена" if position_exists else "не знайдена", units,
            )

            return {
                "position_exists": position_exists,
                "units_in_schedule": int(units),
                "message": (
                    f"Посада '{position_name}' в підрозділі {department_number}: "
                    f"{'знайдена (' + str(int(units)) + ' од.)' if position_exists else 'не знайдена'}"
                ),
            }
        finally:
            cursor.close()
    finally:
        conn.close()


# ── 2. Перевірка одиниць ─────────────────────────────────────────────


def _check_units_available(
    position_name: str,
    department_number: str,
    org_name: str,
) -> dict:
    """Перевірити чи є вакантні одиниці.

    Крок 1: кількість одиниць зі штатного (_Reference12429._Fld27127)
    Крок 2: кількість проведених прийомів (_Document12438)
             де та сама посада + підрозділ + організація
    """
    conn = _get_connection()
    try:
        cursor = conn.cursor(as_dict=True)
        try:
            # Крок 1: одиниці в штатному
            cursor.execute("""
                SELECT TOP 1 p._Fld27127 AS units
                FROM _Reference12429 p
                JOIN _Reference12325 pos ON p._Fld27124RRef = pos._IDRRef
                JOIN _Reference100 dept ON p._Fld27123RRef = dept._IDRRef
                JOIN _Reference90 org ON p._OwnerIDRRef = org._IDRRef
                WHERE pos._Description = %s
                  AND dept._Description LIKE %s
                  AND org._Description = %s
                  AND p._Marked = 0x00
                  AND p._Fld27127 > 0
                ORDER BY p._Fld27129 DESC
            """, (position_name, f"{department_number}%", org_name))

            row = cursor.fetchone()
            total_units = int(float(row["units"])) if row else 0

            # Крок 2: кількість зайнятих (проведені прийоми)
            cursor.execute("""
                SELECT COUNT(*) AS occupied
                FROM _Document12438 d
                JOIN _Reference12325 pos ON d._Fld13203RRef = pos._IDRRef
                JOIN _Reference100 dept ON d._Fld13193RRef = dept._IDRRef
                JOIN _Reference90 org ON d._Fld13192RRef = org._IDRRef
                WHERE pos._Description = %s
                  AND dept._Description LIKE %s
                  AND org._Description = %s
                  AND d._Posted = 0x01
                  AND d._Marked = 0x00
            """, (position_name, f"{department_number}%", org_name))

            row2 = cursor.fetchone()
            occupied_count = row2["occupied"] if row2 else 0

            has_vacancy = total_units > occupied_count
            need_more_units = not has_vacancy

            logger.info(
                "bdu-check-units: '%s' в '%s' → %d од., зайнято %d, вакансія: %s",
                position_name, department_number,
                total_units, occupied_count, has_vacancy,
            )

            return {
                "need_more_units": need_more_units,
                "has_vacancy": has_vacancy,
                "total_units": total_units,
                "occupied_count": occupied_count,
                "available_units": max(0, total_units - occupied_count),
                "message": (
                    f"Посада '{position_name}': {total_units} од., "
                    f"зайнято {occupied_count}, "
                    f"{'є вакансія' if has_vacancy else 'вакансій немає'}"
                ),
            }
        finally:
            cursor.close()
    finally:
        conn.close()


# ── Handler registration ────────────────────────────────────────────


def register_bdu_handlers(worker: ZeebeWorker) -> None:
    """Register BDU task handlers."""

    @worker.task(task_type="bdu-check-position", timeout_ms=60_000)
    async def bdu_check_position(
        x_studio_camunda_position_id: str = "",
        x_studio_camunda_pidrozdil_name: str = "",
        x_studio_camunda_organization_name: str = "",
        **kwargs: Any,
    ) -> dict:
        """Перевірити чи є посада в штатному розкладі БДУ.

        Input variables (з Camunda/Odoo):
            x_studio_camunda_position_id (str): назва посади
            x_studio_camunda_pidrozdil_name (str): назва підрозділу
            x_studio_camunda_organization_name (str): назва організації

        Output variables:
            position_exists (bool): чи знайдена посада
            units_in_schedule (int): кількість одиниць
            message (str): опис результату
        """
        department_number = _extract_department_number(x_studio_camunda_pidrozdil_name)
        logger.info(
            "bdu-check-position: посада='%s', підрозділ='%s'→'%s', орг='%s'",
            x_studio_camunda_position_id,
            x_studio_camunda_pidrozdil_name,
            department_number,
            x_studio_camunda_organization_name,
        )
        return await asyncio.to_thread(
            _check_position_exists,
            x_studio_camunda_position_id,
            department_number,
            x_studio_camunda_organization_name,
        )

    @worker.task(task_type="bdu-check-units", timeout_ms=60_000)
    async def bdu_check_units(
        x_studio_camunda_position_id: str = "",
        x_studio_camunda_pidrozdil_name: str = "",
        x_studio_camunda_organization_name: str = "",
        **kwargs: Any,
    ) -> dict:
        """Перевірити чи достатньо одиниць в штатному.

        Input variables (з Camunda/Odoo):
            x_studio_camunda_position_id (str): назва посади
            x_studio_camunda_pidrozdil_name (str): назва підрозділу
            x_studio_camunda_organization_name (str): назва організації

        Output variables:
            need_more_units (bool): потрібно додати одиниці
            has_vacancy (bool): є вакансія
            total_units (int): всього одиниць
            occupied_count (int): зайнято
            available_units (int): вільних
            message (str): опис результату
        """
        department_number = _extract_department_number(x_studio_camunda_pidrozdil_name)
        logger.info(
            "bdu-check-units: посада='%s', підрозділ='%s'→'%s', орг='%s'",
            x_studio_camunda_position_id,
            x_studio_camunda_pidrozdil_name,
            department_number,
            x_studio_camunda_organization_name,
        )
        return await asyncio.to_thread(
            _check_units_available,
            x_studio_camunda_position_id,
            department_number,
            x_studio_camunda_organization_name,
        )

    logger.info(
        "BDU handlers registered: bdu-check-position, bdu-check-units"
    )
