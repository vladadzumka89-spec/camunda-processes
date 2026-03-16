"""BDU handlers — перевірка штатного розкладу та створення прийому в БАС Бухгалтерія.

Task types:
    - bdu-check-position   — чи є посада в штатному розкладі
    - bdu-check-units      — чи достатньо вакантних одиниць
    - bdu-add-position     — додати позицію в штатний розклад
    - bdu-increase-units   — збільшити кількість одиниць в штатному
    - bdu-create-admission — створити співробітника + непроведений прийом
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from datetime import datetime, timedelta
from typing import Any

from pyzeebe import ZeebeWorker

logger = logging.getLogger(__name__)

# BAS зберігає дати з offset +2000 років
BAS_YEAR_OFFSET = 2000


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
    """'101 Call-center' → '101', 'Дитячий світ' → 'Дитячий світ'."""
    number = ""
    for ch in department_name:
        if ch.isdigit():
            number += ch
        else:
            break
    return number if number else department_name


def _to_bas_date(iso_date: str) -> datetime:
    """'2026-03-16' → datetime(4026, 3, 16)."""
    dt = datetime.fromisoformat(iso_date)
    return dt.replace(year=dt.year + BAS_YEAR_OFFSET)


def _generate_1c_guid() -> bytes:
    """Згенерувати GUID в форматі 1С (16 bytes)."""
    return uuid.uuid4().bytes


def _empty_ref() -> bytes:
    """Порожнє посилання 1С."""
    return b'\x00' * 16


# ── 1. Перевірка посади ──────────────────────────────────────────────


def _check_position_exists(
    position_name: str,
    department_number: str,
    org_okpo: str,
) -> dict:
    """Перевірити чи є посада в штатному розкладі.

    Таблиці:
        _Reference12429 — Позиції штатного розкладу
        _Reference12325 — Посади (Fld27124RRef)
        _Reference100   — Підрозділи (Fld27123RRef)
        _Reference90    — Організації (OwnerIDRRef, Fld1494 = ЄДРПОУ)
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
                  AND org._Fld1494 = %s
                  AND p._Marked = 0x00
                  AND p._Fld27127 > 0
                ORDER BY p._Fld27129 DESC
            """, (position_name, f"{department_number}%", org_okpo))

            row = cursor.fetchone()
            position_exists = row is not None
            units = float(row["units"]) if row else 0

            logger.info(
                "bdu-check-position: '%s' в '%s' (ЄДРПОУ %s) → %s (одиниць: %s)",
                position_name, department_number, org_okpo,
                "знайдена" if position_exists else "не знайдена", units,
            )

            return {
                "x_studio_camunda_position_exists": position_exists,
                "position_name": position_name,
                "department_number": department_number,
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
    org_okpo: str,
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
                  AND org._Fld1494 = %s
                  AND p._Marked = 0x00
                  AND p._Fld27127 > 0
                ORDER BY p._Fld27129 DESC
            """, (position_name, f"{department_number}%", org_okpo))

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
                  AND org._Fld1494 = %s
                  AND d._Posted = 0x01
                  AND d._Marked = 0x00
            """, (position_name, f"{department_number}%", org_okpo))

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
                "x_studio_camunda_need_more_units": need_more_units,
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


# ── 3. Додати позицію в штатний ───────────────────────────────────────


def _add_position_to_schedule(
    position_name: str,
    department_number: str,
    org_okpo: str,
) -> dict:
    """Додати нову позицію в штатний розклад (_Reference12429) з 1 одиницею."""
    conn = _get_connection()
    try:
        cursor = conn.cursor(as_dict=True)
        try:
            # Знайти посаду, підрозділ, організацію
            pos_ref = _find_position_ref(cursor, position_name)
            if not pos_ref:
                return {
                    "position_added": False,
                    "message": f"Посада '{position_name}' не знайдена в довіднику (_Reference12325)",
                }

            dept_ref = _find_dept_ref(cursor, department_number)
            if not dept_ref:
                return {
                    "position_added": False,
                    "message": f"Підрозділ '{department_number}' не знайдений (_Reference100)",
                }

            org_ref = _find_org_ref(cursor, org_okpo)
            if not org_ref:
                return {
                    "position_added": False,
                    "message": f"Організація з ЄДРПОУ '{org_okpo}' не знайдена",
                }

            # Знайти батьківську папку (підрозділ в _Reference12429)
            cursor.execute("""
                SELECT TOP 1 _IDRRef
                FROM _Reference12429
                WHERE _Description LIKE %s
                  AND _Folder = 0x00
                  AND _OwnerIDRRef = %s
                  AND _Marked = 0x00
            """, (f"{department_number}%", org_ref))
            parent_row = cursor.fetchone()
            parent_ref = bytes(parent_row["_IDRRef"]) if parent_row else _empty_ref()

            # Створити позицію
            now = datetime.now()
            bas_now = now.replace(year=now.year + BAS_YEAR_OFFSET)
            pos_id = _generate_1c_guid()
            description = f"{position_name} /{department_number}/"

            cursor.execute("""
                INSERT INTO _Reference12429 (
                    _IDRRef, _Marked, _PredefinedID, _OwnerIDRRef, _ParentIDRRef,
                    _Folder, _Description,
                    _Fld27123RRef, _Fld27124RRef, _Fld27125RRef, _Fld27126RRef,
                    _Fld27127, _Fld27128, _Fld27129, _Fld27130, _Fld27131,
                    _Fld27132, _Fld27133, _Fld27134, _Fld27135,
                    _Fld27136, _Fld27137, _Fld27138, _Fld27139,
                    _Fld27140RRef, _Fld27141RRef, _Fld27142RRef,
                    _Fld27143RRef, _Fld27144RRef,
                    _Fld27145, _Fld27146, _Fld27147, _Fld27148, _Fld27149,
                    _Fld514
                ) VALUES (
                    %s, 0x00, %s, %s, %s,
                    0x01, %s,
                    %s, %s, %s, %s,
                    1, 0x01, %s, 0x00, %s,
                    0, 0, 0, '',
                    0, 0, 0, '',
                    %s, %s, %s, %s, %s,
                    0, 0, 0, 0x00, 0,
                    0
                )
            """, (
                pos_id, _empty_ref(), org_ref, parent_ref,
                description,
                dept_ref, pos_ref, _empty_ref(), _empty_ref(),
                bas_now, datetime(2001, 1, 1),
                _empty_ref(), _empty_ref(), _empty_ref(), _empty_ref(), _empty_ref(),
            ))

            conn.commit()

            logger.info(
                "Додано позицію в штатний: '%s' (1 од.)", description,
            )

            return {
                "position_added": True,
                "x_studio_camunda_position_exists": True,
                "message": f"Позицію '{description}' додано в штатний розклад (1 одиниця)",
            }

        except Exception as e:
            conn.rollback()
            raise
        finally:
            cursor.close()
    finally:
        conn.close()


# ── 4. Збільшити кількість одиниць ───────────────────────────────────


def _increase_units(
    position_name: str,
    department_number: str,
    org_okpo: str,
) -> dict:
    """Збільшити кількість одиниць позиції в штатному на 1."""
    conn = _get_connection()
    try:
        cursor = conn.cursor(as_dict=True)
        try:
            cursor.execute("""
                UPDATE p SET p._Fld27127 = p._Fld27127 + 1
                FROM _Reference12429 p
                JOIN _Reference12325 pos ON p._Fld27124RRef = pos._IDRRef
                JOIN _Reference100 dept ON p._Fld27123RRef = dept._IDRRef
                JOIN _Reference90 org ON p._OwnerIDRRef = org._IDRRef
                WHERE pos._Description = %s
                  AND dept._Description LIKE %s
                  AND org._Fld1494 = %s
                  AND p._Marked = 0x00
            """, (position_name, f"{department_number}%", org_okpo))

            rows_affected = cursor.rowcount
            conn.commit()

            if rows_affected > 0:
                logger.info(
                    "Збільшено одиниці: '%s' в '%s' (+1)", position_name, department_number,
                )
                return {
                    "units_increased": True,
                    "x_studio_camunda_need_more_units": False,
                    "message": f"Кількість одиниць для '{position_name}' в підрозділі {department_number} збільшено на 1",
                }
            else:
                return {
                    "units_increased": False,
                    "message": f"Позицію '{position_name}' в підрозділі {department_number} не знайдено для оновлення",
                }

        except Exception as e:
            conn.rollback()
            raise
        finally:
            cursor.close()
    finally:
        conn.close()


# ── 5. Створення прийому ─────────────────────────────────────────────


def _find_person_ref(cursor, employee_name: str) -> bytes | None:
    """Знайти фіз.особу в _Reference151 за ПІБ."""
    cursor.execute("""
        SELECT TOP 1 _IDRRef
        FROM _Reference151
        WHERE _Description = %s AND _Marked = 0x00
    """, (employee_name,))
    row = cursor.fetchone()
    return bytes(row["_IDRRef"]) if row else None


def _find_org_ref(cursor, org_okpo: str) -> bytes | None:
    """Знайти організацію в _Reference90 за ЄДРПОУ."""
    cursor.execute("""
        SELECT TOP 1 _IDRRef
        FROM _Reference90
        WHERE _Fld1494 = %s AND _Marked = 0x00
    """, (org_okpo,))
    row = cursor.fetchone()
    return bytes(row["_IDRRef"]) if row else None


def _find_dept_ref(cursor, department_number: str) -> bytes | None:
    """Знайти підрозділ в _Reference100 за номером."""
    cursor.execute("""
        SELECT TOP 1 _IDRRef
        FROM _Reference100
        WHERE _Description LIKE %s AND _Marked = 0x00
    """, (f"{department_number}%",))
    row = cursor.fetchone()
    return bytes(row["_IDRRef"]) if row else None


def _find_position_ref(cursor, position_name: str) -> bytes | None:
    """Знайти посаду в _Reference12325 за назвою."""
    cursor.execute("""
        SELECT TOP 1 _IDRRef
        FROM _Reference12325
        WHERE _Description = %s AND _Marked = 0x00
    """, (position_name,))
    row = cursor.fetchone()
    return bytes(row["_IDRRef"]) if row else None


def _find_or_create_employee(cursor, employee_name: str, person_ref: bytes, dept_ref: bytes) -> bytes:
    """Знайти або створити співробітника в _Reference102."""
    # Спробуємо знайти існуючого
    cursor.execute("""
        SELECT TOP 1 _IDRRef
        FROM _Reference102
        WHERE _Description = %s AND _Marked = 0x00
    """, (employee_name,))
    row = cursor.fetchone()
    if row:
        logger.info("Співробітник '%s' вже існує", employee_name)
        return bytes(row["_IDRRef"])

    # Створюємо нового
    emp_id = _generate_1c_guid()
    cursor.execute("""
        INSERT INTO _Reference102 (
            _IDRRef, _Marked, _PredefinedID, _Description,
            _Fld1674RRef, _Fld27517RRef,
            _Fld1673, _Fld1675, _Fld1676, _Fld1677, _Fld1678, _Fld1679,
            _Fld1680, _Fld27846, _Fld514
        ) VALUES (
            %s, 0x00, %s, %s,
            %s, %s,
            0x00, '', 0x00, 0x00, 0x00, 0x00,
            0x, '', 0
        )
    """, (emp_id, _empty_ref(), employee_name, person_ref, dept_ref))

    logger.info("Створено співробітника '%s'", employee_name)
    return emp_id


def _get_next_doc_number(cursor, year: int) -> str:
    """Отримати наступний номер документа прийому."""
    bas_year_prefix = datetime(year + BAS_YEAR_OFFSET, 1, 1)
    cursor.execute("""
        SELECT TOP 1 _Number
        FROM _Document12438
        WHERE _NumberPrefix = %s
        ORDER BY _Number DESC
    """, (bas_year_prefix,))
    row = cursor.fetchone()

    if row:
        last_num = row["_Number"].strip()
        # Формат: "ХХХХ-000018" — беремо числову частину
        parts = last_num.rsplit("-", 1)
        if len(parts) == 2 and parts[1].isdigit():
            prefix = parts[0]
            next_num = int(parts[1]) + 1
            return f"{prefix}-{next_num:06d}"

    # Fallback
    return f"AUTO-{year}-{uuid.uuid4().hex[:6]}"


def _create_admission(
    employee_name: str,
    position_name: str,
    department_number: str,
    org_okpo: str,
    admission_date: str,
) -> dict:
    """Створити співробітника та непроведений документ прийому в БДУ."""
    conn = _get_connection()
    try:
        cursor = conn.cursor(as_dict=True)
        try:
            # 1. Знайти фіз.особу
            person_ref = _find_person_ref(cursor, employee_name)
            if not person_ref:
                return {
                    "admission_created": False,
                    "message": f"Фіз.особа '{employee_name}' не знайдена в БДУ (_Reference151)",
                }

            # 2. Знайти організацію
            org_ref = _find_org_ref(cursor, org_okpo)
            if not org_ref:
                return {
                    "admission_created": False,
                    "message": f"Організація з ЄДРПОУ '{org_okpo}' не знайдена в БДУ",
                }

            # 3. Знайти підрозділ
            dept_ref = _find_dept_ref(cursor, department_number)
            if not dept_ref:
                return {
                    "admission_created": False,
                    "message": f"Підрозділ '{department_number}' не знайдений в БДУ",
                }

            # 4. Знайти посаду
            pos_ref = _find_position_ref(cursor, position_name)
            if not pos_ref:
                return {
                    "admission_created": False,
                    "message": f"Посада '{position_name}' не знайдена в БДУ (_Reference12325)",
                }

            # 5. Знайти або створити співробітника
            emp_ref = _find_or_create_employee(cursor, employee_name, person_ref, dept_ref)

            # 6. Створити документ прийому (НЕ проведений)
            doc_id = _generate_1c_guid()
            now = datetime.now()
            bas_now = now.replace(year=now.year + BAS_YEAR_OFFSET)
            bas_admission = _to_bas_date(admission_date)
            bas_year_prefix = datetime(now.year + BAS_YEAR_OFFSET, 1, 1)
            doc_number = _get_next_doc_number(cursor, now.year)

            cursor.execute("""
                INSERT INTO _Document12438 (
                    _IDRRef, _Marked, _Date_Time, _NumberPrefix, _Number, _Posted,
                    _Fld13191,
                    _Fld13192RRef,
                    _Fld13193RRef,
                    _Fld13194RRef,
                    _Fld13195RRef,
                    _Fld13196RRef,
                    _Fld13197RRef,
                    _Fld13198RRef,
                    _Fld13199,
                    _Fld13200,
                    _Fld13201,
                    _Fld13202RRef,
                    _Fld13203RRef,
                    _Fld13204RRef,
                    _Fld13205RRef,
                    _Fld13206,
                    _Fld13207,
                    _Fld13208,
                    _Fld13209,
                    _Fld13210RRef,
                    _Fld13211,
                    _Fld13212RRef,
                    _Fld13213,
                    _Fld13214RRef,
                    _Fld514,
                    _Fld10989
                ) VALUES (
                    %s, 0x00, %s, %s, %s, 0x00,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    100,
                    0,
                    0,
                    %s,
                    %s,
                    %s,
                    %s,
                    0x00,
                    0x00,
                    '',
                    0x00,
                    %s,
                    %s,
                    %s,
                    '',
                    %s,
                    0,
                    0x00
                )
            """, (
                doc_id, bas_now, bas_year_prefix, doc_number,
                bas_admission,          # _Fld13191 дата прийому
                org_ref,                # _Fld13192 організація
                dept_ref,               # _Fld13193 підрозділ
                _empty_ref(),           # _Fld13194
                _empty_ref(),           # _Fld13195
                _empty_ref(),           # _Fld13196
                _empty_ref(),           # _Fld13197
                _empty_ref(),           # _Fld13198
                _empty_ref(),           # _Fld13202
                pos_ref,                # _Fld13203 посада
                _empty_ref(),           # _Fld13204
                _empty_ref(),           # _Fld13205
                _empty_ref(),           # _Fld13210
                employee_name,          # _Fld13211 ПІБ текстом
                emp_ref,                # _Fld13212 співробітник
                _empty_ref(),           # _Fld13214
            ))

            conn.commit()

            logger.info(
                "Створено прийом '%s': %s → '%s', підрозділ %s, дата %s",
                doc_number, employee_name, position_name, department_number, admission_date,
            )

            return {
                "admission_created": True,
                "admission_number": doc_number,
                "admission_date": admission_date,
                "employee_name": employee_name,
                "position_name": position_name,
                "department_number": department_number,
                "message": (
                    f"Прийом створено (непроведений): №{doc_number}, "
                    f"{employee_name}, посада '{position_name}', "
                    f"підрозділ {department_number}, дата {admission_date}"
                ),
            }

        except Exception as e:
            conn.rollback()
            raise
        finally:
            cursor.close()
    finally:
        conn.close()


# ── Handler registration ────────────────────────────────────────────


def register_bdu_handlers(worker: ZeebeWorker) -> None:
    """Register all BDU task handlers."""

    @worker.task(task_type="bdu-check-position", timeout_ms=60_000)
    async def bdu_check_position(
        position_name: str = "",
        department_name: str = "",
        org_okpo: str = "",
        **kwargs: Any,
    ) -> dict:
        """Перевірити чи є посада в штатному розкладі БДУ.

        Input variables:
            position_name (str): назва посади ('Продавець-консультант')
            department_name (str): назва підрозділу ('101 Call-center')
            org_okpo (str): код ЄДРПОУ організації

        Output variables:
            x_studio_camunda_position_exists (bool): чи знайдена посада
            message (str): опис результату
        """
        department_number = _extract_department_number(department_name)
        logger.info(
            "bdu-check-position: посада='%s', підрозділ='%s'→'%s', орг='%s'",
            position_name, department_name, department_number, org_okpo,
        )
        return await asyncio.to_thread(
            _check_position_exists, position_name, department_number, org_okpo,
        )

    @worker.task(task_type="bdu-check-units", timeout_ms=60_000)
    async def bdu_check_units(
        position_name: str = "",
        department_name: str = "",
        org_okpo: str = "",
        **kwargs: Any,
    ) -> dict:
        """Перевірити чи достатньо одиниць в штатному.

        Input variables:
            position_name (str): назва посади
            department_name (str): назва підрозділу
            org_okpo (str): код ЄДРПОУ

        Output variables:
            x_studio_camunda_need_more_units (bool): потрібно додати одиниці
            has_vacancy (bool): є вакансія
            total_units (int): всього одиниць
            occupied_count (int): зайнято
            message (str): опис результату
        """
        department_number = _extract_department_number(department_name)
        logger.info(
            "bdu-check-units: посада='%s', підрозділ='%s'→'%s', орг='%s'",
            position_name, department_name, department_number, org_okpo,
        )
        return await asyncio.to_thread(
            _check_units_available, position_name, department_number, org_okpo,
        )

    @worker.task(task_type="bdu-add-position", timeout_ms=60_000)
    async def bdu_add_position(
        position_name: str = "",
        department_name: str = "",
        org_okpo: str = "",
        **kwargs: Any,
    ) -> dict:
        """Додати позицію в штатний розклад БДУ.

        Input variables:
            position_name (str): назва посади
            department_name (str): назва підрозділу
            org_okpo (str): код ЄДРПОУ

        Output variables:
            position_added (bool): успішно додано
            x_studio_camunda_position_exists (bool): true після додавання
            message (str): опис результату
        """
        department_number = _extract_department_number(department_name)
        logger.info(
            "bdu-add-position: посада='%s', підрозділ='%s', орг='%s'",
            position_name, department_number, org_okpo,
        )
        return await asyncio.to_thread(
            _add_position_to_schedule, position_name, department_number, org_okpo,
        )

    @worker.task(task_type="bdu-increase-units", timeout_ms=60_000)
    async def bdu_increase_units(
        position_name: str = "",
        department_name: str = "",
        org_okpo: str = "",
        **kwargs: Any,
    ) -> dict:
        """Збільшити кількість одиниць позиції в штатному на 1.

        Input variables:
            position_name (str): назва посади
            department_name (str): назва підрозділу
            org_okpo (str): код ЄДРПОУ

        Output variables:
            units_increased (bool): успішно збільшено
            x_studio_camunda_need_more_units (bool): false після збільшення
            message (str): опис результату
        """
        department_number = _extract_department_number(department_name)
        logger.info(
            "bdu-increase-units: посада='%s', підрозділ='%s', орг='%s'",
            position_name, department_number, org_okpo,
        )
        return await asyncio.to_thread(
            _increase_units, position_name, department_number, org_okpo,
        )

    @worker.task(task_type="bdu-create-admission", timeout_ms=120_000)
    async def bdu_create_admission(
        employee_name: str = "",
        position_name: str = "",
        department_name: str = "",
        org_okpo: str = "",
        admission_date: str = "",
        **kwargs: Any,
    ) -> dict:
        """Створити прийом на роботу в БДУ (непроведений).

        Input variables:
            employee_name (str): ПІБ працівника
            position_name (str): назва посади
            department_name (str): назва підрозділу
            org_okpo (str): код ЄДРПОУ
            admission_date (str): дата прийому (ISO)

        Output variables:
            admission_created (bool): успішно створено
            admission_number (str): номер документа
            message (str): опис результату
        """
        if not admission_date:
            admission_date = datetime.now().strftime("%Y-%m-%d")

        department_number = _extract_department_number(department_name)
        logger.info(
            "bdu-create-admission: '%s' → посада='%s', підрозділ='%s', дата='%s'",
            employee_name, position_name, department_number, admission_date,
        )
        return await asyncio.to_thread(
            _create_admission,
            employee_name, position_name, department_number, org_okpo, admission_date,
        )

    logger.info(
        "BDU handlers registered: bdu-check-position, bdu-check-units, "
        "bdu-add-position, bdu-increase-units, bdu-create-admission"
    )
