# BDU Admission Worker — Design Spec

## Overview

Окремий Zeebe worker (`worker_bdu/`) для роботи з БАС Бухгалтерія (MSSQL).
Перевіряє штатний розклад та створює документ прийому на роботу (непроведений).

## Task Types

### 1. `bdu-check-position`
Перевірити чи є посада в штатному розкладі.

**Input:** `position_name`, `department_name`, `org_okpo`
**Output:** `x_studio_camunda_position_exists: bool`

**SQL логіка:**
```sql
SELECT TOP 1 p._Fld27127 AS units
FROM _Reference12429 p
JOIN _Reference12325 pos ON p._Fld27124RRef = pos._IDRRef
JOIN _Reference100 dept ON p._Fld27123RRef = dept._IDRRef
JOIN _Reference90 org ON p._OwnerIDRRef = org._IDRRef
WHERE pos._Description = @position_name
  AND dept._Description LIKE @department_number + '%'
  AND org._Fld1494 = @org_okpo
  AND p._Marked = 0x00
  AND p._Fld27127 > 0
ORDER BY p._Fld27129 DESC
```

### 2. `bdu-check-units`
Перевірити чи достатньо вакантних одиниць.

**Input:** `position_name`, `department_name`, `org_okpo`
**Output:** `x_studio_camunda_need_more_units: bool`, `total_units: int`, `occupied_count: int`

**SQL логіка:**
1. Отримати `_Fld27127` (кількість одиниць) з `_Reference12429`
2. Порахувати проведені прийоми з `_Document12438` де та сама посада + підрозділ + організація
3. Порівняти: `need_more_units = occupied >= total_units`

### 3. `bdu-create-admission`
Створити співробітника та непроведений документ прийому.

**Input:** `employee_name`, `position_name`, `department_name`, `org_okpo`, `admission_date`
**Output:** `admission_created: bool`, `admission_number: str`

**Логіка:**
1. Знайти фіз.особу в `_Reference151` за ПІБ
2. Створити запис в `_Reference102` (Співробітник) з посиланням на фіз.особу
3. Створити запис в `_Document12438` (Прийом) з `_Posted = 0x00`

## Database Schema (BAS Бухгалтерія)

### Таблиці

| Таблиця | Призначення |
|---|---|
| `_Reference12429` | Позиції штатного розкладу |
| `_Reference12325` | Посади |
| `_Reference100` | Підрозділи |
| `_Reference90` | Організації |
| `_Reference151` | Фізичні особи |
| `_Reference102` | Співробітники |
| `_Document12438` | Прийом на роботу |

### Маппінг полів

**_Reference12429 (Позиції штатного):**
- `_Fld27123RRef` → `_Reference100` (Підрозділ)
- `_Fld27124RRef` → `_Reference12325` (Посада)
- `_OwnerIDRRef` → `_Reference90` (Організація)
- `_Fld27127` — кількість одиниць

**_Reference102 (Співробітники):**
- `_Description` — ПІБ
- `_Fld1674RRef` → `_Reference151` (Фізична особа)
- `_Fld27517RRef` → `_Reference100` (Підрозділ)

**_Document12438 (Прийом на роботу):**
- `_Fld13191` — дата прийому (+ 2000 років)
- `_Fld13192RRef` → `_Reference90` (Організація)
- `_Fld13193RRef` → `_Reference100` (Підрозділ)
- `_Fld13203RRef` → `_Reference12325` (Посада)
- `_Fld13212RRef` → `_Reference102` (Співробітник)
- `_Fld13199` — ставка (100)
- `_Posted = 0x00` — непроведений

### Зміщення дат
BAS зберігає дати з offset +2000 років: `2025-01-01` → `4025-01-01`

## Architecture

```
worker_bdu/
├── __init__.py
├── __main__.py      # python -m worker_bdu
├── worker.py        # Zeebe connection, reconnect, exception handler
└── handlers.py      # 3 task types: bdu-check-position, bdu-check-units, bdu-create-admission
```

Окремий процес, не залежить від основного worker/.
Використовує ті ж env змінні: `BAS_DB_HOST`, `BAS_DB_PORT`, `BAS_DB_USER`, `BAS_DB_PASSWORD`, `BAS_DB_NAME`.

## BPMN Integration

Підпроцес "Офіційний прийом на роботу" (`Process_0fx4kkx`):
- Service Task "Перевірити чи є посада" → task type `bdu-check-position`
- Service Task "Перевірити кількість одиниць" → task type `bdu-check-units`
- Service Task "Створити прийом у БДУ" → task type `bdu-create-admission`

## Camunda Variables

| Змінна | Тип | Джерело |
|---|---|---|
| `employee_name` | string | ПІБ з Odoo (hr.applicant) |
| `admission_date` | string | ISO дата прийому |
| `department_name` | string | Назва підрозділу ("101 Call-center") |
| `position_name` | string | Назва посади ("Продавець-консультант") |
| `org_okpo` | string | ЄДРПОУ організації |

## Risks & Mitigations

| Ризик | Мітігація |
|---|---|
| Прямий INSERT в 1С БД | Документ створюється непроведеним (`_Posted=0x00`), бухгалтер перевіряє |
| Фіз.особа не знайдена за ПІБ | Повертає помилку, User Task для ручного створення |
| Дублювання співробітника | Перевірка EXISTS перед INSERT |
| Некоректний GUID | Використовуємо `uuid.uuid4()` в форматі 1С (bytes) |
