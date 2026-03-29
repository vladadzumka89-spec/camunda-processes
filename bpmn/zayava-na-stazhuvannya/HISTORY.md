# БП "Заява на стажування" — Історія розробки

## Загальна інформація

- **BPMN Process ID**: `Process_13xdec3`
- **Проєкт Odoo**: "Прийом на роботу" (project_id: 550)
- **Stage ID**: 7739 (В процесі)
- **Prod Odoo**: `o.tut.ua`
- **Camunda**: `camunda25.a.local:8088`
- **Webhook #119**: "Створення задачі Camunda" (prod)

---

## Архітектура процесу

### Запуск процесу
- **З Odoo (hr.applicant)**: Автоматизація "Camunda: Запуск прийом з картки кандидата" — тригер: стадія "Запрошено на стажування"
- **З Odoo (project.task)**: Автоматизація "Запуск процесу Camunda" — тригер: стадія "В процесі", умова: Ключ процесу Camunda = ""
- Обидві використовують `collect_camunda_variables()` для збору x_studio_camunda_* полів
- **Process Definition ID**: `Process_13xdec3` (раніше був `Process_0p6254o` — виправлено)

### Основний процес
- Start → XOR (odoo_check) → Створити головне завдання (http-request-smart) → User Tasks
- Call Activities: "Видача ОЗ", "Створення фіз особи в ДО3 та прийому в ЄРП"

### Підпроцеси (Call Activity)
- Мають ізольований scope — потребують **Propagate all variables** або input mappings
- `parent_process_instance_key` визначається воркером через Operate API v1

---

## Odoo автоматизації (prod)

### Webhook #119 — "Створення задачі Camunda"
- Створює головну задачу (create_process: true) або підзадачу
- Пошук батьківської задачі: 3 кроки (по process_instance_key → будь-яка → по parent_process_instance_key)
- Динамічно додає x_studio_camunda_* поля з payload
- **ВАЖЛИВО**: блок динамічного додавання полів має перевіряти тип поля (many2one/integer) щоб уникнути помилки `res.users()`

### "Запуск процесу Camunda" (project.task)
- Модель: Завдання
- Тригер: Етап встановлений на "В процесі"
- Домен: Проєкт = Запуск процесів / Прийом на роботу / Зміна терміналу ФОП
- Умова: Стадія = В процесі AND Ключ процесу Camunda = ""
- Код: get_camunda_oauth_token() → collect_camunda_variables() → collect_file_references() → start_camunda_process()

### "Camunda: заповнити керівника з підрозділу"
- Тригер: On create and edit, під час оновлення поля "Підрозділ"
- Бере `x_studio_camunda_pidrozdil` (many2one → hr.department)
- Записує `dept.manager_id.user_id` в `x_studio_camunda_manager_user_id`

### "Camunda: призначення виконавця"
- Тригер: On create
- Бере `x_studio_camunda_user_id` і записує в `user_ids` (Уповноважені)
- Також має перевіряти `x_studio_camunda_role_code` для ролей

### "Camunda: зняти виконавців після виконання"
- Тригер: On create and edit, під час оновлення Стадії
- Коли задача виконана і є кілька виконавців — залишає тільки того хто виконав

---

## Ключові поля Camunda (project.task)

| Поле | Технічна назва | Тип |
|------|---------------|-----|
| Прізвище | x_studio_camunda_surname | char |
| Ім'я | x_studio_camunda_first_name | char |
| По-батькові | x_studio_camunda_patronymic | char |
| ІПН | x_studio_camunda_tax_id | char |
| Стать | x_studio_camunda_gender | char/selection |
| Дата народження | x_studio_camunda_birth_date | date |
| Email | x_studio_camunda_email | char |
| Телефон | x_studio_camunda_phone | char |
| Серія паспорта | x_studio_camunda_passport_series | char |
| Номер паспорта | x_studio_camunda_passport_number | char |
| Дата видачі паспорта | x_studio_camunda_passport_date | date |
| Орган що видав | x_studio_camunda_passport_authority | char |
| Адреса | x_studio_camunda_address | char |
| Підрозділ (ID) | x_studio_camunda_pidrozdil | many2one → hr.department |
| Підрозділ (назва) | x_studio_camunda_pidrozdil_name | char |
| Посада | x_studio_camunda_position_id | char |
| Організація | x_studio_camunda_organization | integer |
| Організація (назва) | x_studio_camunda_organization_name | char |
| Дата початку стажування | x_studio_camunda_start_date | date |
| Керівник | x_studio_camunda_manager_user_id | many2one → res.users |
| Наставник | x_studio_camunda_mentor_id | many2one → res.users |
| Виконавець (ID) | x_studio_camunda_user_id | integer |
| Код ролі | x_studio_camunda_role_code | char |

---

## Модель "Ролі процесів Camunda"

- **Модель**: x_studio_camunda_process_role
- **Розташування**: Проєкт → Налаштування → Ролі процесів Camunda
- **Поля**: Назва, Код ролі, Вид процесу, Підрозділ, Виконавці (many2many), Активна, Проєкт

### Ролі:
| Назва | Код |
|-------|-----|
| Ревізор комерційний | revisor_commercial |
| Фінансист | financist |
| Бухгалтер | accountant |
| Діловод | dilovod |
| Технічна підтримка | tech_support |
| Аналітик | analyst |

### Використання в BPMN:
```
x_studio_camunda_role_code: "tech_support"
```

---

## Виправлення 29.03.2026

### Webhook #119 — фікс `res.users()`
Блок динамічного додавання полів перевіряє тип поля (many2one/integer) і пропускає невалідні рядки типу `"res.users()"`:
```python
if ftype in ('many2one', 'integer'):
    try:
        int_val = int(value)
        if int_val > 0:
            task_vals[key] = int_val
    except Exception:
        pass
```
**Увага:** в safe_eval Odoo `ValueError` не доступний — використовувати `except Exception`.

### Дублі підзавдань — Task Listener
**Причина:** User Tasks мали одночасно body inputs (method/url/headers/body) І Task Listener `Creating: http-request-smart` — воркер викликався двічі.
**Рішення:** Видалити Task Listener з User Tasks які мають body inputs.

### Модель ролей на проді
- **Модель**: `x_camunda` (не `x_studio_camunda_process_role`)
- **Поле активності**: `x_active` (не `x_studio_camunda_is_active`)
- **Код ролі**: `x_studio_camunda_code` (тип text)
- **Виконавці**: `x_studio_camunda_user_ids` (many2many → res.users)
- **Підрозділ**: `x_studio_camunda_department_id` (many2many)
- **Вид процесу**: `x_studio_camunda_process_type` (many2many)

### Автоматизація "Camunda: призначення виконавця"
Шукає по `x_studio_camunda_role_code` в моделі `x_camunda`, підставляє виконавців з ролі в `user_ids`. Fallback — `x_studio_camunda_user_id` (прямий ID).

### Автоматизація "Camunda: зняти виконавців після виконання"
При виконанні задачі з кількома виконавцями — залишає тільки того хто виконав.

---

## Відомі проблеми та рішення

### 1. Помилка `res.users()` при створенні підзадачі
**Причина**: Порожній many2one recordset передається як рядок "res.users()" в payload
**Рішення**: В webhook блок динамічного додавання полів має перевіряти тип поля:
```python
if ftype in ('many2one', 'integer'):
    try:
        int_val = int(value)
        if int_val > 0:
            task_vals[key] = int_val
    except (ValueError, TypeError):
        pass
```

### 2. Батьківська задача не знайдена (Call Activity)
**Причина**: Підпроцес має свій process_instance_key, відмінний від батьківського
**Рішення**: Воркер визначає parent_process_instance_key через Operate API v1; webhook шукає по ньому (Крок 3)
**Додатково**: Elasticsearch indexing lag може спричинити None — потрібен retry

### 3. Дублі процесів
**Причина**: Task Listener `Creating: http-request-smart` на User Task створює повторний виклик
**Рішення**: Видалити Task Listener з User Tasks які вже мають body inputs

### 4. Виконавець не заповнюється
**Причина**: User Task типу "Camunda user task" (Zeebe-native) не обробляється воркером
**Рішення**: Змінити Type на "Job worker" з task definition `http-request-smart`, АБО використовувати Task Listener

### 5. Process Definition ID не знайдено
**Причина**: bpmn_process_id не передається в payload
**Рішення**: Воркер має передавати job.bpmn_process_id; або зробити fallback в webhook

---

## Воркер (worker/http_request_smart.py)

- **Job type**: http-request-smart
- **Контейнер**: python-worker
- Передає: process_instance_key, element_instance_key, bpmn_process_id, element_id, job_key, user_task_key
- Визначає parent_process_instance_key через Operate API v1
- Клас TaskListenerCompleted — для обробки Task Listener jobs
- **ВАЖЛИВО**: після змін коду потрібен `docker compose build python-worker && docker compose up -d python-worker`

---

## Інтеграція з ДО3

### Service Task "Створити документ" (REST Outbound Connector)
```json
{
  "featureCode": "СтворюватиФізичнуОсобу",
  "database": "DO3",
  "createDocument": true,
  "documentType": "Заява на стажування.",
  "properties": {
    "Подразделение": x_studio_camunda_pidrozdil_name,
    "Прізвище": x_studio_camunda_surname,
    "Імя": x_studio_camunda_first_name,
    "ПоБатькові": x_studio_camunda_patronymic,
    "ІПН": x_studio_camunda_tax_id,
    "СеріяПаспорта": x_studio_camunda_passport_series,
    "НомерПаспорту": x_studio_camunda_passport_number,
    "ДатаВидачіПаспорта": x_studio_camunda_passport_date,
    "ОрганЩоВидавПаспорт": x_studio_camunda_passport_authority,
    "АдресаПроживання": x_studio_camunda_address,
    "ДатаНародження": x_studio_camunda_birth_date,
    "ДатаПочаткуСтажування": x_studio_camunda_start_date,
    "ОсобистийEmail": x_studio_camunda_email,
    "НомерТелефонуКандидата": x_studio_camunda_phone,
    "Стать": x_studio_camunda_gender,
    "Посада": x_studio_camunda_position_id
  },
  "attachedFile": attachedFile
}
```

---

## DMN таблиці

### "Які ОЗ видати по підрозділу" (oz-by-department)
- Input: ID підрозділу (number), Посада (string)
- Output: Перелік ОЗ (string)
- Result variable: `oz_list`
- Hit policy: First

---

## Інтеграція з БДУ (worker_bdu)

- **Job types**: bdu-check-position, bdu-check-units
- Перевірка штатного розкладу в БАС Бухгалтерія
- Окремий воркер worker_bdu (не python-worker)
- Поля: x_studio_camunda_position_id, x_studio_camunda_pidrozdil_name, x_studio_camunda_organization_name

---

## GitHub sync (Web Modeler)

### Налаштування
| Поле | Значення |
|------|----------|
| Provider | GitHub |
| Client ID | Iv23liK1bhjCWMOPj9bB |
| Installation ID | 118185714 |
| Repository URL | https://github.com/vladadzumka89-spec/camunda-processes |
| Branch | main |

### Repository paths
| Проєкт | Path |
|--------|------|
| Заява на стажування | bpmn/zayava-na-stazhuvannya |
| Чек лист по вітринам | bpmn/Чек лист по вітринам |
| Задача для адміністратора | bpmn/Задача для адміністратора |
| Збір розміру світшоту | bpmn/Збір розміру світшоту |
| Прийом для тестування | bpmn/Прийом для тестування |
