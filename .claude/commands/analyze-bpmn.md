---
name: analyze-bpmn
description: "📊 Аналізувати BPMN-файл на відповідність правилам проєкту та стандартам BPMN 2.0"
---

# 📊 Analyze BPMN — аналіз BPMN-процесу

Аналізує вказаний BPMN-файл на відповідність правилам з CLAUDE.md та світовим стандартам BPMN 2.0.
Генерує звіт з помилками, попередженнями та рекомендаціями. **Нічого не виправляє автоматично.**

## Крок 1: Визнач файл для аналізу

Якщо `$ARGUMENTS` не порожній — використай як шлях до BPMN-файлу.

Якщо `$ARGUMENTS` порожній — знайди всі `.bpmn` файли в директорії `bpmn/`:
```bash
ls bpmn/*.bpmn
```
Запитай через AskUserQuestion який файл аналізувати.

Прочитай вміст BPMN-файлу (XML) за допомогою Read.

Якщо файл не існує або не є валідним XML — повідом і заверши.

Запам'ятай:
- `BPMN_FILE` — шлях до файлу
- `BPMN_XML` — вміст файлу

## Крок 2: Загугли стандарти BPMN 2.0

Використай WebSearch щоб знайти актуальні best practices:
- Запит: "BPMN 2.0 best practices common errors validation rules 2025"
- Запит: "BPMN modeling guidelines gateway sequence flow validation"

Запам'ятай ключові рекомендації зі знайдених джерел для використання в Кроці 4.

## Крок 3: Перевірки відповідності правилам проєкту (CLAUDE.md)

Проаналізуй BPMN XML та виконай ВСІ перевірки нижче. Для кожної перевірки — фіксуй результат: PASS / FAIL / WARN.

### 3.1 Базова структура процесу

- **isExecutable**: `<bpmn:process>` має `isExecutable="true"`. Якщо `false` або відсутній — FAIL.
- **versionTag**: `<zeebe:versionTag value="..."/>` присутній всередині процесу. Якщо немає — FAIL.
- **XOR gateway на початку**: Перший елемент після Start Event — XOR Gateway з перевіркою `odoo_task_id`. Шукай `is defined(odoo_task_id)` у conditionExpression. Якщо немає — FAIL.

### 3.2 Gateways

- **Default flow на XOR**: Кожен `<bpmn:exclusiveGateway>` з 2+ outgoing flows має атрибут `default="..."`. Якщо немає — FAIL.
- **FEEL умови починаються з `=`**: Кожен `<bpmn:conditionExpression>` має починатися з `=`. Якщо ні — FAIL.
- **Gateway має 2+ outgoing**: Кожен splitting gateway (exclusive, inclusive, parallel) має мінімум 2 outgoing sequence flows. Якщо менше — WARN.

### 3.3 User Tasks

- **Boundary events**: Кожен `<bpmn:userTask>` має рівно 2 boundary timer events: нагадування та дедлайн. Шукай `<bpmn:boundaryEvent attachedToRef="USER_TASK_ID">` з `<bpmn:timerEventDefinition>`. Якщо менше 2 — FAIL.
- **Non-interrupting**: Обидва boundary events мають `cancelActivity="false"`. Якщо `true` або не вказано — FAIL.
- **Ескалація в Lane_manager**: Ескалаційні User Tasks (після дедлайн boundary event) мають бути в lane з id, що містить `manager` або `Lane_manager`. Назва має містити `ЕСКАЛАЦІЯ`. Якщо ні — WARN.

### 3.4 Swim Lanes

- **Мінімум 3 lanes**: Процес має мінімум 3 `<bpmn:lane>`: система, виконавець, керівник (ескалація). Якщо менше — WARN.
- **Lane для системи**: Є lane з id/назвою що вказує на систему (`Lane_system`, "Система" тощо). Якщо немає — WARN.

### 3.5 Odoo інтеграція

- **Префікс `x_studio_camunda_`**: У body service tasks шукай поля Odoo. Якщо є поля що передаються до Odoo без `x_studio_camunda_` префіксу (крім стандартних: `name`, `description`, `_model`, `_id`, `create_process`, `process_instance_key`, `action_identifier`, `context`, `payload`, `groups`) — WARN.

### 3.6 ID конвенції

Перевір ID елементів на відповідність конвенціям:

| Тип | Очікуваний префікс |
|-----|-------------------|
| Service Task | `ST_` або `Task_` |
| User Task | `UT_` або `Task_` |
| Call Activity | `CA_` |
| Boundary Event | `BE_` |
| Exclusive Gateway | `GW_` або `XOR_` |
| Sequence Flow | `Flow_` |
| End Event | `End_` |

Якщо ID не відповідає конвенції — WARN (не FAIL, бо автогенеровані ID допустимі).

### 3.7 Назви та мова

- **Назви елементів українською**: Перевір `name` атрибути основних елементів (tasks, gateways). Якщо назви англійською — WARN.
- **ID латиницею**: Перевір `id` атрибути. Якщо ID містить кирилицю — FAIL.

### 3.8 DMN та бізнес-правила

- Якщо є XOR/inclusive gateways з складними FEEL умовами (більше ніж проста перевірка змінної) — WARN, рекомендація винести в DMN.

### 3.9 Батьківська задача на початку процесу

Кожен головний процес (НЕ Call Activity підпроцес) повинен створювати **батьківську задачу в Odoo** одразу на початку — після XOR gateway перевірки `odoo_task_id`.

**Як реалізовано в проєкті:**
- Service Task з job type `send-notification` та `notification_type` зі значенням `"feature_start"`, `"sync_start"` або аналогічним
- Хендлер Python викликає `odoo.create_task(..., create_process=True)` — прапор `create_process=True` сигналізує Odoo створити задачу-контейнер (батьківську)
- Результат зберігається у змінну `odoo_task_id`

**Перевірки:**
- Одразу після XOR gateway `GW_odoo_check` (на default-гілці) має бути Service Task що створює батьківську задачу. Якщо такого немає — FAIL.
- Input mapping має містити `notification_type` зі значенням що закінчується на `_start` (або аналогічне, що позначає створення батьківської задачі). Або, якщо використовується `http-request-smart`, body має містити `create_process: true`. Якщо ні — WARN.
- **Виняток:** Call Activity підпроцеси (наприклад `server-deploy.bpmn`) НЕ створюють батьківську задачу — вони працюють в контексті процесу-викликача.

**Еталонна структура (feature-pipeline.bpmn):**
```
Start Event
  → GW_odoo_check (XOR: "Задача в Odoo існує?")
      ├── [default] → task_create_odoo (send-notification, notification_type="feature_start")
      │                    → Merge_odoo
      └── [= is defined(odoo_task_id) ...] → Merge_odoo (skip)
  → [далі по процесу]
```

### 3.10 Підзадачі прив'язані до батьківської задачі

Усі наступні задачі що створюються в Odoo протягом процесу повинні бути **підзадачами** батьківської задачі.

**Як реалізовано в проєкті:**
- Підзадачі прив'язуються до батьківської через змінну `process_instance_key` — Odoo на своїй стороні використовує цей ключ для встановлення зв'язку батько→дитина
- Підзадачі НЕ використовують `create_process: true` (або хендлер передає `create_process=False`)
- Два варіанти створення підзадач:
  1. **`http-request-smart`** — прямий HTTP POST на webhook з `process_instance_key` в body
  2. **`send-notification` / `create-odoo-task`** — Python хендлер, який автоматично передає `process_instance_key` через `OdooClient`

**Перевірки:**
- Кожен Service Task що створює задачу в Odoo (тип `http-request-smart` з webhook URL, або `send-notification`/`create-odoo-task`) ПІСЛЯ батьківської задачі повинен передавати `process_instance_key`. Якщо `process_instance_key` відсутній у body — FAIL.
- Підзадачі НЕ повинні мати `create_process: true` в body. Якщо є — FAIL.
- Кожна підзадача має мати змістовне `name` що описує конкретний етап процесу. Якщо `name` відсутнє або generic — WARN.

**Приклади підзадач (feature-pipeline.bpmn):**
```
task_subtask_verify_staging   → "Перевірити staging: PR #..." (http-request-smart)
task_subtask_merge_main       → "Review та merge PR в main: ..." (http-request-smart)
task_subtask_verify_prod      → "Перевірити production: PR #..." (http-request-smart)
```

**Приклади підзадач (enterprise-sync.bpmn):**
```
task_notify_conflicts → "Виправити конфлікти (N модулів)" (create-odoo-task)
ST_review_sync        → "Переглянути аналіз оновлення" (create-odoo-task)
```

### 3.11 Error/Fallback — створення підзадачі з описом помилки

Кожен процес повинен мати **обробку помилок** що створює підзадачу в Odoo з описом помилки при збоях.

**Як реалізовано в проєкті:**
Використовується **Event-Triggered Error Subprocess** (`triggeredByEvent="true"`) на рівні головного процесу:

```xml
<bpmn:subProcess id="subprocess_error_..." triggeredByEvent="true">
  <bpmn:startEvent id="evt_error_catch_...">
    <!-- ловить всі помилки, зберігає код та повідомлення -->
    <zeebe:errorEventDefinition
      errorCodeVariable="caught_error_code"
      errorMessageVariable="caught_error_message" />
  </bpmn:startEvent>
  → Service Task (send-notification) з описом помилки
  → [опційно: rollback]
  → End Event
</bpmn:subProcess>
```

**Перевірки:**
- Процес має містити хоча б один `<bpmn:subProcess triggeredByEvent="true">` з error start event всередині. Якщо немає — FAIL.
- Error subprocess повинен містити Service Task що створює задачу в Odoo з описом помилки. Шукай `send-notification` з `notification_type` що містить `error` (наприклад `"pipeline_error"`, `"sync_error"`, `"deploy_error"`). Якщо немає — FAIL.
- `message_body` або `description` error-задачі повинен містити інформацію про помилку: `caught_error_code` та `caught_error_message`. Якщо ці змінні не передаються — WARN.
- Error-задача повинна створюватись як підзадача (без `create_process: true`), щоб вона потрапила до батьківської задачі. Якщо є `create_process: true` — FAIL.
- **Виняток для Call Activity:** підпроцес `deploy-process.bpmn` після створення error-задачі виконує rollback і **re-throw** помилки через Error End Event (`errorCode="DEPLOY_FAILED"`) — щоб батьківський процес теж міг її обробити. Це рекомендований паттерн для Call Activity.

**Еталонні реалізації:**

| Процес | Error Subprocess ID | Service Task | notification_type | Додатково |
|--------|-------------------|--------------|-------------------|-----------|
| feature-to-production | `subprocess_error_pipeline` | `ST_error_notify_pipeline` | `pipeline_error` | — |
| upstream-sync | `subprocess_error_sync` | `ST_error_notify_sync` | `sync_error` | — |
| deploy-process | `subprocess_error` | `ST_error_odoo_task` | `deploy_error` | + rollback + re-throw |

### 3.12 CI/CD BPMN — Correlation keys та error templates

Виконувати тільки якщо `BPMN_FILE` містить `deploy-process`, `deploy-scheduler` або `feature-to-production` в назві.

**Перевірка: correlation_key для msg_deploy_done**

Знайди всі Service Tasks з `<zeebe:taskDefinition type="publish-message".../>` що передають `message_name = "msg_deploy_done"`.  
Для кожного такого task знайди input `target="correlation_key"` і перевір `source`:

- Якщо `source` містить `trigger_sha` (і не містить `pr_number`) — **FAIL**
  - Правило: FTP підписується з `correlation_key = pr_number`. Використання `trigger_sha` означає, що FTP ніколи не отримає меседж → "Unknown error" в коментарі PR
- Якщо `source` містить `pr_number` — PASS
- Якщо `msg_deploy_done` з `deploy_failed=true` не передає `error_type` — **WARN**
  - Правило: FTP використовує `error_type` в `GW_retry_type` для вибору між retry і rework

**Перевірка: error comment tasks містять реальну помилку**

Знайди всі Service Tasks з `<zeebe:taskDefinition type="github-comment".../>` де назва task містить "помилка", "failed", "fail", "error" (або елемент знаходиться після error boundary event).  
Для кожного знайди input `target="comment_text"` і перевір `source`:

- Якщо `source` — хардкодований рядок (починається з `="..."`) без посилання на `error_message`, `caught_error_message` або `error_traceback` — **FAIL**
  - Правило: error comment без реальної помилки — розробник не може діагностувати проблему. Шаблон ОБОВ'ЯЗКОВО має містити хоча б одну зі змінних: `error_message`, `caught_error_message`, `error_traceback`
- Якщо `source` містить `error_message` або `caught_error_message` — PASS

## Крок 4: Перевірки за стандартами BPMN 2.0

Виконай додаткові перевірки валідності BPMN:

### 4.1 Обов'язкові елементи

- **Start Event**: Процес має хоча б один `<bpmn:startEvent>`. Якщо немає — FAIL.
- **End Event**: Процес має хоча б один `<bpmn:endEvent>`. Якщо немає — FAIL.

### 4.2 Зв'язність

- **Sequence flows з'єднані**: Кожен `<bpmn:sequenceFlow>` має `sourceRef` і `targetRef` що існують як ID елементів в процесі. Якщо reference на неіснуючий елемент — FAIL.
- **Висячі елементи**: Кожен елемент (крім Start Event і End Event) має хоча б один incoming І один outgoing sequence flow. Boundary events — виняток (мають тільки outgoing). Якщо елемент без зв'язків — WARN.

### 4.3 Дублікати ID

- Всі `id` атрибути в документі унікальні. Якщо є дублікати — FAIL.

### 4.4 Рекомендації зі знайдених стандартів

На основі результатів WebSearch з Кроку 2 — додай релевантні рекомендації (best practices від Camunda, OMG, спільноти). Наприклад:
- Використання error boundary events для обробки помилок
- Документація процесу (annotations)
- Іменування conventions
- Уникнення складних вкладених gateways

## Крок 5: Згенеруй звіт

Згрупуй результати і виведи звіт у форматі нижче. Рахуй кількість помилок, попереджень і пройдених перевірок.

```
📊 Аналіз BPMN: {BPMN_FILE}
═══════════════════════════════════════

📋 Загальна інформація:
- Process ID: {id}
- Назва: {name}
- Version Tag: {version або "ВІДСУТНІЙ"}
- isExecutable: {true/false}
- Кількість елементів: {N} (tasks: {N}, gateways: {N}, events: {N})

───────────────────────────────────────
❌ ПОМИЛКИ ({N}) — порушення обов'язкових правил:

1. [Категорія] Опис проблеми
   → Елемент: {id} ({name})
   → Правило: {що порушено}

───────────────────────────────────────
⚠️ ПОПЕРЕДЖЕННЯ ({N}):

1. [Категорія] Опис
   → Елемент: {id} ({name})
   → Рекомендація: {що зробити}

───────────────────────────────────────
✅ ПРОЙДЕНО ({N}):

- isExecutable = true
- versionTag присутній
- XOR gateway на початку з перевіркою odoo_task_id
- Батьківська задача створюється після XOR gateway з create_process=true
- Усі підзадачі передають process_instance_key
- Error subprocess з створенням error-підзадачі в Odoo
- ...

───────────────────────────────────────
📖 РЕКОМЕНДАЦІЇ (з BPMN 2.0 стандартів):

1. {рекомендація з посиланням на джерело}
2. ...

───────────────────────────────────────
📊 Підсумок: {N} помилок, {N} попереджень, {N} пройдено
```

Якщо помилок 0:
> ✅ **Файл відповідає всім обов'язковим правилам проєкту!**

Якщо є помилки:
> ❌ **Знайдено {N} порушень обов'язкових правил. Рекомендується виправити перед деплоєм.**

**Не пропонуй автоматичне виправлення.** Лише звіт.
