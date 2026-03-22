# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Git policy

**ЗАБОРОНЕНО** виконувати будь-які записуючі git-операції (commit, push, merge, rebase, reset, checkout, branch -D, tag, stash drop тощо) без прямої явної вказівки користувача. Читання (status, log, diff, branch --list) — дозволено завжди.

## Project overview

Camunda 8.8 BPMN workflow orchestrator integrated with Odoo ERP and GitHub. Manages CI/CD pipelines (feature-to-production), upstream Odoo module synchronization, and reusable deployment sub-processes.

**Stack**: Python 3.12 async, Camunda 8.8.3 (Zeebe gRPC), Docker, asyncssh, httpx, aiohttp, pyzeebe.

## Build and run

```bash
# Local dev stack (Zeebe + Elasticsearch + Operate + Connectors + Worker)
docker compose up -d

# Full stack with Keycloak/Identity/Web Modeler
docker compose -f docker-compose-full.yaml up -d

# Run worker locally (outside Docker)
pip install -r requirements.txt
python -m worker

# Deploy BPMN to Zeebe (basic auth demo:demo)
curl -s -X POST "http://localhost:8088/v2/deployments" \
  -u "demo:demo" -F "resources=@bpmn/ci-cd/feature-to-production.bpmn"
```

## Tests

```bash
pytest                                           # all tests
pytest tests/test_handlers_deploy.py             # single file
pytest tests/test_handlers_deploy.py::test_git_pull_success  # single test
pytest -vv -s                                    # verbose with stdout
pytest tests/integration/                        # integration (requires Docker Zeebe)
```

Tests use `pytest` + `pytest-asyncio` + `unittest.mock`. Fixtures in `tests/conftest.py` provide `app_config`, `mock_ssh`, `mock_github`, `mock_odoo`, `mock_worker` (captures handlers via `worker.task` decorator interception).

## Architecture

```
                  GitHub/Odoo webhooks
                         │
              WebhookServer (aiohttp :9001)
                         │ publish Zeebe message
                         ▼
              Zeebe Engine (gRPC :26500)
                         │ poll jobs
                         ▼
              ZeebeWorker (pyzeebe, 26 task types)
                  │          │          │
              SSH cmds   GitHub API   Odoo webhook
           (staging/prod) (PR ops)  (task creation)
```

**Entry point**: `worker/__main__.py` → `worker.worker.main()` runs `worker_loop()` + `WebhookServer.start()` via `asyncio.gather()`.

**Handlers** (`worker/handlers/`): Each module exports `register_*_handlers(worker, config, ssh, ...)` that decorates async functions with `@worker.task(task_type=...)`. Groups:
- `deploy.py` (10): git-pull, detect-modules, docker-build, docker-up, module-update, cache-clear, smoke-test, http-verify, save-deploy-state, rollback
- `github.py` (4): pr-agent-review, github-merge, github-comment, github-create-pr
- `sync.py` (8): fetch-current-version, fetch-runbot, clone-upstream, sync-modules, diff-report, impact-analysis, git-commit-push, github-pr-ready
- `audit.py` (1): audit-analysis
- `clickbot.py` (1): clickbot-test
- `notify.py` (2): send-notification, create-odoo-task

**Clients**: `GitHubClient` (REST API), `OdooClient` (webhook POST), `AsyncSSHClient` (connection pooling + remote exec).

**Config**: `AppConfig.from_env()` loads from `.env.camunda`. Frozen dataclasses: `ZeebeConfig`, `GitHubConfig`, `OdooConfig`, `WebhookConfig`, `ServerConfig`. Servers dict keyed by name (staging, production, kozak_demo).

## BPMN processes

Three executable processes in `bpmn/`:
- **feature-to-production.bpmn** — Main CI/CD: PR event → PR-Agent review → score check → staging deploy → prod deploy. Uses message correlation (`msg_pr_event`, `msg_pr_updated`, `msg_odoo_task_done`).
- **upstream-sync.bpmn** — Nightly upstream Odoo module sync: fetch Runbot CI → clone → sync → audit → PR.
- **deploy-process.bpmn** — Reusable call activity: git-pull → detect-modules → docker-build → module-update → smoke-test → rollback on failure.

Camunda UI forms in `bpmn/forms/`. Root-level Ukrainian `.bpmn`/`.dmn` files are Odoo invoice/payment processes (use `http-request-smart` connectors, not the Python worker).

## Key ports

| Service | Port |
|---------|------|
| Zeebe gRPC | 26500 |
| Camunda REST + Operate UI | 8088 |
| Connectors | 8086 |
| Elasticsearch | 9200 |
| Webhook server | 9001 |

## Правила створення BPMN-процесів (Camunda 8.8 + Odoo)

## Обов'язкові правила (порушувати ЗАБОРОНЕНО)

1. **Кожен процес починається з XOR gateway** перевірки `odoo_task_id` — без винятків
2. **Кожен User Task має рівно 2 boundary events** — нагадування + дедлайн, **обидва non-interrupting**
3. **Усі поля Odoo мають префікс `x_studio_camunda_`** — без нього worker ігнорує поле
4. **Swim lanes обов'язкові** — мінімум 3: Система / Виконавець / Керівник (ескалація)
5. **Бізнес-правила — у DMN**, не в FEEL-умовах gateway якщо можуть змінюватися

---

## Загальна структура процесу

```
Start Event
  → XOR Gateway (id: "GW_odoo_check", name: "Задача в Odoo існує?", default: → ST_create_main)
      ├── [= is defined(odoo_task_id) and odoo_task_id != null] → Merge Gateway
      └── [default] → ST_create_main → Merge Gateway
                              ↓
                       [далі по процесу]
```

Умова на гілці "вгору" (задача вже існує):
```
= is defined(odoo_task_id) and odoo_task_id != null
```

---

## Правила роботи з Gateway

- **XOR:** ЗАВЖДИ вказувати **default flow** — інакше процес зависне якщо жодна умова не спрацює
- **Inclusive:** може активувати кілька гілок одночасно; merge-gateway чекає ВСІ активовані гілки
- Всі умови на sequence flow — FEEL-вирази, **обов'язково починаються з `=`**
- Бізнес-логіку що може змінюватися — **виносити в DMN**, не в FEEL на gateway

---

## Типи User Task — не плутати

| Тип | XML маркер | API | Tasklist |
|-----|-----------|-----|----------|
| **Zeebe-native** | `<zeebe:userTask />` в `<bpmn:extensionElements>` | `/v2/user-tasks/` | Так |
| **Job-based** | відсутній | НЕ видно через `/v2/user-tasks/` | Ні |

Перевірити тип: відкрити BPMN XML, шукати `<zeebe:userTask />`. Якщо немає — job-based.
**У цьому проєкті всі User Task є job-based** (обробляються воркером Odoo).

---

## Service Task: "Створити головне завдання" (`ST_create_main`)

- **Job type:** `http-request-smart`
- **Lane:** `Lane_system`

| Input | Local variable name | Значення |
|-------|-------------------|----------|
| method | `method` | `= "POST"` |
| url | `url` | `= "http://odoo.dev.dobrom.com/web/hook/21c8dbff-86e8-4005-9bfc-9f77ee9b5c57"` |
| headers | `headers` | `= {"Content-Type":"application/json"}` |
| body | `body` | див. нижче |

```
= {name: "<НАЗВА ПРОЦЕСУ>", create_process: true, _id: <ID_ПРОЄКТУ>}
```

> **ВАЖЛИВО:** `_id` — це ID проєкту в Odoo. Він різний для кожного процесу. **Завжди запитуй у користувача** ID проєкту перед тим як прописувати. НЕ використовуй захардкоджене значення.
> Щоб дізнатись ID: в Odoo відкрити потрібний проєкт → в URL буде число (наприклад `/web#id=252` → `_id: 252`).

```text
Приклад: _id: 252 — це проєкт "Запуск процесів". Для іншого проєкту буде інший ID.
```

---

## Service Task: "Отримати список працівників з Odoo"

- **Job type:** `http-request-smart`
- **Lane:** `Lane_system`

| Input | Local variable name | Значення |
|-------|-------------------|----------|
| method | `method` | `= "POST"` |
| url | `url` | `= "https://o.tut.ua/api/server-action"` |
| headers | `headers` | `= {"Content-Type":"application/json", "X-API-Key":"632b5ed6-091f-48db-bd01-1e60aeb10bfc"}` |
| body | `body` | див. нижче |
| result_variable_name | `result_variable_name` | (уточнити) |

```
= {
    action_identifier: "studio_customization._03c8968e-ba5a-43c9-839d-90197c29c03d",
    context: {},
    payload: { groups: [<ID групи працівників>] }
}
```

**Output:** `employees = result.data`

---

## User Task: налаштування

- **Lane:** `Lane_responsible` (або специфічна lane виконавця)
- **Multi-instance:** елемент колекції — `emp`, **НЕ** `employees`

| Input | Local variable name | Значення |
|-------|-------------------|----------|
| method | `method` | `= "POST"` |
| url | `url` | `= "http://odoo.dev.dobrom.com/web/hook/21c8dbff-86e8-4005-9bfc-9f77ee9b5c57"` |
| headers | `headers` | `= {"Content-Type":"application/json"}` |
| body | `body` | див. нижче |

```
= {
  name: "<назва підзадачі>" + emp.employee_name,
  description: "<опис задачі>",
  x_studio_camunda_user_ids: emp.user_id,
  process_instance_key: process_instance_key
}
```

- `name` — назва підзадачі для працівника в Odoo (вказує замовник)
- `description` — опис що потрібно зробити
- `x_studio_camunda_user_ids: emp.user_id` — призначає виконавця
- `process_instance_key` — зв'язок з інстансом процесу в Camunda

---

## Обов'язкові boundary events на кожному User Task

**Обидва таймери — non-interrupting (пунктирна рамка).** User Task залишається активним після спрацювання — працівник все ще може виконати задачу.

```
User Task
  ├─ BE_rem_xxx [cancelActivity="false", R/PT4H]
  │      └→ ST_rem_xxx (нагадування) → End_rem_xxx
  │
  └─ BE_ded_xxx [cancelActivity="false", P3D]
         └→ UT_esc_xxx (керівник, паралельно) → End_esc_xxx
```

| Boundary | `cancelActivity` | Типові значення |
|----------|-----------------|----------------|
| Нагадування (пунктирна) | `false` | `R/PT4H`, `R/PT24H` |
| Дедлайн (пунктирна) | `false` | `PT8H`, `P1D`, `P3D`, `P5D` |

### Service Task нагадування (`ST_rem_xxx`) — Lane_system:
```
body: = {
  name: "⚠️ НАГАДУВАННЯ: <назва задачі>",
  description: "...",
  _model: "project.project", _id: <ID_ПРОЄКТУ>,
  x_studio_camunda_user_ids: emp.user_id,
  process_instance_key: process_instance_key
}
```

### User Task ескалації (`UT_esc_xxx`) — Lane_manager:
```
body: = {
  name: "🔴 ЕСКАЛАЦІЯ: <що не зроблено>",
  description: "...",
  _model: "project.project", _id: <ID_ПРОЄКТУ>,
  process_instance_key: process_instance_key
}
```

---

## Swim Lanes — стандарт

| ID | Назва (приклади) | Що містить |
|----|-----------------|-----------|
| `Lane_system` | "Система (автоматично)", "Автоматизатор" | Start Event, Service Tasks, Gateways, Timer Intermediate |
| `Lane_responsible` + специфічна | "Бухгалтер", "Рекрутер", "Фінансист" | User Tasks — виконавець |
| `Lane_manager` | "Керівник (ескалація)" | Ескалаційні User Tasks |

Якщо кілька виконавців — кожному своя lane з бізнес-назвою ролі.

---

## Іменування (конвенції ID)

| Тип елемента | Префікс | Приклад |
|-------------|---------|---------|
| Service Task | `ST_` або `Task_` | `ST_create_main`, `Task_autofill` |
| User Task | `UT_` або `Task_` | `UT_manual_fix`, `Task_approve` |
| Call Activity | `CA_` | `CA_tp_reissue`, `CA_nego_self` |
| Boundary Event | `BE_` | `BE_rem_manual`, `BE_ded_enter` |
| Exclusive Gateway | `GW_` або `XOR_` | `GW_odoo_check`, `XOR_is_dup` |
| Merge Gateway | `Merge_` або `GW_..._merge` | `Merge_odoo`, `GW_odoo_merge` |
| Sequence Flow | `Flow_` | `Flow_to_check_dup` |
| End Event | `End_` | `End_final`, `End_esc_manual` |

**Назви елементів — українською, ID — латиниця.**

---

## Job Types

| Job type | Коли використовувати |
|----------|---------------------|
| `http-request-smart` | Усі HTTP-запити до Odoo (webhook, server-action) |
| `local-file-converter` | Конвертація файлів (PDF, Excel тощо) |
| `do3-feature` | Інтеграція з DO3 |

**Business Rule Task** (не Service Task) — для виклику DMN:
```xml
<bpmn:businessRuleTask id="ST_detect_format" name="Визначити формат [DMN]">
  <zeebe:calledDecision decisionId="<dmn-id>" resultVariable="<var>" />
```

---

## Endpoints Odoo

| Призначення | URL | Auth |
|-------------|-----|------|
| Webhook (створення задач, prod) | `https://o.tut.ua/web/hook/8531324a-2785-48d1-8f4d-ddd66a267d50` | — |
| Webhook (створення задач, dev) | `http://odoo.dev.dobrom.com/web/hook/21c8dbff-86e8-4005-9bfc-9f77ee9b5c57` | — |
| Server Action (дані, списки) | `https://o.tut.ua/api/server-action` | `X-API-Key: 632b5ed6-091f-48db-bd01-1e60aeb10bfc` |
| Верифікація особи | `https://o.tut.ua/api/verify-person` | `X-API-Key` |

---

## Змінні та scope

- **Call Activity** має ізольований scope — потрібні явні **input mappings** (вниз) і **output mappings** (вгору)
- **Перевірка існування:** `= is defined(var) and var != null`
- **Автоматичного rollback не існує** — кожен крок компенсації = окремий Service Task

| Змінна | Опис |
|--------|------|
| `odoo_task_id` | ID задачі в Odoo (при запуску з Odoo) |
| `process_instance_key` | Ключ інстансу Camunda |
| `employees` | Список працівників (масив) |
| `emp.employee_name` | Ім'я (ітератор multi-instance) |
| `emp.user_id` | ID користувача Odoo (ітератор multi-instance) |
| `responsible_user_id` | ID конкретного виконавця (якщо не multi-instance) |

---

## Composability — підпроцеси (Call Activity)

**Правило:** якщо блок повторюється у 2+ процесах — виносити в окремий `.bpmn` файл.

| Файл | Призначення |
|------|-------------|
| `nadannya-dostupiv-oz-v2.bpmn` | Надання доступів ОЗ |
| `nadannya-dostupiv-admin-v2.bpmn` | Надання доступів адміністратора |
| `oficiynyy-pryyom-v2.bpmn` | Офіційне прийняття на роботу |

Файли-підпроцеси — `kebab-case`, головні процеси — українська назва.

---

## Naming convention для полів в Odoo

Всі поля в Odoo ОБОВ'ЯЗКОВО мають починатися з:
```
x_studio_camunda_
```
Воркер передає в Camunda ЛИШЕ поля з цим префіксом. Поля без нього ігноруються.

Приклади:
- `x_studio_camunda_user_ids` — виконавець задачі
- `x_studio_camunda_<назва_поля>` — будь-яке інше поле

---

## Обов'язкові атрибути процесу (XML)

```xml
<bpmn:process id="Process_..." isExecutable="true">
  <zeebe:versionTag value="1.0" />   <!-- збільшувати при змінах -->
```

---

## Чеклист перед збереженням

- [ ] Є XOR gateway на початку з перевіркою `is defined(odoo_task_id)`
- [ ] Кожен User Task має 2 boundary events (обидва non-interrupting, пунктирна рамка)
- [ ] Ескалаційний User Task у `Lane_manager` з `🔴 ЕСКАЛАЦІЯ:` у назві
- [ ] Усі `x_studio_camunda_*` поля є там де потрібно
- [ ] Swim lanes відповідають ролям, назви lanes — бізнесові
- [ ] Call Activity замість copy-paste для блоків що повторюються
- [ ] DMN для бізнес-правил (хто погоджує, яка сума, який формат)
- [ ] `versionTag` оновлено якщо це нова версія процесу

---

## Анти-патерни (ЗАБОРОНЕНО)

❌ Прямий `Start Event → перший User Task` без XOR `odoo_check`
❌ User Task без boundary events
❌ Interrupting дедлайн на User Task — ескалація йде паралельно, задача залишається активною
❌ Бізнес-логіку що змінюється — в FEEL умовах gateway замість DMN
❌ Поля Odoo без `x_studio_camunda_` префіксу
❌ Повторюваний блок copy-paste в кількох процесах замість Call Activity
❌ `isExecutable="false"` у головному процесі
❌ Стрілки з 2 waypoints по діагоналі через інші елементи

---

## Правила візуальної розкладки BPMN (DI координати)

### Розміри елементів

| Елемент | width x height |
|---------|---------------|
| Start/End Event | 36 x 36 |
| Gateway | 50 x 50 |
| Task (Service/User/Business Rule) | 100 x 80 |
| Boundary Event | 36 x 36 |

### Базові відступи

| Константа | Значення | Опис |
|-----------|----------|------|
| POOL_X | 105 | Лівий край пулу |
| POOL_Y | 90 | Верхній край пулу |
| LANE_X | 135 | Початок елементів (POOL_X + 30 на label) |
| H_SPACING | 80 | Мінімальний горизонтальний зазор між елементами |
| V_PADDING | 30 | Вертикальний відступ від краю lane |
| MIN_LANE_HEIGHT | 170 | Мінімальна висота lane з tasks |

### Розрахунок розмірів

```
Pool width = max(2000, X_правого_елемента + 80 + 30)
Pool height = сума висот всіх lanes
Lane width = Pool width - 30
Lane height = max(170, кількість_рядків * 120 + 60)
```

### Розташування елементів

- Елементи розміщуються **по колонках** зліва направо з кроком ~180px
- В кожній lane елементи центруються вертикально
- Паралельні гілки (після AND gateway) — рознести по різних рядках з відступом мінімум 120px по Y
- **НЕ ставити два елементи на однакові або близькі координати**

### Маршрутизація стрілок (КРИТИЧНО)

**❌ ЗАБОРОНЕНО:** діагональні стрілки з 2 waypoints через елементи

**Шаблони маршрутизації:**

**1. Горизонтальна (та сама lane, той самий Y):** 2 waypoints
```xml
<di:waypoint x="[source_right]" y="[center_y]" />
<di:waypoint x="[target_left]" y="[center_y]" />
```

**2. L-форма (та сама lane, різний Y):** 4 waypoints
```xml
<di:waypoint x="[source_right]" y="[source_cy]" />
<di:waypoint x="[source_right + 30]" y="[source_cy]" />
<di:waypoint x="[source_right + 30]" y="[target_cy]" />
<di:waypoint x="[target_left]" y="[target_cy]" />
```

**3. Z-форма (крос-lane, вперед):** 4 waypoints
```xml
<di:waypoint x="[source_right]" y="[source_cy]" />
<di:waypoint x="[mid_x]" y="[source_cy]" />
<di:waypoint x="[mid_x]" y="[target_cy]" />
<di:waypoint x="[target_left]" y="[target_cy]" />
```
де `mid_x = (source_right + target_left) / 2`

**4. Зворотній потік (loop назад):** 6 waypoints — маршрут ВГОРУ через коридор над пулом
```xml
<di:waypoint x="[source_left]" y="[source_cy]" />
<di:waypoint x="[source_left - 20]" y="[source_cy]" />
<di:waypoint x="[source_left - 20]" y="[POOL_Y - 10]" />
<di:waypoint x="[target_left - 20]" y="[POOL_Y - 10]" />
<di:waypoint x="[target_left - 20]" y="[target_cy]" />
<di:waypoint x="[target_left]" y="[target_cy]" />
```

**5. Від boundary event (вниз/вбік):** 2-3 waypoints
```xml
<di:waypoint x="[be_cx]" y="[be_bottom]" />
<di:waypoint x="[be_cx]" y="[target_cy]" />
<di:waypoint x="[target_left]" y="[target_cy]" />
```

### Boundary events — розташування на задачі

```
Нагадування: x = task_x + 10,  y = task_y + task_h - 18
Дедлайн:     x = task_x + 54,  y = task_y + task_h - 18
```
Обидва на нижній межі задачі, з відступом 44px між собою.

### Анти-патерни розкладки

❌ Два елементи з однаковими координатами (накладання)
❌ Стрілка з 2 waypoints що йде по діагоналі через інший елемент
❌ Елемент виходить за межі своєї lane
❌ Boundary event відірваний від батьківського task
❌ Паралельні гілки на однаковому Y (злипаються)
❌ Зворотній потік (loop) що перетинає елементи — має йти через коридор над пулом

---

## Правила роботи з Git та GitHub

- **НЕ виконувати жодних git-дій** (commit, push, checkout, branch, merge, rebase тощо) без прямої вказівки користувача
- **НЕ виконувати жодних дій з GitHub** (створення PR, коментарі до PR/issues, закриття issues, створення releases, `gh` команди тощо) без прямої вказівки користувача
- Дозволено лише **читання** стану репозиторію (`git status`, `git log`, `git diff`) без додаткового підтвердження
