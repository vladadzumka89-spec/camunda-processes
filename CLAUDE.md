# CLAUDE.md

## Wiki

Wiki schema and operations → `docs/wiki/schema.md`. Skill: `wiki`.

## Git policy

**ЗАБОРОНЕНО** виконувати записуючі git-операції (commit, push, merge тощо) без прямої вказівки користувача.
**ЗАБОРОНЕНО** виконувати дії з GitHub (PR, коментарі, issues) без прямої вказівки.
Читання (status, log, diff) — дозволено завжди.

## Project overview

Camunda 8.8 BPMN orchestrator + Odoo ERP + GitHub. Python 3.12 async, pyzeebe, Docker.

## Build and run

```bash
docker compose up -d                    # dev stack
docker compose -f docker-compose-full.yaml up -d  # full stack
pytest                                  # tests
```

## Architecture

Worker → Zeebe (gRPC :26500) → HTTP requests to Odoo webhooks.
Entry: `worker/__main__.py`. Handlers in `worker/handlers/`. Webhook server on :9001.

| Service | Port |
|---------|------|
| Zeebe gRPC | 26500 |
| Camunda REST + Operate | 8088 |
| Connectors | 8086 |
| Webhook server | 9001 |

## BPMN workflow

Перед деплоєм будь-якого BPMN-файлу — **обов'язково** запустити `/analyze-bpmn <файл>`.  
Деплоїти тільки після відсутності FAIL у звіті.

## BPMN правила (ключові)

Повні правила: [docs/bpmn-rules.md](docs/bpmn-rules.md)
Розкладка DI: [docs/bpmn-layout.md](docs/bpmn-layout.md)

1. Кожен процес починається з **XOR gateway** перевірки `odoo_task_id`
2. Кожен User Task має **2 boundary events** (non-interrupting): нагадування + дедлайн
3. Усі поля Odoo — префікс `x_studio_camunda_`
4. **Swim lanes** обов'язкові: Система / Виконавець / Керівник
5. User Task має **Task Listener**: Creating, `http-request-smart`, **Retries = 3**
6. `_id` проєкту — **завжди запитувати** у користувача
7. Webhook prod: `https://o.tut.ua/web/hook/8531324a-2785-48d1-8f4d-ddd66a267d50`
8. Webhook dev: `http://odoo.dev.dobrom.com/web/hook/21c8dbff-86e8-4005-9bfc-9f77ee9b5c57`

## Призначення виконавця в BPMN body

- По ролі: `x_studio_camunda_role_code: "tech_support"`
- По керівнику: `x_studio_camunda_user_id: x_studio_camunda_manager_user_id`
- Вид задачі: `x_studio_camunda_task_type: "full"` (головна) / `"short"` (підзадача)

## Анти-патерни

❌ Start → User Task без XOR odoo_check
❌ User Task без boundary events або без Task Listener
❌ Одночасно Task Listener І Job worker type (дублі!)
❌ Поля Odoo без `x_studio_camunda_` префіксу
❌ `isExecutable="false"` | діагональні стрілки
❌ Захардкоджений `_id` проєкту

## ID конвенції

ST_ (Service Task), UT_ (User Task), CA_ (Call Activity), BE_ (Boundary), GW_ (Gateway), Flow_, End_
Назви — українською, ID — латиницею.
