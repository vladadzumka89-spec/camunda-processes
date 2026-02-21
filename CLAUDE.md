# Camunda 8.8 — Правила створення BPMN-процесів

Цей документ містить обов'язкові правила для створення BPMN-процесів у Camunda 8.8 з інтеграцією Odoo.

## Загальна структура процесу

Кожен процес ОБОВ'ЯЗКОВО починається з такої схеми:

```
Start Event → XOR Gateway
  ├─ (default) → Service Task "Створити головне завдання"
  └─ (умова) → далі по процесу (пропускає створення задачі)
```

### Умова на гілці "вгору" (якщо задача вже існує):
```
= is defined(odoo_task_id) and odoo_task_id != null
```
Ця гілка спрацьовує коли процес запущено з Odoo і головне завдання вже створене.

---

## Service Task: "Створити головне завдання"

Створює головне завдання (проект) в Odoo.

- **Job type:** `http-request-smart`
- **Inputs (4):**

| Input | Local variable name | Значення |
|-------|-------------------|----------|
| method | `method` | `= "POST"` |
| url | `url` | `= "https://o.tut.ua/web/hook/67f62d7c-2612-444c-baf3-ad409c769bbe"` |
| headers | `headers` | `= {"Content-Type":"application/json"}` |
| body | `body` | див. нижче |

### Body:
```
= {name: "<НАЗВА ПРОЦЕСУ — вказує замовник БП>",
   create_process: true,
   _model: "project.project",
   _id: 252}
```

- `name` — назва головної задачі/процесу (єдине що змінюється, вказує замовник)
- `create_process: true` — завжди обов'язково
- `_model: "project.project"` — завжди фіксоване
- `_id: 252` — ID проекту в Odoo (фіксований, всі процеси в одному проекті)

---

## Service Task: "Отримати список працівників з Odoo"

Отримує список працівників з певної групи в Odoo. Результат використовується для multi-instance User Task.

- **Job type:** `http-request-smart`
- **Inputs (5):**

| Input | Local variable name | Значення |
|-------|-------------------|----------|
| method | `method` | `= "POST"` |
| url | `url` | `= "https://o.tut.ua/api/server-action"` |
| headers | `headers` | `= {"Content-Type":"application/json", "X-API-Key":"632b5ed6-091f-48db-bd01-1e60aeb10bfc"}` |
| body | `body` | див. нижче |
| result_variable_name | `result_variable_name` | (значення уточнити) |

### Body:
```
= {
    "action_identifier": "studio_customization._03c8968e-ba5a-43c9-839d-90197c29c03d",
    "context": {},
    "payload": {
      "groups": [<ID групи працівників>]
    }
}
```

- `action_identifier` — посилання на серверну дію в Odoo
- `groups` — масив ID груп, з яких потрібні працівники (вказує замовник)

### Outputs (1):
- Process variable name: `employees`
- Value: `= result.data`

---

## User Task: налаштування

Кожен User Task використовує HTTP-запит для створення підзадачі в Odoo.

- **Inputs (4):**

| Input | Local variable name | Значення |
|-------|-------------------|----------|
| method | `method` | `= "POST"` |
| url | `url` | `= "https://o.tut.ua/web/hook/67f62d7c-2612-444c-baf3-ad409c769bbe"` |
| headers | `headers` | `= {"Content-Type":"application/json"}` |
| body | `body` | див. нижче |

### Body:
```
= {
  name: "<назва підзадачі>" + emp.employee_name,
  description: "<опис задачі>",
  _model: "project.project",
  _id: 252,
  x_studio_camunda_user_ids: emp.user_id,
  process_instance_key: process_instance_key
}
```

- `name` — назва підзадачі для працівника в Odoo (вказує замовник)
- `description` — опис що потрібно зробити
- `x_studio_camunda_user_ids: emp.user_id` — призначає виконавця
- `process_instance_key` — зв'язок з інстансом процесу в Camunda

User Task має маркер **multi-instance** (три вертикальні лінії) — створює підзадачу для кожного працівника зі списку `employees`.

---

## Обов'язкові boundary events на кожному User Task

Кожен User Task ОБОВ'ЯЗКОВО має два timer boundary events:

### 1. Нагадування (non-interrupting timer, пунктирна рамка)
- Спрацьовує періодично
- Веде до Service Task "Надіслати нагадування працівнику"
- Зациклений — після надсилання повертається і чекає знову

### 2. Дедлайн прострочено (interrupting timer, суцільна рамка)
- Перериває User Task коли дедлайн минув
- Веде до підпроцесу "Ескалація"
- Після завершення ескалації → End Event

```
                    ┌─ Timer (нагадування) ──→ Надіслати нагадування ─┐
                    │       (loop)            працівнику             ←─┘
User Task ─────────►│
                    └─ Timer (дедлайн) ──→ [Ескалація] ──→ End
```

---

## Naming convention для полів в Odoo

Всі поля в Odoo для інтеграції з Camunda ОБОВ'ЯЗКОВО мають починатися з префіксу:

```
x_studio_camunda_
```

Воркер передає в Camunda ЛИШЕ поля з цим префіксом. Поля без нього ігноруються.

Приклади:
- `x_studio_camunda_user_ids` — виконавець задачі
- `x_studio_camunda_<назва_поля>` — будь-яке інше поле

---

## Ключові змінні процесу

| Змінна | Опис |
|--------|------|
| `odoo_task_id` | ID задачі в Odoo (приходить при запуску з Odoo) |
| `employees` | Список працівників з Odoo (результат серверної дії) |
| `emp.employee_name` | Ім'я працівника (з елемента списку employees) |
| `emp.user_id` | ID користувача в Odoo (з елемента списку employees) |
| `process_instance_key` | Ключ інстансу процесу в Camunda |
