---
name: odoo-automations
description: Довідник всіх автоматизацій та webhook коду Odoo для інтеграції з Camunda
user_invocable: true
---

# Автоматизації Odoo ↔ Camunda

Покажи користувачу актуальний код потрібної автоматизації.

---

## Webhook #119 — "Створення задачі Camunda" (prod o.tut.ua)

```python
# ============================================================
# CAMUNDA WEBHOOK - PROJECT TASK CREATOR
# Тип: Studio Webhook
# Доступні змінні: env, payload, UserError, log, _logger
# ============================================================

_logger.info("=== CAMUNDA WEBHOOK START ===")

# --- Отримання даних з payload ---
task_name = payload.get('name')
process_instance_key = payload.get('process_instance_key')
parent_process_instance_key = payload.get('parent_process_instance_key')
element_instance_key = payload.get('element_instance_key')
user_task_key = payload.get('user_task_key')
create_process = payload.get('create_process')
project_id = payload.get('_id')
description = payload.get('description', '')
process_definition_id = payload.get('bpmn_process_id')

# --- Валідація ---
if not task_name:
    raise UserError("Camunda Webhook: відсутня назва задачі (name)")

if not process_instance_key:
    raise UserError("Camunda Webhook: відсутній process_instance_key")

# --- Пошук батьківської задачі ---
Task = env['project.task'].sudo()
parent_task = None

if create_process:
    if not project_id:
        raise UserError("Camunda Webhook: відсутній project_id для створення процесу")

    PT = env['x_camunda_process_type'].sudo()

    process_type = PT.search([
        ('x_camunda_process_definition_id', '=', str(process_definition_id))
    ], limit=1)

    if not process_type:
        raise UserError("Camunda Webhook: не висначений тип процесу для вказаного process_definition_id")

    task_vals = {
        'name': task_name,
        'project_id': int(project_id),
        'x_studio_camunda_process_definition_id': process_type.id,
        'description': description,
        'x_studio_camunda_process_instance_key': str(process_instance_key),
        'stage_id': 7739
    }
else:
    if not element_instance_key:
        raise UserError("Camunda Webhook: відсутній element_instance_key для підзадачі")

    parent_task = Task.search([
        ('x_studio_camunda_process_instance_key', '=', str(process_instance_key)),
        '|',
        ('x_studio_camunda_element_instance_key', '=', False),
        ('x_studio_camunda_element_instance_key', '=', ''),
    ], limit=1)

    if not parent_task and parent_process_instance_key:
        parent_task = Task.search([
            ('x_studio_camunda_process_instance_key', '=', str(parent_process_instance_key)),
            '|',
            ('x_studio_camunda_element_instance_key', '=', False),
            ('x_studio_camunda_element_instance_key', '=', ''),
        ], limit=1)

    if not parent_task:
        raise UserError(f"Camunda Webhook: не знайдено батьківську задачу з process_instance_key={process_instance_key}")

    _logger.info("Found parent task: %s (ID: %s)", parent_task.name, parent_task.id)

    task_vals = {
        'name': task_name,
        'parent_id': parent_task.id,
        'project_id': parent_task.project_id.id,
        'description': description,
        'x_studio_camunda_process_instance_key': str(process_instance_key),
        'x_studio_camunda_element_instance_key': str(element_instance_key),
        'x_studio_camunda_user_task_key': str(user_task_key)
    }

# Динамічно додаємо всі поля x_studio_camunda_* з payload (не перезаписуємо вже встановлені)
for key, value in payload.items():
    if key.startswith('x_studio_camunda_') and value is not None:
        if key in Task._fields and key not in task_vals:
            ftype = Task._fields[key].type
            if ftype in ('many2one', 'integer'):
                try:
                    int_val = int(value)
                    if int_val > 0:
                        task_vals[key] = int_val
                except Exception:
                    pass
            elif ftype not in ('one2many', 'many2many'):
                task_vals[key] = value

# Наслідуємо x_studio_camunda_* поля з батьківського завдання
if parent_task:
    if parent_task.x_studio_camunda_process_definition_id:
        task_vals['x_studio_camunda_process_definition_id'] = parent_task.x_studio_camunda_process_definition_id.id
    for fname in parent_task._fields:
        if fname.startswith('x_studio_camunda_') and fname not in task_vals:
            val = parent_task[fname]
            if val and val is not False:
                try:
                    ftype = parent_task._fields[fname].type
                    if ftype == 'many2one':
                        if hasattr(val, 'id') and val.id:
                            task_vals[fname] = val.id
                    elif ftype not in ('one2many', 'many2many', 'binary'):
                        task_vals[fname] = val
                except Exception:
                    pass

# --- Створення нової задачі ---
new_task = Task.create(task_vals)

parent_info = parent_task.id if parent_task else 'None (root)'
_logger.info("Task created: %s (ID: %s, Parent: %s)", new_task.name, new_task.id, parent_info)
_logger.info("=== CAMUNDA WEBHOOK FINISH ===")
```

### Важливо:
- `stage_id: 7739` — стадія "В процесі" на проді
- `stage_id: 1427` — стадія "В процесі" на демо
- В safe_eval Odoo `ValueError` не доступний — використовувати `except Exception`
- `res.users()` — порожній recordset як рядок, блок dynamic fields пропускає через перевірку типу

---

## "Запуск процесу Camunda" (project.task)

- **Модель**: Завдання
- **Тригер**: Етап встановлений на "В процесі"
- **Домен**: Проєкт = Запуск процесів / Прийом на роботу / Зміна терміналу ФОП, Стадія = Новий
- **Умова подачі**: Стадія = В процесі AND Ключ процесу Camunda = ""

Код: `get_camunda_oauth_token()` → `collect_camunda_variables()` → `collect_file_references()` → `start_camunda_process()`

---

## "Camunda: заповнити керівника з підрозділу"

- **Модель**: Завдання
- **Тригер**: On create and edit, під час оновлення поля "Підрозділ"

```python
for task in records:
    dept = task.x_studio_camunda_pidrozdil
    if not dept:
        continue

    manager = dept.manager_id
    if not manager:
        continue

    user = manager.sudo().user_id
    if user and user.sudo().exists():
        task.write({
            'x_studio_camunda_manager_user_id': user.id
        })
```

---

## "Camunda: призначення виконавця"

- **Модель**: Завдання
- **Тригер**: On create
- **Домен**: Проєкт = Запуск процесів або Прийом на роботу

```python
for task in records:
    if task.user_ids:
        continue

    role_code = None
    if 'x_studio_camunda_role_code' in task._fields:
        role_code = task.x_studio_camunda_role_code

    if role_code:
        Role = env['x_camunda'].sudo()
        role = Role.search([
            ('x_studio_camunda_code', '=', role_code),
        ], limit=1)
        if role and role.x_studio_camunda_user_ids:
            task.write({'user_ids': [(6, 0, role.x_studio_camunda_user_ids.ids)]})
            continue

    if 'x_studio_camunda_user_id' in task._fields:
        assignee_id = task.x_studio_camunda_user_id
        if assignee_id and isinstance(assignee_id, int) and assignee_id > 0:
            user = env['res.users'].sudo().browse(assignee_id)
            if user.exists():
                task.write({'user_ids': [(6, 0, [user.id])]})
```

### Призначення виконавця в BPMN body:
- По ролі: `x_studio_camunda_role_code: "tech_support"`
- По керівнику: `x_studio_camunda_user_id: x_studio_camunda_manager_user_id`

### Модель ролей на проді:
- **Модель**: `x_camunda`
- **Поля**: `x_studio_camunda_code` (text), `x_studio_camunda_user_ids` (many2many → res.users), `x_active` (boolean)

---

## "Camunda: зняти виконавців після виконання"

- **Модель**: Завдання
- **Тригер**: On create and edit, під час оновлення Стадії

```python
for task in records:
    if task.stage_id.name not in ('Виконано', 'Done', 'Завершено'):
        continue

    if len(task.user_ids) > 1:
        current_user = env.user
        if current_user in task.user_ids:
            task.write({'user_ids': [(6, 0, [current_user.id])]})
            log(f"Task {task.id}: залишено виконавцем {current_user.name}, решту знято")
```

---

## Коди ролей

| Назва | Код |
|-------|-----|
| Ревізор комерційний | `revisor_commercial` |
| Фінансист | `financist` |
| Бухгалтер | `accountant` |
| Діловод | `dilovod` |
| Технічна підтримка | `tech_support` |
| Аналітик | `analyst` |
| Рекрутер | `recruiter` |
