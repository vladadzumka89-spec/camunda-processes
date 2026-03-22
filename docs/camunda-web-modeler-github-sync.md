# Налаштування синхронізації Camunda Web Modeler з GitHub

## Для чого це потрібно

Щоб BPMN-процеси з Camunda Web Modeler автоматично зберігались у GitHub репозиторій (папка `bpmn/deployed/`).

---

## Що потрібно зробити кожному користувачу

Кожен користувач створює **свій власний** GitHub App та отримує **свої унікальні** значення:

| Що отримаєте | Де взяти | Унікальне для кожного? |
|-------------|----------|----------------------|
| **Client ID** | Крок 1 — при створенні GitHub App | Так, у кожного свій |
| **Private Key** (.pem файл) | Крок 2 — генерується для вашого App | Так, у кожного свій |
| **Installation ID** | Крок 3 — при встановленні App на репозиторій | Так, у кожного свій |

Наступні поля **однакові для всіх**:

| Поле | Значення |
|------|----------|
| Repository URL | `https://github.com/vladadzumka89-spec/camunda-processes` |
| Branch | `main` |
| Repository path | `bpmn/deployed` |

---

## Крок 1: Створити GitHub App

1. Відкрити GitHub → натиснути на свій аватар (праворуч вгорі) → **Settings**
2. Зліва внизу → **Developer settings**
3. **GitHub Apps** → **New GitHub App**
4. Заповнити:
   - **App name**: будь-яка унікальна назва (наприклад `Camunda Modeler <ваше імʼя>`)
   - **Homepage URL**: `http://camunda-demo.a.local:8070`
   - **Webhook**: зняти галочку **Active** (вимкнути)
   - **Permissions** → Repository permissions → **Contents** → обрати **Read and write**
5. Натиснути **Create GitHub App**
6. Скопіювати **Client ID** — він відобразиться на сторінці App (рядок типу `Iv23liK1bhjCWMOPj9bB`)

---

## Крок 2: Згенерувати Private Key

1. На сторінці вашого GitHub App (де щойно скопіювали Client ID)
2. Прокрутити вниз до секції **Private keys**
3. Натиснути **Generate a private key**
4. Браузер скачає файл `.pem`
5. Відкрити його **текстовим редактором** (Блокнот / Notepad++ / VS Code)
6. Скопіювати **весь вміст** — включно з рядками:
   ```
   -----BEGIN RSA PRIVATE KEY-----
   ...багато символів...
   -----END RSA PRIVATE KEY-----
   ```

---

## Крок 3: Встановити App на репозиторій

1. На сторінці вашого GitHub App зліва в меню → **Install App**
2. Натиснути **Install** біля свого акаунту
3. Обрати **Only select repositories** → обрати `camunda-processes` → **Install**
4. Після установки подивитись на **URL в браузері** — там буде число:
   ```
   github.com/settings/installations/118185714
                                      ^^^^^^^^
                                      це ваш Installation ID
   ```
5. Скопіювати це число

---

## Крок 4: Налаштувати Web Modeler

1. Відкрити Camunda Web Modeler
2. Натиснути **Sync with GitHub** (або значок GitHub у правому верхньому куті)
3. **Configure repository connection**
4. Обрати провайдер: **GitHub**
5. Заповнити поля:

| Поле | Що вписати |
|------|-----------|
| **Client ID** | Ваш Client ID з кроку 1 |
| **Installation ID** | Ваше число з кроку 3 |
| **Private key** | Весь вміст `.pem` файлу з кроку 2 |
| **Repository URL** | `https://github.com/vladadzumka89-spec/camunda-processes` |
| **Branch** | `main` |
| **Repository path** | `bpmn/deployed` |

6. Натиснути **Save configuration**

### Частi помилки при заповненні

- **Repository URL** — це URL репозиторію, НЕ URL папки. Правильно: `https://github.com/vladadzumka89-spec/camunda-processes`
- **Repository path** — це шлях до папки ВСЕРЕДИНІ репозиторію, НЕ URL. Правильно: `bpmn/deployed`
- **Project ID** — залишити порожнім (це поле для GitLab)

---

## Як синхронізувати процеси

### Відправити процеси з Web Modeler → GitHub
1. В Web Modeler натиснути **Sync with GitHub**
2. Якщо є конфлікт — обрати **Web Modeler** (зберегти те що в Modeler)
3. Натиснути **Resolve conflict**

### Підтягнути процеси з GitHub → Web Modeler
1. В Web Modeler натиснути **Sync with GitHub**
2. Обрати **GitHub** (перезаписати локальне версією з GitHub)
3. Натиснути **Resolve conflict**

---

## Важливо: як знайти Process ID

Process ID — це ідентифікатор процесу, який використовується для запуску з Odoo.

### Процес БЕЗ пулу (без доріжок)
- Клікнути на порожнє місце на діаграмі
- В правій панелі → поле **ID**

### Процес ВСЕРЕДИНІ пулу (з доріжками)
- Клікнути на **рамку пулу** (зовнішня межа з назвою)
- В правій панелі → секція **PARTICIPANT** → поле **Process ID**
- **НЕ плутати** з Participant ID та Collaboration ID — вони не використовуються для запуску

### Перевірка перед кожним деплоєм
1. Знайти Process ID (як описано вище)
2. Переконатись що він збігається з полем `processDefinitionId` у записі `x_camunda_process_type` в Odoo
3. Якщо ID змінився (наприклад після додавання пулу) — виправити через **Edit XML** у Web Modeler

---

## Типові помилки та рішення

| Помилка | Причина | Що робити |
|---------|---------|-----------|
| `404 NOT_FOUND: process definition not found` | Process ID в Odoo не збігається з ID в BPMN, або BPMN не задеплоєний | 1. Перевірити Process ID в BPMN. 2. Перевірити запис в `x_camunda_process_type` в Odoo. 3. Задеплоїти BPMN якщо ще не зроблено |
| Process ID змінився сам | Web Modeler автогенерує новий ID при додаванні пулу/доріжок | Відкрити Edit XML → знайти `<bpmn:process id="..."` → виправити на правильний ID |
| Конфлікт при синхронізації | Файл змінений і в Modeler, і на GitHub | Обрати джерело з актуальною версією (Web Modeler або GitHub) |
| `Repository path` містить URL | Неправильно заповнене поле | Має бути шлях: `bpmn/deployed`, а НЕ URL |
| `Name is already taken` при створенні GitHub App | Назва App вже зайнята глобально | Додати своє імʼя до назви: `Camunda Modeler <Імʼя>` |