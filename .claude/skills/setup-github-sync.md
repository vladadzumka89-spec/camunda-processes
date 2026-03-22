---
name: setup-github-sync
description: Налаштування синхронізації Camunda Web Modeler з GitHub — покрокова інструкція
user_invocable: true
---

# Налаштування Camunda Web Modeler ↔ GitHub Sync

Проведи користувача покроково через налаштування синхронізації. На кожному кроці чекай підтвердження перед переходом до наступного.

## Крок 1: Створення GitHub App

Скажи користувачу:

> Спочатку створимо GitHub App. Перейдіть:
> 1. GitHub → ваш аватар → **Settings** → **Developer settings** → **GitHub Apps** → **New GitHub App**
> 2. Заповніть:
>    - **App name**: `Camunda Modeler <ваше імʼя>` (має бути унікальна назва)
>    - **Homepage URL**: `http://camunda-demo.a.local:8070`
>    - **Webhook**: зніміть галочку **Active**
>    - **Permissions** → Repository permissions → **Contents** → **Read and write**
> 3. Натисніть **Create GitHub App**
> 4. Скопіюйте **Client ID** зі сторінки App
>
> Покажіть скрін або скажіть Client ID коли буде готово.

Чекай поки користувач покаже Client ID. Запамʼятай його.

## Крок 2: Генерація Private Key

Скажи користувачу:

> Тепер згенеруємо Private Key:
> 1. На сторінці вашого GitHub App прокрутіть вниз до **Private keys**
> 2. Натисніть **Generate a private key**
> 3. Скачається `.pem` файл
> 4. Відкрийте його текстовим редактором (Блокнот / Notepad++)
> 5. Скопіюйте **весь вміст** (включно з `-----BEGIN RSA PRIVATE KEY-----` та `-----END RSA PRIVATE KEY-----`)
>
> Скажіть коли готово (приватний ключ не потрібно мені надсилати — вставите його в форму самостійно).

## Крок 3: Встановлення App на репозиторій

Скажи користувачу:

> Тепер встановимо App на репозиторій:
> 1. На сторінці GitHub App зліва → **Install App**
> 2. Натисніть **Install** біля свого акаунту
> 3. Оберіть **Only select repositories** → оберіть `camunda-processes` → **Install**
> 4. Після установки подивіться на **URL в браузері** — там буде число:
>    ```
>    github.com/settings/installations/12345678
>                                      ^^^^^^^^ це Installation ID
>    ```
> 5. Скопіюйте це число
>
> Покажіть скрін або скажіть Installation ID.

Чекай поки користувач покаже Installation ID. Запамʼятай його.

## Крок 4: Заповнення форми в Web Modeler

Після отримання Client ID та Installation ID, скажи користувачу:

> Тепер відкрийте Camunda Web Modeler:
> 1. Натисніть **Sync with GitHub** (або значок GitHub)
> 2. **Configure repository connection**
> 3. Оберіть провайдер: **GitHub**
> 4. Заповніть поля:
>
> | Поле | Значення |
> |------|----------|
> | **Client ID** | `<Client ID з кроку 1>` |
> | **Installation ID** | `<Installation ID з кроку 3>` |
> | **Private key** | Вставте весь вміст `.pem` файлу з кроку 2 |
> | **Repository URL** | `https://github.com/vladadzumka89-spec/camunda-processes` |
> | **Branch** | `main` |
> | **Repository path** | `bpmn/deployed` |
>
> 5. Натисніть **Save configuration**

Підстав реальні значення Client ID та Installation ID які користувач надав раніше.

**ВАЖЛИВО — типові помилки при заповненні:**
- **Repository URL** — це URL репозиторію (`https://github.com/vladadzumka89-spec/camunda-processes`), НЕ URL папки
- **Repository path** — це шлях всередині репозиторію (`bpmn/deployed`), НЕ URL
- **Project ID** — залишити порожнім

## Крок 5: Перша синхронізація

Скажи користувачу:

> Тепер спробуйте синхронізувати:
> 1. Натисніть **Sync with GitHub**
> 2. Якщо зʼявиться конфлікт — оберіть **Web Modeler** (щоб відправити ваші процеси на GitHub)
> 3. Натисніть **Resolve conflict**
>
> Покажіть результат.

Якщо все пройшло успішно — повідом користувача що налаштування завершено.

## Нагадування про Process ID

Після успішного налаштування, нагадай:

> **Важливо про Process ID:**
> Коли процес знаходиться всередині пулу (з доріжками) — Process ID знаходиться не там де зазвичай.
> Клікніть на **рамку пулу** → права панель → **Process ID**.
> Не плутайте з Participant ID та Collaboration ID.
> При додаванні пулу Web Modeler може автоматично змінити Process ID — завжди перевіряйте!

## Додаткова інформація

Повна документація: `docs/camunda-web-modeler-github-sync.md` в репозиторії.