"""Invoice OCR handler — extracts data from PDF/JPG/PNG/XLS/XLSX invoices.

Primary: Gemini 3.1 Flash-Lite vision (via OpenRouter) for image-based extraction.
Fallback: tesseract OCR + regex parsing.

Task type: invoice-data-extractor
"""

import base64
import json as json_module
import logging
import os
import re
from io import BytesIO
from typing import Any

import httpx
import openpyxl
import pytesseract
import xlrd
from pdf2image import convert_from_bytes
from PIL import Image, ImageOps
from pyzeebe import Job, ZeebeWorker

from ..config import AppConfig

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# OCR settings (env vars — не входять в AppConfig)
# ---------------------------------------------------------------------------
OCR_LANG = os.getenv("OCR_LANG", "ukr")
OCR_DPI = int(os.getenv("OCR_DPI", "300"))
HTTP_TIMEOUT = int(os.getenv("OCR_HTTP_TIMEOUT", "60"))

# Gemini via OpenRouter
OPENROUTER_API_KEY = os.getenv(
    "OPENROUTER_API_KEY",
    "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",
)
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "google/gemini-3.1-flash-lite-preview")
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
GEMINI_TIMEOUT = int(os.getenv("GEMINI_TIMEOUT", "90"))

# ---------------------------------------------------------------------------
# Відомі назви послуг (для пошуку в OCR-тексті)
# ---------------------------------------------------------------------------
KNOWN_SERVICES = [
    "суборенда приміщення",
    "експлуатаційна суборендна плата",
    "маркетингова суборендна плата",
    "розміщення рекламного матеріалу",
    "оренда частини нежитлового приміщення",
    "оренда нерухомого майна",
    "орендна плата",
    "надання послуг з прибирання",
    "рекламні послуги",
    "компенсація комунальних",
    "відшкодування ком.послуг",
    "сервісне обслуговування",
    "маркетингові послуги",
    "експлуатаційні витрати",
    "послуги з маркетингу",
]

# ---------------------------------------------------------------------------
# Отримання файлу (base64 або URL)
# ---------------------------------------------------------------------------
ODOO_URL = os.getenv("OCR_ODOO_URL") or os.getenv("ODOO_URL", "https://odoo.dev.dobrom.com:2689/odoo")
ODOO_DB = os.getenv("OCR_ODOO_DB") or os.getenv("ODOO_DB", "odoo19")
ODOO_USER = os.getenv("OCR_ODOO_USER") or os.getenv("ODOO_USER", "")
ODOO_PASSWORD = os.getenv("OCR_ODOO_PASSWORD") or os.getenv("ODOO_PASSWORD", "")


async def _odoo_jsonrpc(url: str, service: str, method: str, args: list) -> Any:
    """Call Odoo JSON-RPC."""
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "id": 1,
        "params": {"service": service, "method": method, "args": args},
    }
    base = ODOO_URL.rstrip("/").removesuffix("/odoo")
    async with httpx.AsyncClient(timeout=30, verify=False) as client:
        resp = await client.post(f"{base}/jsonrpc", json=payload)
        resp.raise_for_status()
        result = resp.json()
    if result.get("error"):
        raise ValueError(f"Odoo RPC error: {result['error']}")
    return result.get("result")


async def _odoo_authenticate() -> int:
    """Authenticate with Odoo and return uid."""
    uid = await _odoo_jsonrpc(
        ODOO_URL, "common", "authenticate",
        [ODOO_DB, ODOO_USER, ODOO_PASSWORD, {}],
    )
    if not uid:
        raise ValueError("Odoo authentication failed")
    return uid


async def _fetch_file_from_odoo(task_id: int) -> tuple[str, str]:
    """Fetch invoice file from Odoo task via JSON-RPC.

    Returns (base64_content, file_extension).
    """
    uid = await _odoo_authenticate()

    # Перевіряємо, чи існує поле filename на моделі — на prod його може не бути
    task_fields = await _odoo_jsonrpc(
        ODOO_URL, "object", "execute_kw",
        [ODOO_DB, uid, ODOO_PASSWORD, "project.task", "fields_get",
         [["x_studio_camunda_invoice_file_filename"]], {"attributes": ["type"]}],
    ) or {}
    has_filename = "x_studio_camunda_invoice_file_filename" in task_fields

    read_fields = ["x_studio_camunda_invoice_file"]
    if has_filename:
        read_fields.append("x_studio_camunda_invoice_file_filename")

    records = await _odoo_jsonrpc(
        ODOO_URL, "object", "execute_kw",
        [ODOO_DB, uid, ODOO_PASSWORD, "project.task", "read",
         [[task_id], read_fields]],
    )

    if not records:
        raise ValueError(f"Odoo task {task_id} not found")

    rec = records[0]
    file_content = rec.get("x_studio_camunda_invoice_file") or ""
    filename = rec.get("x_studio_camunda_invoice_file_filename") or "" if has_filename else ""

    # Якщо файл не в полі задачі — шукаємо в підзадачах
    if not file_content:
        children = await _odoo_jsonrpc(
            ODOO_URL, "object", "execute_kw",
            [ODOO_DB, uid, ODOO_PASSWORD, "project.task", "search_read",
             [[["parent_id", "=", task_id],
               ["x_studio_camunda_invoice_file", "!=", False]]],
             {"fields": read_fields, "limit": 1}],
        )
        if children:
            file_content = children[0].get("x_studio_camunda_invoice_file") or ""
            if has_filename:
                filename = children[0].get("x_studio_camunda_invoice_file_filename") or filename

    if not file_content:
        raise ValueError(f"No file found in Odoo task {task_id} or its subtasks")

    ext = ""
    if filename and "." in filename:
        ext = filename.rsplit(".", 1)[-1].lower()

    logger.info("Fetched file from Odoo task %d: %s (%d chars base64, has_filename=%s)",
                task_id, filename or "<no filename field>", len(file_content), has_filename)
    return file_content, ext


async def _acquire_file(file_data) -> bytes:
    """Отримати файл: base64-декодування або завантаження по URL з Odoo."""
    if not file_data:
        raise ValueError("x_studio_camunda_invoice_file is empty")

    # Якщо вже bytes — повернути як є
    if isinstance(file_data, bytes):
        return file_data

    file_data = str(file_data)

    # Odoo binary field may come as string repr of bytes: "b'...'"
    if file_data.startswith("b'") and file_data.endswith("'"):
        file_data = file_data[2:-1]

    if not file_data.startswith(("http://", "https://")):
        return base64.b64decode(file_data)

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        resp = await client.get(file_data)
        resp.raise_for_status()
        return resp.content

# ---------------------------------------------------------------------------
# Gemini vision — primary extraction engine
# ---------------------------------------------------------------------------
_GEMINI_PROMPT = """\
Ти — система для витягування даних з українських рахунків на оплату.

Проаналізуй зображення рахунку та поверни JSON з такими полями:
- partner_name: назва постачальника/орендодавця (без "ФОП", "ТОВ", "Фізична особа-підприємець")
- invoice_number: номер рахунку (тільки номер, наприклад "170" або "Н0000034572")
- invoice_date: дата рахунку у форматі ISO "YYYY-MM-DD" (наприклад "2026-02-02")
- invoice_line_name: ОБОВ'ЯЗКОВО — повна назва послуги/товару з табличної частини рахунку. \
Наприклад: "Оренда частини нежитлового приміщення за адресою м.Хмельницький, вул.Молодіжна,6 21-Б, за березень 2026р". \
Якщо є кілька рядків — бери перший (основний).
- invoice_amount: фінальна сума до оплати (число, з ПДВ якщо є)
- invoice_amount_no_vat: сума без ПДВ (число або null)
- vat_amount: сума ПДВ (число або null)
- partner_code: ЄДРПОУ / ДРФО / ІПН постачальника (тільки цифри)
- contract: номер та дата договору (рядок або null)
- fop_name: назва покупця/орендаря (без "ФОП", "ТОВ", "Фізична особа-підприємець")
- quantity: ОБОВ'ЯЗКОВО — кількість з табличної частини рахунку (число). \
Наприклад: 470, 1, 12.5. Завжди шукай стовпець "Кількість" в таблиці.
- unit: ОБОВ'ЯЗКОВО — одиниця виміру з табличної частини (рядок). \
Наприклад: "м2", "шт", "послуга", "грн", "год", "кг". Завжди шукай у стовпці "Од." або поруч з кількістю.
- unit_price: ціна за одиницю з табличної частини рахунку (число або null). \
Шукай у стовпці "Ціна" або "Ціна без ПДВ".
- partner_iban: IBAN постачальника — 29 символів, починається з "UA" + 27 цифр. \
Зазвичай поруч з міткою "IBAN", "п/р", "р/р". Повертай тільки сам IBAN без пробілів, без префіксів.
- partner_bank_name: назва банку постачальника (рядок або null). \
Зазвичай поруч з міткою "Банк", "у банку", після IBAN. Наприклад: "АТ ОЩАДБАНК", "АТ КБ ПриватБанк".
- service_period: період, за який виставлено рахунок, у форматі "MM.YYYY" (наприклад "03.2026"). \
Шукай у назві послуги фрази типу "за березень 2026", "за 03.2026", "березень 2026р", "період: 03/2026". \
Місяці українською: січень=01, лютий=02, березень=03, квітень=04, травень=05, червень=06, \
липень=07, серпень=08, вересень=09, жовтень=10, листопад=11, грудень=12. \
Якщо період не вказано явно — null.

Правила:
- Суми повертай як числа (float), НЕ рядки
- Якщо ПДВ немає — invoice_amount_no_vat і vat_amount = null
- Якщо ПДВ є — invoice_amount = сума з ПДВ (Всього із ПДВ)
- partner_name — постачальник або орендодавець, fop_name — покупець або орендар
- Видали юридичну форму з імен: "ФОП", "Фізична особа-підприємець", "ТОВ" тощо
- quantity, unit, invoice_line_name — НІКОЛИ не повертай null якщо вони є на зображенні. \
Уважно дивись на табличну частину рахунку (стовпці: №, Товари/послуги, Кількість, Ціна, Сума).
- partner_iban: формат UA + 27 цифр (всього 29 символів), приклад: UA213223130000026007233566001
- service_period: ТІЛЬКИ MM.YYYY формат (наприклад "03.2026" не "березень 2026" і не "2026-03")
- Якщо не можеш розпізнати поле — постав null

Поверни ТІЛЬКИ валідний JSON (масив об'єктів якщо на зображенні кілька рахунків, або один об'єкт якщо один).
НЕ додавай markdown, коментарі чи пояснення — тільки JSON.
"""


def _pil_to_base64(img: Image.Image, fmt: str = "JPEG") -> str:
    """Конвертувати PIL Image в base64 data URL."""
    buf = BytesIO()
    if img.mode in ("RGBA", "P"):
        img = img.convert("RGB")
    img.save(buf, format=fmt, quality=85)
    b64 = base64.b64encode(buf.getvalue()).decode()
    mime = "image/jpeg" if fmt == "JPEG" else "image/png"
    return f"data:{mime};base64,{b64}"


async def _gemini_extract_from_images(images: list[Image.Image]) -> list[dict] | None:
    """Відправити зображення в Gemini і отримати структуровані дані рахунків.

    Returns None якщо Gemini недоступний або повернув невалідну відповідь.
    """
    if not OPENROUTER_API_KEY:
        return None

    content: list[dict] = [{"type": "text", "text": _GEMINI_PROMPT}]
    for img in images:
        content.append({
            "type": "image_url",
            "image_url": {"url": _pil_to_base64(img)},
        })

    payload = {
        "model": GEMINI_MODEL,
        "messages": [{"role": "user", "content": content}],
        "temperature": 0,
        "max_tokens": 4096,
    }

    try:
        async with httpx.AsyncClient(timeout=GEMINI_TIMEOUT) as client:
            resp = await client.post(
                OPENROUTER_URL,
                headers={
                    "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )
            resp.raise_for_status()

        data = resp.json()
        text = data["choices"][0]["message"]["content"]
        logger.info("Gemini response: %d chars", len(text))

        # Прибрати markdown code fences якщо є
        text = text.strip()
        if text.startswith("```"):
            text = re.sub(r"^```\w*\n?", "", text)
            text = re.sub(r"\n?```$", "", text)
            text = text.strip()

        parsed = json_module.loads(text)

        # Нормалізувати: завжди список
        if isinstance(parsed, dict):
            parsed = [parsed]
        if not isinstance(parsed, list):
            logger.warning("Gemini returned unexpected type: %s", type(parsed))
            return None

        # Конвертувати в наш формат
        items = []
        for raw in parsed:
            item = _empty_invoice_item()
            for key in item:
                if key in raw and raw[key] is not None:
                    item[key] = raw[key]
            # Гарантувати float для сум
            for amt_key in ("invoice_amount", "invoice_amount_no_vat", "vat_amount", "unit_price"):
                if item[amt_key] is not None:
                    try:
                        item[amt_key] = float(item[amt_key])
                    except (ValueError, TypeError):
                        item[amt_key] = None
            # IBAN — прибрати пробіли, перевірити формат UA + 27 цифр
            raw_iban = item.get("partner_iban")
            if raw_iban is not None:
                iban = re.sub(r"\s+", "", str(raw_iban)).upper()
                if re.match(r"^UA\d{27}$", iban):
                    item["partner_iban"] = iban
                else:
                    logger.info("Gemini returned invalid IBAN %r (normalized: %r) — dropped", raw_iban, iban)
                    item["partner_iban"] = None
            # service_period — валідація формату MM.YYYY
            if item["service_period"] is not None:
                period = str(item["service_period"]).strip()
                if not re.match(r"^(0[1-9]|1[0-2])\.\d{4}$", period):
                    item["service_period"] = None
            # Очистка назв від юридичних форм і OCR-артефактів
            for name_key in ("partner_name", "fop_name"):
                if item[name_key] is not None:
                    item[name_key] = _clean_partner_name(str(item[name_key]))
            # quantity як рядок
            if item["quantity"] is not None:
                item["quantity"] = str(item["quantity"]).rstrip("0").rstrip(".")
            # Нормалізація invoice_date у ISO формат для Odoo (Date field)
            if item["invoice_date"]:
                iso = _normalize_ua_date(str(item["invoice_date"]))
                if iso:
                    item["invoice_date"] = iso
            items.append(item)

        logger.info("Gemini extracted %d invoice(s)", len(items))
        return items if items else None

    except httpx.HTTPStatusError as e:
        logger.warning("Gemini API error %s: %s", e.response.status_code, e.response.text[:200])
        return None
    except (json_module.JSONDecodeError, KeyError, IndexError) as e:
        logger.warning("Gemini parse error: %s", e)
        return None
    except Exception as e:
        logger.warning("Gemini failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Preprocessing / OCR (tesseract fallback)
# ---------------------------------------------------------------------------
def _preprocess_image(pil_image: Image.Image, binarize: bool = True) -> Image.Image:
    """Підготовка зображення для OCR: grayscale -> autocontrast -> (optional) binarization.

    binarize=True  — для PDF-сканів (високий контраст, чіткий текст)
    binarize=False — для фото з телефону (зберегти більше деталей)
    """
    img = pil_image.convert("L")
    img = ImageOps.autocontrast(img)
    if binarize:
        img = img.point(lambda x: 255 if x > 180 else 0, "1")
    return img


def ocr_pdf(data: bytes) -> list[str]:
    """PDF -> список текстів (одна сторінка = один рахунок)."""
    images = convert_from_bytes(data, dpi=OCR_DPI)
    texts = []
    for i, img in enumerate(images):
        processed = _preprocess_image(img, binarize=True)
        text = pytesseract.image_to_string(processed, lang=OCR_LANG)
        logger.info("OCR page %d: %d chars", i + 1, len(text))
        texts.append(text)
    return texts


def ocr_image(data: bytes) -> str:
    """JPG/PNG -> текст (одне фото = один рахунок).

    Фото з телефону: upscale для кращого OCR, без бінаризації, PSM 4.
    """
    img = Image.open(BytesIO(data))
    # Upscale низькорозподільних фото для кращого OCR
    w, h = img.size
    if max(w, h) < 3000:
        scale = max(2, 3000 // max(w, h) + 1)
        img = img.resize((w * scale, h * scale), Image.LANCZOS)
        logger.info("Upscaled image %dx%d -> %dx%d (scale %dx)",
                     w, h, w * scale, h * scale, scale)
    processed = _preprocess_image(img, binarize=False)
    text = pytesseract.image_to_string(processed, lang=OCR_LANG, config="--psm 4")
    logger.info("OCR image: %d chars", len(text))
    return text

# ---------------------------------------------------------------------------
# Хелпери для нормалізації української дати та періоду
# ---------------------------------------------------------------------------
_UA_MONTHS = {
    "січня": "01", "лютого": "02", "березня": "03", "квітня": "04",
    "травня": "05", "червня": "06", "липня": "07", "серпня": "08",
    "вересня": "09", "жовтня": "10", "листопада": "11", "грудня": "12",
    "січень": "01", "лютий": "02", "березень": "03", "квітень": "04",
    "травень": "05", "червень": "06", "липень": "07", "серпень": "08",
    "вересень": "09", "жовтень": "10", "листопад": "11", "грудень": "12",
}


def _normalize_ua_date(date_str: str) -> str | None:
    """«01 березня 2026» → «2026-03-01». Повертає None якщо не розпізнала."""
    if not date_str:
        return None
    m = re.search(r"(\d{1,2})\s+(\w+)\s+(\d{4})", date_str)
    if m:
        day = m.group(1).zfill(2)
        month = _UA_MONTHS.get(m.group(2).lower())
        year = m.group(3)
        if month:
            return f"{year}-{month}-{day}"
    # Якщо вже ISO — повертаємо як є
    if re.match(r"^\d{4}-\d{2}-\d{2}$", date_str.strip()):
        return date_str.strip()
    return None


def _enrich_items_from_text(items: list[dict], text: str) -> None:
    """Добиває відсутні поля (IBAN/банк/період) з сирого тексту tesseract OCR.

    Викликається після Gemini, коли він пропустив ці поля на зображенні.
    Мутує items на місці.
    """
    if not text:
        return

    # IBAN
    iban_m = re.search(r"\b(UA\d{27})\b", text.replace(" ", ""))
    iban = iban_m.group(1) if iban_m else None

    # Банк
    bank = None
    bank_m = re.search(
        r"(?:у\s+банку|Банк)[:\s]+([А-ЯІЇЄҐA-Z\"][^\n]{3,80}?)(?:\n|,\s*МФО|МФО|IBAN|$)",
        text,
    )
    if bank_m:
        b = re.sub(r"\s+", " ", bank_m.group(1)).strip(" ,.")
        if len(b) > 3:
            bank = b

    # Період послуги
    period = None
    pm = re.search(r"за\s+(\w+)\s+(\d{4})", text, re.IGNORECASE)
    if pm:
        mn = _UA_MONTHS.get(pm.group(1).lower())
        if mn:
            period = f"{mn}.{pm.group(2)}"
    if not period:
        pm2 = re.search(r"(0[1-9]|1[0-2])[./](\d{4})", text)
        if pm2:
            period = f"{pm2.group(1)}.{pm2.group(2)}"

    for it in items:
        if not it.get("partner_iban") and iban:
            it["partner_iban"] = iban
            logger.info("Enriched partner_iban from tesseract: %s", iban)
        if not it.get("partner_bank_name") and bank:
            it["partner_bank_name"] = bank
            logger.info("Enriched partner_bank_name from tesseract: %s", bank)
        if not it.get("service_period") and period:
            it["service_period"] = period
            logger.info("Enriched service_period from tesseract: %s", period)


# ---------------------------------------------------------------------------
# XLSX парсинг (прямий, без OCR)
# ---------------------------------------------------------------------------
def _parse_xlsx(data: bytes) -> list[dict]:
    """Прямий парсинг Excel-файлу."""
    wb = openpyxl.load_workbook(BytesIO(data), read_only=True, data_only=True)
    ws = wb.active
    items = []
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        wb.close()
        return items

    header_row = None
    header_idx = -1
    for i, row in enumerate(rows):
        cells = [str(c).lower().strip() if c else "" for c in row]
        if any("постачальник" in c or "контрагент" in c for c in cells):
            header_row = cells
            header_idx = i
            break

    if header_row is None:
        wb.close()
        return items

    col_map = {}
    for j, cell in enumerate(header_row):
        if "постачальник" in cell or "контрагент" in cell:
            col_map["partner"] = j
        elif "номер" in cell and "рахун" in cell:
            col_map["number"] = j
        elif "дата" in cell:
            col_map["date"] = j
        elif "сума" in cell or "всього" in cell:
            col_map["amount"] = j
        elif "послуг" in cell or "найменування" in cell or "опис" in cell:
            col_map["service"] = j
        elif "єдрпоу" in cell or "код" in cell:
            col_map["code"] = j

    for row in rows[header_idx + 1 :]:
        if not row or all(c is None for c in row):
            continue
        item = _empty_invoice_item()
        if "partner" in col_map and row[col_map["partner"]]:
            item["partner_name"] = str(row[col_map["partner"]]).strip()
        if "number" in col_map and row[col_map["number"]]:
            item["invoice_number"] = str(row[col_map["number"]]).strip()
        if "date" in col_map and row[col_map["date"]]:
            item["invoice_date"] = str(row[col_map["date"]]).strip()
        if "amount" in col_map and row[col_map["amount"]]:
            try:
                item["invoice_amount"] = float(row[col_map["amount"]])
            except (ValueError, TypeError):
                item["needs_review"] = True
        if "service" in col_map and row[col_map["service"]]:
            item["invoice_line_name"] = str(row[col_map["service"]]).strip()
        if "code" in col_map and row[col_map["code"]]:
            item["partner_code"] = str(row[col_map["code"]]).strip()
        if item["partner_name"] or item["invoice_amount"]:
            items.append(item)

    wb.close()
    return items


# ---------------------------------------------------------------------------
# XLS парсинг (старий BIFF/OLE2 формат, xlrd)
# ---------------------------------------------------------------------------
def _parse_xls(data: bytes) -> list[dict]:
    """Парсинг старого .xls рахунку (OLE2/BIFF формат)."""
    wb = xlrd.open_workbook(file_contents=data)
    ws = wb.sheet_by_index(0)

    item = _empty_invoice_item()

    # Знаходимо заголовний рядок таблиці для маппінгу колонок
    col_qty = None  # К-сть
    col_unit = None  # Од.
    col_price = None  # Ціна без ПДВ
    col_amount = None  # Сума без ПДВ
    col_nomenclature = None  # Номенклатура
    header_row_idx = -1

    for i in range(ws.nrows):
        row_text = " ".join(
            str(ws.cell_value(i, j)) for j in range(ws.ncols) if ws.cell_value(i, j)
        )
        if "Номенклатура" in row_text or ("К-сть" in row_text and "Од." in row_text):
            header_row_idx = i
            for j in range(ws.ncols):
                v = str(ws.cell_value(i, j)).strip()
                vl = v.lower()
                if "к-сть" in vl or "кількість" in vl:
                    col_qty = j
                elif vl == "од." or vl == "од":
                    col_unit = j
                elif "номенклатура" in vl or "найменування" in vl or "товар" in vl:
                    col_nomenclature = j
                elif "ціна" in vl:
                    col_price = j
                elif "сума" in vl and "пдв" not in vl:
                    col_amount = j
            break

    for i in range(ws.nrows):
        cells = []
        for j in range(ws.ncols):
            v = ws.cell_value(i, j)
            if v != "" and v is not None:
                cells.append((j, v))
        if not cells:
            continue

        row_text = " ".join(str(v) for _, v in cells)

        # --- Номер і дата рахунку ---
        num_m = re.search(
            r"Рахунок\s*№\s*(\S+)\s+від\s+(\d{2}\s+\w+\s+\d{4})", row_text
        )
        if num_m and not item["invoice_number"]:
            item["invoice_number"] = num_m.group(1)
            item["invoice_date"] = num_m.group(2)

        # --- Постачальник ---
        if any("Постачальник" in str(v) for _, v in cells):
            for _, v in reversed(cells):
                s = str(v).strip()
                if s and "Постачальник" not in s and len(s) > 3:
                    item["partner_name"] = _clean_partner_name(s)
                    break

        # --- Покупець ---
        if any("Покупець" in str(v) for _, v in cells):
            for _, v in reversed(cells):
                s = str(v).strip()
                if s and "Покупець" not in s and len(s) > 3:
                    item["fop_name"] = _clean_partner_name(s)
                    break

        # --- Договір ---
        if "Договір" in row_text and not item["contract"]:
            for _, v in cells:
                s = str(v).strip()
                if s and "Договір" not in s and len(s) > 3:
                    item["contract"] = s
                    break

        # --- ІПН / ЄДРПОУ (можуть бути в багаторядковій комірці) ---
        for _, v in cells:
            sv = str(v)
            if not item["partner_code"]:
                ipn_m = re.search(r"ІПН\s*(\d{10})", sv)
                if ipn_m:
                    item["partner_code"] = ipn_m.group(1)
                edr_m = re.search(r"ЄДРПОУ\s*(\d{6,10})", sv)
                if edr_m:
                    item["partner_code"] = edr_m.group(1)

        # --- Рядок номенклатури (послуга + кількість + одиниця + сума) ---
        if i > header_row_idx > -1 and not item["invoice_line_name"]:
            # Використовуємо header-based маппінг колонок
            nom_val = None
            qty_val = None
            unit_val = None

            if col_nomenclature is not None:
                v = ws.cell_value(i, col_nomenclature)
                if isinstance(v, str) and len(v.strip()) > 3:
                    nom_val = v.strip()
            if col_qty is not None:
                v = ws.cell_value(i, col_qty)
                if isinstance(v, (int, float)) and v > 0:
                    qty_val = v
            if col_unit is not None:
                v = str(ws.cell_value(i, col_unit)).strip()
                if v:
                    unit_val = v

            if nom_val:
                item["invoice_line_name"] = nom_val
                if qty_val is not None:
                    item["quantity"] = str(qty_val).rstrip("0").rstrip(".")
                if unit_val:
                    u = unit_val.lower()
                    if u in ("м.кв", "кв.м", "м²"):
                        unit_val = "м2"
                    item["unit"] = unit_val

        # Fallback: рядок з числовим №, текстом і сумою (без header mapping)
        if not item["invoice_line_name"] and len(cells) >= 3:
            num_cell = False
            text_cell = None
            amount_cell = None
            qty_cell = None
            unit_cell = None
            for j, v in cells:
                if isinstance(v, float) and v == int(v) and 0 < v < 100 and j < 5:
                    num_cell = True
                elif isinstance(v, str) and len(v) > 10 and text_cell is None:
                    text_cell = v.strip()
                elif isinstance(v, str) and v.strip().lower() in (
                    "м2", "м²", "м.кв", "кв.м", "шт", "грн", "послуга",
                ):
                    unit_cell = v.strip()
                elif isinstance(v, float) and v > 100:
                    amount_cell = v
                elif isinstance(v, float) and 0 < v < 100000 and qty_cell is None and num_cell:
                    qty_cell = v
            if num_cell and text_cell and amount_cell is not None:
                item["invoice_line_name"] = text_cell
                if qty_cell is not None:
                    item["quantity"] = str(qty_cell).rstrip("0").rstrip(".")
                if unit_cell:
                    u = unit_cell.lower()
                    if u in ("м.кв", "кв.м", "м²"):
                        unit_cell = "м2"
                    item["unit"] = unit_cell

        # --- Суми ---
        if "Разом без ПДВ" in row_text or "Разом:" in row_text:
            for _, v in reversed(cells):
                if isinstance(v, (int, float)) and v > 0:
                    item["invoice_amount_no_vat"] = float(v)
                    break

        if re.search(r"ПДВ\s*:", row_text) and "Разом" not in row_text and "Всього" not in row_text:
            for _, v in reversed(cells):
                if isinstance(v, (int, float)) and v > 0:
                    item["vat_amount"] = float(v)
                    break

        if "Всього з ПДВ" in row_text or re.search(r"Всього\s*:", row_text):
            for _, v in reversed(cells):
                if isinstance(v, (int, float)) and v > 0:
                    item["invoice_amount"] = float(v)
                    break

    # --- IBAN / Банк / Період послуги — повнотекстовий пошук по всіх клітинках ---
    full_text = "\n".join(
        str(ws.cell_value(i, j))
        for i in range(ws.nrows) for j in range(ws.ncols)
        if ws.cell_value(i, j) not in (None, "")
    )

    if not item["partner_iban"]:
        iban_m = re.search(r"\b(UA\d{27})\b", full_text.replace(" ", ""))
        if iban_m:
            item["partner_iban"] = iban_m.group(1)

    if not item["partner_bank_name"]:
        bank_m = re.search(
            r"(?:у\s+банку|Банк)[:\s]+([А-ЯІЇЄҐA-Z\"][^\n]{3,80}?)(?:\n|,\s*МФО|МФО|IBAN|$)",
            full_text,
        )
        if bank_m:
            bank = re.sub(r"\s+", " ", bank_m.group(1)).strip(" ,.")
            if len(bank) > 3:
                item["partner_bank_name"] = bank

    if not item["service_period"]:
        period_m = re.search(r"за\s+(\w+)\s+(\d{4})", full_text, re.IGNORECASE)
        if period_m:
            month_num = _UA_MONTHS.get(period_m.group(1).lower())
            if month_num:
                item["service_period"] = f"{month_num}.{period_m.group(2)}"
        if not item["service_period"]:
            p2 = re.search(r"(0[1-9]|1[0-2])[./](\d{4})", full_text)
            if p2:
                item["service_period"] = f"{p2.group(1)}.{p2.group(2)}"

    # --- Нормалізація invoice_date («01 березня 2026» → «2026-03-01») ---
    if item["invoice_date"]:
        iso = _normalize_ua_date(str(item["invoice_date"]))
        if iso:
            item["invoice_date"] = iso

    # --- unit_price з quantity і суми без ПДВ ---
    if (item["unit_price"] is None and item["quantity"]
            and item["invoice_amount_no_vat"]):
        try:
            qty = float(str(item["quantity"]).replace(",", "."))
            if qty > 0:
                item["unit_price"] = round(item["invoice_amount_no_vat"] / qty, 2)
        except (ValueError, TypeError):
            pass

    wb.release_resources()

    # Якщо є "Разом без ПДВ" але немає окремого "Всього" — це сума без ПДВ
    if item["invoice_amount"] is None and item["invoice_amount_no_vat"] is not None:
        item["invoice_amount"] = item["invoice_amount_no_vat"]

    if not item["partner_name"] and not item["invoice_number"]:
        item["needs_review"] = True

    return [item] if (item["partner_name"] or item["invoice_amount"]) else []


# ---------------------------------------------------------------------------
# Парсинг OCR-тексту одного рахунку
# ---------------------------------------------------------------------------
def _empty_invoice_item() -> dict:
    return {
        "partner_name": None,
        "invoice_number": None,
        "invoice_date": None,
        "invoice_line_name": None,
        "invoice_amount": None,
        "invoice_amount_no_vat": None,
        "vat_amount": None,
        "payment_date": None,
        "invoice_type": None,
        "partner_code": None,
        "contract": None,
        "fop_name": None,
        "quantity": None,
        "unit": None,
        "unit_price": None,
        "partner_iban": None,
        "partner_bank_name": None,
        "service_period": None,
        "needs_review": False,
    }


def _fix_ocr_amount(raw: str) -> float:
    """Виправити OCR-артефакти у числах і конвертувати в float.

    Кирилиця -> цифри: т->7, Т->7, і->1, І->1, а->4, о->0, О->0, з->3, З->3.
    Евристика десяткового роздільника: якщо немає крапки/коми і остання
    група через пробіл - 2 цифри (копійки), трактуємо як десяткову частину.
    """
    s = raw
    s = s.replace("\u0442", "7").replace("\u0422", "7")  # т/Т -> 7
    s = s.replace("\u0456", "1").replace("\u0406", "1")  # і/І -> 1
    s = s.replace("\u0430", "4")                          # а -> 4
    s = s.replace("\u043e", "0").replace("\u041e", "0")  # о/О -> 0
    s = s.replace("\u0437", "3").replace("\u0417", "3")  # з/З -> 3

    s = s.replace(",", ".")

    if "." not in s:
        parts = s.strip().split()
        digit_parts = [p for p in parts if re.match(r"^\d+$", p)]
        if len(digit_parts) >= 2 and len(digit_parts[-1]) == 2:
            integer = "".join(digit_parts[:-1])
            decimal = digit_parts[-1]
            return float(f"{integer}.{decimal}")

    s = s.replace(" ", "")
    s = re.sub(r"[^\d.]", "", s)

    # Крапка як роздільник тисяч: "1.284.33" → залишити лише останню крапку
    if s.count(".") > 1:
        parts = s.rsplit(".", 1)
        s = parts[0].replace(".", "") + "." + parts[1]

    if not s:
        raise ValueError(f"Cannot parse amount: {raw!r}")
    return float(s)


def _clean_partner_name(name: str) -> str:
    """Прибрати юридичну форму з назви постачальника."""
    prefixes = [
        "Фізична особа - підприємець",
        "Фізична особа-підприємець",
        "Фізична особа підприємець",
        "Товариство з обмеженою відповідальністю",
        "Приватне підприємство",
    ]
    result = name
    for pfx in prefixes:
        if result.lower().startswith(pfx.lower()):
            result = result[len(pfx) :].strip()
            break
    result = result.strip("\" \u00ab\u00bb\u201c\u201e\u201d")
    # Суфікси юридичної форми (XLS: "Деркач ... фізична особа підприємець")
    suffixes = [
        r"\s+фізична\s+особа[\s-]*підприємець\s*$",
        r"\s+ФОП\s*$",
        r"\s+фамо$",
    ]
    for sfx in suffixes:
        result = re.sub(sfx, "", result, flags=re.IGNORECASE)
    return result.strip()


def parse_single_invoice(text: str) -> dict:
    """Розпарсити OCR-текст одного рахунку в структуровані дані."""
    item = _empty_invoice_item()

    # --- Знайти секцію рахунку (після "Рахунок на оплату") ---
    rakh_match = re.search(r"Рахунок\s+на\s+оплату", text)
    section = text[rakh_match.start() :] if rakh_match else text

    # --- 1. Номер і дата ---
    # OCR рендерить "№" як "Мо", "Ме", "М0"
    # Рік може бути усічений OCR: "202" замість "2025"
    num_m = re.search(
        r"Рахунок\s+на\s+оплату\s+"
        r"(?:Мо|М[ео0О]|№|No|N0)\s*"
        r"([А-Яа-яA-Za-z\d]+)\s+"
        r"від\s+(\d{2})\s+(\w+)\s+(\d{3,4})",
        section,
    )
    if num_m:
        inv_num = num_m.group(1)
        # OCR: Cyrillic О -> 0 у номерах (Н0000034572 -> НО000034572)
        inv_num = inv_num.replace("\u041e", "0").replace("\u043e", "0")
        item["invoice_number"] = inv_num
        year = num_m.group(4)
        if len(year) == 3:
            year = year + "5"  # "202" -> "2025" (поточний контекст)
        item["invoice_date"] = f"{num_m.group(2)} {num_m.group(3)} {year}"

    # --- 2. Постачальник / Орендодавець ---
    # Шукаємо ТІЛЬКИ в секції рахунку (не в шапці "Зразок заповнення")
    sup_m = re.search(
        r"(?:Постачальник|Орендодавець)[.:\s]+"
        r"(.*?)"
        r"(?:\n\s*п/р|\n\s*р/р|\n\s*код|\n\s*ЄДРПОУ|\n\s*IBAN"
        r"|\n\s*Покупець|\n\s*Орендар|\n\s*\d{5}|\n\n)",
        section,
        re.DOTALL,
    )
    if sup_m:
        raw = re.sub(r"\s+", " ", sup_m.group(1)).strip().rstrip(".")
        cleaned = _clean_partner_name(raw)
        if not re.match(r"^\d{4,5}", cleaned):
            item["partner_name"] = cleaned

    # Fallback 1: "Фізична особ* підприємець <Ім'я>" (OCR може ламати мітку)
    if not item["partner_name"]:
        fop_m = re.search(
            r"Фізична\s+особ\w*\s*[-\u2013\u2014]?\s*підприємець\s+"
            r"([\w][\w\s]+?)(?:,|\n|$)",
            section,
        )
        if fop_m:
            name = fop_m.group(1).strip()
            name = re.sub(r"\s+ФОП\s*$", "", name)
            item["partner_name"] = name

    # Fallback 2: перший рядок з назвою компанії після заголовка рахунку
    if not item["partner_name"] and rakh_match:
        _company_re = re.compile(
            r"((?:ТОВАРИСТВО|ТОВ|ФОП|Фізична\s+особа|Приватне\s+підприємство)"
            r".*?)$",
            re.MULTILINE,
        )
        companies = _company_re.findall(section)
        if companies:
            item["partner_name"] = _clean_partner_name(
                re.sub(r"\s+", " ", companies[0]).strip()
            )

    # --- 3. Покупець / Орендар ---
    buy_m = re.search(
        r"(?:Покупець|Орендар\s*(?:\(суборендар\))?)[.:]*\s*(.*?)(?:\n|$)",
        section,
    )
    if buy_m:
        buyer = buy_m.group(1).strip()
        buyer = re.sub(r"\s+фамо$", "", buyer)
        # OCR може злити "Орендар(суборендар)" → "Орендарісуборендар!" —
        # тоді captured text починається з "ісуборендар" або адреси (поштовий індекс)
        if re.match(r"^[іi]?суборенд", buyer) or re.match(r"^\d{4,5}", buyer) or not buyer:
            match_pos = buy_m.start()
            before_text = section[:match_pos].rstrip()
            for line in reversed(before_text.split("\n")):
                line = line.strip().rstrip(",.")
                if (
                    line
                    and re.search(r"[А-ЯІЇЄҐа-яіїєґ]{2,}", line)
                    and not re.match(r"^\d{4,5}", line)
                    and not re.match(r"^(?:п/р|р/р|код|ЄДРПОУ|IBAN)", line, re.IGNORECASE)
                ):
                    buyer = line
                    break
        item["fop_name"] = buyer

    # Fallback: друга назва компанії = покупець
    if not item["fop_name"] and rakh_match:
        _company_re = re.compile(
            r"((?:ТОВАРИСТВО|ТОВ|ФОП|Фізична\s+особа|Приватне\s+підприємство)"
            r".*?)$",
            re.MULTILINE,
        )
        companies = _company_re.findall(section)
        if len(companies) >= 2:
            item["fop_name"] = re.sub(r"\s+", " ", companies[1]).strip()

    # --- 4. ЄДРПОУ / ДРФО ---
    code_m = re.search(
        r"(?:ЄДРПОУ|ДРФО|код\s+за\s+ЄДРПОУ|код\s+за\s+ДРФО)\s*(\d{6,10})",
        section,
    )
    if code_m:
        item["partner_code"] = code_m.group(1)
    if not item["partner_code"] and rakh_match and rakh_match.start() > 0:
        header = text[: rakh_match.start()]
        code_fb = re.search(r"(?:Код|код)[:\s]+(\d{8,10})", header)
        if not code_fb:
            code_fb = re.search(r"\b(\d{8,10})\b", header)
        if code_fb:
            item["partner_code"] = code_fb.group(1)

    # --- 4.1. IBAN постачальника ---
    iban_m = re.search(r"\b(UA\d{27})\b", section.replace(" ", ""))
    if iban_m:
        item["partner_iban"] = iban_m.group(1)

    # --- 4.2. Банк постачальника ---
    bank_m = re.search(
        r"(?:у\s+банку|Банк)[:\s]+([А-ЯІЇЄҐA-Z][^\n]{3,60}?)(?:\n|МФО|IBAN|$)",
        section,
    )
    if bank_m:
        bank = re.sub(r"\s+", " ", bank_m.group(1)).strip(" ,.")
        if len(bank) > 3:
            item["partner_bank_name"] = bank

    # --- 4.3. Період послуги ---
    _MONTH_MAP = {
        "січня": "01", "лютого": "02", "березня": "03", "квітня": "04",
        "травня": "05", "червня": "06", "липня": "07", "серпня": "08",
        "вересня": "09", "жовтня": "10", "листопада": "11", "грудня": "12",
        "січень": "01", "лютий": "02", "березень": "03", "квітень": "04",
        "травень": "05", "червень": "06", "липень": "07", "серпень": "08",
        "вересень": "09", "жовтень": "10", "листопад": "11", "грудень": "12",
    }
    # "за березень 2026" / "за 03.2026" / "03/2026"
    period_m = re.search(
        r"за\s+(\w+)\s+(\d{4})", text, re.IGNORECASE,
    )
    if period_m:
        month_name = period_m.group(1).lower()
        month_num = _MONTH_MAP.get(month_name)
        if month_num:
            item["service_period"] = f"{month_num}.{period_m.group(2)}"
    if not item["service_period"]:
        period_m2 = re.search(r"(0[1-9]|1[0-2])[./](\d{4})", text)
        if period_m2:
            item["service_period"] = f"{period_m2.group(1)}.{period_m2.group(2)}"

    # --- 5. Договір ---
    ctr_m = re.search(r"Договір[.:]*\s*(\S+)", section)
    if ctr_m:
        contract = ctr_m.group(1).strip()
        if contract.lower() in ("ріпку", "ріпки", "ріпкі", "ріпка"):
            contract = "pinky"
        elif contract.lower().startswith("товар"):
            contract = None
        elif len(contract) < 3 or contract.startswith(("(", "[", "{")):
            contract = None
        item["contract"] = contract

    # --- 6. Послуга (пошук відомих фраз) ---
    text_lower = text.lower()
    for svc in KNOWN_SERVICES:
        if svc in text_lower:
            idx = text_lower.index(svc)
            remaining = text[idx:]
            end_m = re.search(r"(\d{1,3}[,. ]\d|\n\s*\n|\s{3,}\d)", remaining)
            if end_m:
                svc_name = remaining[: end_m.start()].strip()
            else:
                svc_name = remaining.split("\n")[0].strip()
            svc_name = re.sub(r"[|]", "", svc_name).strip()
            svc_name = re.sub(r"\s+", " ", svc_name)
            svc_name = re.sub(r'\s*"(?:що|шт|што).*$', "", svc_name)
            item["invoice_line_name"] = svc_name
            break

    # Fallback: шукаємо назву послуги після заголовку таблиці
    if not item["invoice_line_name"]:
        table_header_m = re.search(
            r"(?:Товари|Найменування|Послуги).*?(?:Кількість|К-сть|Ціна)",
            text, re.IGNORECASE | re.DOTALL,
        )
        if table_header_m:
            after_header = text[table_header_m.end():]
            for line in after_header.split("\n"):
                line = line.strip()
                if not line or re.match(r"^[\d\s.,|:_\-]+$", line):
                    continue
                # Фільтруємо заголовки стовпців та сміття OCR
                if re.match(
                    r"^(?:Ціна|Сума|ПДВ|без|Од\.|Разом|Всього|Кількість|К-сть|,|\-)",
                    line, re.IGNORECASE,
                ):
                    continue
                # Рядок не повинен складатися лише з фрагментів заголовків таблиці
                _header_words = {"ціна", "сума", "пдв", "без", "кількість", "од", "ум", "оса"}
                words = set(re.findall(r"[а-яіїєґ]+", line.lower()))
                if words and words.issubset(_header_words):
                    continue
                # Фільтруємо суму прописом та службові рядки
                if re.search(
                    r"(?:тисяч|гривень|копійок|сімсот|двісті|"
                    r"виписав|підпис|директор|бухгалтер|печатка|"
                    r"найменувань|на суму|оплата цього)",
                    line, re.IGNORECASE,
                ):
                    continue
                if re.search(r"[А-ЯІЇЄҐа-яіїєґ]{3,}", line):
                    svc_name = re.sub(r"^\d+[.\s]*", "", line)
                    svc_name = re.sub(r"\s+", " ", svc_name).strip()
                    # Мінімум 10 символів і хоча б 2 слова — щоб відфільтрувати сміття
                    if len(svc_name) > 10 and " " in svc_name:
                        item["invoice_line_name"] = svc_name
                        break

    # --- 7. Суми ---
    razom_m = re.search(r"Разом\s*:?\s*(.+?)$", text, re.MULTILINE)
    vat_m = re.search(r"Сума\s+ПДВ\s*:?\s*(.+?)$", text, re.MULTILINE)
    total_vat_m = re.search(
        r"Всього\s+із\s+ПДВ\s*:?\s*(.+?)$", text, re.MULTILINE
    )

    if razom_m and vat_m:
        # З ПДВ — cross-validation: Разом + ПДВ = Всього
        try:
            no_vat = _fix_ocr_amount(razom_m.group(1))
            vat = _fix_ocr_amount(vat_m.group(1))
            computed_total = round(no_vat + vat, 2)

            total = computed_total
            if total_vat_m:
                try:
                    parsed_total = _fix_ocr_amount(total_vat_m.group(1))
                    if abs(parsed_total - computed_total) <= 1.0:
                        total = parsed_total
                except ValueError:
                    pass

            item["invoice_amount_no_vat"] = no_vat
            item["vat_amount"] = vat
            item["invoice_amount"] = total
        except ValueError:
            item["needs_review"] = True
    else:
        # Без ПДВ
        verif_m = re.search(
            r"на\s+суму\s+([\d\s,. ]+)\s*(?:UAH|ЦАН|ДАН|ЧАН|грн)", text
        )
        total_matches = re.findall(r"Всього:\s*(.+?)$", text, re.MULTILINE)

        verif_amount = None
        if verif_m:
            try:
                verif_amount = _fix_ocr_amount(verif_m.group(1))
            except ValueError:
                pass

        total_amount = None
        if total_matches:
            try:
                total_amount = _fix_ocr_amount(total_matches[-1])
            except ValueError:
                pass

        if verif_amount is not None:
            item["invoice_amount"] = verif_amount
        elif total_amount is not None:
            item["invoice_amount"] = total_amount
        else:
            item["needs_review"] = True

    # --- 8. Кількість та одиниця ---
    # Одиниці виміру: слово має бути повним (не початок іншого слова)
    _units_re = r"(м2|м²|м\.кв|кв\.м|шт\.?|послуга|послуг|грн|год|кг|компл|рул|уп)(?:\b|[.\s,;)])"

    # Спершу шукаємо число + одиниця поруч (470 м2, 1 шт)
    qty_m = re.search(
        r"(\d+[,.]?\d*)\s*(?:[|])?\s*" + _units_re,
        text_lower,
    )
    if qty_m:
        item["quantity"] = qty_m.group(1).replace(",", ".")
        unit = qty_m.group(2).rstrip(".")
        if unit in ("м.кв", "кв.м", "м2", "м²"):
            unit = "м2"
        elif unit.startswith("послуг"):
            unit = "послуга"
        item["unit"] = unit

    # Fallback: шукаємо кількість + одиницю в рядку таблиці (після номенклатури)
    # Працює тільки якщо знайдено назву послуги і текст після неї містить число+одиницю
    if not item["quantity"] and item["invoice_line_name"]:
        svc = item["invoice_line_name"]
        svc_idx = text_lower.find(svc.lower()[:15])
        if svc_idx >= 0:
            after_svc = text_lower[svc_idx:]
            qty_unit_m = re.search(
                r"(\d+[,.]?\d*)\s*(?:" + _units_re + r")", after_svc,
            )
            if qty_unit_m:
                raw_qty = qty_unit_m.group(1).replace(",", ".")
                try:
                    qty_val = float(raw_qty)
                    if 0.01 <= qty_val <= 99999:
                        item["quantity"] = raw_qty
                        unit = qty_unit_m.group(2)
                        if unit in ("м.кв", "кв.м", "м2", "м²"):
                            unit = "м2"
                        elif unit.startswith("послуг"):
                            unit = "послуга"
                        item["unit"] = unit
                except ValueError:
                    pass

    # Fallback: одиниця окремо — шукаємо тільки поруч зі знайденою кількістю
    if not item["unit"] and item["quantity"]:
        qty_str = item["quantity"]
        qty_idx = text_lower.find(qty_str)
        if qty_idx >= 0:
            vicinity = text_lower[max(0, qty_idx - 10):qty_idx + len(qty_str) + 20]
            unit_m = re.search(_units_re, vicinity)
            if unit_m:
                unit = unit_m.group(1).rstrip(".")
                if unit in ("м.кв", "кв.м", "м2", "м²"):
                    unit = "м2"
                elif unit.startswith("послуг"):
                    unit = "послуга"
                item["unit"] = unit

    # Якщо є ціна за одиницю і загальна сума — розрахувати кількість
    if not item["quantity"] and item["invoice_amount"] and item["invoice_line_name"]:
        svc_name = item["invoice_line_name"]
        svc_idx = text_lower.find(svc_name.lower()[:20])
        if svc_idx >= 0:
            after_svc = text[svc_idx + len(svc_name):]
            first_line = after_svc.split("\n")[0]
            # Числа в українському форматі: "1.284,33" або "55 354,62"
            # Кома + 1-2 цифри = десяткова частина (роздільник чисел)
            raw_numbers = re.findall(r"\d[\d\s.]*,\d{1,2}", first_line)
            amounts = []
            for n in raw_numbers:
                try:
                    amounts.append(_fix_ocr_amount(n))
                except ValueError:
                    pass
            # ціна × кількість = сума → кількість = сума / ціна
            if len(amounts) >= 2:
                total = item["invoice_amount"]
                for price in amounts:
                    if price > 0 and abs(price - total) > 1.0:
                        qty = total / price
                        if 0.1 <= qty <= 100000 and abs(qty * price - total) < 1.0:
                            item["quantity"] = str(round(qty, 2)).rstrip("0").rstrip(".")
                            if item["unit_price"] is None:
                                item["unit_price"] = price
                            break

    # Якщо є quantity і invoice_amount_no_vat — розрахувати unit_price
    if (item["unit_price"] is None and item["quantity"]
            and item["invoice_amount_no_vat"]):
        try:
            qty = float(item["quantity"])
            if qty > 0:
                item["unit_price"] = round(item["invoice_amount_no_vat"] / qty, 2)
        except (ValueError, TypeError):
            pass

    if not item["partner_name"] and not item["invoice_number"]:
        item["needs_review"] = True

    # Нормалізація invoice_date у ISO формат для Odoo (Date field)
    if item["invoice_date"]:
        iso = _normalize_ua_date(str(item["invoice_date"]))
        if iso:
            item["invoice_date"] = iso

    return item

def _build_ocr_summary(items: list[dict]) -> str:
    """Build human-readable summary of recognized invoices."""
    lines = []
    for it in items:
        num = it.get("invoice_number") or "?"
        name = it.get("partner_name") or "?"
        amount = it.get("invoice_amount") or 0
        lines.append(f"• №{num} — {name} — {amount} грн")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Handler registration
# ---------------------------------------------------------------------------
def register_ocr_handlers(
    worker: ZeebeWorker,
    config: AppConfig,
) -> None:
    """Register invoice OCR extraction handler.

    Task types registered:
        invoice-data-extractor: OCR-розпізнавання рахунків (PDF/JPG/PNG/XLSX)
    """

    @worker.task(task_type="invoice-data-extractor", timeout_ms=120_000)
    async def invoice_data_extractor(
        x_studio_camunda_invoice_file: str = "",
        file_extension: str = "",
        **kwargs: Any,
    ) -> dict:
        """Витягнути дані з рахунку через OCR або прямий парсинг.

        Input:
            x_studio_camunda_invoice_file — base64 або URL файлу
            file_extension — pdf / jpg / jpeg / png / xlsx / xls

        Output:
            invoice_items — список розпарсених рахунків
            recognized — True якщо хоча б один рахунок знайдено
            total_invoices — кількість рахунків
            total_amount — загальна сума
        """
        odoo_task_id = kwargs.get("odoo_task_id")
        logger.info("invoice-data-extractor | ext=%s, odoo_task_id=%s, file_present=%s",
                     file_extension, odoo_task_id, bool(x_studio_camunda_invoice_file))

        try:
            # Якщо файл не передано через Camunda — завантажити з Odoo напряму
            if not x_studio_camunda_invoice_file and odoo_task_id:
                logger.info("File not in Camunda vars, fetching from Odoo task %s", odoo_task_id)
                x_studio_camunda_invoice_file, file_extension = await _fetch_file_from_odoo(
                    int(odoo_task_id)
                )

            # Визначити ext з filename якщо є
            x_studio_camunda_invoice_file_filename = kwargs.get("x_studio_camunda_invoice_file_filename", "")
            if not file_extension and x_studio_camunda_invoice_file_filename:
                fn = str(x_studio_camunda_invoice_file_filename)
                if "." in fn:
                    file_extension = fn.rsplit(".", 1)[-1]
                    logger.info("Got extension from filename '%s': %s", fn, file_extension)

            logger.info("File value first 80 chars: %s", repr(str(x_studio_camunda_invoice_file)[:80]))

            file_data = await _acquire_file(x_studio_camunda_invoice_file)
            ext = (file_extension or "").lower().strip(".")

            # Автовизначення розширення з magic bytes якщо не вказано
            if not ext and file_data:
                logger.info("Magic bytes (first 16): %s", file_data[:16].hex())
            if not ext and file_data:
                if file_data[:4] == b"%PDF":
                    ext = "pdf"
                elif file_data[:8] == b"\x89PNG\r\n\x1a\n":
                    ext = "png"
                elif file_data[:2] == b"\xff\xd8":
                    ext = "jpg"
                elif file_data[:2] == b"PK":
                    ext = "xlsx"
                elif file_data[:8] == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1":
                    ext = "xls"
                logger.info("Auto-detected file extension: %s", ext)

            items: list[dict] = []

            if ext in ("xlsx", "xls"):
                # Spreadsheets — прямий парсинг, Gemini не потрібен
                items = _parse_xlsx(file_data) if ext == "xlsx" else _parse_xls(file_data)

            elif ext in ("pdf", "jpg", "jpeg", "png"):
                # Зображення — спершу Gemini (посторінково), fallback на tesseract
                if ext == "pdf":
                    images = convert_from_bytes(file_data, dpi=OCR_DPI)
                else:
                    img = Image.open(BytesIO(file_data))
                    if img.mode in ("RGBA", "P"):
                        img = img.convert("RGB")
                    images = [img]

                # --- Gemini (primary, per-page) ---
                all_gemini_items = []
                gemini_failed = False
                for page_idx, page_img in enumerate(images):
                    page_items = await _gemini_extract_from_images([page_img])
                    if page_items:
                        all_gemini_items.extend(page_items)
                        logger.info("Gemini page %d: %d invoice(s)", page_idx + 1, len(page_items))
                    elif page_items is None and not all_gemini_items:
                        gemini_failed = True
                        break
                    else:
                        logger.info("Gemini page %d: no invoices", page_idx + 1)

                if all_gemini_items:
                    items = all_gemini_items
                    logger.info("Using Gemini result (%d items from %d pages)", len(items), len(images))
                    # Enrichment: якщо Gemini пропустив IBAN/банк/період — добиваємо через tesseract
                    needs_enrichment = any(
                        not it.get("partner_iban") or not it.get("partner_bank_name") or not it.get("service_period")
                        for it in items
                    )
                    if needs_enrichment:
                        try:
                            if ext == "pdf":
                                texts = ocr_pdf(file_data)
                                tess_text = "\n".join(texts)
                            else:
                                tess_text = ocr_image(file_data)
                            logger.info("Enrichment: tesseract text %d chars", len(tess_text))
                            _enrich_items_from_text(items, tess_text)
                        except Exception as e:
                            logger.warning("Enrichment failed: %s", e)
                elif gemini_failed:
                    # --- Tesseract (fallback) ---
                    logger.info("Gemini unavailable, falling back to tesseract")
                    if ext == "pdf":
                        texts = ocr_pdf(file_data)
                        for text in texts:
                            items.append(parse_single_invoice(text))
                    else:
                        text = ocr_image(file_data)
                        items.append(parse_single_invoice(text))

            else:
                return {
                    "recognized": False,
                    "invoice_items": [],
                    "total_invoices": 0,
                    "total_amount": 0,
                    "error": f"Unsupported file extension: {ext}",
                }

            total = sum(i["invoice_amount"] or 0 for i in items)
            result = {
                "invoice_items": items,
                "recognized": len(items) > 0,
                "total_invoices": len(items),
                "total_amount": round(total, 2),
                "ocr_summary": _build_ocr_summary(items),
            }

            # Розпакувати поля першого рахунку як окремі змінні
            # для використання в FEEL body без індексації масиву
            if items:
                first = items[0]
                result["first_partner_name"] = first.get("partner_name")
                result["first_invoice_number"] = first.get("invoice_number")
                result["first_invoice_date"] = first.get("invoice_date")
                result["first_invoice_line_name"] = first.get("invoice_line_name")
                result["first_invoice_amount_no_vat"] = first.get("invoice_amount_no_vat")
                result["first_vat_amount"] = first.get("vat_amount")
                result["first_partner_code"] = first.get("partner_code")
                result["first_contract"] = first.get("contract")
                result["first_fop_name"] = first.get("fop_name")
                result["first_quantity"] = first.get("quantity")
                result["first_unit"] = first.get("unit")
                result["first_unit_price"] = first.get("unit_price")
                result["first_partner_iban"] = first.get("partner_iban")
                result["first_partner_bank_name"] = first.get("partner_bank_name")
                result["first_service_period"] = first.get("service_period")
                result["first_invoice_type"] = first.get("invoice_type")

            logger.info("invoice-data-extractor | %d invoices, total %.2f",
                         len(items), total)
            return result

        except Exception as e:
            logger.exception("invoice-data-extractor failed: %s", e)
            return {
                "recognized": False,
                "invoice_items": [],
                "total_invoices": 0,
                "total_amount": 0,
                "error": str(e),
            }
