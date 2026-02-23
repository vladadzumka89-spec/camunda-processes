"""
Camunda 8.8 Job Worker для інтеграції з ДО 3.0

Підписується на Zeebe job type "do3-document",
відправляє HTTP-запити до HTTP-сервісу ДО 3.0,
повертає результат назад у Camunda.
"""

import asyncio
import logging
import os
import sys

import aiohttp
from pyzeebe import ZeebeWorker, Job, create_insecure_channel

# ---------------------------------------------------------------------------
# Конфігурація (через змінні оточення)
# ---------------------------------------------------------------------------
ZEEBE_ADDRESS = os.getenv("ZEEBE_ADDRESS", "localhost:26500")
DO3_URL       = os.getenv("DO3_URL", "http://localhost/do3/hs/CamundaConnector/document")
JOB_TYPE      = os.getenv("JOB_TYPE", "do3-document")
JOB_TIMEOUT   = int(os.getenv("JOB_TIMEOUT", "30"))       # секунд
MAX_JOBS      = int(os.getenv("MAX_JOBS", "10"))           # одночасних завдань
HTTP_TIMEOUT  = int(os.getenv("HTTP_TIMEOUT", "30"))       # секунд

# Службові ключі — НЕ передаються як реквізити документа в ДО 3.0
SERVICE_KEYS = frozenset({
    "createDocument", "documentType", "documentId",
    # Camunda internal variables
    "process_instance_key", "bpmn_process_id",
})

# ---------------------------------------------------------------------------
# Логування
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("do3-worker")

# ---------------------------------------------------------------------------
# HTTP-клієнт
# ---------------------------------------------------------------------------
async def send_to_do3(session: aiohttp.ClientSession, payload: dict) -> dict:
    """Відправляє JSON у ДО 3.0 і повертає розпарсену відповідь."""

    logger.info("→ DO3 POST %s | payload keys: %s", DO3_URL, list(payload.keys()))

    async with session.post(
        DO3_URL,
        json=payload,
        timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT),
    ) as resp:
        body = await resp.json(content_type=None)
        logger.info("← DO3 %s | %s", resp.status, body)

        if resp.status >= 400 or body.get("status") != "OK":
            msg = body.get("message", f"HTTP {resp.status}")
            raise RuntimeError(f"DO3 error: {msg}")

        return body

# ---------------------------------------------------------------------------
# Побудова payload для ДО 3.0
# ---------------------------------------------------------------------------
def build_payload(variables: dict) -> dict:
    """
    З усіх змінних Camunda формує JSON для ДО 3.0.

    Службові ключі (createDocument, documentType, documentId) —
    передаються як є (вони керують логікою HTTP-сервісу ДО 3.0).

    Решта ключів — це реквізити документа (ім'я для розробників).
    """
    payload = {}

    # Режим: створення
    if variables.get("createDocument"):
        payload["createDocument"] = True
        doc_type = variables.get("documentType", "")
        if not doc_type:
            raise ValueError("createDocument=true, але documentType не вказано")
        payload["documentType"] = doc_type

    # Режим: оновлення
    elif variables.get("documentId"):
        payload["documentId"] = variables["documentId"]

    else:
        raise ValueError("Не вказано ні createDocument, ні documentId")

    # Всі інші змінні → реквізити документа
    for key, value in variables.items():
        if key not in SERVICE_KEYS:
            payload[key] = value

    return payload

# ---------------------------------------------------------------------------
# Job handler
# ---------------------------------------------------------------------------
async def handle_do3_document(job: Job) -> dict:
    """
    Обробник Zeebe job type: do3-document

    Вхід  (job variables):
      - createDocument: true/false
      - documentType:   назва виду документа (якщо створення)
      - documentId:     UUID документа (якщо оновлення)
      - <будь-які інші>: реквізити документа (ім'я для розробників = ключ)

    Вихід (повертає в Camunda):
      - documentId: UUID створеного/оновленого документа
      - do3_status: "OK" або "ERROR"
    """
    variables = job.variables
    logger.info("Job %s | type=%s | variables=%s", job.key, JOB_TYPE, list(variables.keys()))

    payload = build_payload(variables)

    async with aiohttp.ClientSession() as session:
        result = await send_to_do3(session, payload)

    # Змінні які повертаємо в Camunda
    output = {"do3_status": "OK"}
    if "documentId" in result:
        output["documentId"] = result["documentId"]

    logger.info("Job %s completed | output=%s", job.key, output)
    return output

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def main():
    logger.info("Starting DO3 worker")
    logger.info("  Zeebe:    %s", ZEEBE_ADDRESS)
    logger.info("  DO3 URL:  %s", DO3_URL)
    logger.info("  Job type: %s", JOB_TYPE)

    channel = create_insecure_channel(grpc_address=ZEEBE_ADDRESS)
    worker = ZeebeWorker(channel)

    worker.task(
        task_type=JOB_TYPE,
        max_jobs_to_activate=MAX_JOBS,
        timeout_ms=JOB_TIMEOUT * 1000,
        max_running_jobs=MAX_JOBS,
    )(handle_do3_document)

    logger.info("Worker subscribed to job type '%s'. Waiting for jobs...", JOB_TYPE)
    await worker.work()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker stopped")
        sys.exit(0)
