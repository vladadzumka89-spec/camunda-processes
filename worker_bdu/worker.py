"""BDU Worker — окремий Zeebe worker для роботи з БАС Бухгалтерія.

Перевірка штатного розкладу та створення прийому на роботу.
Підключається до тієї ж БД BAS що й fop_monitor, але працює як окремий процес.

Запуск:
    python -m worker_bdu
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from pathlib import Path

from dotenv import load_dotenv
from pyzeebe import Job, ZeebeWorker
from pyzeebe.job.job import JobController

load_dotenv(Path(__file__).resolve().parent.parent / '.env.camunda')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

RECONNECT_DELAY = 10
_shutdown = False


# ── Zeebe connection ─────────────────────────────────────────────────


def _create_channel():
    """Create gRPC channel for Zeebe (same auth as main worker)."""
    import grpc

    gateway = os.environ.get("ZEEBE_ADDRESS", "zeebe:26500")
    client_id = os.environ.get("ZEEBE_CLIENT_ID", "")
    client_secret = os.environ.get("ZEEBE_CLIENT_SECRET", "")
    token_url = os.environ.get("ZEEBE_TOKEN_URL", "")
    use_oauth = bool(client_id and client_secret and token_url)

    if use_oauth:
        import httpx

        resp = httpx.post(
            token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "audience": os.environ.get("ZEEBE_TOKEN_AUDIENCE", ""),
            },
            timeout=30,
        )
        resp.raise_for_status()
        token = resp.json()["access_token"]

        credentials = grpc.access_token_call_credentials(token)
        channel_creds = grpc.ssl_channel_credentials()
        composite = grpc.composite_channel_credentials(channel_creds, credentials)
        channel = grpc.aio.secure_channel(gateway, composite)
        logger.info("Zeebe channel: OAuth2 → %s", gateway)
    else:
        channel = grpc.aio.insecure_channel(gateway)
        logger.info("Zeebe channel: insecure → %s", gateway)

    return channel


# ── Exception handler ────────────────────────────────────────────────


async def _exception_handler(exc: Exception, job: Job, job_controller: JobController) -> None:
    """Last retry → BPMN Error, otherwise fail normally."""
    if job.retries <= 1:
        error_code = type(exc).__name__
        error_msg = str(exc)[:500]
        logger.error(
            "Job %s [%s] exhausted retries — BPMN Error: %s: %s",
            job.key, job.type, error_code, error_msg,
        )
        await job_controller.set_error_status(
            message=error_msg,
            error_code=error_code,
        )
    else:
        logger.warning(
            "Job %s [%s] failed (retries left: %d): %s",
            job.key, job.type, job.retries - 1, exc,
        )
        await job_controller.set_failure_status(message=f"Failed: {exc}")


# ── Main loop ────────────────────────────────────────────────────────


async def main() -> None:
    """Start BDU worker."""
    global _shutdown

    logger.info("=" * 60)
    logger.info("BDU Worker starting (Прийом в БДУ)")
    logger.info("=" * 60)

    def signal_handler(sig, frame):
        global _shutdown
        logger.info("Shutdown signal received")
        _shutdown = True

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    while not _shutdown:
        try:
            channel = _create_channel()
            worker = ZeebeWorker(channel, exception_handler=_exception_handler)

            # Register BDU handlers
            from .handlers import register_bdu_handlers
            register_bdu_handlers(worker)

            logger.info("BDU Worker polling for jobs...")
            await worker.work()

        except Exception as e:
            if _shutdown:
                break
            logger.error("BDU Worker error: %s — reconnecting in %ds", e, RECONNECT_DELAY)
            await asyncio.sleep(RECONNECT_DELAY)

    logger.info("BDU Worker stopped")
