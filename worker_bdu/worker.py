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
    """Create gRPC channel for Zeebe (insecure + OAuth2 Bearer interceptor)."""
    import grpc

    gateway = os.environ.get("ZEEBE_ADDRESS", "zeebe:26500")
    client_id = os.environ.get("ZEEBE_CLIENT_ID", "")
    client_secret = os.environ.get("ZEEBE_CLIENT_SECRET", "")
    token_url = os.environ.get("ZEEBE_TOKEN_URL", "")
    use_oauth = bool(client_id and client_secret and token_url)

    keepalive_options = [
        ('grpc.keepalive_time_ms', 60_000),
        ('grpc.keepalive_timeout_ms', 20_000),
        ('grpc.keepalive_permit_without_calls', 1),
        ('grpc.http2.max_pings_without_data', 0),
        ('grpc.http2.min_time_between_pings_ms', 60_000),
        ('grpc.http2.min_ping_interval_without_data_ms', 60_000),
    ]

    if use_oauth:
        from worker.auth import TokenManager, _UnaryUnaryTokenInterceptor, _UnaryStreamTokenInterceptor

        token_manager = TokenManager(
            client_id=client_id,
            client_secret=client_secret,
            token_url=token_url,
            audience=os.environ.get("ZEEBE_TOKEN_AUDIENCE", ""),
        )
        token_manager.refresh_token()

        interceptors = [
            _UnaryUnaryTokenInterceptor(token_manager),
            _UnaryStreamTokenInterceptor(token_manager),
        ]
        channel = grpc.aio.insecure_channel(
            gateway, interceptors=interceptors, options=keepalive_options,
        )
        logger.info("Zeebe channel: insecure + OAuth2 Bearer → %s", gateway)
    else:
        channel = grpc.aio.insecure_channel(gateway, options=keepalive_options)
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
