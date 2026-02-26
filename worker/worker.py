"""Camunda Zeebe worker — main entry point.

Registers all task handlers and starts polling for jobs.
Supports both insecure and OAuth2-authenticated Zeebe connections.
Auto-reconnects on failure with token refresh.
"""

from __future__ import annotations

import asyncio
import logging
import signal

from pyzeebe import Job, ZeebeWorker
from pyzeebe.job.job import JobController

from .auth import ZeebeAuthConfig, create_channel, get_token_manager
from .config import AppConfig
from .github_client import GitHubClient
from .handlers import register_all_handlers
from .odoo_client import OdooClient
from .ssh import AsyncSSHClient
from .webhook import WebhookServer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


async def _exception_handler(exc: Exception, job: Job, job_controller: JobController) -> None:
    """Custom exception handler: last retry → BPMN Error instead of incident.

    When retries are exhausted (retries <= 1), throw a BPMN Error so that
    error event subprocesses can catch it and create an Odoo task.
    Otherwise, fail the job normally so Zeebe retries it.
    """
    if job.retries <= 1:
        error_code = type(exc).__name__
        error_msg = str(exc)[:500]
        logger.error(
            "Job %s [%s] exhausted retries — throwing BPMN Error: %s: %s",
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
        await job_controller.set_failure_status(message=f"Failed job. Error: {exc}")


async def create_worker(config: AppConfig) -> ZeebeWorker:
    """Create a ZeebeWorker with all handlers registered."""
    auth_config = ZeebeAuthConfig(
        gateway_address=config.zeebe.gateway_address,
        client_id=config.zeebe.client_id,
        client_secret=config.zeebe.client_secret,
        token_url=config.zeebe.token_url,
        audience=config.zeebe.audience,
        use_tls=config.zeebe.use_tls,
    )
    channel = create_channel(auth_config)
    worker = ZeebeWorker(channel, exception_handler=_exception_handler)

    # Shared clients
    ssh = AsyncSSHClient(key_path=config.ssh_key_path)
    github = GitHubClient(
        token=config.github.token,
        deploy_pat=config.github.deploy_pat,
    )
    odoo = OdooClient(config.odoo)

    register_all_handlers(worker, config=config, ssh=ssh, github=github, odoo=odoo)

    return worker


async def worker_loop(config: AppConfig, stop_event: asyncio.Event) -> None:
    """Zeebe worker loop with auto-restart on gRPC failures.

    When pyzeebe pollers crash (DEADLINE_EXCEEDED, channel closed),
    we recreate the worker with a fresh gRPC channel and resume polling.
    Zeebe will reassign timed-out jobs automatically.
    """
    restart_delay = 5  # seconds between restart attempts

    while not stop_event.is_set():
        try:
            worker = await create_worker(config)
            logger.info("Worker started. Listening for jobs...")

            worker_task = asyncio.create_task(worker.work())
            stop_task = asyncio.create_task(stop_event.wait())

            done, _ = await asyncio.wait(
                [worker_task, stop_task],
                return_when=asyncio.FIRST_COMPLETED,
            )

            if stop_task in done:
                worker_task.cancel()
                try:
                    await worker_task
                except asyncio.CancelledError:
                    pass
                break  # clean shutdown

            # worker.work() exited unexpectedly — restart
            try:
                worker_task.result()
            except Exception as exc:
                logger.error("Worker crashed: %s — restarting in %ds", exc, restart_delay)

        except Exception as exc:
            logger.error("Failed to create worker: %s — retrying in %ds", exc, restart_delay)

        await asyncio.sleep(restart_delay)

    logger.info("Worker loop stopped.")


async def main() -> None:
    config = AppConfig.from_env()

    logger.info("Connecting to Zeebe at %s", config.zeebe.gateway_address)

    # Graceful shutdown
    loop = asyncio.get_event_loop()
    stop_event = asyncio.Event()

    def _shutdown() -> None:
        logger.info("Shutdown signal received")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown)

    # Run worker + webhook server in parallel
    webhook = WebhookServer(config)
    await asyncio.gather(
        worker_loop(config, stop_event),
        webhook.start(),
    )

    logger.info("All services stopped.")


if __name__ == "__main__":
    asyncio.run(main())
