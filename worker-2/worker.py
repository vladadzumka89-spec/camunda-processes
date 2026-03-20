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
from .incident_janitor import (
    JANITOR_INTERVAL_SECONDS,
    cleanup_stale_incidents,
)
from .webhook import WebhookServer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Track in-flight jobs so we can release them on shutdown
_active_jobs: dict[int, tuple[Job, JobController]] = {}


async def _exception_handler(exc: Exception, job: Job, job_controller: JobController) -> None:
    """Custom exception handler: last retry → BPMN Error instead of incident.

    When retries are exhausted (retries <= 1), throw a BPMN Error so that
    error event subprocesses can catch it and create an Odoo task.
    Otherwise, fail the job normally so Zeebe retries it.
    """
    _active_jobs.pop(job.key, None)

    # Task Listener jobs are completed via REST API — skip gRPC error handling
    from worker.http_request_smart import TaskListenerCompleted
    if isinstance(exc, TaskListenerCompleted):
        logger.info("Job %s [%s] — Task Listener already completed via REST API", job.key, job.type)
        return
    if job.retries <= 1:
        error_code = type(exc).__name__
        error_msg = str(exc)[:500]
        logger.error(
            "Job %s [%s] exhausted retries — throwing BPMN Error: %s: %s",
            job.key, job.type, error_code, error_msg,
        )
        variables = getattr(exc, "variables", None)
        await job_controller.set_error_status(
            message=error_msg,
            error_code=error_code,
            variables=variables,
        )
    else:
        logger.warning(
            "Job %s [%s] failed (retries left: %d): %s",
            job.key, job.type, job.retries - 1, exc,
        )
        await job_controller.set_failure_status(message=f"Failed job. Error: {exc}")


def _wrap_handler(original_handler):
    """Wrap a task handler to track active jobs and release them on cancel."""
    async def wrapper(job: Job, job_controller: JobController) -> None:
        _active_jobs[job.key] = (job, job_controller)
        try:
            result = await original_handler(job, job_controller)
            return result
        except asyncio.CancelledError:
            # Worker is shutting down — release the job back to Zeebe
            logger.info(
                "Job %s [%s] interrupted by shutdown — releasing back to Zeebe",
                job.key, job.type,
            )
            try:
                await job_controller.set_failure_status(
                    message="Worker shutdown — job released for retry",
                )
            except Exception:
                pass  # gRPC channel might already be closed
            raise
        finally:
            _active_jobs.pop(job.key, None)
    return wrapper


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
    odoo = OdooClient(config=config.odoo)
    register_all_handlers(worker, config=config, ssh=ssh, github=github, odoo=odoo)

    # Wrap all registered handlers to track active jobs
    for task in worker.tasks:
        task.job_handler = _wrap_handler(task.job_handler)

    return worker


async def _release_active_jobs() -> None:
    """On shutdown, fail all in-flight jobs so Zeebe releases them immediately."""
    if not _active_jobs:
        return
    logger.info("Releasing %d in-flight job(s)...", len(_active_jobs))
    for key, (job, controller) in list(_active_jobs.items()):
        try:
            await controller.set_failure_status(
                message="Worker shutdown — job released for retry",
            )
            logger.info("Released job %s [%s]", key, job.type)
        except Exception as exc:
            logger.warning("Could not release job %s: %s", key, exc)
    _active_jobs.clear()


async def _cleanup_orphan_clickbot(config: AppConfig) -> None:
    """Kill orphan clickbot containers on all servers at startup.

    When worker restarts, SSH sessions are lost but clickbot Docker
    containers may still be running on remote servers. Clean them up
    so the next deploy doesn't conflict.
    """
    ssh = AsyncSSHClient(key_path=config.ssh_key_path)
    for name, server in config.servers.items():
        try:
            result = await ssh.run(
                server,
                f"cd {server.repo_dir} && docker compose -f docker-compose.clickbot.yml down -v 2>/dev/null || true",
                timeout=30,
            )
            logger.info("Clickbot cleanup on %s (%s): exit %d", name, server.host, result.exit_code)
        except Exception as exc:
            logger.warning("Clickbot cleanup failed on %s: %s", name, exc)


async def worker_loop(config: AppConfig, stop_event: asyncio.Event) -> None:
    """Zeebe worker loop with auto-restart on gRPC failures.

    When pyzeebe pollers crash (DEADLINE_EXCEEDED, channel closed),
    we recreate the worker with a fresh gRPC channel and resume polling.
    Zeebe will reassign timed-out jobs automatically.
    """
    restart_delay = 5  # seconds between restart attempts

    # Clean up orphan clickbot containers from previous run
    await _cleanup_orphan_clickbot(config)

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
                logger.info("Shutdown signal — stopping worker...")
                worker_task.cancel()
                try:
                    await worker_task
                except asyncio.CancelledError:
                    pass
                # Release any jobs that weren't caught by the wrapper
                await _release_active_jobs()
                break  # clean shutdown

            # worker.work() exited unexpectedly — restart
            try:
                worker_task.result()
            except Exception as exc:
                logger.error("Worker crashed: %s — restarting in %ds", exc, restart_delay, exc_info=True)

        except Exception as exc:
            logger.error("Failed to create worker: %s — retrying in %ds", exc, restart_delay)

        await asyncio.sleep(restart_delay)

    logger.info("Worker loop stopped.")


async def _incident_janitor_loop(config: AppConfig, stop_event: asyncio.Event) -> None:
    """Periodically clean up stale incidents (every hour)."""
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=JANITOR_INTERVAL_SECONDS)
            break  # stop_event was set
        except asyncio.TimeoutError:
            pass  # interval elapsed — run cleanup

        try:
            await cleanup_stale_incidents(config)
        except Exception as exc:
            logger.error("Incident janitor loop error: %s", exc)


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

    # Run worker + webhook server + incident janitor in parallel
    webhook = WebhookServer(config)
    await asyncio.gather(
        worker_loop(config, stop_event),
        webhook.start(),
        _incident_janitor_loop(config, stop_event),
    )

    logger.info("All services stopped.")


if __name__ == "__main__":
    asyncio.run(main())
