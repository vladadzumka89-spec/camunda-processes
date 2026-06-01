"""Camunda Zeebe worker — main entry point.

Registers all task handlers and starts polling for jobs.
Supports both insecure and OAuth2-authenticated Zeebe connections.
Auto-reconnects on failure with token refresh.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import traceback

from pyzeebe import Job, ZeebeWorker
from pyzeebe.job.job import JobController
from pyzeebe.worker.job_poller import JobPoller

from .auth import ZeebeAuthConfig, close_channel, create_channel
from .config import AppConfig
from .github_client import GitHubClient
from .handlers import register_all_handlers
from .incident_janitor import (
    JANITOR_INTERVAL_SECONDS,
    cleanup_stale_incidents,
)
from .odoo_client import OdooClient
from .runtime_state import (
    job_health_status,
    mark_heartbeat,
    mark_job_finished,
    mark_job_started,
    mark_poll_attempt,
    mark_worker,
)
from .ssh import AsyncSSHClient
from .startup_guard import guard_stale_jobs
from .webhook import WebhookServer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
# asyncssh logs the full remote command at INFO, which can include authenticated
# Git URLs. Keep worker-level command logging explicit and redacted in ssh.py.
logging.getLogger("asyncssh").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# Track in-flight jobs so we can release them on shutdown
_active_jobs: dict[int, tuple[Job, JobController]] = {}
DEFAULT_REQUEST_TIMEOUT_MS = 60_000
DEFAULT_POLL_RETRY_DELAY_SECONDS = 15
DEFAULT_STALE_GUARD_INTERVAL_SECONDS = 60


def _patch_job_poller_health() -> None:
    """Record real ActivateJobs polling attempts for Docker healthcheck."""
    if getattr(JobPoller.poll_once, "_worker2_health_wrapped", False):
        return

    original_poll_once = JobPoller.poll_once

    async def poll_once_with_health(self: JobPoller) -> None:
        task_type = str(getattr(getattr(self, "task", None), "type", ""))
        mark_poll_attempt(task_type)
        await original_poll_once(self)

    setattr(poll_once_with_health, "_worker2_health_wrapped", True)
    JobPoller.poll_once = poll_once_with_health


async def _exception_handler(exc: Exception, job: Job, job_controller: JobController) -> None:
    """Custom exception handler: last retry → BPMN Error instead of incident.

    When retries are exhausted (retries <= 1), throw a BPMN Error so that
    error event subprocesses can catch it and create an Odoo task.
    Otherwise, fail the job normally so Zeebe retries it.
    """
    _active_jobs.pop(job.key, None)

    # Task Listener jobs are completed via REST API — skip gRPC error handling
    from .http_request_smart import TaskListenerCompleted
    if isinstance(exc, TaskListenerCompleted):
        logger.info("Job %s [%s] — Task Listener already completed via REST API", job.key, job.type)
        return
    if job.retries <= 1:
        from .errors import BpmnError
        error_code = exc.error_code if isinstance(exc, BpmnError) else "PROCESS_ERROR"
        error_msg = str(exc)[:500]
        error_tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        # Include traceback in the message itself — Zeebe propagates the message string
        # via zeebe:errorMessageVariable, but throwError.variables don't reach event subprocesses.
        message_with_tb = f"{error_msg}\n\n{error_tb[-1500:]}" if error_tb.strip() else error_msg
        logger.error(
            "Job %s [%s] exhausted retries — throwing BPMN Error: %s: %s",
            job.key, job.type, error_code, error_msg,
        )
        variables = dict(getattr(exc, "variables", None) or {})
        variables["caught_error_message"] = error_msg
        variables["error_traceback"] = error_tb
        if not variables.get("error_type"):
            from .ssh import SSHConnectionError
            variables["error_type"] = "infra" if isinstance(exc, SSHConnectionError) else "code"
        await job_controller.set_error_status(
            message=message_with_tb,
            error_code=error_code,
            variables=variables,
        )
    else:
        logger.warning(
            "Job %s [%s] failed (retries left: %d): %s",
            job.key, job.type, job.retries - 1, exc,
        )
        await job_controller.set_failure_status(
            message=f"Failed job. Error: {exc}",
            retry_back_off_ms=10_000,
        )


def _wrap_handler(original_handler):
    """Wrap a task handler to track active jobs and release them on cancel."""
    async def wrapper(job: Job, job_controller: JobController) -> None:
        _active_jobs[job.key] = (job, job_controller)
        mark_job_started(job)
        try:
            result = await original_handler(job, job_controller)
            mark_job_finished(job, job_health_status(job))
            return result
        except asyncio.CancelledError:
            # Worker is shutting down — release the job back to Zeebe
            logger.info(
                "Job %s [%s] interrupted by shutdown — releasing back to Zeebe",
                job.key, job.type,
            )
            if int(getattr(job, "retries", 0) or 0) > 1:
                try:
                    await job_controller.set_failure_status(
                        message="Worker shutdown — job released for retry",
                    )
                except Exception:
                    pass  # gRPC channel might already be closed
            else:
                logger.warning(
                    "Job %s [%s] has one retry left — leaving it for Zeebe timeout "
                    "instead of creating a shutdown incident",
                    job.key,
                    job.type,
                )
            mark_job_finished(job, "cancelled")
            raise
        except Exception:
            mark_job_finished(job, "failed")
            raise
        finally:
            _active_jobs.pop(job.key, None)
    return wrapper


async def create_worker(config: AppConfig) -> tuple[ZeebeWorker, object]:
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
    request_timeout_ms = int(os.getenv("ZEEBE_WORKER_REQUEST_TIMEOUT_MS", str(DEFAULT_REQUEST_TIMEOUT_MS)))
    poll_retry_delay = int(os.getenv("ZEEBE_WORKER_POLL_RETRY_DELAY_SECONDS", str(DEFAULT_POLL_RETRY_DELAY_SECONDS)))
    worker = ZeebeWorker(
        channel,
        request_timeout=request_timeout_ms,
        poll_retry_delay=poll_retry_delay,
        exception_handler=_exception_handler,
    )

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

    return worker, channel


async def _release_active_jobs() -> None:
    """On shutdown, fail all in-flight jobs so Zeebe releases them immediately."""
    if not _active_jobs:
        return
    logger.info("Releasing %d in-flight job(s)...", len(_active_jobs))
    for key, (job, controller) in list(_active_jobs.items()):
        if int(getattr(job, "retries", 0) or 0) <= 1:
            logger.warning(
                "Job %s [%s] has one retry left — leaving it for Zeebe timeout "
                "instead of creating a shutdown incident",
                key,
                job.type,
            )
            continue
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


async def _health_heartbeat_loop(stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        mark_heartbeat()
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=15)
        except asyncio.TimeoutError:
            continue


async def _stale_job_guard_loop(task_types: set[str], stop_event: asyncio.Event) -> None:
    interval = int(
        os.getenv(
            "WORKER_STALE_JOB_GUARD_INTERVAL_SECONDS",
            str(DEFAULT_STALE_GUARD_INTERVAL_SECONDS),
        )
    )
    if interval <= 0:
        logger.info("Runtime stale-job guard disabled")
        return

    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
            break
        except asyncio.TimeoutError:
            pass

        try:
            await guard_stale_jobs(
                task_types,
                active_job_keys=list(_active_jobs.keys()),
                context="Runtime",
            )
        except Exception as exc:
            logger.error("Runtime stale-job guard failed: %s", exc, exc_info=True)


async def worker_loop(config: AppConfig, stop_event: asyncio.Event) -> None:
    """Zeebe worker loop with auto-restart on gRPC failures.

    When pyzeebe pollers crash (DEADLINE_EXCEEDED, channel closed),
    we recreate the worker with a fresh gRPC channel and resume polling.
    Zeebe will reassign timed-out jobs automatically.
    """
    _patch_job_poller_health()
    restart_delay = 5  # seconds between restart attempts

    # Clean up orphan clickbot containers from previous run
    await _cleanup_orphan_clickbot(config)

    while not stop_event.is_set():
        channel = None
        polling_stop = asyncio.Event()
        heartbeat_task: asyncio.Task | None = None
        stale_guard_task: asyncio.Task | None = None
        try:
            worker, channel = await create_worker(config)
            task_types = {task.type for task in worker.tasks}
            await guard_stale_jobs(task_types, context="Startup")
            logger.info("Worker started. Listening for jobs...")
            mark_worker("running", task_types=sorted(task_types))

            worker_task = asyncio.create_task(worker.work())
            stop_task = asyncio.create_task(stop_event.wait())
            heartbeat_task = asyncio.create_task(_health_heartbeat_loop(polling_stop))
            stale_guard_task = asyncio.create_task(_stale_job_guard_loop(task_types, polling_stop))

            done, _ = await asyncio.wait(
                [worker_task, stop_task],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for pending in {worker_task, stop_task} - done:
                pending.cancel()

            if stop_task in done:
                logger.info("Shutdown signal — stopping worker...")
                worker_task.cancel()
                try:
                    await worker_task
                except asyncio.CancelledError:
                    pass
                # Release any jobs that weren't caught by the wrapper
                await _release_active_jobs()
                mark_worker("stopped")
                break  # clean shutdown

            # worker.work() exited unexpectedly — restart
            try:
                worker_task.result()
            except Exception as exc:
                logger.error("Worker crashed: %s — restarting in %ds", exc, restart_delay, exc_info=True)
                mark_worker("restarting", last_error=str(exc))

        except Exception as exc:
            logger.error("Failed to create worker: %s — retrying in %ds", exc, restart_delay)
            mark_worker("restarting", last_error=str(exc))
        finally:
            polling_stop.set()
            if heartbeat_task:
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass
            if stale_guard_task:
                stale_guard_task.cancel()
                try:
                    await stale_guard_task
                except asyncio.CancelledError:
                    pass
            await close_channel(channel)

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


def _validate_config(config: AppConfig) -> None:
    """Warn about missing optional credentials at startup."""
    if not config.github.token:
        logger.warning("GITHUB_TOKEN is empty — PR review/merge operations will fail")
    if not config.github.deploy_pat:
        logger.warning("DEPLOY_PAT is empty — upstream sync clone will fail")
    if not config.odoo.webhook_url:
        logger.warning("ODOO_WEBHOOK_URL is empty — Odoo task creation will need URL from BPMN")
    if not config.zeebe.token_url:
        logger.warning("ZEEBE_TOKEN_URL is empty — OAuth2 authentication disabled")


async def main() -> None:
    config = AppConfig.from_env()
    _validate_config(config)
    _patch_job_poller_health()

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
