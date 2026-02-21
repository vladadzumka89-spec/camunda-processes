"""Camunda Zeebe worker — main entry point.

Registers all task handlers and starts polling for jobs.
Supports both insecure and OAuth2-authenticated Zeebe connections.
Auto-reconnects on failure with token refresh.
"""

from __future__ import annotations

import asyncio
import logging
import signal

from pyzeebe import ZeebeWorker

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
    worker = ZeebeWorker(channel)

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
    """Auto-reconnecting Zeebe worker loop."""
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
                break

        except Exception as exc:
            logger.error("Worker error: %s", exc)
            token_mgr = get_token_manager()
            if token_mgr:
                logger.info("Refreshing OAuth2 token...")
                try:
                    token_mgr.refresh_token()
                except Exception as token_err:
                    logger.error("Token refresh failed: %s", token_err)
            logger.info("Restarting worker in 5 seconds...")
            await asyncio.sleep(5)

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
