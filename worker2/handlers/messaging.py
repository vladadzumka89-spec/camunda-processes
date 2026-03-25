"""Message publishing handler — publishes Zeebe messages from BPMN processes."""

import logging
from pyzeebe import Job, ZeebeWorker, ZeebeClient
from ..auth import ZeebeAuthConfig, create_channel
from ..config import AppConfig
from ..errors import ConfigError

logger = logging.getLogger(__name__)


def register_messaging_handlers(worker: ZeebeWorker, config: AppConfig) -> None:
    """Register message publishing task handlers."""

    def _create_client() -> ZeebeClient:
        auth_config = ZeebeAuthConfig(
            gateway_address=config.zeebe.gateway_address,
            client_id=config.zeebe.client_id,
            client_secret=config.zeebe.client_secret,
            token_url=config.zeebe.token_url,
            audience=config.zeebe.audience,
            use_tls=config.zeebe.use_tls,
        )
        return ZeebeClient(create_channel(auth_config))

    @worker.task(task_type="publish-message", timeout_ms=30_000)
    async def publish_message(
        job: Job,
        message_name: str = "",
        correlation_key: str = "",
        **kwargs,
    ) -> dict:
        """Publish a Zeebe message with process variables as payload."""
        if not message_name:
            raise ConfigError("message_name is required")
        if not correlation_key:
            raise ConfigError("correlation_key is required")

        client = _create_client()
        await client.publish_message(
            name=message_name,
            correlation_key=str(correlation_key),
            variables=dict(job.variables),
            time_to_live_in_milliseconds=3_600_000,
        )
        logger.info(
            "Published message %s (correlation=%s)",
            message_name, correlation_key,
        )
        return {"message_published": True}
