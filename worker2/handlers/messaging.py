"""Message publishing handler — publishes Zeebe messages from BPMN processes."""

import logging
from collections.abc import Mapping
from typing import Any

from pyzeebe import Job, ZeebeWorker
from ..auth import ZeebeAuthConfig, zeebe_client
from ..config import AppConfig
from ..errors import ConfigError

logger = logging.getLogger(__name__)


def register_messaging_handlers(worker: ZeebeWorker, config: AppConfig) -> None:
    """Register message publishing task handlers."""

    def _auth_config() -> ZeebeAuthConfig:
        auth_config = ZeebeAuthConfig(
            gateway_address=config.zeebe.gateway_address,
            client_id=config.zeebe.client_id,
            client_secret=config.zeebe.client_secret,
            token_url=config.zeebe.token_url,
            audience=config.zeebe.audience,
            use_tls=config.zeebe.use_tls,
        )
        return auth_config

    @worker.task(task_type="publish-message", timeout_ms=30_000, max_jobs_to_activate=4)
    async def publish_message(
        job: Job,
        message_name: str = "",
        correlation_key: str = "",
        ttl_ms: int = 300_000,
        message_variables: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict:
        """Publish a Zeebe message with full or explicitly scoped variables."""
        if not message_name:
            raise ConfigError("message_name is required")
        if not correlation_key:
            raise ConfigError("correlation_key is required")

        variables = (
            dict(message_variables)
            if isinstance(message_variables, Mapping)
            else dict(job.variables)
        )

        async with zeebe_client(_auth_config()) as client:
            await client.publish_message(
                name=message_name,
                correlation_key=str(correlation_key),
                variables=variables,
                time_to_live_in_milliseconds=ttl_ms,
            )
        logger.info(
            "Published message %s (correlation=%s)",
            message_name, correlation_key,
        )
        return {"message_published": True}
