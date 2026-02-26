"""Zeebe authentication — supports both insecure and OAuth2 connections."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import grpc

logger = logging.getLogger(__name__)

# Global token manager reference for refresh on reconnect
_token_manager = None


@dataclass
class ZeebeAuthConfig:
    """Authentication configuration for Zeebe."""

    gateway_address: str = 'zeebe:26500'
    client_id: str = ''
    client_secret: str = ''
    token_url: str = ''
    audience: str = ''
    use_tls: bool = False

    @property
    def use_oauth(self) -> bool:
        return bool(self.client_id and self.client_secret and self.token_url)


class TokenManager:
    """Manages OAuth2 tokens for Zeebe connection."""

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        token_url: str,
        audience: str = '',
    ) -> None:
        self._client_id = client_id
        self._client_secret = client_secret
        self._token_url = token_url
        self._audience = audience
        self._token: str | None = None

    def refresh_token(self) -> str:
        """Fetch a new OAuth2 token."""
        import httpx

        data = {
            'grant_type': 'client_credentials',
            'client_id': self._client_id,
            'client_secret': self._client_secret,
        }
        if self._audience:
            data['audience'] = self._audience

        resp = httpx.post(self._token_url, data=data, timeout=30.0)
        resp.raise_for_status()
        self._token = resp.json()['access_token']
        logger.info('OAuth2 token refreshed successfully')
        return self._token

    @property
    def token(self) -> str:
        if not self._token:
            return self.refresh_token()
        return self._token


class _BearerTokenInterceptor(grpc.aio.UnaryUnaryClientInterceptor):
    """gRPC interceptor that injects a Bearer token into insecure channels."""

    def __init__(self, token_manager: TokenManager) -> None:
        self._token_manager = token_manager

    async def intercept_unary_unary(self, continuation, client_call_details, request):
        metadata = list(client_call_details.metadata or [])
        metadata.append(('authorization', f'Bearer {self._token_manager.token}'))
        new_details = grpc.aio.ClientCallDetails(
            client_call_details.method,
            client_call_details.timeout,
            metadata,
            client_call_details.credentials,
            client_call_details.wait_for_ready,
        )
        return await continuation(new_details, request)


def _keepalive_options() -> list[tuple]:
    """gRPC keepalive options to prevent channel closure during long tasks.

    Zeebe server has minKeepAliveInterval=10s, but too-frequent pings
    can trigger GOAWAY "too_many_pings" under load. Use 60s intervals
    as a safe balance between liveness and server tolerance.
    """
    return [
        ('grpc.keepalive_time_ms', 60_000),
        ('grpc.keepalive_timeout_ms', 20_000),
        ('grpc.keepalive_permit_without_calls', 1),
        ('grpc.http2.max_pings_without_data', 0),
        ('grpc.http2.min_time_between_pings_ms', 60_000),
        ('grpc.http2.min_ping_interval_without_data_ms', 60_000),
    ]


def create_channel(config: ZeebeAuthConfig) -> grpc.aio.Channel:
    """Create a gRPC channel for Zeebe — insecure or OAuth2-authenticated."""
    global _token_manager

    options = _keepalive_options()

    if not config.use_oauth:
        logger.info('Using insecure Zeebe channel to %s', config.gateway_address)
        return grpc.aio.insecure_channel(config.gateway_address, options=options)

    # OAuth2 — initialise token manager
    _token_manager = TokenManager(
        client_id=config.client_id,
        client_secret=config.client_secret,
        token_url=config.token_url,
        audience=config.audience,
    )
    _token_manager.refresh_token()

    if not config.use_tls:
        # Insecure channel + Bearer token interceptor (Docker internal network)
        logger.info(
            'Using insecure OAuth2 Zeebe channel to %s', config.gateway_address,
        )
        interceptor = _BearerTokenInterceptor(_token_manager)
        return grpc.aio.insecure_channel(
            config.gateway_address, interceptors=[interceptor], options=options,
        )

    # TLS channel with composite credentials (external / cloud)
    call_credentials = grpc.access_token_call_credentials(_token_manager.token)
    channel_credentials = grpc.ssl_channel_credentials()
    composite = grpc.composite_channel_credentials(channel_credentials, call_credentials)

    logger.info('Using TLS OAuth2 Zeebe channel to %s', config.gateway_address)
    return grpc.aio.secure_channel(config.gateway_address, composite, options=options)


def get_token_manager() -> TokenManager | None:
    """Return the global token manager (for refresh on reconnect)."""
    return _token_manager
