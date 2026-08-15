import time
import json
from datetime import datetime
from typing import Optional, Any

import httpx

from app.core.config import Config
from app.schemas.service import ApiServiceModal, ApiResponseModal
from app.utils.loggers import get_logger

logger = get_logger()

# ---------------------------------------------------------------------------
# Shared HTTP client
#
# One pooled client for the whole process instead of a new AsyncClient (and a full
# TCP+TLS handshake) per probe. Created lazily so that merely importing this module
# never opens sockets — the consumer process must stay free of them.
#
# keepalive_expiry is deliberately SHORTER than the 60s check interval: a pooled
# socket the server already closed raises on reuse and httpx does not transparently
# retry, which on a monitor means a FALSE unhealthy reading. 15s guarantees sockets
# are recycled well before typical 60-75s server idle timeouts, while still giving
# within-cycle reuse across endpoints sharing a host.
# ---------------------------------------------------------------------------
_client: Optional[httpx.AsyncClient] = None

_LIMITS = httpx.Limits(
    max_connections=100,          # must exceed HEALTH_CHECK_CONCURRENCY
    max_keepalive_connections=50,
    keepalive_expiry=15.0,
)
_TIMEOUT = httpx.Timeout(connect=5.0, read=10.0, write=5.0, pool=5.0)


async def init_http_client() -> httpx.AsyncClient:
    """Create the shared client. Idempotent; called from the FastAPI lifespan."""
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(timeout=_TIMEOUT, limits=_LIMITS)
        logger.info("Shared HTTP client initialised (pooled, keepalive 15s).")
    return _client


async def close_http_client() -> None:
    """Close the shared client. Called from the FastAPI lifespan."""
    global _client
    if _client is not None and not _client.is_closed:
        await _client.aclose()
        logger.info("Shared HTTP client closed.")
    _client = None


def get_http_client() -> httpx.AsyncClient:
    """
    Return the shared client, creating it on first use.

    The lazy fallback keeps check_api_health usable outside the FastAPI lifespan
    (scripts, tests, a REPL) instead of failing on a None client.
    """
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(timeout=_TIMEOUT, limits=_LIMITS)
    return _client


async def check_api_health(service: ApiServiceModal) -> ApiResponseModal:
    """
    Perform a single API health check for the given service.
    Handles various HTTP methods, parses JSON/text responses,
    measures latency, and returns a structured ApiResponseModal.
    Never raises — failures are reported in the returned model.
    """

    checked_at = datetime.utcnow()
    start_time = time.perf_counter()
    status_code: Optional[int] = None
    response_body: Optional[Any] = None
    error_message: Optional[str] = None
    method = (service.http_method or "GET").upper()
    headers = service.request_headers or {}
    body = service.request_body
    max_bytes = Config.MAX_RESPONSE_BODY_BYTES

    try:
        client = get_http_client()
        request_kwargs: dict[str, Any] = {"headers": headers}

        if body is not None:
            if isinstance(body, (dict, list)):
                request_kwargs["json"] = body
            else:
                request_kwargs["content"] = str(body)

        response = await client.request(method, str(service.url), **request_kwargs)
        status_code = response.status_code

        # Check the byte length FIRST so oversized payloads are never parsed or
        # re-serialised. The previous code json.dumps'd the whole body to measure
        # it (CPU-bound, on the event loop) and then stored the full object anyway.
        raw = response.content
        if len(raw) > max_bytes:
            response_body = {
                "_truncated": True,
                "size_bytes": len(raw),
                "preview": raw[:2000].decode("utf-8", "replace"),
            }
        else:
            try:
                response_body = response.json()
            except Exception:
                response_body = {"_raw_text": raw.decode("utf-8", "replace")}

    except httpx.PoolTimeout:
        # Monitor-side saturation, NOT an endpoint failure. Must be distinguished:
        # PoolTimeout subclasses TimeoutException, so without this branch three in a
        # row would fabricate an incident for a perfectly healthy service.
        error_message = "Monitor connection pool saturated (not an endpoint failure)"
        logger.error(f"Pool timeout while checking {service.name} — monitor is overloaded")

    except httpx.TimeoutException:
        error_message = "Request timed out"
        logger.warning(f"Timeout for {service.name} ({service.url})")

    except httpx.RequestError as e:
        error_message = f"Request failed: {e}"
        logger.error(f"Network error for {service.name}: {e}")

    except Exception as e:
        error_message = f"Unexpected error: {e}"
        logger.exception(f"Unexpected error while hitting {service.name}")

    finally:
        response_time_ms = int((time.perf_counter() - start_time) * 1000)

    return ApiResponseModal(
        checked_at=checked_at,
        response_time_ms=response_time_ms,
        status_code=status_code,
        response_body=response_body,
        error_message=error_message,
    )
