"""
Redis plumbing — connection access and a fail-open call wrapper.

Redis is strictly OPTIONAL in this system. Every call goes through redis_call(),
which returns the caller's default on any failure (including Redis being absent).
A Redis outage must degrade features, never break requests.

No business logic lives here; see cache.py for the feature helpers.
"""
import asyncio
from typing import Any, Callable, Optional

from redis.exceptions import RedisError

from app.utils.connect import db
from app.utils.loggers import get_logger

logger = get_logger()

# Tracks whether we are currently in a degraded (Redis unavailable) state, so an
# outage logs once on transition instead of once per request.
_degraded = False


def get_redis():
    """Return the shared Redis client, or None when Redis is not configured."""
    return db.redis_client


def _mark_degraded(exc: Exception) -> None:
    global _degraded
    if not _degraded:
        _degraded = True
        logger.warning(f"Redis unavailable — degrading gracefully. Cause: {exc}")


def _mark_recovered() -> None:
    global _degraded
    if _degraded:
        _degraded = False
        logger.info("Redis is reachable again.")


async def redis_call(fn: Callable, *args, default: Any = None, **kwargs) -> Any:
    """
    Execute a Redis coroutine, returning `default` on any failure.

    Deliberately swallows errors: callers pass the fail-open value (e.g. "not rate
    limited", "not denylisted") so that losing Redis never locks users out.
    """
    client = get_redis()
    if client is None:
        return default

    try:
        result = await fn(*args, **kwargs)
        _mark_recovered()
        return result
    except (RedisError, asyncio.TimeoutError, OSError) as e:
        _mark_degraded(e)
        return default
    except Exception as e:  # never let a cache bug break a request
        logger.error(f"Unexpected Redis error: {e}", exc_info=True)
        return default


def is_degraded() -> bool:
    """True when the last Redis call failed. Used by the health endpoint."""
    return _degraded
