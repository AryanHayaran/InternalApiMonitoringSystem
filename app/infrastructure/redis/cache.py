"""
Redis-backed security state.

NOT a cache. Postgres remains the source of truth for everything the product shows.
This module holds only short-lived state that is (a) not reconstructable from
Postgres and (b) safe to lose entirely: a FLUSHALL here resets some rate-limit
windows and un-revokes some already-logged-out tokens for the remainder of their
short lifetime. No monitoring data is affected.

Every function fails OPEN — if Redis is down, rate limiting allows and the denylist
reports "not revoked". A monitoring dashboard going dark because a cache is
unavailable is worse than a logged-out token working for under an hour.
"""
from datetime import datetime, timezone
from typing import Optional

from app.core.config import Config
from app.infrastructure.redis.client import get_redis, redis_call
from app.utils.loggers import get_logger

logger = get_logger()

# ---------------------------------------------------------------------------
# Key schemes
# ---------------------------------------------------------------------------
JTI_DENYLIST_KEY = "auth:jti:denied:{jti}"
RATE_LIMIT_IP_KEY = "rl:login:ip:{ip}"
RATE_LIMIT_EMAIL_KEY = "rl:login:email:{email}"


# ---------------------------------------------------------------------------
# JWT jti denylist — makes logout actually end a session
# ---------------------------------------------------------------------------
async def deny_jti(jti: str, exp: Optional[int] = None) -> bool:
    """
    Revoke an access token by its jti until it would have expired anyway.

    TTL is the token's own remaining lifetime: past exp the JWT is rejected on
    signature/expiry alone, so a longer-lived entry is pure waste. Per-key TTL is
    precisely why Redis is the right tool here — no reaper job is needed.
    """
    client = get_redis()
    if client is None or not jti:
        return False

    if exp is not None:
        ttl = int(exp - datetime.now(timezone.utc).timestamp())
    else:
        ttl = Config.ACCESS_TOKEN_EXPIRY
    ttl = max(1, min(ttl, Config.ACCESS_TOKEN_EXPIRY))

    result = await redis_call(
        client.set, JTI_DENYLIST_KEY.format(jti=jti), "1", default=None, ex=ttl
    )
    return result is not None


async def is_jti_denied(jti: str) -> bool:
    """True when this token has been revoked. Fails OPEN (returns False)."""
    client = get_redis()
    if client is None or not jti:
        return False

    exists = await redis_call(
        client.exists, JTI_DENYLIST_KEY.format(jti=jti), default=0
    )
    return bool(exists)


# ---------------------------------------------------------------------------
# Login rate limiting
# ---------------------------------------------------------------------------
async def _incr_with_window(key: str, window_s: int) -> Optional[int]:
    """
    INCR + EXPIRE NX in one round trip.

    A fixed-window counter, not a sorted-set sliding window: a ZSET stores one member
    per attempt, so memory would grow with attack volume — backwards for a defence
    against brute force. EXPIRE ... NX keeps it a true fixed window rather than a
    lockout that extends on every attempt.
    """
    client = get_redis()
    if client is None:
        return None

    async def _run():
        pipe = client.pipeline()
        pipe.incr(key)
        pipe.expire(key, window_s, nx=True)
        results = await pipe.execute()
        return results[0]

    return await redis_call(_run, default=None)


async def check_login_rate_limit(ip: str, email: str) -> bool:
    """
    Return True when this login attempt should be BLOCKED.

    Called before the DB lookup and before bcrypt, so a blocked request costs
    essentially nothing — which is the point, since bcrypt is ~250ms of CPU.
    Fails OPEN: a Redis outage must not lock everyone out of login.
    """
    if get_redis() is None:
        return False

    ip_count = await _incr_with_window(
        RATE_LIMIT_IP_KEY.format(ip=ip or "unknown"),
        Config.LOGIN_RATE_LIMIT_IP_WINDOW_S,
    )
    if ip_count is not None and ip_count > Config.LOGIN_RATE_LIMIT_IP_MAX:
        logger.warning(f"Login rate limit hit for ip={ip}")
        return True

    email_count = await _incr_with_window(
        RATE_LIMIT_EMAIL_KEY.format(email=(email or "").lower()),
        Config.LOGIN_RATE_LIMIT_EMAIL_WINDOW_S,
    )
    if email_count is not None and email_count > Config.LOGIN_RATE_LIMIT_EMAIL_MAX:
        logger.warning("Login rate limit hit for an email address")
        return True

    return False


async def clear_login_rate_limit(email: str) -> None:
    """
    Clear the EMAIL counter after a successful login.

    The IP counter is deliberately left alone — otherwise one valid credential would
    reset an attacker's entire budget for that address.
    """
    client = get_redis()
    if client is None:
        return
    await redis_call(
        client.delete, RATE_LIMIT_EMAIL_KEY.format(email=(email or "").lower()), default=0
    )
