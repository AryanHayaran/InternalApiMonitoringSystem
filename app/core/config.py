from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional
class Settings(BaseSettings):
    REDIS_HOST: Optional[str] = None
    REDIS_PORT: int = 6379
    REDIS_USER: Optional[str] = None
    REDIS_PASSWORD: Optional[str] = None

    PGHOST: Optional[str] = None
    PGDATABASE: Optional[str] = None
    PGUSER: Optional[str] = None
    PGPASSWORD: Optional[str] = None
    PGPORT: Optional[int] = 5432

    KAFKA_BROKER: str = "localhost:9092"
    KAFKA_BROKER_URL: str = "localhost:9092"  # Alias for consistency
    KAFKA_USERNAME: str | None = None
    KAFKA_PASSWORD: str | None = None
    KAFKA_SECURITY_PROTOCOL: str = "PLAINTEXT"  # PLAINTEXT, SASL_PLAINTEXT, SASL_SSL, SSL
    KAFKA_TOPIC_NAME: str = "api-monitoring-results"
    KAFKA_MAX_RETRIES: int = 5
    KAFKA_RETRY_DELAY_S: int = 2
    KAFKA_REQUEST_TIMEOUT_MS: int = 30000
    KAFKA_ENABLE_IDEMPOTENCE: bool = True
    KAFKA_ACKS: str = "all"  # 0, 1, all
    KAFKA_COMPRESSION_TYPE: str = "gzip"  # none, gzip, snappy, lz4, zstd
    KAFKA_CONSUMER_GROUP: str = "monitoring_consumer_group"
    KAFKA_ROLLUP_GROUP: str = "metrics-rollup-group"
    KAFKA_AUTO_OFFSET_RESET: str = "latest"  # 'earliest' replays the whole backlog

    # Health-check loop
    HEALTH_CHECK_CONCURRENCY: int = 25
    HEALTH_CHECK_TIMEOUT_S: int = 20
    MAX_RESPONSE_BODY_BYTES: int = 8192

    # Login rate limiting (Redis-backed, fail-open)
    # 30/5min per IP still stops brute force (which needs thousands of attempts) and
    # caps bcrypt at ~2.5% of the event loop, while staying clear of the integration
    # suite, which performs ~11 logins from a single address.
    LOGIN_RATE_LIMIT_IP_MAX: int = 30
    LOGIN_RATE_LIMIT_IP_WINDOW_S: int = 300
    LOGIN_RATE_LIMIT_EMAIL_MAX: int = 5
    LOGIN_RATE_LIMIT_EMAIL_WINDOW_S: int = 900

    NOTIFY_EMAIL: str | None = None
    NOTIFY_WEBHOOK: str | None = None
    
    JWT_ALGORITHM: str = "HS256"
    # No default on purpose: a publicly-known fallback key silently signs real tokens
    # when .env is missing. Validated at startup by require_secret_key().
    SECRET_KEY: Optional[str] = None
    ACCESS_TOKEN_EXPIRY: int = 3600      # seconds
    REFRESH_TOKEN_EXPIRY: int = 86400    # SECONDS (not days — security.py passes this
                                         # straight to timedelta(seconds=...). The old
                                         # default of 1 meant refresh tokens died in 1s.)

    SMTP_SERVER: Optional[str] = None
    Port: Optional[int] = None
    Login: Optional[str] = None
    Password: Optional[str] = None

    BREVO_SMTP_SERVER: Optional[str] = None
    BREVO_SMTP_PORT: Optional[int] = None
    BREVO_SMTP_USERNAME: Optional[str] = None
    BREVO_SMTP_PASSWORD: Optional[str] = None
    SENDER_EMAIL: Optional[str] = None

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8",extra="ignore")

Config = Settings()

# Placeholder values that must never be used to sign real tokens.
_INSECURE_SECRETS = {"", "change-this-secret", "secret", "changeme"}


def require_secret_key() -> None:
    """
    Fail fast when SECRET_KEY is missing or a known placeholder.

    Called from the FastAPI lifespan rather than at import time so that tooling
    (alembic, the consumers) can import this module without a JWT secret.
    """
    if not Config.SECRET_KEY or Config.SECRET_KEY.strip().lower() in _INSECURE_SECRETS:
        raise RuntimeError(
            "SECRET_KEY is missing or set to a placeholder. Set a strong, unique "
            "SECRET_KEY in .env before starting the API."
        )