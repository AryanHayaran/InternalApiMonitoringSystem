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

    NOTIFY_EMAIL: str | None = None
    NOTIFY_WEBHOOK: str | None = None
    
    JWT_ALGORITHM: str = "HS256"
    SECRET_KEY: str ="change-this-secret"
    ACCESS_TOKEN_EXPIRY: int = 3600  # in seconds
    REFRESH_TOKEN_EXPIRY: int = 1  # in days

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