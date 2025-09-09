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
    KAFKA_USERNAME: str | None = None
    KAFKA_PASSWORD: str | None = None

    NOTIFY_EMAIL: str | None = None
    NOTIFY_WEBHOOK: str | None = None

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8",extra="ignore")

Config = Settings()