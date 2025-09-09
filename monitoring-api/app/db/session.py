from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
import redis.asyncio as redis
from app.core.config import Config

class DB:
    def __init__(self):
        self.redis_client = None
        self.pg_engine = None
        self.pg_session_factory = None

    async def init_db(self):
        """
        Initialize database connections.
        """
        # Redis connection
        if Config.REDIS_HOST and Config.REDIS_PORT:
            redis_url = (
                f"redis://{Config.REDIS_USER or ''}:{Config.REDIS_PASSWORD or ''}@"
                f"{Config.REDIS_HOST}:{Config.REDIS_PORT}"
            )
            self.redis_client = redis.from_url(redis_url, encoding="utf-8", decode_responses=True)

        # Postgres connection
        if all([Config.PGHOST, Config.PGDATABASE, Config.PGUSER, Config.PGPASSWORD, Config.PGPORT]):
            db_url = (
                f"postgresql+asyncpg://{Config.PGUSER}:{Config.PGPASSWORD}"
                f"@{Config.PGHOST}:{Config.PGPORT}/{Config.PGDATABASE}"
            )
            self.pg_engine = create_async_engine(db_url, pool_pre_ping=True)

            # Use async_sessionmaker instead of sessionmaker for AsyncSession
            self.pg_session_factory = async_sessionmaker(
                self.pg_engine,
                expire_on_commit=False,
                autocommit=False,
                autoflush=False,
                class_=AsyncSession,
            )

    async def close_db(self):
        """
        Close database connections.
        """
        if self.redis_client:
            await self.redis_client.close()

        if self.pg_engine:
            await self.pg_engine.dispose()


db = DB()