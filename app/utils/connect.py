from urllib.parse import quote

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
import redis.asyncio as redis
from app.core.config import Config
from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncSession
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
                f"redis://{quote(Config.REDIS_USER or '', safe='')}:"
                f"{quote(Config.REDIS_PASSWORD or '', safe='')}@"
                f"{Config.REDIS_HOST}:{Config.REDIS_PORT}"
            )
            # Timeouts are what make Redis genuinely OPTIONAL. They default to None
            # (infinite), so a hung Redis — as opposed to a refused one — would block
            # every awaiting request forever.
            self.redis_client = redis.from_url(
                redis_url,
                encoding="utf-8",
                decode_responses=True,
                socket_timeout=0.25,
                socket_connect_timeout=0.25,
                retry_on_timeout=False,
                health_check_interval=30,
            )

        # Postgres connection
        if all([Config.PGHOST, Config.PGDATABASE, Config.PGUSER, Config.PGPASSWORD, Config.PGPORT]):
            db_url = (
                f"postgresql+asyncpg://{Config.PGUSER}:{Config.PGPASSWORD}"
                f"@{Config.PGHOST}:{Config.PGPORT}/{Config.PGDATABASE}"
            )
            # Pool must comfortably exceed HEALTH_CHECK_CONCURRENCY — the concurrent
            # health-check loop opens a session per endpoint for its log insert, and
            # the SQLAlchemy default (5 + 10 overflow) would raise QueuePool errors
            # that surface as scattered per-endpoint failures.
            self.pg_engine = create_async_engine(
                db_url,
                pool_pre_ping=True,
                pool_size=20,
                max_overflow=10,
                pool_timeout=30,
            )

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
            # close() is the deprecated alias in redis-py 5.x
            await self.redis_client.aclose()

        if self.pg_engine:
            await self.pg_engine.dispose()
    async def get_db_session(self) -> AsyncGenerator[AsyncSession, None]:
        """
        Dependency that yields an AsyncSession instance.
        Usage in FastAPI: Depends(db.get_db_session)
        """
        if self.pg_session_factory is None:
            raise RuntimeError("pg_session_factory is not initialized")
        async with self.pg_session_factory() as session:
            yield session

db = DB()



