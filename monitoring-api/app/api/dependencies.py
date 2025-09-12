from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import db

async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    if db.pg_session_factory is None:
        raise RuntimeError("pg_session_factory is not initialized")
    async with db.pg_session_factory() as session:
        yield session
