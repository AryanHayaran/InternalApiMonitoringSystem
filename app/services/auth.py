from sqlmodel import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
from app.db.models import Users
from  app.schemas.auth import UserCreate
from app.core.security import get_password_hash
from typing import Dict, Any
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Dict, Any, Optional
from app.db.models import Users

from app.core.security import get_password_hash  # or your existing hash function

from typing import Optional, Dict, Any
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Users
from app.core.security import get_password_hash

class ApiServices:
    # ----------------------------
    # User methods
    # ----------------------------
    async def user_exists(self, email: str, session: AsyncSession) -> bool:
        user = await self.get_user_by_email(email, session)
        return user is not None

    async def get_user_by_email(self, email: str, session: AsyncSession) -> Optional[Users]:
        result = await session.execute(select(Users).where(Users.email == email))
        return result.scalars().first()

    async def create_user(self, user_data: UserCreate, session: AsyncSession) -> Users:
        user_dict = user_data.model_dump(exclude={'password'})
        db_user = Users(**user_dict, password=get_password_hash(user_data.password))
        session.add(db_user)
        try:
            await session.commit()
        except IntegrityError:
            await session.rollback()
            raise
        await session.refresh(db_user)
        return db_user  # return ORM object

    # ----------------------------
    # Refresh Token methods (updated)
    # ----------------------------
    async def save_refresh_token(self, user_id: str, token: str, session: AsyncSession):
        """Store refresh token directly in users table"""
        await session.execute(
            update(Users)
            .where(Users.id == user_id)
            .values(refresh_token=token)
        )
        await session.commit()

    async def delete_user_refresh_tokens(self, user_id: str, session: AsyncSession):
        """Clear refresh token during logout"""
        await session.execute(
            update(Users)
            .where(Users.id == user_id)
            .values(refresh_token=None)
        )
        await session.commit()

    async def get_refresh_token_for_user(self, user_id: str, session: AsyncSession) -> Optional[str]:
        """Fetch stored refresh token"""
        result = await session.execute(
            select(Users.refresh_token).where(Users.id == user_id)
        )
        return result.scalar_one_or_none()