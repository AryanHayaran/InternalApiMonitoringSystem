from  app.schemas.auth import UserCreate
from typing import Optional
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.models import Users
from app.core.security import get_password_hash

class UserServices:
    """Service class for handling user-related database operations."""

    # ----------------------------
    # User methods
    # ----------------------------
    async def user_exists(self, email: str, session: AsyncSession) -> bool:
        """Check if a user with the given email exists."""
        return await self.get_user_by_email(email, session) is not None

    async def get_user_by_email(self, email: str, session: AsyncSession) -> Optional[Users]:
        """Fetch a user by email."""
        result = await session.execute(select(Users).where(Users.email == email))
        return result.scalars().first()

    async def create_user(self, user_data: UserCreate, session: AsyncSession) -> Users:
        """Create a new user with hashed password."""
        user_dict = user_data.model_dump(exclude={"password"})
        db_user = Users(**user_dict, password=get_password_hash(user_data.password))
        session.add(db_user)
        try:
            await session.commit()
        except IntegrityError:
            await session.rollback()
            raise
        await session.refresh(db_user)
        return db_user

    # ----------------------------
    # Refresh Token methods
    # ----------------------------
    async def save_refresh_token(self, user_id: str, token: str, session: AsyncSession) -> None:
        """Store refresh token for the user."""
        await session.execute(
            update(Users)
            .where(Users.id == user_id)
            .values(refresh_token=token)
        )
        await session.commit()

    async def delete_user_refresh_tokens(self, user_id: str, session: AsyncSession) -> None:
        """Clear the refresh token during logout."""
        await session.execute(
            update(Users)
            .where(Users.id == user_id)
            .values(refresh_token=None)
        )
        await session.commit()

    async def get_refresh_token_for_user(self, user_id: str, session: AsyncSession) -> Optional[str]:
        """Retrieve the stored refresh token for a user."""
        result = await session.execute(
            select(Users.refresh_token).where(Users.id == user_id)
        )
        return result.scalar_one_or_none()
