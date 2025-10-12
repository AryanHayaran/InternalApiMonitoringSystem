from typing import Optional, Dict, Any
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from app.schemas.auth import UserCreate
from app.core.security import get_password_hash


class UserServices:

    async def user_exists(self, email: str, session: AsyncSession) -> bool:
        """Check if a user with the given email exists."""
        query = text("""
            SELECT 1 FROM users WHERE email = :email LIMIT 1;
        """)
        result = await session.execute(query, {"email": email})
        return result.scalar_one_or_none() is not None

    async def get_user_by_email(self, email: str, session: AsyncSession):
        """Fetch a user by email."""
        query = text("""
            SELECT *
            FROM users
            WHERE email = :email;
        """)
        result = await session.execute(query, {"email": email})
        row = result.mappings().first()
        return dict(row) if row else None

    async def create_user(self, user_data: UserCreate, session: AsyncSession) -> Optional[Dict[str, Any]]:
        """Create a new user with hashed password and return its details."""
        hashed_password = get_password_hash(user_data.password)

        query = text("""
            INSERT INTO users (full_name, email, password)
            VALUES (:full_name, :email, :password)
            RETURNING id, full_name, email, created_at;
        """)

        try:
            result = await session.execute(query, {
                "full_name": user_data.full_name,
                "email": user_data.email,
                "password": hashed_password
            })
            await session.commit()

            user_row = result.mappings().first()
            return dict(user_row) if user_row else None

        except IntegrityError as e:
            await session.rollback()
            raise ValueError(
                f"User with email '{user_data.email}' already exists.") from e
        except Exception as e:
            await session.rollback()
            raise RuntimeError(f"Failed to create user: {str(e)}") from e

    # ----------------------------
    async def save_refresh_token(self, user_id: str, token: str, session: AsyncSession) -> None:
        """Store refresh token for the user."""
        query = text("""
            UPDATE users
            SET refresh_token = :token, last_login_at = NOW()
            WHERE id = :user_id;
        """)
        await session.execute(query, {"token": token, "user_id": user_id})
        await session.commit()

    async def delete_user_refresh_tokens(self, user_id: str, session: AsyncSession) -> None:
        """Clear the refresh token during logout."""
        query = text("""
            UPDATE users
            SET refresh_token = NULL, last_login_at = NOW()
            WHERE id = :user_id;
        """)
        await session.execute(query, {"user_id": user_id})
        await session.commit()

    async def get_refresh_token_for_user(self, user_id: str, session: AsyncSession) -> Optional[str]:
        """Retrieve the stored refresh token for a user."""
        query = text("""
            SELECT refresh_token FROM users WHERE id = :user_id;
        """)
        result = await session.execute(query, {"user_id": user_id})
        return result.scalar_one_or_none()
