from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select,update
from sqlalchemy.exc import IntegrityError
from app.db.models import Users
from app.schemas.auth import UserCreate

class AuthRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def user_exists(self, email: str) -> bool:
        query = select(Users).where(Users.email == email)
        result = await self.session.execute(query)
        return result.scalar_one_or_none() is not None

    async def get_user_by_email(self, email: str) -> Optional[Users]:
        query = select(Users).where(Users.email == email)
        result = await self.session.execute(query)
        return result.scalar_one_or_none()

    async def create_user(self, user_data: UserCreate, hashed_password: str) -> Users:
        user = Users(
            full_name=user_data.full_name, 
            email=user_data.email, 
            password=hashed_password
        )
        self.session.add(user)
        try:
            await self.session.commit()
            await self.session.refresh(user)
            return user
        except IntegrityError:
            await self.session.rollback()
            raise ValueError(f"User with email '{user_data.email}' already exists.")
        except Exception as e:
            await self.session.rollback()
            raise RuntimeError(f"Failed to create user: {str(e)}")

    async def save_refresh_token(self, user_id: str, token: str) -> None:
        query = update(Users).where(Users.id == user_id).values(refresh_token=token)
        await self.session.execute(query)
        await self.session.commit() 

    async def delete_user_refresh_tokens(self, user_id: str) -> None:
        query = update(Users).where(Users.id == user_id).values(refresh_token=None)
        await self.session.execute(query)
        await self.session.commit() 

    async def get_refresh_token_for_user(self, user_id: str) -> Optional[str]:
        query = select(Users.refresh_token).where(Users.id == user_id)
        result = await self.session.execute(query)
        return result.scalar_one_or_none()  