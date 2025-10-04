from sqlmodel import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
from app.db.models import Users
from  app.schemas.auth import UserCreate
from app.core.security import get_password_hash
from typing import Dict, Any


class ApiServices:
    async def user_exists(self, email: str, session: AsyncSession) -> bool:
        user = await self.get_user_by_email(email, session)
        return user is not None
    
    async def get_user_by_email(self, email: str, session: AsyncSession) -> Users | None:
        result = await session.execute(select(Users).where(Users.email == email))
        user = result.scalars().first()
        return user

    async def create_user(self, user_data: UserCreate, session: AsyncSession) -> Dict[str, Any]:
        user_dict = user_data.model_dump(exclude={'password'})
        db_user = Users(
            **user_dict, password=get_password_hash(user_data.password))
        session.add(db_user)
        try:
            await session.commit()
        except IntegrityError:
            await session.rollback()
            raise
        await session.refresh(db_user)
        return {"id": db_user.id, "email": db_user.email,"full_name": db_user.full_name}
    

