import secrets
from typing import Optional, Dict, Any
from starlette.concurrency import run_in_threadpool
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from app.schemas.auth import UserCreate
from app.core.security import get_password_hash
import jwt
from app.core.config import Config
from app.core.security import _get_jwt_algorithm, decode_token, create_access_token
from datetime import datetime
from app.repositories.auth_repository import AuthRepository
from app.utils.loggers import get_logger

logger = get_logger()

class UserServices:
 
    async def user_exists(self, email: str, session: AsyncSession) -> bool:
        """Checks if user with this email already exists"""
        user_repository = AuthRepository(session)
        return await user_repository.user_exists(email)

    async def get_user_by_email(self, email: str, session: AsyncSession):
        """Gets user by email"""
        user_repository = AuthRepository(session)
        user = await user_repository.get_user_by_email(email)
        return user.model_dump() if user else None

    async def create_user(self, user_data: UserCreate, session: AsyncSession) -> Optional[Dict[str, Any]]:
        """Create a new user with hashed password and return its details."""
        # bcrypt is CPU-bound (~250ms); keep it off the event loop that APScheduler
        # shares with the health-check job.
        hashed_password = await run_in_threadpool(get_password_hash, user_data.password)
        auth_repository = AuthRepository(session)
        
        user = await auth_repository.create_user(user_data, hashed_password)
        
        return {
            "id": str(user.id),
            "full_name": user.full_name,
            "email": user.email,
            "created_at": user.created_at
        }
    async def save_refresh_token(self, user_id: str, token: str, session: AsyncSession) -> None:
        """Store refresh token for the user."""
        user_repository = AuthRepository(session)
        await user_repository.save_refresh_token(user_id, token)  


    async def delete_user_refresh_tokens(self, user_id: str, session: AsyncSession) -> None:
        """Clear the refresh token during logout."""
        user_repository = AuthRepository(session)
        await  user_repository.delete_user_refresh_tokens(user_id)

    async def get_refresh_token_for_user(self, user_id: str, session: AsyncSession) -> Optional[str]:
        """Retrieve the stored refresh token for a user."""
        user_repository = AuthRepository(session)
        return await user_repository.get_refresh_token_for_user(user_id)

    async def validate_refresh_token(self, refresh_token: str, session: AsyncSession) -> Optional[str]:
        """
        Validate a refresh token and return a new access token.

        Three checks that were previously missing, each individually exploitable:
          1. The PRESENTED token must equal the one stored for that user. Previously
             only the user_uid was read from it and the DB's copy was validated, so
             any validly-signed token for that user — including a plain access
             token — minted a fresh access token.
          2. Expiry is verified (was explicitly disabled).
          3. The `refresh` claim must be true, so an access token cannot be used here.
        """
        # Accept both "Bearer <token>" and a bare token.
        raw = (refresh_token or "").strip()
        if raw.lower().startswith("bearer "):
            raw = raw.split(" ", 1)[1].strip()
        if not raw:
            return None

        try:
            decoded = jwt.decode(
                raw,
                Config.SECRET_KEY,
                algorithms=[_get_jwt_algorithm()],
                options={"verify_exp": True},
            )
        except Exception:
            return None

        # (3) must actually be a refresh token
        if not decoded.get("refresh"):
            return None

        try:
            user_id = decoded["user"]["user_uid"]
        except (KeyError, TypeError):
            return None

        db_refresh_token = await self.get_refresh_token_for_user(user_id, session)
        if not db_refresh_token:
            return None

        # (1) the presented token must be the one we issued and still hold
        if not secrets.compare_digest(raw, db_refresh_token.strip()):
            logger.warning("Refresh token mismatch — presented token is not the stored one")
            return None

        # (2) the stored copy must still be valid too
        refresh_data = decode_token(db_refresh_token)
        if not refresh_data:
            await self.delete_user_refresh_tokens(user_id, session)
            return None

        # Generate new access token
        new_access_token = create_access_token(user_data=refresh_data["user"])
        return new_access_token
