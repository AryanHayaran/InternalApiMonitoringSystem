from datetime import datetime, timedelta
from typing import Any, Dict, Optional
from fastapi import HTTPException, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import uuid
from passlib.context import CryptContext
from app.core.config import Config
from itsdangerous import URLSafeTimedSerializer
import jwt
from jwt import ExpiredSignatureError, InvalidTokenError as JWTError
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Password hashing context
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def _get_jwt_algorithm() -> str:
    # support either Config.ALGORITHM or Config.JWT_ALGORITHM for compatibility
    return getattr(Config, "ALGORITHM", None) or getattr(Config, "JWT_ALGORITHM", "HS256")


def create_access_token(
    user_data: Dict[str, Any],
    expiry: Optional[timedelta] = None,
    refresh: bool = False,
) -> str:
    """
    Create JWT token:
      - payload['user'] = user_data
      - payload['exp'] = now_utc + expiry (datetime in UTC)
      - payload['jti'] = uuid
      - payload['refresh'] = refresh flag
    """
    now_utc = datetime.utcnow()
    exp_time = now_utc + (expiry if expiry is not None else timedelta(seconds=Config.ACCESS_TOKEN_EXPIRY))
    payload: Dict[str, Any] = {
        "user": user_data,
        "exp": exp_time,
        "jti": str(uuid.uuid4()),
        "refresh": bool(refresh),
    }

    if not Config.SECRET_KEY:
        raise ValueError("SECRET_KEY must not be None")
    token = jwt.encode(payload, Config.SECRET_KEY, algorithm=_get_jwt_algorithm())
    return token


def decode_token(token: str) -> Optional[Dict[str, Any]]:
    """
    Decode and validate JWT. Returns payload dict on success, None on failure.
    """
    try:
        token_data = jwt.decode(token, Config.SECRET_KEY, algorithms=[_get_jwt_algorithm()])
        return token_data
    except ExpiredSignatureError:
        logger.exception("Token expired")
        return None
    except JWTError:
        logger.exception("Invalid token")
        return None


serializer = URLSafeTimedSerializer(
    secret_key=Config.SECRET_KEY,
    salt="email-verification",
)


def create_url_safe_token(data: dict):
    token = serializer.dumps(data, salt="email-verification")
    return token


def decode_url_safe_token(token: str):
    try:
        token_data = serializer.loads(token)
        return token_data
    except Exception as e:
        logging.error(str(e))


class TokenBearer(HTTPBearer):
    def __init__(self, auto_error=True):
        super().__init__(auto_error=auto_error)

    async def __call__(self, request: Request) -> Dict[str, Any] | None:
        creds: Optional[HTTPAuthorizationCredentials] = await super().__call__(request)
        if creds is None:
            # no Authorization header or auto_error=False
            raise HTTPException(status_code=401, detail="Authorization credentials not provided")

        token = creds.credentials
        token_data = decode_token(token)

        if not token_data:
            raise HTTPException(status_code=401, detail="Invalid or expired token")

        self.verify_token_data(token_data)
        return token_data

    def verify_token_data(self, token_data: dict):
        raise NotImplementedError("Override this in subclasses")


class AccessTokenBearer(TokenBearer):
    def verify_token_data(self, token_data: dict) -> None:
        if token_data.get("refresh"):
            raise HTTPException(status_code=401, detail="Access token required")


class RefreshTokenBearer(TokenBearer):
    def verify_token_data(self, token_data: dict) -> None:
        if not token_data.get("refresh"):
            raise HTTPException(status_code=401, detail="Refresh token required")
