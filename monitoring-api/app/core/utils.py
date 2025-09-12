from datetime import datetime, timedelta
from typing import Any, Dict, Optional

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


def create_access_token(
    user_data: Dict[str, Any],
    expiry: Optional[timedelta] = None,
    refresh: bool = False,
) -> str:
    """
    Create JWT token following the pattern you provided:
      - payload['user'] = user_data
      - payload['exp'] = now + (expiry or ACCESS_TOKEN_EXPIRY seconds)
      - payload['jti'] = uuid
      - payload['refresh'] = refresh flag
    If you want refresh tokens to have longer expiry, call with expiry=timedelta(days=...)
    """
    payload: Dict[str, Any] = {}
    payload["user"] = user_data
    payload["exp"] = datetime.now() + (expiry if expiry is not None else timedelta(seconds=Config.ACCESS_TOKEN_EXPIRY))
    payload["jti"] = str(uuid.uuid4())
    payload["refresh"] = bool(refresh)

    if Config.SECRET_KEY is None:
        raise ValueError("SECRET_KEY must not be None")
    token = jwt.encode(payload, Config.SECRET_KEY, algorithm=Config.JWT_ALGORITHM)
    return token    

def decode_token(token: str) -> Optional[Dict[str, Any]]:
    """
    Decode and validate JWT. Returns payload dict on success, None on failure.
    """
    try:
        token_data = jwt.decode(token, Config.SECRET_KEY, algorithms=[Config.JWT_ALGORITHM])
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

def create_url_safe_token(data:dict):
    
    token = serializer.dumps(data,salt="email-verification")

    return token

def decode_url_safe_token(token:str):
    try:
        token_data = serializer.loads(token)
        
        return token_data
    except Exception as e:
        logging.error(str(e))
