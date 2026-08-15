from datetime import datetime, timedelta
from typing import Any, Dict, Optional
from fastapi import HTTPException, Request, status
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


# -----------------------------
# ✅ Create JWT Tokens
# -----------------------------
def create_access_token(
    user_data: Dict[str, Any],
    expiry: Optional[timedelta] = None,
    refresh: bool = False,
) -> str:
    """
    Create JWT token:
      - payload['user'] = user_data
      - payload['exp'] = now_utc + expiry
      - payload['jti'] = uuid
      - payload['refresh'] = True for refresh tokens
    """
    now_utc = datetime.utcnow()

    # choose expiry based on type
    exp_time = now_utc + (expiry if expiry else timedelta(seconds=(
        Config.REFRESH_TOKEN_EXPIRY if refresh else Config.ACCESS_TOKEN_EXPIRY
    )))

    payload: Dict[str, Any] = {
        "user": user_data,
        "exp": exp_time,
        "jti": str(uuid.uuid4()),
        "refresh": bool(refresh),
        "iat": now_utc.timestamp(),
    }

    if not Config.SECRET_KEY:
        raise ValueError("SECRET_KEY must not be None")

    token = jwt.encode(payload, Config.SECRET_KEY, algorithm=_get_jwt_algorithm())
    return token


# -----------------------------
# ✅ Decode & Validate JWT
# -----------------------------
def decode_token(token: str, verify_exp: bool = True) -> Optional[Dict[str, Any]]:
    """
    Decode and validate JWT. Returns payload dict on success, None on failure.
    """
    try:
        token_data = jwt.decode(
            token,
            Config.SECRET_KEY,
            algorithms=[_get_jwt_algorithm()],
            options={"verify_exp": verify_exp},
        )
        return token_data
    except ExpiredSignatureError:
        logger.warning("Access token expired")
        return None
    except JWTError:
        logger.warning("Invalid token")
        return None


# -----------------------------
# ✅ URL Safe Tokens (optional)
# -----------------------------
serializer = URLSafeTimedSerializer(secret_key=Config.SECRET_KEY, salt="email-verification")


def create_url_safe_token(data: dict):
    return serializer.dumps(data, salt="email-verification")


def decode_url_safe_token(token: str):
    try:
        return serializer.loads(token)
    except Exception as e:
        logging.error(f"URLSafeToken decode error: {e}")

def get_current_user_uid(request: Request):
    """
    Resolve the caller's user_uid from the Authorization header.

    Parsing is defensive: a missing or malformed header previously raised
    AttributeError/IndexError and surfaced as a 500 instead of a 401. Expiry is now
    verified here too — it was disabled, which meant an expired token authenticated
    at the dependency layer and only the middleware kept it out.
    """
    auth_header = request.headers.get("Authorization") or ""
    parts = auth_header.split(" ", 1)
    access_token = parts[1].strip() if len(parts) == 2 and parts[0].lower() == "bearer" else None

    if not access_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Access token missing"
        )

    try:
        decoded = jwt.decode(
            access_token,
            Config.SECRET_KEY,
            algorithms=[_get_jwt_algorithm()],
            options={"verify_exp": True},
        )
        return decoded["user"]["user_uid"]

    except ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Access token expired"
        )
    except (JWTError, KeyError, TypeError):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid access token"
        )

