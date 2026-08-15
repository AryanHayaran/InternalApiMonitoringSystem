from datetime import datetime
from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
import jwt
from app.core.config import Config
from app.core.security import _get_jwt_algorithm, decode_token, create_access_token
from app.infrastructure.redis.cache import is_jti_denied
from app.utils.connect import db
from app.services.auth import UserServices
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


get_db_session = db.get_db_session
user_services = UserServices()


class TokenRefreshMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):

        if request.method == "OPTIONS":
            return await call_next(request)

        # Skip login/logout routes
        if request.url.path in [
            "/api/auth/login",
            "/api/auth/signup",
            "/api/auth/refresh",
            "/api/services/health",
            "/api/services/readyz",
            "/api/docs",
            "/api/redoc",
            "/api/openapi.json",
        ]:
            return await call_next(request)

        auth_header = request.headers.get("Authorization")
        if not auth_header:
            return JSONResponse({"detail": "Token is not present"}, status_code=401)

        access_token = auth_header.split(" ")[1] if " " in auth_header else None
        if access_token:
            token_data = decode_token(access_token)
            if token_data:
                # Revoked at logout? Single choke point for every authenticated
                # route, so no router or dependency needs to change. Fails OPEN:
                # a Redis outage must not sign everyone out.
                if await is_jti_denied(token_data.get("jti")):
                    return JSONResponse(
                        {"detail": "Token has been revoked"}, status_code=401
                    )

                request.state.user = token_data["user"]
                # The token is already decoded here — expose the full payload so
                # logout can read jti/exp without decoding a second time.
                request.state.token_payload = token_data
                return await call_next(request)
            else:
                return JSONResponse(
                    {"detail": "Token is expired or invalid"}, status_code=401
                )
        else:
            return JSONResponse({"detail": "Token is not present"}, status_code=401)
