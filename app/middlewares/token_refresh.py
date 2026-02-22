from datetime import datetime
from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
import jwt
from app.core.config import Config
from app.core.security import _get_jwt_algorithm, decode_token, create_access_token
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
            "api/services/health",
            "/api/docs",
            "/api/redoc",
            "/api/openapi.json",
        ]:
            return await call_next(request)

        access_token = request.headers.get("Authorization").split(" ")[1]
        if access_token:
            token_data = decode_token(access_token)
            if token_data:
                request.state.user = token_data["user"]
                return await call_next(request)
            else:
                return JSONResponse(
                    {"detail": "Token is expired or invalid"}, status_code=401
                )
        else:
            return JSONResponse({"detail": "Token is not present"}, status_code=401)
