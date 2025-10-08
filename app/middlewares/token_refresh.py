from datetime import datetime
from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
import jwt
from app.core.config import Config
from app.core.security import _get_jwt_algorithm, decode_token, create_access_token
from app.utils.connect import db
from app.services.auth import ApiServices
get_db_session = db.get_db_session
api_services =  ApiServices()
class TokenRefreshMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Skip login/logout routes
        if request.url.path in ["/api/auth/login","/api/auth/signup", "api/services/health"]:
            return await call_next(request)

        access_token = request.cookies.get("access_token")
        if access_token:
            token_data = decode_token(access_token)
            if token_data:
                request.state.user = token_data["user"]
                return await call_next(request)

        # If token expired / invalid
        if not access_token:
            return JSONResponse({"detail": "Not authenticated"}, status_code=401)

        try:
            decoded = jwt.decode(
                access_token,
                Config.SECRET_KEY,
                algorithms=[_get_jwt_algorithm()],
                options={"verify_exp": False},  # ignore expiry
            )
            user_id = decoded["user"]["user_uid"]
        except Exception:
            return JSONResponse({"detail": "Invalid token"}, status_code=401)

        session = await get_db_session()
        db_refresh_token = await api_services.get_refresh_token_for_user(user_id, session)
        if not db_refresh_token:
            return JSONResponse({"detail": "Login required"}, status_code=401)

        refresh_data = decode_token(db_refresh_token)
        if not refresh_data or datetime.fromtimestamp(refresh_data["exp"]) < datetime.utcnow():
            await api_services.delete_user_refresh_tokens(user_id, session)
            return JSONResponse({"detail": "Session expired"}, status_code=401)

        # Generate new access token
        new_access_token = create_access_token(user_data=refresh_data["user"])
        response = await call_next(request)
        response.set_cookie(
            key="access_token",
            value=new_access_token,
            httponly=True,
            secure=True,
            samesite="lax",
            max_age=Config.ACCESS_TOKEN_EXPIRY,
        )
        request.state.user = refresh_data["user"]
        return response
