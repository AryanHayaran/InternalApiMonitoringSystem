import datetime
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from sqlalchemy.ext.asyncio import AsyncSession
from app.services.auth import UserServices
from app.schemas.auth import UserCreate, UserResponse, UserLogoutResponse, UserData
from app.core.security import get_current_user_uid, verify_password, create_access_token, _get_jwt_algorithm
from datetime import datetime, timedelta
from app.core.config import Config
from fastapi.responses import JSONResponse
from app.utils.response_handler import success_response, error_response
from app.schemas.auth import AuthResponse
from app.utils.connect import db
from fastapi import status
import jwt

router = APIRouter()
api_services = UserServices()
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
get_db_session = db.get_db_session


# ----------------------------
# Signup
# ----------------------------
@router.post("/signup", response_model=UserResponse)
async def signup_user(user_data: UserCreate, session: AsyncSession = Depends(get_db_session)):
    try:
        user_exists = await api_services.user_exists(user_data.email, session)
        if user_exists:
            return error_response("User with this email already exists", 400)

        user = await api_services.create_user(user_data, session)
        logger.info("User created successfully: %s", user.email)

        return success_response(
            {"email": user.email, "uid": str(user.id)},
            "Signup successful",
            201
        )

    except Exception as e:
        logger.error("Error during signup: %s", e, exc_info=True)
        return error_response(f"Error during signup: {str(e)}", 500)


# ----------------------------
# Login
# ----------------------------
@router.post("/login", response_model=UserResponse)
async def login_user(response: Response, login_data: UserData, session: AsyncSession = Depends(get_db_session)):
    try:
        user = await api_services.get_user_by_email(login_data.email, session)
        if not user or not verify_password(login_data.password, user.password):
            return error_response("Invalid credentials", 401)

        # Create tokens
        access_token = create_access_token({"email": user.email, "user_uid": str(user.id)})
        refresh_token = create_access_token({"email": user.email, "user_uid": str(user.id)}, refresh=True)

        # Save refresh token in DB
        await api_services.save_refresh_token(user.id, refresh_token, session)

        # Send access token in cookie
        response.set_cookie(
            key="access_token",
            value=access_token,
            httponly=True,
            secure=True,
            samesite="lax",
            max_age=Config.ACCESS_TOKEN_EXPIRY,
        )

        logger.info("User logged in successfully: %s", user.email)
        return success_response(
            {"email": user.email, "uid": str(user.id)},
            "Login successful"
        )

    except Exception as e:
        logger.error("Error during login: %s", e, exc_info=True)
        return error_response(f"Error during login: {str(e)}", 500)


# ----------------------------
# Logout
# ----------------------------
@router.get("/logout", response_model=UserLogoutResponse)
async def logout_user(user_uid: str = Depends(get_current_user_uid), session: AsyncSession = Depends(get_db_session)):
    try:
        await api_services.delete_user_refresh_tokens(user_uid, session)

        response = Response(
            content=success_response({}, "Logged out successfully", 200).body,
            media_type="application/json"
        )
        response.delete_cookie("access_token")
        logger.info("User logged out successfully: %s", user_uid)
        return response

    except Exception as e:
        logger.error("Error during logout for user %s: %s", user_uid, e, exc_info=True)
        return error_response(f"Error during logout: {str(e)}", 500)
