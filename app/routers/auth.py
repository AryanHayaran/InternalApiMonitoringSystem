import datetime
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from sqlalchemy.ext.asyncio import AsyncSession
from app.services.auth import UserServices
from app.schemas.auth import UserCreate, UserResponse,UserResponseSignup, UserLogoutResponse, UserLogin, UserRefresh, UserResponseRefreshToken
from app.core.security import get_current_user_uid, verify_password, create_access_token, _get_jwt_algorithm
from datetime import datetime, timedelta
from app.core.config import Config
from fastapi.responses import JSONResponse
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

@router.post("/signup", response_model=UserResponseSignup)
async def signup_user(response: Response, user_data: UserCreate, session: AsyncSession = Depends(get_db_session)):
    try:
        # Check if user already exists
        user_exists = await api_services.user_exists(user_data.email, session)
        if user_exists:
            response.status_code = 400
            return {
                "success": False,
                "message": "User with this email already exists",
                "data": None
            }

        # Create new user
        user = await api_services.create_user(user_data, session)
        if not user:
            response.status_code = 500
            return {
                "success": False,
                "message": "User creation failed",
                "data": None
            }

        logger.info("User created successfully: %s", user["email"])
        response.status_code = 201
        return {
            "success": True,
            "message": "Signup successful",
            "data": {
                "email": user["email"],
                "uid": str(user["id"])
            }
        }

    except Exception as e:
        logger.error("Error during signup: %s", e, exc_info=True)
        response.status_code = 500
        return {
            "success": False,
            "message": f"Error during signup: {str(e)}",
            "data": None
        }


@router.post("/login", response_model=UserResponse)
async def login_user(response: Response, login_data: UserLogin, session: AsyncSession = Depends(get_db_session)):
    try:
        # Fetch user by email
        user = await api_services.get_user_by_email(login_data.email, session)
        if not user or not verify_password(login_data.password, user["password"]):
            response.status_code = 401
            return {
                "success": False,
                "message": "Invalid email or password",
                "data": None
            }

        # Generate tokens
        access_token = create_access_token({"email": user["email"], "user_uid": str(user["id"])})
        refresh_token = create_access_token(
            {"email": user["email"], "user_uid": str(user["id"])},
            refresh=True
        )

        # Save refresh token
        await api_services.save_refresh_token(user["id"], refresh_token, session)


        logger.info("User logged in successfully: %s", user["email"])
        response.status_code = 200
        return {
            "success": True,
            "message": "Login successful",
            "data": {
                "email": user["email"],
                "uid": str(user["id"]),
                "access_token": f"Bearer {access_token}",
                "refresh_token": f"Bearer {refresh_token}"
            }
        }

    except Exception as e:
        logger.error("Error during login: %s", e, exc_info=True)
        response.status_code = 500
        return {
            "success": False,
            "message": f"Error during login: {str(e)}",
            "data": None
        }


@router.get("/logout", response_model=UserLogoutResponse)
async def logout_user(response: Response, user_uid: str = Depends(get_current_user_uid), session: AsyncSession = Depends(get_db_session)):
    try:
        # Delete refresh tokens
        await api_services.delete_user_refresh_tokens(user_uid, session)

        # Clear access token cookie
        logger.info("User logged out successfully: %s", user_uid)
        response.status_code = 200
        return {
            "success": True,
            "message": "Logged out successfully",
            "data": {}
        }

    except Exception as e:
        logger.error("Error during logout for user %s: %s", user_uid, e, exc_info=True)
        response.status_code = 500
        return {
            "success": False,
            "message": f"Error during logout: {str(e)}",
            "data": None
        }

@router.post("/refresh",response_model=UserResponseRefreshToken)
async def refresh_token(response: Response,Refresh_data: UserRefresh, session: AsyncSession = Depends(get_db_session)):
    try:
        
        access_token = await api_services.validate_refresh_token(Refresh_data.refresh_token, session)
        if not access_token:
            response.status_code = 401
            return {
                "success": False,
                "message": "Invalid refresh token",
                "data": None
            }

        logger.info("Token refreshed successfully")
        response.status_code = 200
        return {
            "success": True,
            "message": "Token refreshed successfully",
            "data": {
                "access_token": access_token
            }
        }
    except Exception as e:
        logger.error("Error during token refresh: %s", e, exc_info=True)
        response.status_code = 500
        return {
            "success": False,
            "message": f"Error during token refresh: {str(e)}",
            "data": None
        }   
