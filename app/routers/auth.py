import datetime
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from sqlalchemy.ext.asyncio import AsyncSession
from app.services.auth import ApiServices
from app.schemas.auth import UserCreate, UserLoginModal,UserLoginResponse
from app.core.security import verify_password, create_access_token, _get_jwt_algorithm
from datetime import datetime, timedelta
from app.core.config import Config
from fastapi.responses import JSONResponse
from app.utils.connect import db
from fastapi import status
import jwt

router = APIRouter()
api_services =  ApiServices()
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
get_db_session = db.get_db_session

@router.post("/signup", response_model=UserLoginResponse)
async def signup_user(user_data: UserCreate, session: AsyncSession = Depends(get_db_session)):
    user_exists = await api_services.user_exists(user_data.email, session)

    if user_exists:
        raise HTTPException(
            status_code=400,
            detail="User with this email already exists"
        )

    user = await api_services.create_user(user_data, session)
    logger.info(f"User created successfully: {user}")   
    return {
        "message": "Signup successful",
        "user": {"email": user.email, "uid": str(user.id)},
    }
    
@router.post("/login", response_model=UserLoginResponse)
async def login_user(response: Response, login_data: UserLoginModal, session=Depends(get_db_session)):
    user = await api_services.get_user_by_email(login_data.email, session)
    if not user or not verify_password(login_data.password, user.password):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    # Create tokens
    access_token = create_access_token(user_data={"email": user.email, "user_uid": str(user.id)})
    refresh_token = create_access_token(
        user_data={"email": user.email, "user_uid": str(user.id)},
        refresh=True
    )

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

    return {
        "message": "Login successful",
        "user": {"email": user.email, "uid": str(user.id)},
    }


@router.get("/logout")
async def logout_user(request: Request, session=Depends(get_db_session)):
    access_token = request.cookies.get("access_token")
    if access_token:
        try:
            decoded = jwt.decode(
                access_token,
                Config.SECRET_KEY,
                algorithms=[_get_jwt_algorithm()],
                options={"verify_exp": False},
            )
            user_id = decoded["user"]["user_uid"]
            await api_services.delete_user_refresh_tokens(user_id, session)
        except Exception:
            pass

    response = Response(content='{"message":"Logged out successfully"}', media_type="application/json")
    response.delete_cookie("access_token")
    return response