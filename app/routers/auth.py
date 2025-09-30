from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.dependencies import get_db_session
from app.crud.service import ApiServices
from app.schemas.service import UserCreate, UserRead, UserLoginModal
from app.core.utils import verify_password,create_access_token
from datetime import timedelta
from app.core.config import Config
from fastapi.responses import JSONResponse
router = APIRouter()
api_services =  ApiServices()
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@router.post("/signup", response_model=UserRead)
async def signup_user(user_data: UserCreate, session: AsyncSession = Depends(get_db_session)):
    user_exists = await api_services.user_exists(user_data.email, session)

    if user_exists:
        raise HTTPException(
            status_code=400,
            detail="User with this email already exists"
        )
        return None

    user = await api_services.create_user(user_data, session)
    logger.info(f"User created successfully: {user}")   
    return user
    
    
@router.post("/login", response_model=UserRead)
async def login_user(login_data: UserLoginModal,session: AsyncSession = Depends(get_db_session)):
    user = await api_services.get_user_by_email(login_data.email, session)
    
    if user is not None:
        password_valid = verify_password(login_data.password, user.password)
        if password_valid:
            access_token = create_access_token(
                user_data={
                    "email": user.email,
                    "user_uid": str(user.id),
                }
            )

            refresh_token = create_access_token(
                user_data={
                    "email": user.email,
                    "user_uid": str(user.id)
                },
                expiry=timedelta(days=Config.REFRESH_TOKEN_EXPIRY),
                refresh=True
            )

            return JSONResponse(
                content={
                    "message": "Login successful",
                    "access_token": access_token,
                    "refresh_token": refresh_token,
                    "user":{
                        "email": user.email,
                        "uid": str(user.id),
                    }
                }
            )

# @router.post("/logout", response_model=UserRead)
# async def revoke_token(token_details:dict = Depends(AccessTokenBearer())):
#     jti = token_details['jti']

#     # await add_jti_to_blocklist(jti)

#     return JSONResponse(
#         content={
#             "message": "logged out successfully",
#         },
#         status_code=status.HTTP_200_OK
#     )
    
