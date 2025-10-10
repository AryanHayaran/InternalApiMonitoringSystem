from typing import Optional, TypeVar, Generic
from sqlmodel import SQLModel
from pydantic import EmailStr
from pydantic.generics import GenericModel

# ----------------------------
# Generic type for ApiResponse
# ----------------------------
DataT = TypeVar("DataT")

# ----------------------------
# Standard API Response Wrapper
# ----------------------------
class AuthResponse(GenericModel, Generic[DataT]):
    success: bool
    message: str
    status_code: int
    data: Optional[DataT] = None

# ----------------------------
# Request Models
# ----------------------------
class UserCreate(SQLModel):
    full_name: Optional[str] = None
    email: EmailStr
    password: str


class UserLoginRequest(SQLModel):
    email: EmailStr
    password: str

# ----------------------------
# Response Data Models
# ----------------------------
class UserData(SQLModel):
    email: EmailStr
    uid: str  # matches your response key "uid"

# ----------------------------
# Response Models
# ----------------------------
class UserResponse(AuthResponse[UserData]):
    pass


class UserLogoutResponse(AuthResponse[dict]):
    pass
