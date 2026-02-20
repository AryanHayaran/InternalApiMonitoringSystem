from typing import Optional, TypeVar, Generic
from sqlmodel import SQLModel
from pydantic import EmailStr
from pydantic.generics import GenericModel

DataT = TypeVar("DataT")
class AuthResponse(GenericModel, Generic[DataT]):
    success: bool
    message: str
    data: Optional[DataT] = None

class UserCreate(SQLModel):
    full_name: str
    email: str
    password: str

class UserLogin(SQLModel):
    email: str
    password: str

# Response Data Models
class UserData(SQLModel):
    email: str
    uid: str  

class UserDataLoginResponse(SQLModel):
    email: str
    uid: str
    access_token: str
    refresh_token: str

class UserResponse(AuthResponse[UserDataLoginResponse]):
    pass

class UserResponseSignup(AuthResponse[dict]):
    pass

class UserLogoutResponse(AuthResponse[dict]):
    pass

class UserRefresh(SQLModel):
    refresh_token: str

class UserResponseRefreshToken(AuthResponse[dict]):
    pass
