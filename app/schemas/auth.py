from sqlmodel import SQLModel
from datetime import datetime
from uuid import UUID

class UserCreate(SQLModel):
    full_name: str | None = None
    email: str
    password: str
    
    
class UserLoginModal(SQLModel):
    email: str
    password: str


class UserLoginData(SQLModel):
    email: str
    uid: str  # matches your response key "uid"

class UserLoginResponse(SQLModel):
    message: str
    user: UserLoginData
