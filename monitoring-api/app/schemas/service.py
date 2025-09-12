from sqlmodel import SQLModel
from datetime import datetime
from uuid import UUID

class UserCreate(SQLModel):
    full_name: str | None = None
    email: str
    password: str


class UserRead(SQLModel):
    id: UUID 
    email: str
    full_name: str | None = None
    
    
class UserLoginModal(SQLModel):
    email: str
    password: str
