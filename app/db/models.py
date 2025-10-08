from uuid import UUID
from sqlmodel import SQLModel, Field
from sqlalchemy.dialects.postgresql import UUID as pgUUID
from sqlalchemy import text, TIMESTAMP
from datetime import datetime
from typing import Optional


class Users(SQLModel, table=True):
    id: UUID = Field(
        default=None,
        primary_key=True,
        sa_type=pgUUID,
        sa_column_kwargs={
            "nullable": False,
            "server_default": text("gen_random_uuid()")
        }
    )

    full_name: str = Field(nullable=False)
    email: str = Field(
        sa_column_kwargs={
            "unique": True,
            "index": True,
            "nullable": False
        }
    )

    password: str = Field(nullable=False)

    created_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column_kwargs={
            "nullable": False,
            "server_default": text("now()")
        }
    )

    last_login_at: Optional[datetime] = Field(default=None)
    refresh_token: Optional[str] = Field(default=None, sa_column_kwargs={"unique": True})
