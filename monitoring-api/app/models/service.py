from uuid import UUID
from sqlmodel import SQLModel, Field
from sqlalchemy.dialects.postgresql import UUID as pgUUID
from sqlalchemy import text, TIMESTAMP
from datetime import datetime

class Users(SQLModel, table=True):

    id: UUID = Field(
        default=None,
        primary_key=True,
        sa_type=pgUUID,  # just the class
        sa_column_kwargs={
            "nullable": False,
            "server_default": text("gen_random_uuid()")
        }
    )

    email: str = Field(
        sa_column_kwargs={
            "unique": True,
            "index": True,
            "nullable": False
        }
    )

    full_name: str | None = None
    password: str = Field(nullable=False)

    created_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column_kwargs={
            "nullable": False,
            "server_default": text("now()")
        }
    )
