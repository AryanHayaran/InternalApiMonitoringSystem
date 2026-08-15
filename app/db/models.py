from uuid import UUID
from sqlmodel import SQLModel, Field
from sqlalchemy.dialects.postgresql import UUID as pgUUID, JSONB
from sqlalchemy import text, TIMESTAMP, ForeignKey, Column
from datetime import datetime
from typing import Optional, Dict, Any


class Users(SQLModel, table=True):
    __tablename__ = "users"

    id: UUID = Field(
        default=None,
        primary_key=True,
        sa_type=pgUUID,
        sa_column_kwargs={
            "nullable": False,
            "server_default": text("uuid_generate_v4()")
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
    
class MonitoredEndpoints(SQLModel, table=True):
    __tablename__ = "monitored_endpoints"

    id: UUID = Field(
        default=None,
        primary_key=True,
        sa_type=pgUUID,
        sa_column_kwargs={
            "nullable": False,
            "server_default": text("uuid_generate_v4()")
        }
    )

    name: str = Field(nullable=False)
    http_method: str = Field(default="GET", nullable=False)
    url: str = Field(nullable=False)

    request_headers: Optional[Dict[str, Any]] = Field(default=None, sa_type=JSONB)
    request_body: Optional[Dict[str, Any]] = Field(default=None, sa_type=JSONB)

    periodic_summary_report: int = Field(default=60, nullable=False)
    expected_status_code: int = Field(nullable=False)
    response_validation: Optional[Dict[str, Any]] = Field(default=None, sa_type=JSONB)
    expected_latency_ms: int = Field(nullable=False)

    is_active: bool = Field(default=True, nullable=False)

    created_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column_kwargs={
            "nullable": False,
            "server_default": text("now()")
        }
    )

    updated_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column_kwargs={
            "nullable": False,
            "server_default": text("now()")
        }
    )

    owner_user_id: UUID = Field(
        sa_column=Column(
            pgUUID,
            ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False
        )
    )

    last_checked_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column_kwargs={
            "nullable": False,
            "server_default": text("now()")
        }
    )


class HealthCheckLogs(SQLModel, table=True):
    __tablename__ = "health_check_logs"

    id: Optional[int] = Field(default=None, primary_key=True)
    endpoint_id: UUID = Field(
        sa_column=Column(
            pgUUID,
            ForeignKey("monitored_endpoints.id", ondelete="CASCADE"),
            nullable=False
        )
    )

    checked_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column_kwargs={
            "nullable": False,
            "server_default": text("now()")
        }
    )

    is_healthy: bool = Field(nullable=False)
    response_time_ms: Optional[int] = None
    status_code: Optional[int] = None
    response_body: Optional[str] = None
    error_message: Optional[str] = None


class EndpointMetricsHourly(SQLModel, table=True):
    """Hourly rollup maintained by the metrics-rollup Kafka consumer group."""
    __tablename__ = "endpoint_metrics_hourly"

    endpoint_id: UUID = Field(
        sa_column=Column(
            pgUUID,
            ForeignKey("monitored_endpoints.id", ondelete="CASCADE"),
            primary_key=True,
            nullable=False
        )
    )
    hour_bucket: datetime = Field(primary_key=True, nullable=False)

    total_checks: int = Field(default=0, nullable=False)
    healthy_checks: int = Field(default=0, nullable=False)
    sum_latency_ms: int = Field(default=0, nullable=False)
    min_latency_ms: Optional[int] = Field(default=None)
    max_latency_ms: Optional[int] = Field(default=None)
    latency_buckets: Optional[Dict[str, Any]] = Field(default=None, sa_type=JSONB)

    updated_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column_kwargs={
            "nullable": False,
            "server_default": text("now()")
        }
    )


class Incidents(SQLModel, table=True):
    __tablename__ = "incidents"

    id: UUID = Field(
        default=None,
        primary_key=True,
        sa_type=pgUUID,
        sa_column_kwargs={
            "nullable": False,
            "server_default": text("uuid_generate_v4()")
        }
    )

    endpoint_id: UUID = Field(
        sa_column=Column(
            pgUUID,
            ForeignKey("monitored_endpoints.id", ondelete="CASCADE"),
            nullable=False
        )
    )

    start_time: datetime = Field(nullable=False)
    end_time: Optional[datetime] = None
    initial_error: Optional[str] = None
