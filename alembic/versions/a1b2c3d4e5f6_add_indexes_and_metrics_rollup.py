"""add monitoring indexes and hourly metrics rollup

Adds the three indexes every hot query needs (Postgres does not auto-index foreign
keys, so every read was a sequential scan + sort), plus the endpoint_metrics_hourly
table maintained by the metrics-rollup consumer group.

Revision ID: a1b2c3d4e5f6
Revises: 2862dbb9d305
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID as pgUUID


# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, None] = '2862dbb9d305'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # --- Indexes -----------------------------------------------------------
    # health_check_logs is the fastest-growing table (one row per endpoint per
    # minute). Every read filters endpoint_id and sorts by checked_at DESC.
    op.create_index(
        'ix_health_check_logs_endpoint_checked_at',
        'health_check_logs',
        ['endpoint_id', 'checked_at'],
    )
    op.create_index(
        'ix_incidents_endpoint_start_time',
        'incidents',
        ['endpoint_id', 'start_time'],
    )
    # Filtered on by every authenticated request, and by the ON DELETE CASCADE.
    op.create_index(
        'ix_monitored_endpoints_owner_user_id',
        'monitored_endpoints',
        ['owner_user_id'],
    )

    # --- Hourly metrics rollup --------------------------------------------
    # Maintained by the metrics-rollup consumer group. Pre-aggregated because
    # computing 30-day uptime on read would scan ~21M rows per dashboard load.
    op.create_table(
        'endpoint_metrics_hourly',
        sa.Column('endpoint_id', pgUUID(), nullable=False),
        sa.Column('hour_bucket', sa.TIMESTAMP(), nullable=False),
        sa.Column('total_checks', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('healthy_checks', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('sum_latency_ms', sa.BigInteger(), nullable=False, server_default='0'),
        sa.Column('min_latency_ms', sa.Integer(), nullable=True),
        sa.Column('max_latency_ms', sa.Integer(), nullable=True),
        # Fixed histogram buckets → approximate p50/p95/p99 with bounded storage.
        sa.Column('latency_buckets', JSONB(), nullable=True),
        sa.Column('updated_at', sa.TIMESTAMP(), nullable=False,
                  server_default=sa.text('now()')),
        sa.ForeignKeyConstraint(['endpoint_id'], ['monitored_endpoints.id'],
                                ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('endpoint_id', 'hour_bucket'),
    )
    op.create_index(
        'ix_endpoint_metrics_hourly_bucket',
        'endpoint_metrics_hourly',
        ['hour_bucket'],
    )


def downgrade() -> None:
    op.drop_index('ix_endpoint_metrics_hourly_bucket', table_name='endpoint_metrics_hourly')
    op.drop_table('endpoint_metrics_hourly')
    op.drop_index('ix_monitored_endpoints_owner_user_id', table_name='monitored_endpoints')
    op.drop_index('ix_incidents_endpoint_start_time', table_name='incidents')
    op.drop_index('ix_health_check_logs_endpoint_checked_at', table_name='health_check_logs')
