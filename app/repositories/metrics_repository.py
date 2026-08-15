from datetime import datetime

from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import EndpointMetricsHourly, HealthCheckLogs

# Only these labels may reach the JSON path expression below.
ALLOWED_BUCKETS = frozenset(
    [f"le_{edge}" for edge in (50, 100, 250, 500, 1000, 2500, 5000)] + ["le_inf"]
)


class MetricsRepository:
    """
    Hourly rollup storage. Follows the same conventions as ApiServiceRepository:
    the session is injected, never opened here, and reads return plain dicts.
    """

    def __init__(self, session: AsyncSession = None):
        self.session = session

    async def record_check(
        self,
        endpoint_id: str,
        hour_bucket: datetime,
        is_healthy: bool,
        latency_ms: int,
        bucket: str,
    ):
        """
        Fold one health check into its hourly bucket with a single atomic UPSERT.

        NOTE ON IDEMPOTENCY: counter increments are not idempotent under at-least-once
        delivery, so a redelivered message double-counts. The drift on a percentage is
        negligible, and reconcile_hour() below recomputes authoritatively from
        health_check_logs. Streaming for freshness, batch for correctness.
        """
        if bucket not in ALLOWED_BUCKETS:
            raise ValueError(f"Unknown latency bucket: {bucket!r}")

        stmt = pg_insert(EndpointMetricsHourly).values(
            endpoint_id=endpoint_id,
            hour_bucket=hour_bucket,
            total_checks=1,
            healthy_checks=1 if is_healthy else 0,
            sum_latency_ms=latency_ms,
            min_latency_ms=latency_ms,
            max_latency_ms=latency_ms,
            latency_buckets={bucket: 1},
            updated_at=func.now(),
        )

        table = EndpointMetricsHourly.__table__
        tbl = table.name

        # Bump this bucket's counter while preserving the others. Inside
        # ON CONFLICT DO UPDATE, a bare table reference means the existing row.
        # `bucket` is validated against ALLOWED_BUCKETS by the caller-facing guard
        # below, so interpolating it into the JSON path is safe.
        buckets_expr = text(
            f"jsonb_set("
            f"  COALESCE({tbl}.latency_buckets, '{{}}'::jsonb), "
            f"  '{{{bucket}}}', "
            f"  to_jsonb(COALESCE(({tbl}.latency_buckets->>'{bucket}')::int, 0) + 1), "
            f"  true)"
        )

        stmt = stmt.on_conflict_do_update(
            index_elements=['endpoint_id', 'hour_bucket'],
            set_={
                'total_checks': table.c.total_checks + 1,
                'healthy_checks': table.c.healthy_checks + (1 if is_healthy else 0),
                'sum_latency_ms': table.c.sum_latency_ms + latency_ms,
                'min_latency_ms': func.least(table.c.min_latency_ms, latency_ms),
                'max_latency_ms': func.greatest(table.c.max_latency_ms, latency_ms),
                'latency_buckets': buckets_expr,
                'updated_at': func.now(),
            },
        )

        await self.session.execute(stmt)
        await self.session.commit()

    async def get_uptime(self, endpoint_id: str, since: datetime):
        """Uptime % and latency summary for one endpoint since a given time."""
        query = select(
            func.coalesce(func.sum(EndpointMetricsHourly.total_checks), 0).label("total_checks"),
            func.coalesce(func.sum(EndpointMetricsHourly.healthy_checks), 0).label("healthy_checks"),
            func.coalesce(func.sum(EndpointMetricsHourly.sum_latency_ms), 0).label("sum_latency_ms"),
            func.min(EndpointMetricsHourly.min_latency_ms).label("min_latency_ms"),
            func.max(EndpointMetricsHourly.max_latency_ms).label("max_latency_ms"),
        ).where(
            EndpointMetricsHourly.endpoint_id == endpoint_id,
            EndpointMetricsHourly.hour_bucket >= since,
        )
        result = await self.session.execute(query)
        row = result.mappings().first()
        return dict(row) if row else None

    async def reconcile_hour(self, hour_bucket: datetime):
        """
        Recompute one hour authoritatively from health_check_logs, overwriting the
        streamed values. This is what bounds the counter drift inherent in
        at-least-once delivery. Cheap thanks to the (endpoint_id, checked_at) index.
        """
        next_hour = hour_bucket.replace(minute=59, second=59, microsecond=999999)

        agg = select(
            HealthCheckLogs.endpoint_id.label("endpoint_id"),
            func.count().label("total_checks"),
            func.count().filter(HealthCheckLogs.is_healthy == True).label("healthy_checks"),
            func.coalesce(func.sum(HealthCheckLogs.response_time_ms), 0).label("sum_latency_ms"),
            func.min(HealthCheckLogs.response_time_ms).label("min_latency_ms"),
            func.max(HealthCheckLogs.response_time_ms).label("max_latency_ms"),
        ).where(
            HealthCheckLogs.checked_at >= hour_bucket,
            HealthCheckLogs.checked_at <= next_hour,
        ).group_by(HealthCheckLogs.endpoint_id)

        rows = (await self.session.execute(agg)).mappings().all()

        for row in rows:
            stmt = pg_insert(EndpointMetricsHourly).values(
                endpoint_id=row["endpoint_id"],
                hour_bucket=hour_bucket,
                total_checks=row["total_checks"],
                healthy_checks=row["healthy_checks"],
                sum_latency_ms=row["sum_latency_ms"],
                min_latency_ms=row["min_latency_ms"],
                max_latency_ms=row["max_latency_ms"],
                updated_at=func.now(),
            ).on_conflict_do_update(
                index_elements=['endpoint_id', 'hour_bucket'],
                set_={
                    'total_checks': row["total_checks"],
                    'healthy_checks': row["healthy_checks"],
                    'sum_latency_ms': row["sum_latency_ms"],
                    'min_latency_ms': row["min_latency_ms"],
                    'max_latency_ms': row["max_latency_ms"],
                    'updated_at': func.now(),
                },
            )
            await self.session.execute(stmt)

        await self.session.commit()
        return len(rows)
