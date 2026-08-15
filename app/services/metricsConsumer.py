"""
Metrics rollup consumer — the SECOND consumer group on api-monitoring-results.

Reads the same event stream as the incident detector, with its own offsets, and
maintains hourly aggregates in endpoint_metrics_hourly. Independent by design: if
this consumer lags or dies, incident detection and alerting are unaffected.

Why pre-aggregate at all: at 500 endpoints health_check_logs grows by ~720k rows a
day, so answering "uptime over the last 30 days" on read would scan ~21M rows per
dashboard load. Aggregating on the write path is the correct answer, and a stream
consumer is the natural place for it.
"""
import asyncio
import random
from datetime import datetime, timezone

from app.core.config import Config
from app.infrastructure.kafka.consumer import KafkaConsumerClient
from app.repositories.metrics_repository import MetricsRepository
from app.utils.connect import db
from app.utils.loggers import get_logger

logger = get_logger()

# Fixed histogram buckets (the Prometheus approach): bounded storage, approximate
# percentiles. Exact percentiles would require retaining every observation.
LATENCY_BUCKETS_MS = [50, 100, 250, 500, 1000, 2500, 5000]


def bucket_label(latency_ms: int) -> str:
    for edge in LATENCY_BUCKETS_MS:
        if latency_ms <= edge:
            return f"le_{edge}"
    return "le_inf"


class MetricsConsumer:
    async def process_message(self, msg) -> bool:
        """Fold one event into its hourly bucket. Never raises."""
        try:
            if not isinstance(msg, dict) or "id" not in msg:
                logger.error(f"Skipping malformed metrics message: {msg!r}")
                return True

            endpoint_id = str(msg["id"])

            checked_at = msg.get("checked_at")
            if isinstance(checked_at, str):
                checked_at = datetime.fromisoformat(checked_at.replace("Z", "+00:00"))
            elif checked_at is None:
                checked_at = datetime.now(timezone.utc)

            # Normalise to naive UTC to match the TIMESTAMP columns used elsewhere.
            if checked_at.tzinfo is not None:
                checked_at = checked_at.astimezone(timezone.utc).replace(tzinfo=None)

            hour_bucket = checked_at.replace(minute=0, second=0, microsecond=0)

            latency = msg.get("response_time_ms") or 0
            is_healthy = msg.get("is_healthy")
            if is_healthy is None:
                # Older events without the enriched field: derive it.
                expected = msg.get("expected_status_code")
                is_healthy = expected is not None and msg.get("status_code") == expected

            async with db.pg_session_factory() as session:
                repo = MetricsRepository(session=session)
                await repo.record_check(
                    endpoint_id=endpoint_id,
                    hour_bucket=hour_bucket,
                    is_healthy=bool(is_healthy),
                    latency_ms=int(latency),
                    bucket=bucket_label(int(latency)),
                )

            return True

        except Exception as e:
            logger.error(f"Error rolling up metrics: {e}", exc_info=True)
            return False


async def start_metrics_consumer():
    """Supervised consume loop with capped exponential backoff."""
    await db.init_db()
    logger.info("Database connection initialized for metrics rollup consumer")

    logic = MetricsConsumer()
    attempt = 0

    while True:
        kafka_consumer = KafkaConsumerClient(group_id=Config.KAFKA_ROLLUP_GROUP)
        try:
            async for record in kafka_consumer.consume_messages():
                attempt = 0

                payload = kafka_consumer.deserialize(record)
                if payload is None:
                    await kafka_consumer.commit()
                    continue

                if await logic.process_message(payload):
                    await kafka_consumer.commit()

        except asyncio.CancelledError:
            await kafka_consumer.close()
            raise
        except Exception as e:
            attempt += 1
            delay = min(2 ** attempt, 30) + random.uniform(0, 1)
            logger.error(
                f"Metrics consumer loop failed (attempt {attempt}); "
                f"reconnecting in {delay:.1f}s: {e}",
                exc_info=True,
            )
            await kafka_consumer.close()
            await asyncio.sleep(delay)


if __name__ == "__main__":
    asyncio.run(start_metrics_consumer())
