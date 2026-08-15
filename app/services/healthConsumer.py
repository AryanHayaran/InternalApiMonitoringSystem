import asyncio
import random
from app.infrastructure.kafka.consumer import KafkaConsumerClient
from app.services.service import ApiService
from app.utils.connect import db
from app.utils.loggers import get_logger

logger = get_logger()
api_service = ApiService()


class HealthConsumer:
    async def process_message(self, msg) -> bool:
        """
        Main business logic after a Kafka message is received.

        Returns True when the message was handled (or is unprocessable and should be
        skipped) — i.e. the offset may be committed. Returns False only on a transient
        failure worth retrying. Never raises: a poison message must not kill the process.
        """
        endpoint_id = None
        try:
            # Shape guard BEFORE anything else — a malformed payload used to escape
            # process_message entirely and terminate the consumer process.
            if not isinstance(msg, dict) or "id" not in msg:
                logger.error(f"Skipping malformed message (no 'id'): {msg!r}")
                return True

            endpoint_id = str(msg["id"])

            async with db.pg_session_factory() as session:
                # --- Fetch data using service layer ---
                api_details = await api_service.getConsumerServiceDetails(session, endpoint_id)
                api_last_three_records = await api_service.getApiLastThreeRecords(session, endpoint_id)

                if not api_details:
                    logger.warning(f"No API details found for endpoint {endpoint_id}")
                    return True

                # Prefer thresholds carried on the event (evaluated as-of-publish);
                # fall back to the DB row for messages produced before enrichment.
                expected_status = msg.get("expected_status_code")
                if expected_status is None:
                    expected_status = api_details["expected_status_code"]

                expected_latency = msg.get("expected_latency_ms")
                if expected_latency is None:
                    expected_latency = api_details["expected_latency_ms"]
                expected_latency = expected_latency or 0

                name = msg.get("name") or api_details["name"]

                actual_status = msg.get("status_code")
                actual_latency = msg.get("response_time_ms") or 0

                # --- Status comparison ---
                if actual_status == expected_status:
                    if actual_latency > expected_latency:
                        await self.handle_latency_warning(
                            session, endpoint_id, api_last_three_records, name, expected_latency
                        )
                    else:
                        logger.info(f"{name}: ✅ Healthy")
                else:
                    await self.handle_failure(session, endpoint_id, api_last_three_records, name)

            return True

        except Exception as e:
            logger.error(f"Error processing message for endpoint {endpoint_id}: {e}", exc_info=True)
            return False

    async def handle_failure(self, session, endpoint_id: str, last_three_records, name: str):
        """Triggered when API status mismatches expected."""
        if len(last_three_records) < 3:
            return

        all_failed = all(not record["is_healthy"] for record in last_three_records)
        if all_failed:
            logger.warning(f"⚠️ {name} failed 3 consecutive checks.")
            await api_service.createOrUpdateIncident(session, endpoint_id, last_three_records, reason="failure")
        else:
            logger.info(f"{name}: Some requests were healthy — skipping failure incident.")


    async def handle_latency_warning(self, session, endpoint_id: str, last_three_records, name: str, expected_latency: int):
        """Triggered when latency > expected threshold."""
        if len(last_three_records) < 3:
            return

        # response_time_ms is nullable in the DB — treat NULL as 0 rather than raising TypeError
        high_latency_count = sum(
            (record["response_time_ms"] or 0) > expected_latency for record in last_three_records
        )

        if high_latency_count == 3:
            logger.warning(f"⚠️ {name} exceeded latency in last 3 checks.")
            await api_service.createOrUpdateIncident(session, endpoint_id, last_three_records, reason="latency")
        else:
            logger.info(f"{name}: Latency spike not consistent — skipping latency incident.")


async def start_health_consumer():
    """
    Run the Kafka consumer and pass messages to business logic.

    Supervised: any consumer-level failure is caught here, the client is closed, and
    the loop reconnects with capped exponential backoff. Previously an error ended the
    async generator, start_health_consumer returned, and the process exited with code 0
    — an invisible failure that only `restart: always` papered over.
    """
    await db.init_db()
    logger.info("Database connection initialized for consumer")

    logic = HealthConsumer()
    attempt = 0

    while True:
        kafka_consumer = KafkaConsumerClient()
        try:
            async for record in kafka_consumer.consume_messages():
                attempt = 0  # a delivered message proves the connection is healthy

                payload = kafka_consumer.deserialize(record)
                if payload is None:
                    # Undecodable bytes: log, commit past it, keep going. Without this
                    # the same record is re-fetched forever (poison loop).
                    await kafka_consumer.commit()
                    continue

                handled = await logic.process_message(payload)
                if handled:
                    await kafka_consumer.commit()

        except asyncio.CancelledError:
            await kafka_consumer.close()
            raise
        except Exception as e:
            attempt += 1
            delay = min(2 ** attempt, 30) + random.uniform(0, 1)
            logger.error(
                f"Consumer loop failed (attempt {attempt}); reconnecting in {delay:.1f}s: {e}",
                exc_info=True,
            )
            await kafka_consumer.close()
            await asyncio.sleep(delay)


if __name__ == "__main__":
    asyncio.run(start_health_consumer())
