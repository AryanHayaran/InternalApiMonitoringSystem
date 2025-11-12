import asyncio
from app.infrastructure.kafka.consumer import KafkaConsumerClient
from app.services.service import ApiService
from app.utils.connect import db
from app.utils.loggers import get_logger
from app.core.config import Config

logger = get_logger()
api_service = ApiService()


class HealthConsumer:
    async def get_db_session(self):
        """Obtain async DB session."""
        session_gen = db.get_db_session()
        try:
            session = await anext(session_gen)
            return session
        except Exception as e:
            logger.error(f"Error creating DB session: {e}", exc_info=True)
        finally:
            await session_gen.aclose()
        return None

    async def process_message(self, msg):
        """
        Main business logic after Kafka message received.
        """
        endpoint_id = str(msg["id"])
        session = await self.get_db_session()
        if not session:
            logger.error("Failed to open DB session.")
            return

        try:
            # --- Fetch data using service layer ---
            api_details = await api_service.getConsumerServiceDetails(session, endpoint_id)
            api_last_three_records = await api_service.getApiLastThreeRecords(session, endpoint_id)

            if not api_details:
                logger.warning(f"No API details found for endpoint {endpoint_id}")
                return

            expected_status = api_details.expected_status_code
            expected_latency = api_details.expected_latency_ms or 0
            actual_status = msg["status_code"]
            actual_latency = msg["response_time_ms"] or 0

            # --- Status comparison ---
            if actual_status == expected_status:
                if actual_latency > expected_latency:
                    await self.handle_latency_warning(session, endpoint_id, api_last_three_records, api_details)
                else:
                    logger.info(f"{api_details.name}: ✅ Healthy")
            else:
                await self.handle_failure(session, endpoint_id, api_last_three_records, api_details)

        except Exception as e:
            logger.error(f"Error processing message for endpoint {endpoint_id}: {e}", exc_info=True)
        finally:
            await session.close()

    async def handle_failure(self, session, endpoint_id: str, last_three_records, api_details):
        """Triggered when API status mismatches expected."""
        if len(last_three_records) < 3:
            return

        all_failed = all(not record["is_healthy"] for record in last_three_records)
        if all_failed:
            logger.warning(f"⚠️ {api_details.name} failed 3 consecutive checks.")
            await api_service.createOrUpdateIncident(session, endpoint_id, last_three_records, reason="failure")
        else:
            logger.info(f"{api_details.name}: Some requests were healthy — skipping failure incident.")


    async def handle_latency_warning(self, session, endpoint_id: str, last_three_records, api_details):
        """Triggered when latency > expected threshold."""
        if len(last_three_records) < 3:
            return

        expected_latency = api_details.expected_latency_ms or 0
        high_latency_count = sum(record["response_time_ms"] > expected_latency for record in last_three_records)

        if high_latency_count == 3:
            logger.warning(f"⚠️ {api_details.name} exceeded latency in last 3 checks.")
            await api_service.createOrUpdateIncident(session, endpoint_id, last_three_records, reason="latency")
        else:
            logger.info(f"{api_details.name}: Latency spike not consistent — skipping latency incident.")


async def start_health_consumer():
    """Run Kafka consumer and pass messages to business logic."""
    # Initialize database connection
    await db.init_db()
    logger.info("Database connection initialized for consumer")
    
    kafka_consumer = KafkaConsumerClient()
    logic = HealthConsumer()

    async for message in kafka_consumer.consume_messages():
        await logic.process_message(message)


if __name__ == "__main__":
    asyncio.run(start_health_consumer())
