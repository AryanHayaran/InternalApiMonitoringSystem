from app.utils.loggers import get_logger
from typing import List
from app.utils.connect import db
from app.services.service import ApiService
from app.infrastructure.clients.api_client import check_api_health
from app.schemas.service import ApiProducerServiceModal, ProducerResultModal, ApiClientLogs
from app.infrastructure.kafka.producer import producer_client

logger = get_logger()
api_service = ApiService()


class Producer:
    def __init__(self):
        pass  # no async in __init__

    async def get_db_session(self):
        """Obtain a single AsyncSession from a fresh generator."""
        session_gen = db.get_db_session()
        try:
            session = await anext(session_gen)
            return session
        except StopAsyncIteration:
            logger.error(
                "Failed to get a database session: generator exhausted")
        except Exception as e:
            logger.error(
                f"Unexpected error obtaining DB session: {e}", exc_info=True)
        finally:
            await session_gen.aclose()
        return None
        

    async def get_all_api(self) -> List[ApiProducerServiceModal]:
        """Fetch all monitored endpoints safely using get_db_session."""
        services_to_check: List[ApiProducerServiceModal] = []
        session = await self.get_db_session()
        if not session:
            return []

        try:
            services_to_check = await api_service.get_all_api_services(session=session)
            logger.info(
                f"Fetched {len(services_to_check)} services from the database.")
        except Exception as e:
            logger.error(
                f"Failed to fetch services from database: {e}", exc_info=True)
        return services_to_check


    async def run_all_health_checks(self):
        """ 
        Main function executed by the scheduler.
        Fetches all active services, checks them concurrently, and logs the results.
        """
        logger.info("--- Starting health check cycle ---")
        raw_services = await self.get_all_api()

        if not raw_services:
            logger.info("No active services to check in this cycle.")
            return

        # Ensure all services are ApiProducerServiceModal objects
        services_to_check = [
            s if isinstance(s, ApiProducerServiceModal) else ApiProducerServiceModal(**s)
            for s in raw_services
        ]

        for service in services_to_check:
            try:
                health_data = await check_api_health(service)
                logger.info(f"Health check completed for {service.name}")
                
                result = ProducerResultModal(
                    id=service.id,
                    checked_at=health_data.checked_at,
                    response_time_ms=health_data.response_time_ms,
                    status_code=health_data.status_code,
                )
                
                # Send to Kafka with error handling
                success = await producer_client.send_result(result)
                if not success:
                    logger.warning(f"Failed to send monitoring result to Kafka for service {service.name}")



                update_logs = ApiClientLogs(
                    id=service.id,
                    checked_at=health_data.checked_at,
                    response_time_ms=health_data.response_time_ms,
                    response_body=health_data.response_body,
                    is_healthy= health_data.status_code==service.expected_status_code,
                    status_code=health_data.status_code,
                    error_message=health_data.error_message 
                )
                session = await self.get_db_session()
                if not session:
                    return []
                await api_service.update_api_logs(session, update_logs)

            except Exception as e:
                logger.error(f"Error checking service {service.name} at {service.url}: {e}", exc_info=True)

