from app.utils.loggers import get_logger
from typing import List
from app.utils.connect import db
from app.services.service import ApiService
from app.infrastructure.clients.api_client import check_api_health
from app.schemas.service import ApiServiceModal

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

    async def get_all_api(self) -> List[ApiServiceModal]:
        """Fetch all monitored endpoints safely using get_db_session."""
        services_to_check: List[ApiServiceModal] = []
        session = await self.get_db_session()
        if not session:
            return []

        try:
            raw_services = await api_service.get_all_api_services(session=session)
            services_to_check = [
                s if isinstance(s, ApiServiceModal) else ApiServiceModal(**s)
                for s in raw_services
            ]
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
        services_to_check: List[ApiServiceModal] = await self.get_all_api()

        if not services_to_check:
            logger.info("No active services to check in this cycle.")
            return

        for service in services_to_check:
            try:
                health_data = await check_api_health(service)
                logger.info(f"Health check completed for {service.name}")
            except Exception as e:
                logger.error(
                    f"Error checking service {service.name} at {service.url}: {e}", exc_info=True)

        return
