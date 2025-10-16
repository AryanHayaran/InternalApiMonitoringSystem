from app.utils.loggers import setup_logger  
from typing import List
from app.utils.connect import db
from app.services.service import get_all_api_services
from app.infrastructure.clients.api_client import check_api_health
from app.infrastructure.redis.cache import update_service_status_in_cache
from app.db.models import MonitoredService
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def run_all_health_checks():
    """
    This is the main function executed by the scheduler.
    It fetches all active services, checks them concurrently, and publishes the results.
    """
    logger.info("--- Starting health check cycle ---")
    
    services_to_check: List[MonitoredService] = []
    
    # Step 1: Get the list of services from the database
    try:
        # Use a database session to fetch the services
        async with db.get_db_session() as session:
            services_to_check = await get_all_api_services(session=session)
    except Exception as e:
        logger.error(f"Failed to fetch services from database: {e}", exc_info=True)
        return # Exit the cycle if we can't get the list of services

    if not services_to_check:
        logger.info("No active services to check in this cycle.")
        return

    logger.info(f"Found {len(services_to_check)} services to check.")

    # Step 2: Create a list of concurrent tasks for the health checks
    # This is highly efficient as it checks all APIs at the same time.
    check_tasks = [check_api_health(service) for service in services_to_check]
    
    # Use asyncio.gather to run all checks in parallel.
    # return_exceptions=True ensures that one failed check doesn't stop the others.
    # results = await asyncio.gather(*check_tasks, return_exceptions=True)

    # # Step 3: Process the results of the health checks
    # kafka_producer = await get_kafka_producer()

    # for result in results:
    #     if isinstance(result, Exception):
    #         logger.error(f"An error occurred during a health check: {result}", exc_info=True)
    #         continue

    #     # Assuming the result is a dictionary with health data
    #     service_id = result.get("service_id")
    #     logger.info(f"Processing result for service {service_id}: {result}")

    #     # Step 4: Publish to Kafka and update Redis cache for each result
    #     try:
    #         # Publish the full result to Kafka for the consumer to process
    #         await kafka_producer.send_and_wait("health_check_results", value=str(result))
            
    #         # Update the real-time status in the Redis cache for the dashboard
    #         await update_service_status_in_cache(service_id, result)

    #     except Exception as e:
    #         logger.error(f"Failed to publish result for service {service_id}: {e}", exc_info=True)
            
    # logger.info("--- Health check cycle completed ---")