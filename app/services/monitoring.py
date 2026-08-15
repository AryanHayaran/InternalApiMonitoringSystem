import asyncio
import time
from typing import List

from app.core.config import Config
from app.infrastructure.clients.api_client import check_api_health
from app.infrastructure.kafka.producer import producer_client
from app.schemas.service import ApiProducerServiceModal, ProducerResultModal, ApiClientLogs
from app.services.service import ApiService
from app.utils.connect import db
from app.utils.loggers import get_logger

logger = get_logger()
api_service = ApiService()


class Producer:
    def __init__(self):
        pass  # no async in __init__

    async def get_all_api(self) -> List[dict]:
        """Fetch all active monitored endpoints safely."""
        services_to_check: List[dict] = []
        try:
            async with db.pg_session_factory() as session:
                services_to_check = await api_service.get_all_api_services(session=session)
                logger.info(
                    f"Fetched {len(services_to_check)} services from the database.")
        except Exception as e:
            logger.error(
                f"Failed to fetch services from database: {e}", exc_info=True)
        return services_to_check

    async def _check_one(self, raw_service, sem: asyncio.Semaphore) -> bool:
        """
        Probe a single endpoint, persist the log, then publish to Kafka.

        Fully self-contained failure boundary: one bad endpoint (or one malformed DB
        row) can never abort the cycle for the others. Returns True on success.
        """
        async with sem:
            service_name = "<unknown>"
            try:
                # Coercion lives INSIDE the boundary — a row with an invalid URL used
                # to raise during a list comprehension and kill the whole cycle.
                service = (
                    raw_service
                    if isinstance(raw_service, ApiProducerServiceModal)
                    else ApiProducerServiceModal(**raw_service)
                )
                service_name = service.name

                # Hard deadline above httpx's own timeouts, so a pathological
                # endpoint can never pin a worker slot indefinitely.
                health_data = await asyncio.wait_for(
                    check_api_health(service),
                    timeout=Config.HEALTH_CHECK_TIMEOUT_S,
                )

                is_healthy = health_data.status_code == service.expected_status_code

                # --- 1. Persist FIRST -------------------------------------------
                # The consumer reads the last-3 window from this table. Writing
                # before publishing means a failed publish delays detection by one
                # cycle instead of producing a wrong verdict.
                update_logs = ApiClientLogs(
                    id=service.id,
                    checked_at=health_data.checked_at,
                    response_time_ms=health_data.response_time_ms,
                    response_body=health_data.response_body,
                    is_healthy=is_healthy,
                    status_code=health_data.status_code,
                    error_message=health_data.error_message,
                )
                async with db.pg_session_factory() as session:
                    await api_service.update_api_logs(session, update_logs)

                # --- 2. Then publish a self-describing event --------------------
                result = ProducerResultModal(
                    id=service.id,
                    name=service.name,
                    checked_at=health_data.checked_at,
                    response_time_ms=health_data.response_time_ms,
                    status_code=health_data.status_code,
                    expected_status_code=service.expected_status_code,
                    expected_latency_ms=service.expected_latency_ms,
                    is_healthy=is_healthy,
                    error_message=health_data.error_message,
                )
                sent = await producer_client.send_result(result)
                if not sent:
                    logger.warning(
                        f"Failed to send monitoring result to Kafka for service {service_name}")

                return True

            except asyncio.TimeoutError:
                logger.error(
                    f"Health check for {service_name} exceeded "
                    f"{Config.HEALTH_CHECK_TIMEOUT_S}s deadline and was abandoned.")
                return False
            except Exception as e:
                logger.error(
                    f"Error checking service {service_name}: {e}", exc_info=True)
                return False

    async def run_all_health_checks(self):
        """
        Main function executed by the scheduler.
        Fetches all active services and checks them CONCURRENTLY, bounded by a
        semaphore so a large endpoint count cannot exhaust sockets or the DB pool.
        """
        cycle_start = time.perf_counter()
        logger.info("--- Starting health check cycle ---")

        raw_services = await self.get_all_api()
        if not raw_services:
            logger.info("No active services to check in this cycle.")
            return

        sem = asyncio.Semaphore(Config.HEALTH_CHECK_CONCURRENCY)

        # return_exceptions=True gives failure isolation structurally: an exception
        # escaping _check_one comes back as a result object rather than cancelling
        # its siblings (which is what asyncio.TaskGroup would do).
        results = await asyncio.gather(
            *[self._check_one(s, sem) for s in raw_services],
            return_exceptions=True,
        )

        succeeded = sum(1 for r in results if r is True)
        failed = len(results) - succeeded
        elapsed = time.perf_counter() - cycle_start
        logger.info(
            f"--- Health check cycle complete in {elapsed:.1f}s | "
            f"total={len(results)} ok={succeeded} failed={failed} "
            f"concurrency={Config.HEALTH_CHECK_CONCURRENCY} ---"
        )
