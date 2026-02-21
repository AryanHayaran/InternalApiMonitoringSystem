from datetime import datetime
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
import json
from ..utils.loggers import get_logger
from ..schemas.service import ApiServiceModal,ApiProducerServiceModal, ApiClientLogs, ConsumerMonitoringData
from ..repositories.service_repository import ApiServiceRepository

logger = get_logger("app")


class ApiService:
    """Service class for managing API services."""

    async def get_services(self, user_uid: str, session: AsyncSession):
        """Fetch all monitored endpoints for a user with latest health info."""
        api_service_repository = ApiServiceRepository(user_uid,session)
        services = await api_service_repository.get_services()
        return services

    async def get_service_by_id(self, user_uid: str, service_id: str, session: AsyncSession):
        """Fetch a single monitored endpoint by ID for a user."""
        api_service_repository = ApiServiceRepository(user_uid,session)
        service = await api_service_repository.get_service_by_id(service_id)
        return service

    async def create_service(self, user_uid: str, api_service_data, session: AsyncSession):
        """Create a new monitored endpoint."""
        logger.info("User %s creating service %s", user_uid, api_service_data.name)
        api_service_repository = ApiServiceRepository(user_uid,session)
        service_id = await api_service_repository.create_service(api_service_data)
        return service_id

    async def get_service_detail_by_id(self, user_uid: str, service_id: str, session: AsyncSession):
        """Get detailed service info including last health check and last 20 latencies."""
        try:
            api_service_repository = ApiServiceRepository(user_uid,session)

            # --- Service info ---
            service = await api_service_repository.get_service_by_id(service_id)
            if not service:
                return None
            data = dict(service)

            # --- Latest health check ---
            latest_health_check = await api_service_repository.get_latest_health_check(service_id)

            if latest_health_check:
                data.update({
                    "is_healthy": latest_health_check["is_healthy"],
                    "checked_at": latest_health_check["checked_at"],
                    "response_time_ms": latest_health_check["response_time_ms"],
                    "status_code": latest_health_check["status_code"],
                })
            else:
                data.update({
                    "is_healthy": False,
                    "checked_at": None,
                    "response_time_ms": None,
                    "status_code": None,
                })

            # --- Last 20 latency records ---
            get_last_20_latencies = await api_service_repository.get_last_20_latencies(service_id)  

            logger.info(
                "Fetched %d latency records for service %s",
                len(get_last_20_latencies),
                service_id,
            )
            logger.debug("Latency records: %s", get_last_20_latencies)

            data["last_20_latencies"] = [
                {"checked_at": row["checked_at"], "response_time_ms": row["response_time_ms"]}
                for row in get_last_20_latencies
            ]

            return data

        except Exception as e:
            logger.error(
                "Error fetching service detail for %s: %s", service_id, e, exc_info=True
            )
            raise

    async def update_service(self, user_uid: str, service_id: str, api_service_data, session: AsyncSession):
        """Update service info."""
        api_service_repository = ApiServiceRepository(user_uid,session)
        updated = await api_service_repository.update_service(service_id, api_service_data)
        logger.info("User %s updated service %s", user_uid, service_id)
        return updated

    async def delete_service(self, user_uid: str, service_id: str, session: AsyncSession):
        """Delete a monitored endpoint."""
        api_service_repository = ApiServiceRepository(user_uid, session)
        deleted = await api_service_repository.delete_service(service_id)
        logger.info("User %s deleted service %s", user_uid, service_id)
        return deleted

    async def get_logs(self, user_uid: str, service_id: str, session: AsyncSession):
        """Fetch all health check logs for a service."""
        api_service_repository = ApiServiceRepository(user_uid, session)
        logs = await api_service_repository.get_logs(service_id)
        return logs

    async def get_incidents_logs(self, user_uid: str, service_id: str, session: AsyncSession):
        """Fetch all incident logs for a service."""
        api_service_repository = ApiServiceRepository(user_uid, session)
        service = await api_service_repository.get_incidents_logs(service_id)
        if not service:
            return None
        return service

    async def get_all_api_services(self, session: AsyncSession):
        """Fetch all monitored endpoints."""
        api_service_repository = ApiServiceRepository(session=session)
        services = await api_service_repository.get_all_services()
        
        # Pydantic will serialize the dictionary list correctly into ApiProducerServiceModal
        # at the router/monitoring service layer.
        return services

    async def update_api_logs(self, session: AsyncSession, data: ApiClientLogs):
        """
        Inserts a new health check log into the database.
        """
        api_service_repository = ApiServiceRepository(session=session)
        await api_service_repository.update_api_logs(data)

    async def getConsumerServiceDetails(self, session: AsyncSession, service_id: str):
        """Fetch a single monitored endpoint by ID for a user."""
        api_service_repository = ApiServiceRepository(session=session)
        service = await api_service_repository.get_consumer_service(service_id)
        if not service:
            return None
        return service

    async def getApiLastThreeRecords(self, session: AsyncSession, service_id: str):
        """Fetch the last 3 health check logs for a given service."""
        api_service_repository = ApiServiceRepository(session=session)
        logs = await api_service_repository.get_last_three_records(service_id)
        return logs

    async def createOrUpdateIncident(self, session: AsyncSession, endpoint_id: str, last_three_records, reason: str):
        """Create or update incident record for failure or latency, using initial_error to determine type."""
        try:
            api_service_repository = ApiServiceRepository(session=session)
            # Error text mapping
            error_message = (
                "The API failed to respond successfully for three consecutive checks, indicating a possible outage or functional issue."
                if reason == "failure"
                else
                "The API response time exceeded the expected performance threshold for three consecutive checks, suggesting performance degradation or server slowdown."
            )

            # Start & end times from last 3 checks
            start_time = last_three_records[-1]["checked_at"]
            end_time   = last_three_records[0]["checked_at"]

            # Fetch last incident for this endpoint
            last_incident = await api_service_repository.get_last_incident(endpoint_id)

            if last_incident:
                last_end = last_incident.end_time
                last_error = last_incident.initial_error or ""

                # Only merge if last incident type matches current reason
                if last_error == error_message and (last_end is None or start_time <= last_end):
                    update_incident = await api_service_repository.update_incident(last_incident.id, end_time, error_message)

                    logger.info(f"{update_incident} Incident updated for endpoint {endpoint_id} ({reason})")
                else:
                    # Different type or ended → create new incident
                    insert_incident = await api_service_repository.insert_incident(endpoint_id, start_time, end_time, error_message)
                    logger.info(f"{insert_incident} New incident created for endpoint {endpoint_id} ({reason})")    
            else:
                # No incident ever → create first one
                insert_incident = await api_service_repository.insert_incident(endpoint_id, start_time, end_time, error_message)
                logger.info(f"{insert_incident} New incident created for endpoint {endpoint_id} ({reason})")    

            await session.commit()

        except Exception as e:
            logger.error(f"Error creating/updating incident: {e}", exc_info=True)
            await session.rollback()
 
    async def get_monitored_apis(self, session: AsyncSession):
        """Fetch all monitored APIs with user info."""
        api_service_repository = ApiServiceRepository(session=session)
        return await api_service_repository.get_all_api_services()

    async def get_incidents_since(self, session: AsyncSession, api_id: str, since_time: datetime):
        """Fetch incidents that happened after a given time."""
        api_service_repository = ApiServiceRepository(session=session)
        incidents = await api_service_repository.get_incidents_since(api_id, since_time)
        return incidents
        


    async def update_last_checked(self, session: AsyncSession, api_id: str):
        """Update last_checked_at for a monitored API after sending alert."""
        api_service_repository = ApiServiceRepository(session=session)
        await api_service_repository.update_last_checked(api_id)
        await session.commit()
