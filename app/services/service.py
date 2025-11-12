from datetime import datetime
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
import json
from ..utils.loggers import get_logger
from ..schemas.service import ApiServiceModal,ApiProducerServiceModal, ApiClientLogs, ConsumerMonitoringData

logger = get_logger("app")


class ApiService:
    """Service class for managing API services."""

    async def get_services(self, user_uid: str, session: AsyncSession):
        """Fetch all monitored endpoints for a user with latest health info."""
        query = text("""
            SELECT me.id,me.name, me.http_method, hcl.is_healthy, hcl.response_time_ms
            FROM monitored_endpoints me
            LEFT JOIN LATERAL (
                SELECT *
                FROM health_check_logs h
                WHERE h.endpoint_id = me.id
                ORDER BY h.checked_at DESC
                LIMIT 1
            ) hcl ON TRUE
            WHERE me.owner_user_id = :user_uid;
        """)
        result = await session.execute(query, {"user_uid": user_uid})
        rows = result.fetchall()
        services = [dict(row._mapping) for row in rows]
        return services

    async def get_service_by_id(self, user_uid: str, service_id: str, session: AsyncSession):
        """Fetch a single monitored endpoint by ID for a user."""
        query = text("""
            SELECT name, http_method, url, request_headers, request_body,
                periodic_summary_report, expected_status_code, response_validation,expected_latency_ms 
            FROM monitored_endpoints
            WHERE id = :service_id AND owner_user_id = :user_uid;
        """)
        result = await session.execute(query, {"service_id": service_id, "user_uid": user_uid})
        row_data = result.fetchone()
        return row_data

    async def create_service(self, user_uid: str, api_service_data, session: AsyncSession):
        """Create a new monitored endpoint."""
        query = text("""
            INSERT INTO monitored_endpoints
            (name, http_method, url, request_headers, request_body, periodic_summary_report,
             expected_status_code, response_validation, owner_user_id,expected_latency_ms)
            VALUES (:name, :http_method, :url, :request_headers, :request_body,
                    :periodic_summary_report, :expected_status_code, :response_validation, :owner_user_id, :expected_latency_ms)
            RETURNING id;
        """)
        values = {
            "name": api_service_data.name,
            "http_method": api_service_data.http_method or "GET",
            "url": str(api_service_data.url),
            "request_headers": json.dumps(api_service_data.request_headers) if api_service_data.request_headers else None,
            "request_body": json.dumps(api_service_data.request_body, ensure_ascii=False) if api_service_data.request_body is not None else None,
            "periodic_summary_report": api_service_data.periodic_summary_report or 60,
            "expected_status_code": api_service_data.expected_status_code or 200,
            "response_validation": json.dumps(api_service_data.response_validation) if api_service_data.response_validation else None,
            "owner_user_id": user_uid,
            "expected_latency_ms" : api_service_data.expected_latency_ms or 200
        }
        result = await session.execute(query, values)
        await session.commit()
        service_id = result.scalar_one()
        logger.info("User %s created service %s", user_uid, service_id)
        return {"id": service_id}

    async def get_service_detail_by_id(self, user_uid: str, service_id: str, session: AsyncSession):
        """Get detailed service info including last health check and last 20 latencies."""
        try:
            # --- Service info ---
            service_query = text("""
                SELECT id, name, http_method, url, request_headers, request_body,
                       periodic_summary_report, expected_status_code, response_validation,
                       expected_latency_ms 
                FROM monitored_endpoints
                WHERE id = :service_id AND owner_user_id = :user_uid;
            """)
            service_result = await session.execute(
                service_query, {"service_id": service_id, "user_uid": user_uid}
            )
            service_row = service_result.fetchone()
            if not service_row:
                return None

            data = dict(service_row._mapping)

            # --- Latest health check ---
            health_query = text("""
                SELECT is_healthy, checked_at, response_time_ms, status_code
                FROM health_check_logs
                WHERE endpoint_id = :service_id
                ORDER BY checked_at DESC
                LIMIT 1;
            """)
            health_result = await session.execute(health_query, {"service_id": service_id})
            health_row = health_result.fetchone()

            if health_row:
                data.update({
                    "is_healthy": health_row.is_healthy,
                    "checked_at": health_row.checked_at,
                    "response_time_ms": health_row.response_time_ms,
                    "status_code": health_row.status_code,
                })
            else:
                data.update({
                    "is_healthy": False,
                    "checked_at": None,
                    "response_time_ms": None,
                    "status_code": None,
                })

            # --- Last 20 latency records ---
            latencies_query = text("""
                SELECT checked_at, response_time_ms
                FROM health_check_logs
                WHERE endpoint_id = :service_id
                ORDER BY checked_at DESC
                LIMIT 20;
            """)
            latencies_result = await session.execute(latencies_query, {"service_id": service_id})
            latencies_rows = latencies_result.fetchall()

            logger.info(
                "Fetched %d latency records for service %s",
                len(latencies_rows),
                service_id,
            )
            logger.debug("Latency records: %s", latencies_rows)

            data["last_20_latencies"] = [
                {"checked_at": row.checked_at, "response_time_ms": row.response_time_ms}
                for row in latencies_rows
            ]

            return data

        except Exception as e:
            logger.error(
                "Error fetching service detail for %s: %s", service_id, e, exc_info=True
            )
            raise

    async def update_service(self, user_uid: str, service_id: str, api_service_data, session: AsyncSession):
        """Update service info."""
        query = text("""
            UPDATE monitored_endpoints
            SET name = :name,
                http_method = :http_method,
                url = :url,
                request_headers = :request_headers,
                request_body = :request_body,
                periodic_summary_report = :periodic_summary_report,
                expected_status_code = :expected_status_code,
                response_validation = :response_validation,
                updated_at = NOW(),
                expected_latency_ms  = :expected_latency_ms 
            WHERE id = :service_id AND owner_user_id = :user_uid
            RETURNING id;
        """)

        values = {
            "name": api_service_data.name,
            "http_method": api_service_data.http_method or "GET",
            "url": str(api_service_data.url),
            "request_headers": json.dumps(api_service_data.request_headers) if api_service_data.request_headers else None,
            "request_body": json.dumps(api_service_data.request_body, ensure_ascii=False) if api_service_data.request_body is not None else None,
            "periodic_summary_report": api_service_data.periodic_summary_report or 60,
            "expected_status_code": api_service_data.expected_status_code or 200,
            "response_validation": json.dumps(api_service_data.response_validation) if api_service_data.response_validation else None,
            "expected_latency_ms": api_service_data.expected_latency_ms,
            "service_id": service_id,
            "user_uid": user_uid
        }

        result = await session.execute(query, values)
        updated_row = result.fetchone()  # ✅ use scalar instead of fetchone()

        if not updated_row:
            logger.warning(
                "Update failed: Service %s not found or not owned by user %s", service_id, user_uid)
            raise ValueError("Service not found or not owned by user")

        await session.commit()
        logger.info("User %s updated service %s", user_uid, service_id)

        return {"service_id": str(updated_row[0])}

    async def delete_service(self, user_uid: str, service_id: str, session: AsyncSession):
        """Delete a monitored endpoint."""
        query = text("""
            DELETE FROM monitored_endpoints
            WHERE id = :service_id AND owner_user_id = :user_uid
            RETURNING id;
        """)
        result = await session.execute(query, {"service_id": service_id, "user_uid": user_uid})
        deleted_row = result.scalar_one_or_none()
        if not deleted_row:
            logger.warning(
                "Delete failed: Service %s not found or not owned by user %s", service_id, user_uid)
            raise ValueError("Service not found or not owned by user")
        await session.commit()
        logger.info("User %s deleted service %s", user_uid, service_id)
        return deleted_row

    async def get_logs(self, user_uid: str, service_id: str, session: AsyncSession):
        """Fetch all health check logs for a service."""
        query = text("""
            SELECT hcl.id, hcl.is_healthy, hcl.checked_at, hcl.response_time_ms, hcl.status_code,
                   hcl.response_body, hcl.error_message
            FROM health_check_logs hcl
            JOIN monitored_endpoints me ON hcl.endpoint_id = me.id
            WHERE me.id = :service_id AND me.owner_user_id = :user_uid
            ORDER BY hcl.checked_at DESC;
        """)
        result = await session.execute(query, {"service_id": service_id, "user_uid": user_uid})
        return [dict(row._mapping) for row in result.fetchall()]

    async def get_incidents_logs(self, user_uid: str, service_id: str, session: AsyncSession):
        """Fetch all incident logs for a service."""
        query = text("""
            SELECT il.id, il.start_time, il.end_time, il.initial_error
            FROM incidents il
            JOIN monitored_endpoints me ON il.endpoint_id = me.id
            WHERE me.id = :service_id AND me.owner_user_id = :user_uid
            ORDER BY il.start_time DESC;
        """)
        result = await session.execute(query, {"service_id": service_id, "user_uid": user_uid})
        return [dict(row._mapping) for row in result.fetchall()]

    async def get_all_api_services(self, session: AsyncSession):
        """Fetch all monitored endpoints."""
        query = text("""
            SELECT  id, name, http_method, url, request_headers, request_body,
                   periodic_summary_report, expected_status_code, response_validation
            FROM monitored_endpoints;
        """)
        result = await session.execute(query)
        rows = result.fetchall()

        services = []
        for row in rows:
            data = dict(row._mapping)

            for key in ["request_headers", "request_body", "response_validation"]:
                if data.get(key):
                    try:
                        data[key] = json.loads(data[key])
                    except (TypeError, json.JSONDecodeError):
                        pass

            services.append(ApiProducerServiceModal(**data))

        return services

    async def update_api_logs(self, session: AsyncSession, data: ApiClientLogs):
        """
        Inserts a new health check log into the database.
        """
        query = text("""
            INSERT INTO health_check_logs
                (endpoint_id, checked_at, is_healthy, response_time_ms, status_code, response_body, error_message)
            VALUES
                (:endpoint_id, :checked_at, :is_healthy, :response_time_ms, :status_code, :response_body, :error_message)
        """)

        await session.execute(query, {
            "endpoint_id": data.id,            # or data.endpoint_id if your schema has it
            "checked_at": data.checked_at,
            "is_healthy": data.is_healthy,
            "response_time_ms": data.response_time_ms,
            "status_code": data.status_code,
            "response_body": str(data.response_body) if data.response_body else None,
            "error_message": str(data.error_message) if data.error_message else None
        })

        await session.commit()

    async def getConsumerServiceDetails(self, session: AsyncSession, service_id: str):
        """Fetch a single monitored endpoint by ID for a user."""
        query = text("""
            SELECT id, name, http_method,
                 expected_status_code, expected_latency_ms 
            FROM monitored_endpoints
            WHERE id = :service_id;
        """)
        result = await session.execute(query, {"service_id": service_id})
        row_data = result.fetchone()
        return row_data

    async def getApiLastThreeRecords(self, session: AsyncSession, service_id: str):
        """Fetch the last 3 health check logs for a given service."""
        query = text("""
            SELECT id, checked_at, response_time_ms, status_code, is_healthy
            FROM health_check_logs 
            WHERE endpoint_id = :service_id
            ORDER BY checked_at DESC 
            LIMIT 3;
        """)
        result = await session.execute(query, {"service_id": service_id})
        rows = result.fetchall()
        return [dict(row._mapping) for row in rows]

    async def createOrUpdateIncident(self, session: AsyncSession, endpoint_id: str, last_three_records, reason: str):
        """Create or update incident record for failure or latency, using initial_error to determine type."""
        try:
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
            last_incident_query = text("""
                SELECT id, start_time, end_time, initial_error
                FROM incidents
                WHERE endpoint_id = :endpoint_id
                ORDER BY start_time DESC
                LIMIT 1;
            """)
            result = await session.execute(last_incident_query, {"endpoint_id": endpoint_id})
            last_incident = result.fetchone()

            if last_incident:
                last_end = last_incident.end_time
                last_error = last_incident.initial_error or ""

                # Only merge if last incident type matches current reason
                if last_error == error_message and (last_end is None or start_time <= last_end):
                    update_query = text("""
                        UPDATE incidents
                        SET end_time = :new_end_time,
                            initial_error = :initial_error
                        WHERE id = :id;
                    """)
                    await session.execute(update_query, {
                        "new_end_time": end_time,
                        "initial_error": error_message,
                        "id": last_incident.id
                    })
                    logger.info(f"Incident updated for endpoint {endpoint_id} ({reason})")
                else:
                    # Different type or ended → create new incident
                    insert_query = text("""
                        INSERT INTO incidents (endpoint_id, start_time, end_time, initial_error)
                        VALUES (:endpoint_id, :start_time, :end_time, :initial_error);
                    """)
                    await session.execute(insert_query, {
                        "endpoint_id": endpoint_id,
                        "start_time": start_time,
                        "end_time": end_time,
                        "initial_error": error_message
                    })
                    logger.info(f"New incident created for endpoint {endpoint_id} ({reason})")
            else:
                # No incident ever → create first one
                insert_query = text("""
                    INSERT INTO incidents (endpoint_id, start_time, end_time, initial_error)
                    VALUES (:endpoint_id, :start_time, :end_time, :initial_error);
                """)
                await session.execute(insert_query, {
                    "endpoint_id": endpoint_id,
                    "start_time": start_time,
                    "end_time": end_time,
                    "initial_error": error_message
                })
                logger.info(f"First incident created for endpoint {endpoint_id} ({reason})")

            await session.commit()

        except Exception as e:
            logger.error(f"Error creating/updating incident: {e}", exc_info=True)
            await session.rollback()
 
    async def get_monitored_apis(self, session: AsyncSession):
        """Fetch all monitored APIs with user info."""
        query = text("""
            SELECT m.id AS api_id, m.owner_user_id, m.periodic_summary_report, m.last_checked_at,
                   u.email AS user_email, u.full_name AS user_name
            FROM monitored_endpoints m
            JOIN users u ON m.owner_user_id = u.id
            WHERE m.is_active = TRUE;
        """)
        result = await session.execute(query)
        return result.mappings().all()

    async def get_incidents_since(self, session: AsyncSession, api_id: str, since_time: datetime):
        """Fetch incidents that happened after a given time."""
        query = text("""
            SELECT id, start_time, end_time, initial_error
            FROM incidents
            WHERE endpoint_id = :api_id AND start_time >= :since_time;
        """)
        result = await session.execute(query, {"api_id": api_id, "since_time": since_time})
        return result.mappings().all()

    async def update_last_checked(self, session: AsyncSession, api_id: str):
        """Update last_checked_at for a monitored API after sending alert."""
        query = text("""
            UPDATE monitored_endpoints
            SET last_checked_at = :now
            WHERE id = :api_id;
        """)
        await session.execute(query, {"now": datetime.utcnow(), "api_id": api_id})
        await session.commit()