from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import json
from ..utils.loggers import get_logger

logger = get_logger("app")


class ApiService:
    """Service class for managing API services."""

    async def get_services(self, user_uid: str, session: AsyncSession) :
        """Fetch all monitored endpoints for a user with latest health info."""
        query = text("""
            SELECT me.name, me.http_method, hcl.is_healthy, hcl.response_time_ms
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
                   check_interval_seconds, expected_status_code, response_validation
            FROM monitored_endpoints
            WHERE id = :service_id AND owner_user_id = :user_uid;
        """)
        result = await session.execute(query, {"service_id": service_id, "user_uid": user_uid})
        return result.fetchone() 
    
    async def create_service(self, user_uid: str, api_service_data, session: AsyncSession):
        """Create a new monitored endpoint."""
        query = text("""
            INSERT INTO monitored_endpoints
            (name, http_method, url, request_headers, request_body, check_interval_seconds,
             expected_status_code, response_validation, owner_user_id)
            VALUES (:name, :http_method, :url, :request_headers, :request_body,
                    :check_interval_seconds, :expected_status_code, :response_validation, :owner_user_id)
            RETURNING id;
        """)
        values = {
            "name": api_service_data.name,
            "http_method": api_service_data.http_method,
            "url": str(api_service_data.url),
            "request_headers": json.dumps(api_service_data.request_headers) if api_service_data.request_headers else None,
            "request_body": api_service_data.request_body,
            "check_interval_seconds": api_service_data.check_interval_seconds,
            "expected_status_code": api_service_data.expected_status_code,
            "response_validation": json.dumps(api_service_data.response_validation) if api_service_data.response_validation else None,
            "owner_user_id": user_uid
        }
        result = await session.execute(query, values)
        await session.commit()
        service_id = result.scalar_one()
        logger.info("User %s created service %s", user_uid, service_id)
        return {"id": service_id}

    async def get_service_detail_by_id(self, user_uid: str, service_id: str, session: AsyncSession):
        """Get detailed service info including last health check and last 20 latencies."""
        try:
            # Service info
            service_query = text("""
                SELECT id, name, http_method, url, request_headers, request_body,
                       check_interval_seconds, expected_status_code, response_validation
                FROM monitored_endpoints
                WHERE id = :service_id AND owner_user_id = :user_uid;
            """)
            service_result = await session.execute(service_query, {"service_id": service_id, "user_uid": user_uid})
            service_row = service_result.fetchone()
            if not service_row:
                return None
            data = dict(service_row._mapping)

            # Latest health
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

            # Last 20 latencies
            latencies_query = text("""
                SELECT checked_at, response_time_ms
                FROM health_check_logs
                WHERE endpoint_id = :service_id
                ORDER BY checked_at DESC
                LIMIT 20;
            """)
            latencies_result = await session.execute(latencies_query, {"service_id": service_id})
            latencies_rows = latencies_result.fetchall()
            logger.info("Fetched %d latency records for service %s", latencies_rows, service_id)
            data["last_20_latencies"] = [{"checked_at": row.checked_at, "response_time_ms": row.response_time_ms} for row in latencies_rows]

            return data

        except Exception as e:
            logger.error("Error fetching service detail for %s: %s", service_id, e, exc_info=True)
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
                check_interval_seconds = :check_interval_seconds,
                expected_status_code = :expected_status_code,
                response_validation = :response_validation,
                updated_at = NOW()
            WHERE id = :service_id AND owner_user_id = :user_uid
            RETURNING id;
        """)

        values = {
            "name": api_service_data.name,
            "http_method": api_service_data.http_method,
            "url": str(api_service_data.url),
            "request_headers": json.dumps(api_service_data.request_headers) if api_service_data.request_headers else None,
            "request_body": api_service_data.request_body,
            "check_interval_seconds": api_service_data.check_interval_seconds,
            "expected_status_code": api_service_data.expected_status_code,
            "response_validation": json.dumps(api_service_data.response_validation) if api_service_data.response_validation else None,
            "service_id": service_id,
            "user_uid": user_uid
        }

        result = await session.execute(query, values)
        updated_row = result.scalar_one_or_none()  # ✅ use scalar instead of fetchone()

        if not updated_row:
            logger.warning("Update failed: Service %s not found or not owned by user %s", service_id, user_uid)
            raise ValueError("Service not found or not owned by user")

        await session.commit()
        logger.info("User %s updated service %s", user_uid, service_id)

        return {"service_id": str(updated_row)}

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
            logger.warning("Delete failed: Service %s not found or not owned by user %s", service_id, user_uid)
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
