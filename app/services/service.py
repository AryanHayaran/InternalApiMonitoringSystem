from sqlalchemy.ext.asyncio import AsyncSession
from ..utils.loggers import get_logger
from .shemas.service import latencyData, ApiServiceDetailModal

logger = get_logger("app")

class ApiService:

    async def get_services(self, user_uid: str, session: AsyncSession):
        query = """
            SELECT name, http_method, is_healthy, response_time_ms, status_code
            FROM monitored_endpoints
            JOIN health_check_logs
            ON monitored_endpoints.id = health_check_logs.endpoint_id
            WHERE owner_user_id = %s;
        """
        result = await session.execute(query, (user_uid,))
        return result.scalars().all()

    async def create_service(self, user_uid: str, api_service_data, session: AsyncSession):
        query = """
            INSERT INTO monitored_endpoints
            (name, http_method, url, request_headers, request_body, check_interval_seconds, expected_status_code, response_validation, owner_user_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
        """
        values = (
            api_service_data.name,
            api_service_data.http_method,
            str(api_service_data.url),
            str(api_service_data.request_headers) if api_service_data.request_headers else None,
            api_service_data.request_body,
            api_service_data.check_interval_seconds,
            api_service_data.expected_status_code,
            str(api_service_data.response_validation) if api_service_data.response_validation else None,
            user_uid
        )
        result = await session.execute(query, values)
        await session.commit()
        service_id = result.scalar_one()
        logger.info("User %s created service %s", user_uid, service_id)
        return service_id

async def get_service_detail_by_id(self, user_uid: str, service_id: int, session: AsyncSession):
    try:
        service_query = """
            SELECT id, name, http_method, url, request_headers, request_body,
                   check_interval_seconds, expected_status_code, response_validation
            FROM monitored_endpoints
            WHERE id = %s AND owner_user_id = %s;
        """
        service_result = await session.execute(service_query, (service_id, user_uid))
        service_row = service_result.fetchone()

        health_query = """
            SELECT is_healthy, last_checked, response_time_ms, status_code
            FROM health_check_logs
            JOIN monitored_endpoints ON health_check_logs.endpoint_id = monitored_endpoints.id
            WHERE monitored_endpoints.id = %s AND monitored_endpoints.owner_user_id = %s
            ORDER BY health_check_logs.timestamp DESC
            LIMIT 1;
        """
        health_result = await session.execute(health_query, (service_id, user_uid))
        health_row = health_result.fetchone()

        latencies_query = """
            SELECT timestamp, response_time_ms
            FROM incidents
            WHERE endpoint_id = %s
            ORDER BY timestamp DESC
            LIMIT 20;
        """
        latencies_result = await session.execute(latencies_query, (service_id,))
        latencies_rows = latencies_result.fetchall()

        data = dict(service_row._mapping)

        if health_row:
            data.update({
                "is_healthy": health_row.is_healthy,
                "last_checked": health_row.last_checked,
                "response_time_ms": health_row.response_time_ms,
                "status_code": health_row.status_code,
            })
        else:
            data.update({
                "is_healthy": False,
                "last_checked": None,
                "response_time_ms": None,
                "status_code": None,
            })

        data["latencies"] = [
            latencyData(timestamp=row.timestamp, response_time_ms=row.response_time_ms)
            for row in latencies_rows
        ]

        return ApiServiceDetailModal(**data)

    except Exception as e:
    



    async def update_service(self, user_uid: str, service_id: int, api_service_data, session: AsyncSession):
        query = """
            UPDATE monitored_endpoints
            SET name = %s,
                http_method = %s,
                url = %s,
                request_headers = %s,
                request_body = %s,
                check_interval_seconds = %s,
                expected_status_code = %s,
                response_validation = %s
            WHERE id = %s AND owner_user_id = %s;
        """
        values = (
            api_service_data.name,
            api_service_data.http_method,
            str(api_service_data.url),
            str(api_service_data.request_headers) if api_service_data.request_headers else None,
            api_service_data.request_body,
            api_service_data.check_interval_seconds,
            api_service_data.expected_status_code,
            str(api_service_data.response_validation) if api_service_data.response_validation else None,
            service_id,
            user_uid
        )
        result = await session.execute(query, values)
        if result.rowcount == 0:
            logger.warning("Update failed: Service %s not found or not owned by user %s", service_id, user_uid)
            raise ValueError("Service not found or not owned by user")
        await session.commit()
        logger.info("User %s updated service %s", user_uid, service_id)
        return await self.get_service_by_id(user_uid, service_id, session)

    async def delete_service(self, user_uid: str, service_id: int, session: AsyncSession):
        query = """
            DELETE FROM monitored_endpoints
            WHERE id = %s AND owner_user_id = %s;
        """
        result = await session.execute(query, (service_id, user_uid))
        if result.rowcount == 0:
            logger.warning("Delete failed: Service %s not found or not owned by user %s", service_id, user_uid)
            raise ValueError("Service not found or not owned by user")
        await session.commit()
        logger.info("User %s deleted service %s", user_uid, service_id)
        return True

    async def get_logs(self, user_uid: str, service_id: int, session: AsyncSession):
        query = """
            SELECT hcl.id, hcl.is_healthy, hcl.timestamp, hcl.response_time_ms, hcl.status_code, hcl.response_body, hcl.error_message
            FROM health_check_logs hcl
            JOIN monitored_endpoints me ON hcl.endpoint_id = me.id
            WHERE me.id = %s AND me.owner_user_id = %s
            ORDER BY hcl.timestamp DESC;
        """
        result = await session.execute(query, (service_id, user_uid))
        return result.scalars().all()

    async def get_incidents_logs(self, user_uid: str, service_id: int, session: AsyncSession):
        query = """
            SELECT il.incident_id, il.start_time, il.end_time, il.initial_error
            FROM incident_logs il
            JOIN monitored_endpoints me ON il.endpoint_id = me.id
            WHERE me.id = %s AND me.owner_user_id = %s
            ORDER BY il.start_time DESC;
        """
        result = await session.execute(query, (service_id, user_uid))
        return result.scalars().all()
