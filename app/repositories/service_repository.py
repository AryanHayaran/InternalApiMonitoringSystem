from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select
from sqlalchemy import true as sa_true, update, delete
from app.db.models import MonitoredEndpoints, HealthCheckLogs, Incidents, Users


class ApiServiceRepository:
    def __init__(self, user_uid: str = None, session: AsyncSession = None):
        self.user_uid = user_uid
        self.session = session

    async def get_services(self):
        # Create a lateral subquery to fetch the latest health check log per endpoint
        hcl_subq = (
            select(
                HealthCheckLogs.endpoint_id,
                HealthCheckLogs.is_healthy,
                HealthCheckLogs.response_time_ms
            )
            .where(HealthCheckLogs.endpoint_id == MonitoredEndpoints.id)
            .order_by(HealthCheckLogs.checked_at.desc())
            .limit(1)
            .lateral("hcl")
        )

        query = (
            select(
                MonitoredEndpoints.id,
                MonitoredEndpoints.name,
                MonitoredEndpoints.http_method,
                hcl_subq.c.is_healthy,
                hcl_subq.c.response_time_ms
            )
            .outerjoin_from(MonitoredEndpoints, hcl_subq, sa_true())
            .where(MonitoredEndpoints.owner_user_id == self.user_uid)
        )

        result = await self.session.execute(query)
        rows = result.fetchall()
        
        services = []
        for row in rows:
            services.append({
                "id": str(row.id),
                "name": row.name,
                "http_method": row.http_method,
                "is_healthy": row.is_healthy,
                "response_time_ms": row.response_time_ms
            })
            
        return services

    async def get_service_by_id(self, service_id: str):
        query = select(
            MonitoredEndpoints.id,
            MonitoredEndpoints.name, 
            MonitoredEndpoints.http_method, 
            MonitoredEndpoints.url, 
            MonitoredEndpoints.request_headers, 
            MonitoredEndpoints.request_body,
            MonitoredEndpoints.periodic_summary_report, 
            MonitoredEndpoints.expected_status_code, 
            MonitoredEndpoints.response_validation,
            MonitoredEndpoints.expected_latency_ms 
        ).where(
            MonitoredEndpoints.id == service_id,
            MonitoredEndpoints.owner_user_id == self.user_uid
        )
        result = await self.session.execute(query)
        row = result.mappings().first()
        return dict(row) if row else None

    async def create_service(self, api_service_data):
        new_service = MonitoredEndpoints(
            name=api_service_data.name,
            http_method=api_service_data.http_method or "GET",
            url=str(api_service_data.url),
            request_headers=api_service_data.request_headers,
            request_body=api_service_data.request_body,
            periodic_summary_report=api_service_data.periodic_summary_report or 60,
            expected_status_code=api_service_data.expected_status_code or 200,
            response_validation=api_service_data.response_validation,
            owner_user_id=self.user_uid,
            expected_latency_ms=api_service_data.expected_latency_ms or 200
        )
        self.session.add(new_service)
        await self.session.commit()
        await self.session.refresh(new_service)
        return {"id": str(new_service.id)}

    async def get_latest_health_check(self, service_id: str):
        query = select(
            HealthCheckLogs.is_healthy,
            HealthCheckLogs.response_time_ms,
            HealthCheckLogs.checked_at,
            HealthCheckLogs.status_code
        ).where(
            HealthCheckLogs.endpoint_id == service_id,
        ).order_by(HealthCheckLogs.checked_at.desc()).limit(1)
        result = await self.session.execute(query)
        row = result.mappings().first()
        return dict(row) if row else None   

    async def get_last_20_latencies(self, service_id: str):
        query = select(
            HealthCheckLogs.checked_at,
            HealthCheckLogs.response_time_ms
        ).where(
            HealthCheckLogs.endpoint_id == service_id,
        ).order_by(HealthCheckLogs.checked_at.desc()).limit(20)
        result = await self.session.execute(query)
        rows = result.mappings().all()
        return [dict(row) for row in rows]

    async def update_service(self, service_id: str, api_service_data):
        from sqlalchemy import func
        query = (
            update(MonitoredEndpoints)
            .where(
                MonitoredEndpoints.id == service_id,
                MonitoredEndpoints.owner_user_id == self.user_uid
            )
            .values(
                name=api_service_data.name,
                http_method=api_service_data.http_method or "GET",
                url=str(api_service_data.url),
                request_headers=api_service_data.request_headers,
                request_body=api_service_data.request_body,
                periodic_summary_report=api_service_data.periodic_summary_report or 60,
                expected_status_code=api_service_data.expected_status_code or 200,
                response_validation=api_service_data.response_validation,
                expected_latency_ms=api_service_data.expected_latency_ms,
                updated_at=func.now()
            )
            .returning(MonitoredEndpoints.id)
        )
        
        result = await self.session.execute(query)
        updated_row = result.scalar_one_or_none()
        
        if not updated_row:
            raise ValueError("Service not found or not owned by user")
            
        await self.session.commit()
        return {"service_id": str(updated_row)}

    async def delete_service(self, service_id: str):
        query = (
            delete(MonitoredEndpoints)
            .where(
                MonitoredEndpoints.id == service_id,
                MonitoredEndpoints.owner_user_id == self.user_uid
            )
            .returning(MonitoredEndpoints.id)
        )
        result = await self.session.execute(query)
        deleted_row = result.scalar_one_or_none()
        
        if not deleted_row:
            raise ValueError("Service not found or not owned by user")
            
        await self.session.commit() 
        return deleted_row

    async def get_logs(self, service_id: str):
        import uuid
        try:
            # Validate UUID before querying to prevent asyncpg.exceptions.DataError
            uuid.UUID(service_id)
        except ValueError:
            return []

        query = select(
            HealthCheckLogs.id,
            HealthCheckLogs.is_healthy,
            HealthCheckLogs.checked_at,
            HealthCheckLogs.response_time_ms,
            HealthCheckLogs.status_code,
            HealthCheckLogs.response_body,
            HealthCheckLogs.error_message
        ).join(
            MonitoredEndpoints, HealthCheckLogs.endpoint_id == MonitoredEndpoints.id
        ).where(
            MonitoredEndpoints.id == service_id,
            MonitoredEndpoints.owner_user_id == self.user_uid
        ).order_by(HealthCheckLogs.checked_at.desc()).limit(30)

        result = await self.session.execute(query)
        rows = result.mappings().all()
        
        return [{**row, "id": str(row["id"])} for row in rows]

    async def get_incidents_logs(self,service_id:str):
        query = select(
            Incidents.id,
            Incidents.start_time,
            Incidents.end_time,
            Incidents.initial_error
        ).join(
            MonitoredEndpoints, Incidents.endpoint_id == MonitoredEndpoints.id
        ).where(
            MonitoredEndpoints.id == service_id,
            MonitoredEndpoints.owner_user_id == self.user_uid
        ).order_by(Incidents.start_time.desc())
        result = await self.session.execute(query)
        rows = result.mappings().all()
        return [dict(row) for row in rows]  


    async def get_all_services(self):
        query = select(
            MonitoredEndpoints.id,
            MonitoredEndpoints.name,
            MonitoredEndpoints.http_method,
            MonitoredEndpoints.url,
            MonitoredEndpoints.request_headers,
            MonitoredEndpoints.request_body,
            MonitoredEndpoints.periodic_summary_report,
            MonitoredEndpoints.expected_status_code,
            MonitoredEndpoints.response_validation,
            MonitoredEndpoints.owner_user_id,
            MonitoredEndpoints.expected_latency_ms,
            MonitoredEndpoints.created_at,
            MonitoredEndpoints.updated_at
        )
        result = await self.session.execute(query)
        rows = result.mappings().all()
        return [dict(row) for row in rows] 

    async def update_api_logs(self, data):
        query = HealthCheckLogs(    
            endpoint_id=data.id,
            checked_at=data.checked_at,
            is_healthy=data.is_healthy,
            response_time_ms=data.response_time_ms,
            status_code=data.status_code,
            response_body=str(data.response_body) if data.response_body else None,
            error_message=str(data.error_message) if data.error_message else None
        )
        self.session.add(query)
        await self.session.commit()
        return {"id": str(query.id)}

    async def get_consumer_service(self, service_id: str):
        query = select(
            MonitoredEndpoints.id,
            MonitoredEndpoints.name,
            MonitoredEndpoints.http_method,
            MonitoredEndpoints.expected_status_code,
            MonitoredEndpoints.expected_latency_ms,
        ).where(
            MonitoredEndpoints.id == service_id,
            MonitoredEndpoints.owner_user_id == self.user_uid
        )
        result = await self.session.execute(query)
        row = result.mappings().first()
        return dict(row) if row else None 

    async def get_last_three_records(self, service_id: str):
        query = select(
            HealthCheckLogs.id,
            HealthCheckLogs.checked_at,
            HealthCheckLogs.response_time_ms,
            HealthCheckLogs.status_code,
            HealthCheckLogs.is_healthy
        ).join(
            MonitoredEndpoints, HealthCheckLogs.endpoint_id == MonitoredEndpoints.id
        ).where(
            MonitoredEndpoints.id == service_id,
            MonitoredEndpoints.owner_user_id == self.user_uid
        ).order_by(HealthCheckLogs.checked_at.desc()).limit(3)
        result = await self.session.execute(query)
        rows = result.mappings().all()
        return [dict(row) for row in rows]  

    async def get_last_incident(self, endpoint_id: str):
        query = select(
            Incidents.id,
            Incidents.start_time,
            Incidents.end_time,
            Incidents.initial_error
        ).where(
            Incidents.endpoint_id == endpoint_id
        ).order_by(Incidents.start_time.desc()).limit(1)
        result = await self.session.execute(query)
        row = result.mappings().first()
        return dict(row) if row else None 

    async def update_incident(self, incident_id: str, end_time: str, error_message: str):
        query = (
            update(Incidents)
            .where(Incidents.id == incident_id)
            .values(
                end_time=end_time,
                initial_error=error_message
            )
            .returning(Incidents.id)
        )
        result = await self.session.execute(query)
        updated_row = result.scalar_one_or_none()
        return {"incident_id": str(updated_row)}

    async def insert_incident(self, endpoint_id: str, start_time: str, end_time: str,error_message: str):
        query = Incidents(
            endpoint_id=endpoint_id,
            start_time=start_time,
            end_time=end_time,
            initial_error=error_message
        )
        self.session.add(query)
        await self.session.commit()
        return {"incident_id": str(query.id)}

    async def get_all_api_services(self):
        query = (
            select(
                MonitoredEndpoints.id.label("api_id"),
                MonitoredEndpoints.owner_user_id,
                MonitoredEndpoints.periodic_summary_report,
                MonitoredEndpoints.last_checked_at,
                Users.email.label("user_email"),
                Users.full_name.label("user_name")
            )
            .select_from(MonitoredEndpoints)
            .join(Users, MonitoredEndpoints.owner_user_id == Users.id)
            .where(MonitoredEndpoints.is_active == True)
        )
        result = await self.session.execute(query)
        rows = result.mappings().all()
        return [dict(row) for row in rows]

    async def get_incidents_since(self, api_id: str, since_time: str):
        query = select(
            Incidents.id,
            Incidents.start_time,
            Incidents.end_time,
            Incidents.initial_error
        ).where(
            Incidents.endpoint_id == api_id,
            Incidents.start_time >= since_time
        ).order_by(Incidents.start_time.desc())
        result = await self.session.execute(query)
        rows = result.mappings().all()
        return [dict(row) for row in rows]

    async def update_last_checked(self, api_id: str):
        query = (
            update(MonitoredEndpoints)
            .where(MonitoredEndpoints.id == api_id)
            .values(
                last_checked_at=func.now()
            )
            .returning(MonitoredEndpoints.id)
        )
        result = await self.session.execute(query)
        updated_row = result.scalar_one_or_none()
        return {"api_id": str(updated_row)}

    

