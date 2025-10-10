from fastapi import APIRouter, Request, Response,Depends
import jwt

from app.core.config import Config
from app.core.security import get_current_user_uid
from ..schemas.service import ApiServiceModal, ApiServiceDetailModal,ApiServiceResponse,ServicesResponse,ApiLogsModal,ApiIncidentLogsModal,ApiResponse
from ..utils.connect import db
from ..services.service import ApiService
from ..utils.response_handler import success_response, error_response
from ..utils.loggers import get_logger
from typing import List

from ..schemas.service import (
    ApiServiceModal,
    ServicesResponse,
    ApiLogsModal,
    ApiIncidentLogsModal,
    ApiResponse
)

router = APIRouter()
api_services = ApiService()
get_db_session = db.get_db_session
logger = get_logger()


# ----------------------------
# Health check
# ----------------------------
@router.get("/health", response_model=ApiResponse[dict])
async def health_check():
    return success_response({"status": "ok"}, "Health check OK", 200)


# ----------------------------
# Get all services
# ----------------------------
@router.get("/", response_model=ApiResponse[List[ServicesResponse]])
async def get_all_services(
    user_uid: str = Depends(get_current_user_uid),
    session=Depends(get_db_session)
):
    try:
        services = await api_services.get_services(user_uid, session)
        return success_response(services, "Services fetched successfully")
    except Exception as e:
        logger.error("Error fetching services for user %s: %s", user_uid, e, exc_info=True)
        return error_response(f"Error fetching services: {str(e)}", 500)


# ----------------------------
# Create new service
# ----------------------------
@router.post("/services", response_model=ApiResponse[dict])
async def create_new_service(
    api_service_data: ApiServiceModal,
    user_uid: str = Depends(get_current_user_uid),
    session=Depends(get_db_session)
):
    try:
        service_id = await api_services.create_service(user_uid, api_service_data, session)
        logger.info("User %s created service %s", user_uid, service_id)
        return success_response({"service_id": service_id}, "Service created successfully", 201)
    except Exception as e:
        logger.error("Error creating service for user %s: %s", user_uid, e, exc_info=True)
        return error_response(f"Error creating service: {str(e)}", 500)


# ----------------------------
# Get service 
# ----------------------------
@router.get("/service/{service_id}", response_model=ApiResponse[ApiServiceModal])
async def get_service(
    service_id: int,
    user_uid: str = Depends(get_current_user_uid),
    session=Depends(get_db_session)
):
    try:
        service_data = await api_services.get_service_by_id(user_uid, service_id, session)
        return success_response(service_data, "Service details fetched successfully")
    except Exception as e:
        logger.error("Error fetching service %s for user %s: %s", service_id, user_uid, e, exc_info=True)
        return error_response(f"Error fetching service details: {str(e)}", 500)


# ----------------------------
# Get service details
# ----------------------------
@router.get("/service_details/{service_id}", response_model=ApiResponse[ApiServiceDetailModal])
async def get_service(
    service_id: int,
    user_uid: str = Depends(get_current_user_uid),
    session=Depends(get_db_session)
):
    try:
        service_data = await api_services.get_service_detail_by_id(user_uid, service_id, session)
        return success_response(service_data, "Service details fetched successfully")
    except Exception as e:
        logger.error("Error fetching service %s for user %s: %s", service_id, user_uid, e, exc_info=True)
        return error_response(f"Error fetching service details: {str(e)}", 500)

# ----------------------------
# Update service
# ----------------------------
@router.put("/services/{service_id}", response_model=ApiResponse[ApiServiceModal])
async def update_service(
    service_id: int,
    api_service_data: ApiServiceModal,
    user_uid: str = Depends(get_current_user_uid),
    session=Depends(get_db_session)
):
    try:
        updated_service = await api_services.update_service(user_uid, service_id, api_service_data, session)
        logger.info("User %s updated service %s", user_uid, service_id)
        return success_response(updated_service, "Service updated successfully")
    except Exception as e:
        logger.error("Error updating service %s for user %s: %s", service_id, user_uid, e, exc_info=True)
        return error_response(f"Error updating service: {str(e)}", 500)


# ----------------------------
# Delete service
# ----------------------------
@router.delete("/services/{service_id}", response_model=ApiResponse[dict])
async def delete_service(
    service_id: int,
    user_uid: str = Depends(get_current_user_uid),
    session=Depends(get_db_session)
):
    try:
        await api_services.delete_service(user_uid, service_id, session)
        logger.info("User %s deleted service %s", user_uid, service_id)
        return success_response({}, "Service deleted successfully")
    except Exception as e:
        logger.error("Error deleting service %s for user %s: %s", service_id, user_uid, e, exc_info=True)
        return error_response(f"Error deleting service: {str(e)}", 500)


# ----------------------------
# Get service logs
# ----------------------------
@router.get("/services/{service_id}/logs", response_model=ApiResponse[List[ApiLogsModal]])
async def get_service_logs(
    service_id: int,
    user_uid: str = Depends(get_current_user_uid),
    session=Depends(get_db_session)
):
    try:
        logs_data = await api_services.get_logs(user_uid, service_id, session)
        return success_response(logs_data, "Service logs fetched successfully")
    except Exception as e:
        logger.error("Error fetching logs for service %s user %s: %s", service_id, user_uid, e, exc_info=True)
        return error_response(f"Error fetching service logs: {str(e)}", 500)


# ----------------------------
# Get service incident logs
# ----------------------------
@router.get("/services/{service_id}/incident-logs", response_model=ApiResponse[List[ApiIncidentLogsModal]])
async def get_service_history(
    service_id: int,
    user_uid: str = Depends(get_current_user_uid),
    session=Depends(get_db_session)
):
    try:
        incident_logs_data = await api_services.get_incidents_logs(user_uid, service_id, session)
        return success_response(incident_logs_data, "Incident logs fetched successfully")
    except Exception as e:
        logger.error("Error fetching incident logs for service %s user %s: %s", service_id, user_uid, e, exc_info=True)
        return error_response(f"Error fetching incident logs: {str(e)}", 500)

