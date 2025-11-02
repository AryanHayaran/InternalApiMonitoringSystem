from fastapi import APIRouter, Depends, Response
from app.core.security import get_current_user_uid
from ..utils.connect import db
from ..services.service import ApiService
from ..utils.loggers import get_logger
from typing import List
from ..schemas.service import (
    ApiServiceModal,
    ServicesResponse,
    ApiLogsModal,
    ApiIncidentLogsModal,
    ApiResponse,
    ApiServiceDetailModal,
    ServiceIdResponse
)
import json
router = APIRouter()
api_services = ApiService()
get_db_session = db.get_db_session
logger = get_logger()

@router.get("/health", response_model=ApiResponse[dict])
async def health_check(response: Response):
    response.status_code = 200
    return {
        "success": True,
        "message": "Health check OK",
        "data": {"status": "ok"}
    }


@router.get("/", response_model=ApiResponse[List[ServicesResponse]])
async def get_all_services(
    response: Response,
    user_uid: str = Depends(get_current_user_uid),
    session=Depends(get_db_session)
):
    try:
        services = await api_services.get_services(user_uid, session)
        response.status_code = 200
        return {
            "success": True,
            "message": "Services fetched successfully",
            "data": services
        }
    except Exception as e:
        logger.error("Error fetching services for user %s: %s", user_uid, e, exc_info=True)
        response.status_code = 500
        return {
            "success": False,
            "message": f"Error fetching services: {str(e)}",
            "data": None
        }


@router.post("/service", response_model=ApiResponse[dict])
async def create_new_service(
    response: Response,
    api_service_data: ApiServiceModal,
    user_uid: str = Depends(get_current_user_uid),
    session=Depends(get_db_session)
):
    try:
        logger.info("Creating service for user %s with data %s", user_uid, api_service_data)
        service_id = await api_services.create_service(user_uid, api_service_data, session)
        logger.info("User %s created service %s", user_uid, service_id)
        response.status_code = 201
        return {
            "success": True,
            "message": "Service created successfully",
            "data": {"service_id": service_id["id"]}
        }
    except Exception as e:
        logger.error("Error creating service for user %s: %s", user_uid, e, exc_info=True)
        response.status_code = 500
        return {
            "success": False,
            "message": f"Error creating service: {str(e)}",
            "data": None
        }


@router.get("/service/{service_id}", response_model=ApiResponse[ApiServiceModal])
async def get_service(
    response: Response,
    service_id: str,
    user_uid: str = Depends(get_current_user_uid),
    session=Depends(get_db_session)
):
    try:
        service_data = await api_services.get_service_by_id(user_uid, service_id, session)
        logger.debug("Fetched service data for service %s: %s", service_id, service_data)
        response.status_code = 200
        return {
            "success": True,
            "message": "Service details fetched successfully",
            "data": service_data
        }
    except Exception as e:
        logger.error("Error fetching service %s for user %s: %s", service_id, user_uid, e, exc_info=True)
        response.status_code = 500
        return {
            "success": False,
            "message": f"Error fetching service details: {str(e)}",
            "data": None
        }
        


@router.get("/service_details/{service_id}", response_model=ApiResponse[ApiServiceDetailModal])
async def get_service_details(
    response: Response,
    service_id: str,
    user_uid: str = Depends(get_current_user_uid),
    session=Depends(get_db_session)
):
    try:
        logger.info("Fetching detailed info for service %s for user %s", service_id, user_uid)
        service_data = await api_services.get_service_detail_by_id(user_uid, service_id, session)
        
                
        response.status_code = 200
        return {
            "success": True,
            "message": "Service details fetched successfully",
            "data": service_data
        }
    except Exception as e:
        logger.error("Error fetching service %s for user %s: %s", service_id, user_uid, e, exc_info=True)
        response.status_code = 500
        return {
            "success": False,
            "message": f"Error fetching service details: {str(e)}",
            "data": None
        }


@router.put("/service/{service_id}", response_model=ApiResponse[ServiceIdResponse])
async def update_service(
    response: Response,
    service_id: str,
    api_service_data: ApiServiceModal,
    user_uid: str = Depends(get_current_user_uid),
    session=Depends(get_db_session)
):
    try:
        updated_service = await api_services.update_service(user_uid, service_id, api_service_data, session)
        logger.info("User %s updated service %s", user_uid, service_id)
        response.status_code = 200
        return {
            "success": True,
            "message": "Service updated successfully",
            "data": updated_service
        }
    except Exception as e:
        logger.error("Error updating service %s for user %s: %s", service_id, user_uid, e, exc_info=True)
        response.status_code = 500
        return {
            "success": False,
            "message": f"Error updating service: {str(e)}",
            "data": None
        }


@router.delete("/service/{service_id}", response_model=ApiResponse[dict])
async def delete_service(
    response: Response,
    service_id: str,
    user_uid: str = Depends(get_current_user_uid),
    session=Depends(get_db_session)
):
    try:
        await api_services.delete_service(user_uid, service_id, session)
        logger.info("User %s deleted service %s", user_uid, service_id)
        response.status_code = 200
        return {
            "success": True,
            "message": "Service deleted successfully",
            "data": {}
        }
    except Exception as e:
        logger.error("Error deleting service %s for user %s: %s", service_id, user_uid, e, exc_info=True)
        response.status_code = 500
        return {
            "success": False,
            "message": f"Error deleting service: {str(e)}",
            "data": None
        }


@router.get("/service/{service_id}/logs", response_model=ApiResponse[List[ApiLogsModal]])
async def get_service_logs(
    response: Response,
    service_id: str,
    user_uid: str = Depends(get_current_user_uid),
    session=Depends(get_db_session)
):
    try:
        logs_data = await api_services.get_logs(user_uid, service_id, session)
        response.status_code = 200
        return {
            "success": True,
            "message": "Service logs fetched successfully",
            "data": logs_data
        }
    except Exception as e:
        logger.error("Error fetching logs for service %s user %s: %s", service_id, user_uid, e, exc_info=True)
        response.status_code = 500
        return {
            "success": False,
            "message": f"Error fetching service logs: {str(e)}",
            "data": None
        }


@router.get("/service/{service_id}/incident-logs", response_model=ApiResponse[List[ApiIncidentLogsModal]])
async def get_service_history(
    response: Response,
    service_id: str,
    user_uid: str = Depends(get_current_user_uid),
    session=Depends(get_db_session)
):
    try:
        incident_logs_data = await api_services.get_incidents_logs(user_uid, service_id, session)
        response.status_code = 200
        return {
            "success": True,
            "message": "Incident logs fetched successfully",
            "data": incident_logs_data
        }
    except Exception as e:
        logger.error("Error fetching incident logs for service %s user %s: %s", service_id, user_uid, e, exc_info=True)
        response.status_code = 500
        return {
            "success": False,
            "message": f"Error fetching incident logs: {str(e)}",
            "data": None
        }
