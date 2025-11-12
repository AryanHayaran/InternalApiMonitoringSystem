from pydantic import BaseModel, HttpUrl, Field
from datetime import datetime
from typing import Optional, Dict, Any, Generic, TypeVar, List, Union
from pydantic.generics import GenericModel
from uuid import UUID

# ----------------------------
# Generic type for ApiResponse
# ----------------------------
DataT = TypeVar("DataT")

# ----------------------------
# Standard API Response Wrapper
# ----------------------------
class ApiResponse(GenericModel, Generic[DataT]):
    success: bool
    message: str
    data: Optional[DataT] = None

# ----------------------------
# Request / Input Models
# ----------------------------
class ApiServiceModal(BaseModel):
    name: str
    http_method: str = "GET"
    url: HttpUrl
    request_headers: Optional[Dict[str, str]] = None
    request_body: Optional[Any] = None
    periodic_summary_report: Optional[int] = 60
    expected_latency_ms: Optional[int] = 200
    expected_status_code: Optional[int] = 200
    response_validation: Optional[Dict[str, Any]] = None

class ApiProducerServiceModal(BaseModel):
    id: UUID
    name: str
    http_method: str = "GET"
    url: HttpUrl
    request_headers: Optional[Dict[str, str]] = None
    request_body: Optional[Any] = None
    periodic_summary_report: Optional[int] = 60
    expected_status_code: Optional[int] = 200
    response_validation: Optional[Dict[str, Any]] = None

# ----------------------------
# Response / Output Models
# ----------------------------
class ServiceIdResponse(BaseModel):
    service_id: str  # used for create/update responses

class ServicesResponse(BaseModel):  
    id: UUID
    name: str
    http_method: str
    is_healthy: Optional[bool] = None
    response_time_ms: Optional[int] = None

class ApiLogsModal(BaseModel):
    id: int
    is_healthy: bool
    checked_at: datetime
    response_time_ms: int
    response_body: Optional[str] = None
    error_message: Optional[str] = None

class ApiIncidentLogsModal(BaseModel):
    id: UUID
    start_time: datetime
    end_time: Optional[datetime] = None
    initial_error: str


class LatencyData(BaseModel):
    checked_at: datetime
    response_time_ms: Optional[int]

class ApiServiceDetailModal(BaseModel):
    id: UUID
    name: str
    http_method: str
    url: HttpUrl
    request_headers: Optional[Dict[str, str]] = None
    request_body: Optional[Dict[str, Any]] = None
    periodic_summary_report: int
    expected_status_code: int
    response_validation: Optional[Dict[str, Any]] = None
    expected_latency_ms: int

    # Health check info
    is_healthy: bool
    checked_at: Optional[datetime] = None
    response_time_ms: Optional[int] = None
    status_code: Optional[int] = None

    # Last 20 latencies for graphing
    last_20_latencies: List[LatencyData] = []
    
    
class ApiResponseModal(BaseModel):
    checked_at: datetime
    response_time_ms : Optional[int] = None
    status_code : Optional[int] = None
    response_body : Optional[Any] = None
    error_message : Optional[str] = None


class ProducerResultModal(BaseModel):
    id: UUID
    checked_at: datetime
    response_time_ms: Optional[int] = None
    status_code: Optional[int] = None

class ApiClientLogs(BaseModel):
    id: UUID
    checked_at: datetime
    response_time_ms: Optional[int] = None
    status_code: Optional[int] = None
    is_healthy: bool
    response_body : Optional[Any] = None
    error_message: Optional[str] = None

class ConsumerMonitoringData(BaseModel):
    id: UUID
    name: str
    http_method: str = "GET"
    expected_status_code: Optional[int] 
    expected_latency_ms: Optional[int] 

class ApiLastThreeRecords(BaseModel):
    id: int
    is_healthy: bool
    checked_at: datetime
    response_time_ms: int
    status_code: Optional[int]


