from fastapi.responses import JSONResponse
from typing import TypeVar, Generic, Optional
from app.schemas.service import ApiResponse

DataT = TypeVar("DataT")

def success_response(data: DataT, message: str = "Request successful", status_code: int = 200) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content=ApiResponse(success=True, message=message, status_code=status_code, data=data).model_dump()
    )

def error_response(message: str, status_code: int = 400, data: Optional[DataT] = None) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content=ApiResponse(success=False, message=message, status_code=status_code, data=data).model_dump()
    )
