from app.utils.loggers import get_logger
import time
from datetime import datetime
import httpx
from typing import Optional, Any
from app.schemas.service import ApiServiceModal, ApiResponseModal
import json

logger = get_logger()


from app.utils.loggers import get_logger
import time
from datetime import datetime
import httpx
from typing import Optional, Any
from app.schemas.service import ApiServiceModal, ApiResponseModal

logger = get_logger()

async def check_api_health(service: ApiServiceModal) -> ApiResponseModal:
    """
    Perform a single API health check for the given service.
    Handles various HTTP methods, parses JSON/text responses,
    measures latency, and returns a structured ApiResponseModal.
    Always returns response_body as a dict or list.
    """

    checked_at = datetime.utcnow()
    start_time = time.perf_counter()
    status_code: Optional[int] = None
    response_body: Optional[Any] = None
    error_message: Optional[str] = None
    method = (service.http_method or "GET").upper()
    headers = service.request_headers or {}
    body = service.request_body

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0)) as client:
            request_kwargs: dict[str, Any] = {"headers": headers}

            if body is not None:
                if isinstance(body, (dict, list)):
                    request_kwargs["json"] = body
                else:
                    request_kwargs["content"] = str(body)

            response = await client.request(method, str(service.url), **request_kwargs)
            status_code = response.status_code
            try:
                json_data = response.json()
                json_str = json.dumps(json_data, ensure_ascii=False)
                if len(json_str) > 100_000:
                    response_body = {
                        "_truncated": True,
                        "preview": json_data 
                    }
                else:
                    response_body = json_data

            except Exception:
                text = response.text
                response_body = {
                    "_raw_text": text[:100_000] + "...[truncated]" if len(text) > 100_000 else text
                }

    except httpx.TimeoutException:
        error_message = "Request timed out"
        logger.warning(f" Timeout for {service.name} ({service.url})")

    except httpx.RequestError as e:
        error_message = f"Request failed: {e}"
        logger.error(f"Network error for {service.name}: {e}")

    except Exception as e:
        error_message = f"Unexpected error: {e}"
        logger.exception(f"Unexpected error while hitting {service.name}")

    finally:
        response_time_ms = int((time.perf_counter() - start_time) * 1000)

    return ApiResponseModal(
        checked_at=checked_at,
        response_time_ms=response_time_ms,
        status_code=status_code,
        response_body=response_body,
        error_message=error_message,
    )

