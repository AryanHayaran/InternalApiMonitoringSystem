from fastapi import APIRouter
from .endpoints import health
from .endpoints import services

router = APIRouter()

router.include_router(health.router, prefix="/api/health", tags=["Health Check"])
router.include_router(services.router, prefix="/api/services", tags=["Services"])
