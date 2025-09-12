from fastapi import APIRouter
from .endpoints import auth
from .endpoints import services

router = APIRouter()

router.include_router(auth.router, prefix="/api/auth", tags=["Health Check"])
router.include_router(services.router, prefix="/api/services", tags=["Services"])

