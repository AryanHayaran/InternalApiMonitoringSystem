import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from .utils.connect import db
from .routers import auth, service
from .middlewares.token_refresh import TokenRefreshMiddleware

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up...")
    await db.init_db()
    yield
    logger.info("Shutting down...")
    await db.close_db()

app=FastAPI(
    title="Internal API Monitoring System",
    description="API for monitoring internal services",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(TokenRefreshMiddleware)
app.include_router(auth.router, prefix="/api/auth", tags=["Auth"])
app.include_router(service.router, prefix="/api/services", tags=["Services"])
