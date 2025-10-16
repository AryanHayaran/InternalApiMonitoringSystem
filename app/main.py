import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlmodel import SQLModel
from .utils.connect import db
from .routers import auth, service
from .middlewares.token_refresh import TokenRefreshMiddleware
from .db import models 

from .infrastructure.kafka.producer import run_all_health_checks

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up application...")
    await db.init_db()

    scheduler.add_job(
        run_all_health_checks,
        'interval',
        minutes=1,
        id="health_check_job"
    )

    scheduler.start()
    logger.info("Scheduler started with the health check job.")

    yield # --- The application is now running ---

    logger.info("Shutting down application...")

    scheduler.shutdown()
    logger.info("Scheduler shut down gracefully.")
    await db.close_db()
    logger.info("Database connection closed.")

app = FastAPI(
    title="Internal API Monitoring System",
    description="API for monitoring internal services",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(TokenRefreshMiddleware)
app.include_router(auth.router, prefix="/api/auth", tags=["Auth"])
app.include_router(service.router, prefix="/api/services", tags=["Services"])
