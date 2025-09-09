import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from .api.router import router
from .db.session import db

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

app.include_router(router)

