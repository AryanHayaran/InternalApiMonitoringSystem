import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from .utils.connect import db
from .routers import auth, service
from .middlewares.token_refresh import TokenRefreshMiddleware
from .services.monitoring import Producer
from .infrastructure.kafka.producer import producer_client
from .services.alert_scheduler import send_user_incident_alerts

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
scheduler = AsyncIOScheduler()
producer = Producer()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up application...")
    
    # Initialize database
    await db.init_db()
    
    # Initialize Kafka producer
    try:
        await producer_client.connect()
        logger.info("Kafka producer initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize Kafka producer: {e}")
        # In production, you might want to fail fast here
        # raise

    # Start scheduler
    scheduler.add_job(
        producer.run_all_health_checks,
        'interval',
        minutes=1,
        id="health_check_job"
    )

    # Add alert notification job every 30 minutes
    scheduler.add_job(
        send_user_incident_alerts,
        'interval',
        minutes=30,
        id="alert_scheduler_job"
    )

    scheduler.start()
    logger.info("Scheduler started with the health check job.")

    yield 

    logger.info("Shutting down application...")

    # Shutdown scheduler
    scheduler.shutdown()
    logger.info("Scheduler shut down gracefully.")
    
    # Close Kafka producer
    await producer_client.close()
    logger.info("Kafka producer closed.")
    
    # Close database
    await db.close_db()
    logger.info("Database connection closed.")

app = FastAPI(
    title="Internal API Monitoring System",
    description="API for monitoring internal services",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/api/docs",        # Swagger UI at /api/docs
    redoc_url="/api/redoc",      # ReDoc at /api/redoc
    openapi_url="/api/openapi.json"  # Raw OpenAPI spec
)

# 👇 Add this CORS setup
origins = [
    "http://localhost:5173",   
    "http://127.0.0.1:5173",  
    "http://localhost:5174",   
    "http://127.0.0.1:5174",  
    "https://685ed0ce8158.ngrok-free.app"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



app.add_middleware(TokenRefreshMiddleware)
app.include_router(auth.router, prefix="/api/auth", tags=["Auth"])
app.include_router(service.router, prefix="/api/services", tags=["Services"])
