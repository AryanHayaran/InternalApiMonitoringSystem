import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, patch
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import AsyncSession
import asyncio

from app.main import app
from app.utils.connect import db
from app.services.auth import UserServices
from app.schemas.auth import UserCreate

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest_asyncio.fixture(scope="session", autouse=True)
async def init_db_connection():
    """Initialize DB connection once for the entire test session."""
    await db.init_db()
    yield
    await db.close_db()

@pytest_asyncio.fixture()
async def session():
    """
    Yields an AsyncSession wrapped in a transaction that is rolled back after the test.
    This prevents test data from polluting the real database.
    """
    async with db.pg_engine.connect() as conn:
        trans = await conn.begin()
        async_session = AsyncSession(conn, expire_on_commit=False)
        yield async_session
        await trans.rollback()
        await async_session.close()

@pytest_asyncio.fixture()
async def test_client(session):
    """
    FastAPI TestClient that overrides the database dependency.
    """
    async def override_get_db():
        yield session

    app.dependency_overrides[db.get_db_session] = override_get_db

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client

    app.dependency_overrides.clear()

@pytest.fixture(autouse=True)
def mock_kafka():
    """Mock Kafka producer methods globally so tests don't try to connect to Kafka."""
    with patch("app.infrastructure.kafka.producer.KafkaProducerClient.connect", new_callable=AsyncMock) as mock_connect:
        with patch("app.infrastructure.kafka.producer.KafkaProducerClient.send_result", new_callable=AsyncMock) as mock_send:
            with patch("app.infrastructure.kafka.producer.KafkaProducerClient.close", new_callable=AsyncMock) as mock_close:
                mock_send.return_value = True
                yield

@pytest.fixture(autouse=True)
def mock_mail():
    """Mock BrevoSMTP email sending globally."""
    with patch("app.utils.mail.aiosmtplib.send", new_callable=AsyncMock) as mock_send:
        yield mock_send

@pytest_asyncio.fixture()
async def test_user_headers(test_client, session):
    """Creates a user, logs them in, and returns authorization headers."""
    user_data = {
        "full_name": "Test User",
        "email": "testuser@example.com",
        "password": "StrongPassword123!"
    }
    # Create user
    api_services = UserServices()
    await api_services.create_user(UserCreate(**user_data), session)

    # Login
    response = await test_client.post("/api/auth/login", json={
        "email": user_data["email"],
        "password": user_data["password"]
    })
    data = response.json()
    access_token = data["data"]["access_token"]
    refresh_token = data["data"]["refresh_token"]

    return {
        "headers": {"Authorization": f"Bearer {access_token}"},
        "refresh_token": refresh_token,
        "user_data": user_data,
        "uid": data["data"]["uid"]
    }
