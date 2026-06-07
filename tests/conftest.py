# tests/conftest.py

import uuid
import os
import httpx
import pytest

# Retrieve the API Base URL from the environment, defaulting to localhost:8000
BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")


@pytest.fixture(scope="session")
def api_client():
    """
    Shared HTTP client for all tests.
    Created once for entire pytest run.
    """
    client = httpx.Client(
        base_url=BASE_URL,
        timeout=30.0
    )
    yield client
    client.close()


@pytest.fixture(scope="module")
def test_user():
    """
    Generates a unique user for signup tests.
    Fresh user for every test module.
    """
    unique_id = uuid.uuid4().hex[:8]
    return {
        "email": f"test_{unique_id}@gmail.com",
        "password": "Test@123",
        "full_name": "Pytest User"
    }


@pytest.fixture(scope="module")
def auth_token(api_client):
    """
    Creates one user and logs in once.
    Reused across all service tests.
    """

    # Signup
    payload = test_user()

    api_client.post(
        "/api/auth/signup",
        json=payload
    )

    # Login
    login_response = api_client.post(
        "/api/auth/login",
        json={
            "email": test_user["email"],
            "password": test_user["password"]
        }
    )

    assert login_response.status_code == 200

    token = login_response.json()["data"]["access_token"]

    return token


@pytest.fixture
def auth_headers(auth_token):
    """
    Authorization header used in all
    authenticated service APIs.
    """

    return {
        "Authorization": f"Bearer {auth_token}"
    }


@pytest.fixture(scope="session")
def service_id(api_client, auth_headers):
    """
    Creates a fresh service for each test.
    Returns service id.
    """

    payload = {
        "name": "Google Test Service",
        "url": "https://google.com",
        "method": "GET",
        "check_interval_seconds": 60
    }

    response = api_client.post(
        "/service",
        json=payload,
        headers=auth_headers
    )

    assert response.status_code in [200, 201]

    service_id = response.json()["data"]["service_id"]

    yield service_id
