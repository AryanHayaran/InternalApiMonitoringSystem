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
    user = {
        "email": f"test_{uuid.uuid4().hex[:8]}@gmail.com",
        "password": "Test@123",
        "full_name": "Pytest User"
    }

    api_client.post(
        "/api/auth/signup",
        json=user
    )

    # Login via FastAPI's /api/auth prefix
    login_response = api_client.post(
        "/api/auth/login",
        json={
            "email": user["email"],
            "password": user["password"]
        }
    )

    assert login_response.status_code == 200

    token = login_response.json()["data"]["access_token"]

    return token


@pytest.fixture(scope="module")  # Changed scope to module to allow service_id to use it
def auth_headers(auth_token):
    """
    Authorization header used in all
    authenticated service APIs.
    """
    return {
        "Authorization": f"Bearer {auth_token}"
    }


@pytest.fixture(scope="module")
def service_id(api_client, auth_headers):
    """
    Creates a fresh service for each test.
    Returns service id.
    """
    request_body = {
        "name": "DummyJSON Auth Login Service",
        "http_method": "POST",
        "url": "https://dummyjson.com/auth/login",
        "request_headers": {
            "Content-Type": "application/json",
            "Accept": "application/json"
        },
        "request_body": {
            "username": "emilys",
            "password": "emilyspass"
        },
        "periodic_summary_report": 30,
        "expected_latency_ms": 500,
        "expected_status_code": 200,
        "response_validation": {
            "json_path": "$.accessToken",
            "expected_value": "null"
        }
    }

    response = api_client.post(
        "/api/services/service",
        json=request_body,
        headers=auth_headers
    )

    assert response.status_code == 201

    service_id = response.json()["data"]["service_id"]
    yield service_id
