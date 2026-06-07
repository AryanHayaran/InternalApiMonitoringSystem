import pytest

@pytest.mark.asyncio
async def test_signup_user(test_client):
    response = await test_client.post("/api/auth/signup", json={
        "full_name": "New User",
        "email": "newuser@example.com",
        "password": "Password123!"
    })
    assert response.status_code == 201
    data = response.json()
    assert data["success"] is True
    assert data["data"]["email"] == "newuser@example.com"
    assert "uid" in data["data"]

@pytest.mark.asyncio
async def test_signup_duplicate_email(test_client, test_user_headers):
    user_email = test_user_headers["user_data"]["email"]
    response = await test_client.post("/api/auth/signup", json={
        "full_name": "Another User",
        "email": user_email,
        "password": "Password123!"
    })
    assert response.status_code == 400
    assert response.json()["success"] is False

@pytest.mark.asyncio
async def test_login_success(test_client, test_user_headers):
    user_data = test_user_headers["user_data"]
    response = await test_client.post("/api/auth/login", json={
        "email": user_data["email"],
        "password": user_data["password"]
    })
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert "access_token" in data["data"]
    assert "refresh_token" in data["data"]

@pytest.mark.asyncio
async def test_login_invalid_credentials(test_client, test_user_headers):
    user_data = test_user_headers["user_data"]
    response = await test_client.post("/api/auth/login", json={
        "email": user_data["email"],
        "password": "WrongPassword!"
    })
    assert response.status_code == 401
    assert response.json()["success"] is False

@pytest.mark.asyncio
async def test_refresh_token(test_client, test_user_headers):
    refresh_token = test_user_headers["refresh_token"]
    response = await test_client.post("/api/auth/refresh", json={
        "refresh_token": refresh_token
    })
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert "access_token" in data["data"]

@pytest.mark.asyncio
async def test_logout(test_client, test_user_headers):
    headers = test_user_headers["headers"]
    response = await test_client.post("/api/auth/logout", headers=headers)
    assert response.status_code == 200
    assert response.json()["success"] is True
    
    # Verify the refresh token is deleted by trying to refresh again
    refresh_token = test_user_headers["refresh_token"]
    refresh_response = await test_client.post("/api/auth/refresh", json={
        "refresh_token": refresh_token
    })
    assert refresh_response.status_code == 401
