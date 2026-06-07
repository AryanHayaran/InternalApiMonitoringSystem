import pytest

@pytest.mark.asyncio
async def test_health_check(test_client):
    response = await test_client.get("/api/services/health")
    assert response.status_code == 200
    assert response.json()["success"] is True
    assert response.json()["data"]["status"] == "ok"

@pytest.mark.asyncio
async def test_create_service(test_client, test_user_headers):
    headers = test_user_headers["headers"]
    response = await test_client.post("/api/services/service", headers=headers, json={
        "name": "Test API",
        "url": "https://httpbin.org/get",
        "http_method": "GET",
        "expected_status_code": 200,
        "expected_latency_ms": 1000,
        "periodic_summary_report": 24,
        "is_active": True
    })
    
    assert response.status_code == 201
    data = response.json()
    assert data["success"] is True
    assert "service_id" in data["data"]

@pytest.mark.asyncio
async def test_get_all_services(test_client, test_user_headers):
    headers = test_user_headers["headers"]
    # Create one first
    await test_client.post("/api/services/service", headers=headers, json={
        "name": "Test API 2",
        "url": "https://httpbin.org/get",
        "http_method": "GET",
        "expected_status_code": 200,
        "expected_latency_ms": 1000,
        "periodic_summary_report": 24,
        "is_active": True
    })
    
    response = await test_client.get("/api/services/", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert isinstance(data["data"], list)
    assert len(data["data"]) >= 1

@pytest.mark.asyncio
async def test_get_service_by_id(test_client, test_user_headers):
    headers = test_user_headers["headers"]
    create_resp = await test_client.post("/api/services/service", headers=headers, json={
        "name": "Test API 3",
        "url": "https://httpbin.org/get",
        "http_method": "GET",
        "expected_status_code": 200,
        "expected_latency_ms": 1000,
        "periodic_summary_report": 24,
        "is_active": True
    })
    service_id = create_resp.json()["data"]["service_id"]
    
    response = await test_client.get(f"/api/services/service/{service_id}", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["data"]["name"] == "Test API 3"

@pytest.mark.asyncio
async def test_update_service(test_client, test_user_headers):
    headers = test_user_headers["headers"]
    create_resp = await test_client.post("/api/services/service", headers=headers, json={
        "name": "Test API 4",
        "url": "https://httpbin.org/get",
        "http_method": "GET",
        "expected_status_code": 200,
        "expected_latency_ms": 1000,
        "periodic_summary_report": 24,
        "is_active": True
    })
    service_id = create_resp.json()["data"]["service_id"]
    
    update_resp = await test_client.put(f"/api/services/service/{service_id}", headers=headers, json={
        "name": "Updated API",
        "url": "https://httpbin.org/get",
        "http_method": "GET",
        "expected_status_code": 201,
        "expected_latency_ms": 500,
        "periodic_summary_report": 12,
        "is_active": False
    })
    
    assert update_resp.status_code == 200
    data = update_resp.json()
    assert data["success"] is True

@pytest.mark.asyncio
async def test_delete_service(test_client, test_user_headers):
    headers = test_user_headers["headers"]
    create_resp = await test_client.post("/api/services/service", headers=headers, json={
        "name": "Test API 5",
        "url": "https://httpbin.org/get",
        "http_method": "GET",
        "expected_status_code": 200,
        "expected_latency_ms": 1000,
        "periodic_summary_report": 24,
        "is_active": True
    })
    service_id = create_resp.json()["data"]["service_id"]
    
    del_resp = await test_client.delete(f"/api/services/service/{service_id}", headers=headers)
    assert del_resp.status_code == 200
    assert del_resp.json()["success"] is True
    
    # Verify it's gone
    get_resp = await test_client.get(f"/api/services/service/{service_id}", headers=headers)
    assert get_resp.status_code == 500  # Based on error handling in router, missing may throw 500 or 404. It returns False success.
    assert get_resp.json()["success"] is False
