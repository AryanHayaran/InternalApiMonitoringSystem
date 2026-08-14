# SERVICE_001 Create Service
# SERVICE_002 Create Service Invalid Token
# SERVICE_003 Create Service Missing Fields
# SERVICE_004 Get Service
# SERVICE_005 Update Service
# SERVICE_006 Verify Updated Service
# SERVICE_007 Get All Services
# SERVICE_008 Get All Services Invalid Token
# SERVICE_009 Get Service Details
# SERVICE_010 Get Logs
# SERVICE_011 Get Incident Logs
# SERVICE_012 Delete Service
# SERVICE_013 Get Deleted Service

import time
import pytest

def test_SERVICE_001_create_service(api_client, auth_headers):
    print(f"\n--- [SERVICE_001] CREATE SERVICE ---")
    print(f"Request: POST /api/services/service")
    request_body = {
        "name": "DummyJSON Auth Login",
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
    service_response = api_client.post("/api/services/service", json=request_body, headers=auth_headers)
    
    print(f"Response Status: {service_response.status_code}")
    print(f"Response Body: {service_response.json()}")
    
    assert service_response.status_code == 201
    assert service_response.json()["success"] is True
    assert service_response.json()["data"]["service_id"] is not None
    print("Result: PASSED")
    
    
def test_SERVICE_002_create_service_invalid_token(api_client):
    print(f"\n--- [SERVICE_002] CREATE SERVICE INVALID TOKEN ---")
    print(f"Request: POST /api/services/service")
    request_body = {
        "name": "DummyJSON Auth Login",
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
    # Call without auth headers
    service_response = api_client.post("/api/services/service", json=request_body)
    
    print(f"Response Status: {service_response.status_code}")
    print(f"Response Body: {service_response.json()}")
    
    assert service_response.status_code == 401
    assert "detail" in service_response.json()
    print("Result: PASSED")
    
    
def test_SERVICE_003_create_service_missing_fields(api_client, auth_headers):
    print(f"\n--- [SERVICE_003] CREATE SERVICE MISSING FIELDS ---")
    print(f"Request: POST /api/services/service")
    # Payload is missing required field 'url'
    request_body = {
        "name": "DummyJSON Auth Login",
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
    service_response = api_client.post("/api/services/service", json=request_body, headers=auth_headers)
    
    print(f"Response Status: {service_response.status_code}")
    print(f"Response Body: {service_response.json()}")
    
    # Missing required field triggers a 422 Unprocessable Entity in FastAPI
    assert service_response.status_code == 422
    assert "detail" in service_response.json()
    print("Result: PASSED")
    
    
def test_SERVICE_004_get_service(api_client, auth_headers, service_id):
    print(f"\n--- [SERVICE_004] GET SERVICE ---")
    print(f"Request: GET /api/services/service/{service_id}")
    service_response = api_client.get(f"/api/services/service/{service_id}", headers=auth_headers)
    
    print(f"Response Status: {service_response.status_code}")
    print(f"Response Body: {service_response.json()}")
    
    assert service_response.status_code == 200
    assert service_response.json()["success"] is True
    # The output ApiServiceModal contains service details like 'name', but does not return a 'service_id' field inside 'data'
    assert service_response.json()["data"]["name"] == "DummyJSON Auth Login Service"
    print("Result: PASSED")
    
    
def test_SERVICE_005_update_service(api_client, auth_headers, service_id):
    print(f"\n--- [SERVICE_005] UPDATE SERVICE ---")
    print(f"Request: PUT /api/services/service/{service_id}")
    request_body = {
        "name": "DummyJSON Auth Login updated",
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
        "periodic_summary_report": 60,
        "expected_status_code": 200,
        "expected_latency_ms": 700,
        "response_validation": {
            "json_path": "$.accessToken",
            "expected_value": "null"
        }
    }
    service_response = api_client.put(f"/api/services/service/{service_id}", json=request_body, headers=auth_headers)
    
    print(f"Response Status: {service_response.status_code}")
    print(f"Response Body: {service_response.json()}")
    
    assert service_response.status_code == 200
    assert service_response.json()["success"] is True
    assert service_response.json()["data"]["service_id"] == service_id
    print("Result: PASSED")
    
    
def test_SERVICE_006_get_all_services(api_client, auth_headers):
    print(f"\n--- [SERVICE_006] GET ALL SERVICES ---")
    print(f"Request: GET /api/services/")
    service_response = api_client.get("/api/services/", headers=auth_headers)
    
    print(f"Response Status: {service_response.status_code}")
    print(f"Response Body: {service_response.json()}")
    
    assert service_response.status_code == 200
    assert service_response.json()["success"] is True
    assert isinstance(service_response.json()["data"], list)
    print("Result: PASSED")
    


def test_SERVICE_007_get_all_services_invalid_token(api_client):
    print(f"\n--- [SERVICE_007] GET ALL SERVICES INVALID TOKEN ---")
    print(f"Request: GET /api/services/")
    service_response = api_client.get("/api/services/")
    
    print(f"Response Status: {service_response.status_code}")
    print(f"Response Body: {service_response.json()}")
    
    assert service_response.status_code == 401
    assert "detail" in service_response.json()
    print("Result: PASSED")
    
    
def test_SERVICE_008_get_service_details(api_client, auth_headers, service_id):
    print(f"\n--- [SERVICE_008] GET SERVICE DETAILS ---")
    print(f"Request: GET /api/services/service_details/{service_id}")
    service_response = api_client.get(f"/api/services/service_details/{service_id}", headers=auth_headers)
    
    print(f"Response Status: {service_response.status_code}")
    print(f"Response Body: {service_response.json()}")
    
    assert service_response.status_code == 200
    assert service_response.json()["success"] is True
    assert service_response.json()["data"]["id"] == service_id
    print("Result: PASSED")
    
    
def test_SERVICE_009_get_service_details_invalid_token(api_client, service_id):
    print(f"\n--- [SERVICE_009] GET SERVICE DETAILS INVALID TOKEN ---")
    print(f"Request: GET /api/services/service_details/{service_id}")
    service_response = api_client.get(f"/api/services/service_details/{service_id}")
    
    print(f"Response Status: {service_response.status_code}")
    print(f"Response Body: {service_response.json()}")
    
    assert service_response.status_code == 401
    assert "detail" in service_response.json()
    print("Result: PASSED")
    
    
def test_SERVICE_010_get_service_logs(api_client, auth_headers, service_id):
    print(f"\n--- [SERVICE_010] GET SERVICE LOGS ---")
    print(f"Request: GET /api/services/service/{service_id}/logs")
    # Wait to allow background checks to populate logs
    print("Sleeping for 65 seconds to allow background check execution...")
    time.sleep(65)
    service_response = api_client.get(f"/api/services/service/{service_id}/logs", headers=auth_headers)
    
    print(f"Response Status: {service_response.status_code}")
    print(f"Response Body: {service_response.json()}")
    
    assert service_response.status_code == 200
    assert service_response.json()["success"] is True
    assert service_response.json()["data"] is not None
    print("Result: PASSED")
    
    
def test_SERVICE_011_get_incident_logs(api_client, auth_headers, service_id):
    print(f"\n--- [SERVICE_011] GET INCIDENT LOGS ---")
    print(f"Request: GET /api/services/service/{service_id}/incident-logs")
    service_response = api_client.get(f"/api/services/service/{service_id}/incident-logs", headers=auth_headers)
    
    print(f"Response Status: {service_response.status_code}")
    print(f"Response Body: {service_response.json()}")
    
    assert service_response.status_code == 200
    assert service_response.json()["success"] is True
    print("Result: PASSED")
    
def test_SERVICE_012_delete_service(api_client, auth_headers, service_id):
    print(f"\n--- [SERVICE_012] DELETE SERVICE ---")
    print(f"Request: DELETE /api/services/service/{service_id}")
    service_response = api_client.delete(f"/api/services/service/{service_id}", headers=auth_headers)
    
    print(f"Response Status: {service_response.status_code}")
    print(f"Response Body: {service_response.json()}")
    
    assert service_response.status_code == 200
    assert service_response.json()["success"] is True
    print("Result: PASSED")
    
def test_SERVICE_013_get_deleted_service(api_client, auth_headers, service_id):
    print(f"\n--- [SERVICE_013] GET DELETED SERVICE ---")
    print(f"Request: GET /api/services/service/{service_id}")
    service_response = api_client.get(f"/api/services/service/{service_id}", headers=auth_headers)
    
    print(f"Response Status: {service_response.status_code}")
    print(f"Response Body: {service_response.json()}")
    
    # Router returns 200 with data set to None for non-existent service
    assert service_response.status_code == 200
    assert service_response.json()["success"] is True
    assert service_response.json()["data"] is None
    print("Result: PASSED")
