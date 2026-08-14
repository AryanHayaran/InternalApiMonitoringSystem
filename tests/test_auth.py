# test cases to implement
# AUTH_001 Signup Valid User
# AUTH_002 Signup Valid User (Duplicate Email)
# AUTH_003 Signup Invalid Email
# AUTH_004 Login Valid User
# AUTH_005 Login Invalid Password
# AUTH_006 Refresh Token Valid
# AUTH_007 Refresh Token Invalid
# AUTH_008 Delete Account (Logout)


def test_AUTH_001_signup_valid_user(api_client, test_user):
    print(f"\n--- [AUTH_001] SIGNUP VALID USER ---")
    print(f"Request: POST /api/auth/signup")
    print(f"Payload: {test_user}")
    
    signup_response = api_client.post("/api/auth/signup", json=test_user)
    
    print(f"Response Status: {signup_response.status_code}")
    print(f"Response Body: {signup_response.json()}")
    
    assert signup_response.status_code == 201
    assert signup_response.json()["success"] is True
    assert signup_response.json()["data"]["uid"] is not None
    assert signup_response.json()["data"]["email"] == test_user["email"]
    print("Result: PASSED")


def test_AUTH_002_signup_with_duplicate_email(api_client, test_user):
    print(f"\n--- [AUTH_002] SIGNUP DUPLICATE EMAIL ---")
    print(f"Step 1: Signing up user initially...")
    api_client.post("/api/auth/signup", json=test_user)
    
    print(f"Step 2: Re-signing up duplicate email: {test_user['email']}")
    signup_response = api_client.post("/api/auth/signup", json=test_user)
    
    print(f"Response Status: {signup_response.status_code}")
    print(f"Response Body: {signup_response.json()}")
    
    assert signup_response.status_code == 400
    assert signup_response.json()["success"] is False
    print("Result: PASSED")


def test_AUTH_003_signup_invalid_email(api_client, test_user):
    print(f"\n--- [AUTH_003] SIGNUP INVALID EMAIL FORMAT ---")
    # Use a copy to avoid mutating the shared test_user fixture
    user_copy = test_user.copy()
    user_copy["email"] = "invalid-email"
    print(f"Request: POST /api/auth/signup")
    print(f"Payload: {user_copy}")
    
    signup_response = api_client.post("/api/auth/signup", json=user_copy)
    
    print(f"Response Status: {signup_response.status_code}")
    print(f"Response Body: {signup_response.json()}")
    
    assert signup_response.status_code in [422]
    print("Result: PASSED")


def test_AUTH_004_login_valid_user(api_client, test_user):
    print(f"\n--- [AUTH_004] LOGIN VALID USER ---")
    print("Step 1: Pre-signup test user...")
    api_client.post("/api/auth/signup", json=test_user)

    login_payload = {
        "email": test_user["email"],
        "password": test_user["password"]
    }
    print(f"Step 2: Login Request POST /api/auth/login")
    print(f"Payload: {login_payload}")
    
    login_response = api_client.post("/api/auth/login", json=login_payload)
    
    print(f"Response Status: {login_response.status_code}")
    print(f"Response Body: {login_response.json()}")
    
    assert login_response.status_code == 200
    assert login_response.json()["success"] is True
    assert login_response.json()["data"]["access_token"] is not None
    assert login_response.json()["data"]["refresh_token"] is not None
    print("Result: PASSED")


def test_AUTH_005_login_invalid_password(api_client, test_user):
    print(f"\n--- [AUTH_005] LOGIN INVALID PASSWORD ---")
    print("Step 1: Pre-signup test user...")
    api_client.post("/api/auth/signup", json=test_user)

    login_payload = {
        "email": test_user["email"],
        "password": "wrong-password"
    }
    print(f"Step 2: Login with incorrect password to POST /api/auth/login")
    print(f"Payload: {login_payload}")
    
    login_response = api_client.post("/api/auth/login", json=login_payload)
    
    print(f"Response Status: {login_response.status_code}")
    print(f"Response Body: {login_response.json()}")
    
    assert login_response.status_code == 401
    assert login_response.json()["success"] is False
    print("Result: PASSED")


def test_AUTH_006_refresh_token_valid(api_client, test_user):
    print(f"\n--- [AUTH_006] REFRESH TOKEN VALID ---")
    print("Step 1: Login...")
    login_response = api_client.post("/api/auth/login", json={
        "email": test_user["email"],
        "password": test_user["password"]
    })
    
    refresh_token = login_response.json()["data"]["refresh_token"]
    print(f"Step 2: Refresh token retrieved: {refresh_token[:20]}...")
    
    refresh_payload = {
        "refresh_token": f"Bearer {refresh_token}"
    }
    print(f"Request: POST /api/auth/refresh")
    print(f"Payload: {refresh_payload}")
    
    refresh_response = api_client.post("/api/auth/refresh", json=refresh_payload)
    
    print(f"Response Status: {refresh_response.status_code}")
    print(f"Response Body: {refresh_response.json()}")
    
    assert refresh_response.status_code == 200
    assert refresh_response.json()["success"] is True
    assert refresh_response.json()["data"]["access_token"] is not None
    print("Result: PASSED")


def test_AUTH_007_refresh_token_invalid(api_client, test_user):
    print(f"\n--- [AUTH_007] REFRESH TOKEN INVALID ---")
    refresh_payload = {
        "refresh_token": "Bearer invalid-token"
    }
    print(f"Request: POST /api/auth/refresh")
    print(f"Payload: {refresh_payload}")
    
    refresh_response = api_client.post("/api/auth/refresh", json=refresh_payload)
    
    print(f"Response Status: {refresh_response.status_code}")
    print(f"Response Body: {refresh_response.json()}")
    
    assert refresh_response.status_code == 401
    assert refresh_response.json()["success"] is False
    print("Result: PASSED")


def test_AUTH_008_logout_account(api_client, test_user):
    print(f"\n--- [AUTH_008] LOGOUT ACCOUNT ---")
    print("Step 1: Login...")

    login_response = api_client.post("/api/auth/login", json={
        "email": test_user["email"],
        "password": test_user["password"]
    })
    
    access_token = login_response.json()["data"]["access_token"]
    auth_headers = {
        "Authorization": f"Bearer {access_token}"
    }
    print(f"Headers: {auth_headers}")   
    print("Step 2: Logout...")   
    
    logout_response = api_client.get("/api/auth/logout", headers=auth_headers)
    
    print(f"Response Status: {logout_response.status_code}")
    print(f"Response Body: {logout_response.json()}")
    
    assert logout_response.status_code == 200
    assert logout_response.json()["success"] is True
    print("Result: PASSED")
