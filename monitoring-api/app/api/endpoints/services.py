from fastapi import APIRouter

router = APIRouter()

@router.get("/health")
async def health_check():
    return {"status": "ok"}

@router.get("/")
async def get_all_services():
    yield
    
    
@router.post("/services")
async def create_new_service():
    yield
    
@router.get("/services/{service_id}")
async def get_service_details(service_id: int):
    yield
    
@router.put("/services/{service_id}")
async def update_service(service_id: int):
    yield
    
@router.delete("/services/{service_id}")
async def delete_service(service_id: int):
    yield
    
@router.get("/services/{service_id}/history")
async def get_service_history(service_id: int):
    yield
