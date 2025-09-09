from fastapi import APIRouter

router = APIRouter()

@router.get("/")
async def get_services():
    return [{"id": 1, "name": "service1", "url": "http://service1.example.com"},
            {"id": 2, "name": "service2", "url": "http://service2.example.com"}]
