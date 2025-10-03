from fastapi import APIRouter
from . import phases

api_router = APIRouter()

api_router.include_router(phases.router, prefix="/phases", tags=["phases"])

@api_router.get("/status")
async def status():
    return {"phases_implemented": 4, "total_phases": 14}
