from fastapi import APIRouter
from . import phases, bi

api_router = APIRouter()

api_router.include_router(phases.router, prefix="/phases", tags=["phases"])
api_router.include_router(bi.router, prefix="/bi", tags=["business-intelligence"])

@api_router.get("/status")
async def status():
    return {"phases_implemented": 4, "total_phases": 14}
