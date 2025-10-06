from fastapi import APIRouter
from . import phases, bi, phase1_clean, phase4_clean, llm_analysis

api_router = APIRouter()

api_router.include_router(phases.router, prefix="/phases", tags=["phases"])
api_router.include_router(bi.router, prefix="/bi", tags=["business-intelligence"])
api_router.include_router(phase1_clean.router, prefix="/phases", tags=["phases-clean"])
api_router.include_router(phase4_clean.router, prefix="/phases", tags=["phases-clean"])
api_router.include_router(llm_analysis.router)

@api_router.get("/status")
async def status():
    return {"phases_implemented": 4, "total_phases": 14}
