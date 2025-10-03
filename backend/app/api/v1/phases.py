"""
Phase-specific API endpoints
"""

from typing import List, Optional
from fastapi import APIRouter, HTTPException, Depends, UploadFile, File
from fastapi.responses import JSONResponse
import pandas as pd

from ...models.schemas import (
    DomainSelection, GoalDefinition, KPIDefinition, 
    Phase1Response, DomainInfo, DomainType
)
from ...services.phase1_goal_kpis import Phase1Service
from ...services.phase0_quality_control import QualityControlService, QualityControlResult

router = APIRouter()

# Dependency to get Phase1Service instance
def get_phase1_service() -> Phase1Service:
    return Phase1Service()


@router.get("/domains", response_model=List[DomainInfo])
async def get_available_domains():
    """Get list of available business domains"""
    service = get_phase1_service()
    return service.get_available_domains()


@router.get("/domains/{domain}", response_model=DomainInfo)
async def get_domain_info(domain: DomainType):
    """Get detailed information about a specific domain"""
    service = get_phase1_service()
    domain_info = service.get_domain_info(domain)
    
    if not domain_info:
        raise HTTPException(status_code=404, detail=f"Domain '{domain}' not found")
    
    return domain_info


@router.post("/domain-selection", response_model=Phase1Response)
async def save_domain_selection(domain_selection: DomainSelection):
    """Save domain selection"""
    service = get_phase1_service()
    return service.save_domain_selection(domain_selection)


@router.post("/goals", response_model=Phase1Response)
async def add_goal(goal: GoalDefinition):
    """Add a new business goal"""
    service = get_phase1_service()
    return service.add_goal(goal)


@router.post("/kpis", response_model=Phase1Response)
async def add_kpi(kpi: KPIDefinition):
    """Add a new KPI"""
    service = get_phase1_service()
    return service.add_kpi(kpi)


@router.get("/config", response_model=Phase1Response)
async def get_phase1_config():
    """Get current Phase 1 configuration"""
    service = get_phase1_service()
    return service.get_config()


@router.get("/validate", response_model=Phase1Response)
async def validate_phase1_config():
    """Validate Phase 1 configuration completeness"""
    service = get_phase1_service()
    return service.validate_config()


@router.post("/quality-control", response_model=QualityControlResult)
async def run_quality_control(
    file: UploadFile = File(...),
    key_columns: Optional[str] = None  # Comma-separated list
):
    """
    Run Phase 0: Quality Control on uploaded dataset
    
    Parameters:
    - file: CSV or Excel file
    - key_columns: Optional comma-separated key columns (e.g., "order_id,customer_id")
    """
    try:
        # Read file
        if file.filename.endswith('.csv'):
            df = pd.read_csv(file.file)
        elif file.filename.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file.file)
        else:
            raise HTTPException(status_code=400, detail="Only CSV and Excel files supported")
        
        # Parse key columns
        keys = [k.strip() for k in key_columns.split(',')] if key_columns else []
        
        # Run quality control
        service = QualityControlService(df=df, key_columns=keys)
        result = service.run()
        
        return result
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status")
async def get_phase1_status():
    """Get Phase 1 status and progress"""
    service = get_phase1_service()
    config_response = service.get_config()
    validation_response = service.validate_config()
    
    if config_response.status == "error":
        raise HTTPException(status_code=500, detail=config_response.message)
    
    config_data = config_response.data or {}
    
    return {
        "phase": 1,
        "name": "Goal & KPIs Definition",
        "status": "active",
        "progress": {
            "domain_selected": config_data.get("domain_selection") is not None,
            "goals_count": len(config_data.get("goals", [])),
            "kpis_count": len(config_data.get("kpis", [])),
            "is_complete": validation_response.data.get("is_complete", False) if validation_response.data else False
        },
        "validation": validation_response.data if validation_response.data else {},
        "timestamp": config_data.get("updated_at")
    }
