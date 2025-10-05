"""
Phase 1: Goal & KPIs - Clean API Endpoint
Minimal implementation without Unicode characters
"""

from fastapi import APIRouter, HTTPException
import pandas as pd
from typing import Optional

from ...config import settings
from ...services.phase1_goal_kpis_clean import GoalKPIsService, GoalKPIsResult

router = APIRouter()


@router.post("/phase1-goal-kpis-clean", response_model=GoalKPIsResult)
async def run_phase1_clean(request_data: dict = None):
    """Clean Phase 1: Goal & KPIs with domain compatibility check"""
    try:
        # Read cleaned data from Phase 0
        cleaned_data_path = settings.artifacts_dir / "cleaned_data.parquet"
        if not cleaned_data_path.exists():
            raise HTTPException(400, "No cleaned data found. Run Phase 0 first.")
        
        # Load data
        df_sample = pd.read_parquet(cleaned_data_path).head(10)
        columns = df_sample.columns.tolist()
        
        # Simple data summary instead of to_string() to avoid Unicode issues
        data_summary = f"Shape: {df_sample.shape}, Columns: {list(df_sample.columns)}"
        
        # Extract domain from request data
        domain = request_data.get("domain", "healthcare") if request_data else "healthcare"
        
        # Run clean service
        service = GoalKPIsService(columns=columns, domain=domain, data_sample=data_summary)
        result = service.run()
        
        # Check compatibility
        if result.compatibility.status == "STOP":
            raise HTTPException(
                status_code=400,
                detail=result.compatibility.message
            )
        
        return result
    
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error in clean Phase 1: {e}")
        raise HTTPException(500, f"Phase 1 failed: {str(e)}")
