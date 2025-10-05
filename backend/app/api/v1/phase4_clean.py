"""
Phase 4: Data Profiling - Clean API Endpoint
Minimal implementation without Unicode characters
"""

from fastapi import APIRouter, HTTPException
import pandas as pd

from ...config import settings
from ...services.phase4_profiling_clean import ProfilingService, ProfilingResult

router = APIRouter()


@router.post("/phase4-profiling-clean", response_model=ProfilingResult)
async def run_phase4_clean():
    """Clean Phase 4: Data Profiling"""
    try:
        # Read cleaned data from Phase 0
        cleaned_data_path = settings.artifacts_dir / "cleaned_data.parquet"
        if not cleaned_data_path.exists():
            raise HTTPException(400, "No cleaned data found. Run Phase 0 first.")
        
        # Load data
        df = pd.read_parquet(cleaned_data_path)
        
        # Run clean profiling service
        service = ProfilingService(df)
        result = service.run()
        
        return result
    
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error in clean Phase 4: {e}")
        raise HTTPException(500, f"Phase 4 failed: {str(e)}")
