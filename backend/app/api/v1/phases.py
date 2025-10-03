"""
Phase-specific API endpoints
"""

from typing import List, Optional
from fastapi import APIRouter, HTTPException, Depends, UploadFile, File, Form
from fastapi.responses import JSONResponse
import pandas as pd
from pathlib import Path

from ...models.schemas import (
    DomainSelection, GoalDefinition, KPIDefinition, 
    Phase1Response, DomainInfo, DomainType,
    DomainCompatibilityResult, Phase1DomainCompatibilityResponse,
    IngestionConfig, Phase2IngestionResponse,
    SchemaValidationResult, Phase3SchemaResponse
)
from ...services.phase1_goal_kpis import Phase1Service, GoalKPIsService, GoalKPIsResult
from ...services.phase2_ingestion import Phase2IngestionService, IngestionService, IngestionResult
from ...services.phase3_schema import Phase3SchemaService, SchemaService, SchemaResult
from ...services.phase0_quality_control import QualityControlService, QualityControlResult
from ...services.domain_packs import DOMAIN_PACKS
from ...config import settings

router = APIRouter()

# Dependency functions
def get_phase1_service() -> Phase1Service:
    return Phase1Service()

def get_phase2_service() -> Phase2IngestionService:
    return Phase2IngestionService()

def get_phase3_service() -> Phase3SchemaService:
    return Phase3SchemaService()


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


# ===== PHASE 1 ENHANCED ENDPOINTS =====

@router.get("/domain-packs")
async def get_domain_packs():
    """Get available domain packs with KPIs and expected columns"""
    return {"domain_packs": DOMAIN_PACKS}


@router.post("/domain-compatibility", response_model=Phase1DomainCompatibilityResponse)
async def check_domain_compatibility(
    domain: str,
    columns: List[str]
):
    """Check domain compatibility based on column names"""
    service = get_phase1_service()
    return service.check_domain_compatibility(domain, columns)


@router.post("/goal-kpis")
async def run_goal_kpis(
    columns: List[str],
    domain: Optional[str] = None
):
    """Execute Phase 1: Goal & KPIs with domain compatibility"""
    service = GoalKPIsService(columns=columns, domain=domain)
    result = service.run()
    return result.dict()


# ===== PHASE 2 ENDPOINTS =====

@router.post("/ingest", response_model=Phase2IngestionResponse)
async def ingest_data(
    source_file: str,
    config: Optional[IngestionConfig] = None
):
    """Ingest data from source file to Parquet format"""
    service = get_phase2_service()
    return service.ingest_data(source_file, config)


@router.post("/ingest-simple")
async def ingest_data_simple(
    file_path: str,
    artifacts_dir: Optional[str] = None
):
    """Simple ingestion service - Phase 2: Ingestion & Landing"""
    from pathlib import Path
    from ...config import settings
    
    # Use provided artifacts_dir or default
    if artifacts_dir:
        artifacts_path = Path(artifacts_dir)
    else:
        artifacts_path = settings.artifacts_dir / "landing"
    
    # Create service and run ingestion
    service = IngestionService(file_path=Path(file_path), artifacts_dir=artifacts_path)
    df, result = service.run()
    
    return {
        "dataframe_info": {
            "shape": df.shape,
            "columns": df.columns.tolist(),
            "dtypes": df.dtypes.to_dict()
        },
        "ingestion_result": result.dict()
    }


@router.get("/ingest/status/{target_file:path}", response_model=Phase2IngestionResponse)
async def get_ingestion_status(target_file: str):
    """Get status of ingested file"""
    service = get_phase2_service()
    return service.get_ingestion_status(target_file)


@router.get("/ingest/files", response_model=Phase2IngestionResponse)
async def list_ingested_files():
    """List all ingested files in landing directory"""
    service = get_phase2_service()
    return service.list_ingested_files()


# ===== PHASE 3 ENDPOINTS =====

@router.post("/schema/validate", response_model=Phase3SchemaResponse)
async def validate_schema(
    file_path: str,
    domain_pack: Optional[str] = None
):
    """Validate and enforce schema on ingested data"""
    service = get_phase3_service()
    return service.validate_and_enforce_schema(file_path, domain_pack)


@router.post("/schema-simple")
async def schema_simple(
    file: UploadFile = File(...)
):
    """Simple schema service - Phase 3: Schema & Dtypes"""
    try:
        # Read file
        if file.filename.endswith('.csv'):
            df = pd.read_csv(file.file)
        elif file.filename.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file.file)
        else:
            raise HTTPException(status_code=400, detail="Only CSV and Excel files supported")
        
        # Run schema service
        service = SchemaService(df=df)
        df_typed, result = service.run()
        
        return {
            "file_info": {
                "filename": file.filename,
                "original_shape": df.shape,
                "typed_shape": df_typed.shape,
                "original_dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
                "typed_dtypes": {col: str(dtype) for col, dtype in df_typed.dtypes.items()}
            },
            "schema_result": result.dict()
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/schema/info/{file_path:path}", response_model=Phase3SchemaResponse)
async def get_schema_info(file_path: str):
    """Get schema information for a file"""
    service = get_phase3_service()
    return service.get_schema_info(file_path)


@router.get("/schema/files", response_model=Phase3SchemaResponse)
async def list_processed_files():
    """List all processed files with schema validation"""
    service = get_phase3_service()
    return service.list_processed_files()


# ===== NEW INDIVIDUAL PHASE ENDPOINTS =====

@router.post("/phase1-goal-kpis", response_model=GoalKPIsResult)
async def run_phase1(
    file: UploadFile = File(...),
    domain: Optional[str] = Form(None)
):
    """Phase 1: Goal & KPIs with domain compatibility check"""
    try:
        # Quick column extraction
        if file.filename.endswith('.csv'):
            df_sample = pd.read_csv(file.file, nrows=10)
        else:
            df_sample = pd.read_excel(file.file, nrows=10)
        
        columns = df_sample.columns.tolist()
        
        # Run Phase 1
        service = GoalKPIsService(columns=columns, domain=domain)
        result = service.run()
        
        # Stop if incompatible
        if result.compatibility.status == "STOP":
            raise HTTPException(
                status_code=400,
                detail=result.compatibility.message
            )
        
        return result
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/phase2-ingestion", response_model=IngestionResult)
async def run_phase2(file: UploadFile = File(...)):
    """Phase 2: Ingestion & Landing to Parquet"""
    try:
        # Save uploaded file temporarily
        temp_path = settings.artifacts_dir / file.filename
        with open(temp_path, "wb") as f:
            content = await file.read()
            f.write(content)
        
        # Run Phase 2
        service = IngestionService(
            file_path=temp_path,
            artifacts_dir=settings.artifacts_dir
        )
        df, result = service.run()
        
        # Clean up temp file
        temp_path.unlink()
        
        return result
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/phase3-schema", response_model=SchemaResult)
async def run_phase3():
    """Phase 3: Schema & Dtypes (runs on ingested Parquet)"""
    try:
        # Load ingested Parquet
        parquet_path = settings.artifacts_dir / "raw_ingested.parquet"
        if not parquet_path.exists():
            raise HTTPException(
                status_code=400,
                detail="No ingested data found. Run Phase 2 first."
            )
        
        df = pd.read_parquet(parquet_path)
        
        # Run Phase 3
        service = SchemaService(df=df)
        df_typed, result = service.run()
        
        # Save typed DataFrame
        df_typed.to_parquet(
            settings.artifacts_dir / "typed_data.parquet",
            compression='zstd'
        )
        
        return result
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/run-phases-1-to-3")
async def run_combined_phases(
    file: UploadFile = File(...),
    domain: Optional[str] = Form(None),
    key_columns: Optional[str] = Form(None)
):
    """Run Phases 1-3 sequentially"""
    results = {}
    
    try:
        # Phase 1: Goal & KPIs
        phase1_result = await run_phase1(file=file, domain=domain)
        results["phase1"] = phase1_result.dict()
        
        # Reset file pointer
        await file.seek(0)
        
        # Phase 2: Ingestion
        phase2_result = await run_phase2(file=file)
        results["phase2"] = phase2_result.dict()
        
        # Phase 3: Schema
        phase3_result = await run_phase3()
        results["phase3"] = phase3_result.dict()
        
        return JSONResponse(content={
            "status": "success",
            "phases_completed": ["phase1", "phase2", "phase3"],
            "results": results
        })
    
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ===== COMBINED WORKFLOW ENDPOINTS =====

@router.post("/workflow/domain-check")
async def workflow_domain_check(
    file: UploadFile = File(...),
    domain: str = "general"
):
    """
    Combined workflow: Upload file, check domain compatibility
    """
    try:
        # Read file
        if file.filename.endswith('.csv'):
            df = pd.read_csv(file.file)
        elif file.filename.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file.file)
        else:
            raise HTTPException(status_code=400, detail="Only CSV and Excel files supported")
        
        # Check domain compatibility
        service = get_phase1_service()
        compatibility_result = service.check_domain_compatibility(domain, df.columns.tolist())
        
        return {
            "file_info": {
                "filename": file.filename,
                "rows": len(df),
                "columns": df.columns.tolist()
            },
            "compatibility": compatibility_result.data.dict() if compatibility_result.data else None,
            "status": compatibility_result.status,
            "message": compatibility_result.message
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/workflow/full-pipeline")
async def workflow_full_pipeline(
    file: UploadFile = File(...),
    domain: str = "general",
    auto_ingest: bool = True,
    auto_validate: bool = True
):
    """
    Full pipeline: Upload → Domain Check → Ingest → Schema Validate
    """
    try:
        # Read file
        if file.filename.endswith('.csv'):
            df = pd.read_csv(file.file)
        elif file.filename.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file.file)
        else:
            raise HTTPException(status_code=400, detail="Only CSV and Excel files supported")
        
        results = {
            "file_info": {
                "filename": file.filename,
                "rows": len(df),
                "columns": df.columns.tolist()
            },
            "steps": []
        }
        
        # Step 1: Domain compatibility check
        phase1_service = get_phase1_service()
        compatibility_result = phase1_service.check_domain_compatibility(domain, df.columns.tolist())
        
        results["steps"].append({
            "step": "domain_compatibility",
            "status": compatibility_result.status,
            "result": compatibility_result.data.dict() if compatibility_result.data else None
        })
        
        # If domain compatibility is OK or WARN, proceed with ingestion
        if compatibility_result.data and compatibility_result.data.status in ["OK", "WARN"] and auto_ingest:
            # Save file temporarily for ingestion
            import tempfile
            import os
            
            with tempfile.NamedTemporaryFile(delete=False, suffix=f".{file.filename.split('.')[-1]}") as tmp_file:
                df.to_csv(tmp_file.name, index=False)
                
                # Step 2: Ingest data
                phase2_service = get_phase2_service()
                ingestion_result = phase2_service.ingest_data(tmp_file.name)
                
                results["steps"].append({
                    "step": "ingestion",
                    "status": ingestion_result.status,
                    "result": ingestion_result.data.dict() if ingestion_result.data else None
                })
                
                # Step 3: Schema validation
                if ingestion_result.data and auto_validate:
                    phase3_service = get_phase3_service()
                    schema_result = phase3_service.validate_and_enforce_schema(
                        ingestion_result.data.target_file, domain
                    )
                    
                    results["steps"].append({
                        "step": "schema_validation",
                        "status": schema_result.status,
                        "result": schema_result.data.dict() if schema_result.data else None
                    })
                
                # Clean up temp file
                os.unlink(tmp_file.name)
        
        return results
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
