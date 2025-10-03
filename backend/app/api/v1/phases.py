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
from ...services.phase4_profiling import ProfilingService, ProfilingResult
from ...services.phase5_missing_data import MissingDataService, ImputationResult
from ...services.phase6_standardization import StandardizationService, StandardizationResult
from ...services.phase7_features import FeatureDraftService, FeatureDraftResult
from ...services.phase7_5_encoding import EncodingScalingService, EncodingScalingResult
import json
from typing import Optional
from fastapi import Form
from ...services.phase8_merging import MergingService, MergingResult
from ...services.phase9_correlations import CorrelationsService, CorrelationsResult
from ...services.phase9_5_business_validation import BusinessValidationService, BusinessValidationResult
from ...services.phase10_packaging import PackagingService, PackagingResult
from ...services.phase10_5_split import SplitService, SplitResult
from ...services.phase11_advanced import AdvancedExplorationService, AdvancedExplorationResult
from ...services.phase11_5_selection import FeatureSelectionService, SelectionResult
from ...services.phase13_monitoring import MonitoringService, MonitoringResult
from ...services.phase12.orchestrator import Phase12Orchestrator, Phase12Result

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

@router.post("/phase4-profiling", response_model=ProfilingResult)
async def run_phase4():
    """Phase 4: Profiling (runs on typed data from Phase 3)"""
    try:
        # Load typed data
        data_path = settings.artifacts_dir / "typed_data.parquet"
        if not data_path.exists():
            raise HTTPException(400, "No typed data found. Run Phase 3 first.")
        
        df = pd.read_parquet(data_path)
        
        # Run Phase 4
        service = ProfilingService(df=df)
        result = service.run()
        
        # Save profile report
        with open(settings.artifacts_dir / "profile_summary.json", "w") as f:
            json.dump(result.model_dump(), f, indent=2)
        
        return result
    
    except Exception as e:
        raise HTTPException(500, str(e))


@router.post("/phase5-missing-data", response_model=ImputationResult)
async def run_phase5(group_column: Optional[str] = Form(None)):
    """Phase 5: Missing Data Handling"""
    try:
        # Load typed data
        data_path = settings.artifacts_dir / "typed_data.parquet"
        if not data_path.exists():
            raise HTTPException(400, "No typed data found. Run Phase 3 first.")
        
        df = pd.read_parquet(data_path)
        
        # Run Phase 5
        service = MissingDataService(df=df, group_col=group_column)
        df_imputed, result = service.run()
        
        # Stop if validation failed
        if result.status == "STOP":
            raise HTTPException(
                400,
                f"Imputation validation failed. Completeness: {result.record_completeness:.1%}"
            )
        
        # Save imputed data
        df_imputed.to_parquet(
            settings.artifacts_dir / "imputed_data.parquet",
            compression='zstd'
        )
        
        # Save imputation policy
        with open(settings.artifacts_dir / "imputation_policy.json", "w") as f:
            json.dump(result.model_dump(), f, indent=2)
        
        return result
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


@router.post("/phase10-packaging", response_model=PackagingResult)
async def run_phase10():
    """Phase 10: Packaging (Pre-Split)"""
    try:
        service = PackagingService(artifacts_dir=settings.artifacts_dir)
        result = service.run()
        return result
    except Exception as e:
        raise HTTPException(500, str(e))


@router.post("/phase10-5-split", response_model=SplitResult)
async def run_phase10_5(
    target_column: Optional[str] = Form(None),
    time_column: Optional[str] = Form(None)
):
    """Phase 10.5: Train/Validation/Test Split"""
    try:
        data_path = settings.artifacts_dir / "merged_data.parquet"
        if not data_path.exists():
            raise HTTPException(400, "No merged data found.")
        
        df = pd.read_parquet(data_path)
        
        service = SplitService(
            df=df,
            target_col=target_column,
            time_col=time_column
        )
        
        df_train, df_val, df_test, result = service.run()
        
        # Save splits
        df_train.to_parquet(settings.artifacts_dir / "train.parquet")
        df_val.to_parquet(settings.artifacts_dir / "validation.parquet")
        df_test.to_parquet(settings.artifacts_dir / "test.parquet")
        
        # Save split indices
        with open(settings.artifacts_dir / "split_indices.json", "w") as f:
            json.dump(result.dict(), f, indent=2)
        
        return result
    except Exception as e:
        raise HTTPException(500, str(e))


@router.post("/phase11-advanced", response_model=AdvancedExplorationResult)
async def run_phase11():
    """Phase 11: Advanced Exploration"""
    try:
        train_path = settings.artifacts_dir / "train.parquet"
        if not train_path.exists():
            raise HTTPException(400, "No train data found. Run Phase 10.5 first.")
        
        df_train = pd.read_parquet(train_path)
        
        service = AdvancedExplorationService(df=df_train)
        result = service.run(settings.artifacts_dir)
        
        return result
    except Exception as e:
        raise HTTPException(500, str(e))


@router.post("/phase11-5-selection", response_model=SelectionResult)
async def run_phase11_5(
    target_column: str = Form(...),
    top_k: int = Form(25)
):
    """Phase 11.5: Feature Selection & Ranking"""
    try:
        train_path = settings.artifacts_dir / "train.parquet"
        val_path = settings.artifacts_dir / "validation.parquet"
        
        if not train_path.exists():
            raise HTTPException(400, "No train data found.")
        
        df_train = pd.read_parquet(train_path)
        df_val = pd.read_parquet(val_path)
        
        service = FeatureSelectionService(
            df_train=df_train,
            df_val=df_val,
            target_col=target_column,
            top_k=top_k
        )
        
        selected_features, result = service.run()
        
        # Save selected features
        with open(settings.artifacts_dir / "selected_features.json", "w") as f:
            json.dump(result.dict(), f, indent=2)
        # Save ranking report CSV
        try:
            import csv
            ranking_csv = settings.artifacts_dir / "ranking_report.csv"
            with open(ranking_csv, "w", newline="") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(["feature", "score", "rank"])
                for fr in result.feature_rankings:
                    writer.writerow([fr.feature, fr.score, fr.rank])
        except Exception:
            pass
        
        return result
    except Exception as e:
        raise HTTPException(500, str(e))


@router.post("/phase13-monitoring", response_model=MonitoringResult)
async def run_phase13():
    """Phase 13: Monitoring & Drift Setup"""
    try:
        train_path = settings.artifacts_dir / "train.parquet"
        if not train_path.exists():
            raise HTTPException(400, "No train data found.")
        
        df_train = pd.read_parquet(train_path)
        
        service = MonitoringService(df=df_train)
        result = service.run()
        
        # Save drift config
        with open(settings.artifacts_dir / "drift_config.json", "w") as f:
            json.dump(result.dict(), f, indent=2)
        
        return result
    except Exception as e:
        raise HTTPException(500, str(e))


@router.post("/phase12-text-features", response_model=Phase12Result)
async def run_phase12():
    """
    Phase 12: Text Features (MVP - Optional)
    
    MVP includes:
    - Text column detection
    - Basic text features (length, word count, etc.)
    - Sentiment analysis (English VADER + Simple Arabic)
    
    Future enhancements (not yet implemented):
    - Topic modeling (LDA)
    - Keyword extraction (TF-IDF, RAKE)
    - Named Entity Recognition (spaCy, CAMeL Tools)
    - Text clustering (KMeans)
    """
    try:
        # Load data (use merged or train data)
        data_path = settings.artifacts_dir / "merged_data.parquet"
        if not data_path.exists():
            data_path = settings.artifacts_dir / "train.parquet"
        
        if not data_path.exists():
            raise HTTPException(400, "No data found. Run previous phases first.")
        
        df = pd.read_parquet(data_path)
        
        # Run Phase 12
        orchestrator = Phase12Orchestrator(df=df)
        result = orchestrator.run(settings.artifacts_dir)
        
        return result
    
    except Exception as e:
        raise HTTPException(500, str(e))


# ===== PHASE 8-9.5 ENDPOINTS =====

@router.post("/phase8-merging", response_model=MergingResult)
async def run_phase8():
    """Phase 8: Merging & Keys"""
    try:
        data_path = settings.artifacts_dir / "features_data.parquet"
        if not data_path.exists():
            raise HTTPException(400, "No feature data found. Run Phase 7 first.")
        
        df = pd.read_parquet(data_path)
        
        # For MVP, no additional tables to merge
        service = MergingService(main_df=df)
        df_merged, result = service.run(settings.artifacts_dir)
        
        if result.status == "STOP":
            raise HTTPException(400, f"Merging failed: {result.issues}")
        
        # Save merged data
        df_merged.to_parquet(
            settings.artifacts_dir / "merged_data.parquet",
            compression='zstd'
        )
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


@router.post("/phase9-correlations", response_model=CorrelationsResult)
async def run_phase9():
    """Phase 9: Summaries & Correlations"""
    try:
        data_path = settings.artifacts_dir / "merged_data.parquet"
        if not data_path.exists():
            raise HTTPException(400, "No merged data found. Run Phase 8 first.")
        
        df = pd.read_parquet(data_path)
        
        service = CorrelationsService(df=df)
        result = service.run()
        
        # Save correlation matrix
        with open(settings.artifacts_dir / "correlation_matrix.json", "w") as f:
            json.dump(result.model_dump(), f, indent=2)
        
        return result
    except Exception as e:
        raise HTTPException(500, str(e))


@router.post("/phase9-5-business-validation", response_model=BusinessValidationResult)
async def run_phase9_5(domain: str = Form("logistics")):
    """Phase 9.5: Business Logic Validation"""
    try:
        # Load correlations from Phase 9
        corr_path = settings.artifacts_dir / "correlation_matrix.json"
        if not corr_path.exists():
            raise HTTPException(400, "No correlations found. Run Phase 9 first.")
        
        with open(corr_path) as f:
            corr_data = json.load(f)
        
        # Extract correlations
        correlations = []
        for item in corr_data.get("numeric_correlations", []) + corr_data.get("categorical_associations", []):
            # Reconstruct minimal structure expected by BusinessValidationService
            from types import SimpleNamespace
            correlations.append(SimpleNamespace(**item))
        
        service = BusinessValidationService(
            correlations=correlations,
            domain=domain
        )
        result = service.run()
        
        if result.status == "STOP":
            raise HTTPException(
                400,
                f"Business validation failed: {len(result.conflicts_detected)} unresolved conflicts"
            )
        
        # Save business veto report
        with open(settings.artifacts_dir / "business_veto_report.json", "w") as f:
            json.dump(result.model_dump(), f, indent=2)
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


@router.post("/phase6-standardization", response_model=StandardizationResult)
async def run_phase6(domain: str = Form("logistics")):
    """Phase 6: Standardization"""
    try:
        data_path = settings.artifacts_dir / "imputed_data.parquet"
        if not data_path.exists():
            raise HTTPException(400, "No imputed data found. Run Phase 5 first.")
        
        df = pd.read_parquet(data_path)
        
        service = StandardizationService(df=df, domain=domain)
        df_std, result = service.run()
        
        # Save standardized data
        df_std.to_parquet(
            settings.artifacts_dir / "standardized_data.parquet",
            compression='zstd'
        )
        
        return result
    except Exception as e:
        raise HTTPException(500, str(e))


@router.post("/phase7-features", response_model=FeatureDraftResult)
async def run_phase7(domain: str = Form("logistics")):
    """Phase 7: Feature Draft"""
    try:
        data_path = settings.artifacts_dir / "standardized_data.parquet"
        if not data_path.exists():
            raise HTTPException(400, "No standardized data found. Run Phase 6 first.")
        
        df = pd.read_parquet(data_path)
        
        service = FeatureDraftService(df=df, domain=domain)
        df_features, result = service.run()
        
        # Save feature data
        df_features.to_parquet(
            settings.artifacts_dir / "features_data.parquet",
            compression='zstd'
        )
        
        # Save feature spec
        with open(settings.artifacts_dir / "feature_spec.json", "w") as f:
            json.dump(result.model_dump(), f, indent=2)
        
        return result
    except Exception as e:
        raise HTTPException(500, str(e))


@router.post("/phase7-5-encoding", response_model=EncodingScalingResult)
async def run_phase7_5(
    target_column: Optional[str] = Form(None),
    domain: str = Form("logistics")
):
    """Phase 7.5: Encoding & Scaling (requires split data)"""
    try:
        # Load split data
        train_path = settings.artifacts_dir / "train.parquet"
        val_path = settings.artifacts_dir / "validation.parquet"
        test_path = settings.artifacts_dir / "test.parquet"
        
        if not train_path.exists():
            raise HTTPException(400, "No split data found. Run Phase 10.5 (split) first.")
        
        df_train = pd.read_parquet(train_path)
        df_val = pd.read_parquet(val_path) if val_path.exists() else None
        df_test = pd.read_parquet(test_path) if test_path.exists() else None
        
        service = EncodingScalingService(
            df_train=df_train,
            df_val=df_val,
            df_test=df_test,
            target_col=target_column,
            domain=domain
        )
        
        df_train_enc, df_val_enc, df_test_enc, result = service.run(settings.artifacts_dir)
        
        # Save encoded data
        df_train_enc.to_parquet(settings.artifacts_dir / "train_encoded.parquet")
        if df_val_enc is not None:
            df_val_enc.to_parquet(settings.artifacts_dir / "val_encoded.parquet")
        if df_test_enc is not None:
            df_test_enc.to_parquet(settings.artifacts_dir / "test_encoded.parquet")
        
        return result
    except Exception as e:
        raise HTTPException(500, str(e))

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
