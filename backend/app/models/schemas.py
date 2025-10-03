"""
Pydantic models for API schemas
"""

from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime
from enum import Enum


class DomainType(str, Enum):
    """Supported business domains"""
    FINANCE = "finance"
    HEALTHCARE = "healthcare"
    RETAIL = "retail"
    MANUFACTURING = "manufacturing"
    TECHNOLOGY = "technology"
    EDUCATION = "education"
    GOVERNMENT = "government"
    GENERAL = "general"


class GoalType(str, Enum):
    """Types of business goals"""
    PREDICTION = "prediction"
    CLASSIFICATION = "classification"
    CLUSTERING = "clustering"
    OPTIMIZATION = "optimization"
    INSIGHT = "insight"
    FORECASTING = "forecasting"


class GoalDefinition(BaseModel):
    """Model for defining business goals"""
    goal_id: str = Field(..., description="Unique identifier for the goal")
    title: str = Field(..., min_length=1, max_length=200, description="Goal title")
    description: str = Field(..., min_length=10, max_length=1000, description="Detailed goal description")
    goal_type: GoalType = Field(..., description="Type of goal")
    domain: DomainType = Field(..., description="Business domain")
    priority: int = Field(..., ge=1, le=5, description="Priority level (1-5)")
    target_metric: str = Field(..., min_length=1, max_length=100, description="Target metric name")
    target_value: Optional[float] = Field(None, description="Target metric value")
    success_criteria: str = Field(..., min_length=10, max_length=500, description="Success criteria")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class KPIType(str, Enum):
    """Types of KPIs"""
    ACCURACY = "accuracy"
    PRECISION = "precision"
    RECALL = "recall"
    F1_SCORE = "f1_score"
    ROC_AUC = "roc_auc"
    MAE = "mae"
    RMSE = "rmse"
    MAPE = "mape"
    BUSINESS_METRIC = "business_metric"
    CUSTOM = "custom"


class KPIDefinition(BaseModel):
    """Model for defining KPIs"""
    kpi_id: str = Field(..., description="Unique identifier for the KPI")
    name: str = Field(..., min_length=1, max_length=100, description="KPI name")
    description: str = Field(..., min_length=10, max_length=500, description="KPI description")
    kpi_type: KPIType = Field(..., description="Type of KPI")
    target_value: Optional[float] = Field(None, description="Target value")
    threshold_min: Optional[float] = Field(None, description="Minimum acceptable value")
    threshold_max: Optional[float] = Field(None, description="Maximum acceptable value")
    unit: Optional[str] = Field(None, max_length=20, description="Unit of measurement")
    calculation_method: str = Field(..., min_length=5, max_length=200, description="How to calculate this KPI")
    is_primary: bool = Field(False, description="Whether this is a primary KPI")
    goal_id: Optional[str] = Field(None, description="Associated goal ID")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class DomainSelection(BaseModel):
    """Model for domain selection"""
    domain: DomainType = Field(..., description="Selected business domain")
    subdomain: Optional[str] = Field(None, max_length=100, description="Specific subdomain")
    industry_context: Optional[str] = Field(None, max_length=500, description="Industry context")
    data_sources: List[str] = Field(default_factory=list, description="Available data sources")
    regulatory_requirements: List[str] = Field(default_factory=list, description="Regulatory requirements")
    business_constraints: List[str] = Field(default_factory=list, description="Business constraints")
    created_at: datetime = Field(default_factory=datetime.utcnow)


class Phase1Config(BaseModel):
    """Complete Phase 1 configuration"""
    domain_selection: DomainSelection
    goals: List[GoalDefinition]
    kpis: List[KPIDefinition]
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class Phase1Response(BaseModel):
    """Response model for Phase 1 operations"""
    status: str = Field(..., description="Operation status")
    message: str = Field(..., description="Response message")
    data: Optional[Dict[str, Any]] = Field(None, description="Response data")
    phase: str = Field("1", description="Phase number")
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class DomainInfo(BaseModel):
    """Information about a business domain"""
    domain: DomainType
    name: str = Field(..., description="Human-readable domain name")
    description: str = Field(..., description="Domain description")
    common_goals: List[str] = Field(default_factory=list, description="Common goals in this domain")
    typical_kpis: List[str] = Field(default_factory=list, description="Typical KPIs for this domain")
    data_characteristics: List[str] = Field(default_factory=list, description="Typical data characteristics")


class DomainCompatibilityResult(BaseModel):
    """Result of domain compatibility check"""
    domain: str = Field(..., description="Domain name")
    match_percentage: float = Field(..., description="Percentage match with expected columns")
    status: str = Field(..., description="Compatibility status: OK, WARN, or STOP")
    matched_columns: List[str] = Field(default_factory=list, description="Columns that matched")
    missing_columns: List[str] = Field(default_factory=list, description="Expected columns not found")
    suggestions: List[str] = Field(default_factory=list, description="Alternative domain suggestions")
    message: str = Field(..., description="Human-readable message")


class Phase1DomainCompatibilityResponse(BaseModel):
    """Response for domain compatibility check"""
    status: str = Field(..., description="Operation status")
    message: str = Field(..., description="Response message")
    data: Optional[DomainCompatibilityResult] = Field(None, description="Compatibility result")
    phase: str = Field("1", description="Phase number")
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class IngestionConfig(BaseModel):
    """Configuration for data ingestion"""
    source_file: str = Field(..., description="Source file path")
    target_format: str = Field(default="parquet", description="Target format (parquet)")
    compression: str = Field(default="zstd", description="Compression algorithm")
    chunk_size: Optional[int] = Field(None, description="Chunk size for large files")
    preserve_index: bool = Field(default=False, description="Whether to preserve DataFrame index")
    created_at: datetime = Field(default_factory=datetime.utcnow)


class IngestionResult(BaseModel):
    """Result of data ingestion process"""
    source_file: str = Field(..., description="Source file path")
    target_file: str = Field(..., description="Target file path")
    rows_ingested: int = Field(..., description="Number of rows ingested")
    columns_ingested: int = Field(..., description="Number of columns ingested")
    file_size_mb: float = Field(..., description="File size in MB")
    compression_ratio: float = Field(..., description="Compression ratio achieved")
    ingestion_time_seconds: float = Field(..., description="Time taken for ingestion")
    status: str = Field(..., description="Ingestion status")
    message: str = Field(..., description="Human-readable message")


class Phase2IngestionResponse(BaseModel):
    """Response for Phase 2 ingestion operations"""
    status: str = Field(..., description="Operation status")
    message: str = Field(..., description="Response message")
    data: Optional[IngestionResult] = Field(None, description="Ingestion result")
    phase: str = Field("2", description="Phase number")
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class SchemaValidationResult(BaseModel):
    """Result of schema validation and data type enforcement"""
    file_path: str = Field(..., description="Path to validated file")
    total_rows: int = Field(..., description="Total number of rows")
    total_columns: int = Field(..., description="Total number of columns")
    schema_violations: int = Field(..., description="Number of schema violations")
    violation_rate: float = Field(..., description="Rate of schema violations")
    status: str = Field(..., description="Validation status: OK, WARN, or ERROR")
    column_types: Dict[str, str] = Field(default_factory=dict, description="Final column types")
    type_conversions: Dict[str, str] = Field(default_factory=dict, description="Type conversions applied")
    warnings: List[str] = Field(default_factory=list, description="Validation warnings")
    message: str = Field(..., description="Human-readable message")


class Phase3SchemaResponse(BaseModel):
    """Response for Phase 3 schema operations"""
    status: str = Field(..., description="Operation status")
    message: str = Field(..., description="Response message")
    data: Optional[SchemaValidationResult] = Field(None, description="Schema validation result")
    phase: str = Field("3", description="Phase number")
    timestamp: datetime = Field(default_factory=datetime.utcnow)