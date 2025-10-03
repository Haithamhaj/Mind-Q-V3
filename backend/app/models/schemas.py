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
