from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field, field_validator


Scalar = Union[str, int, float, bool]


class FilterCondition(BaseModel):
    operator: Literal[
        "equals",
        "not_equals",
        "in",
        "not_in",
        "gt",
        "gte",
        "lt",
        "lte",
        "between",
        "exists",
        "not_exists",
    ] = "equals"
    column: Optional[str] = None
    value: Optional[Scalar] = None
    values: Optional[List[Scalar]] = None
    lower: Optional[Scalar] = None
    upper: Optional[Scalar] = None
    range: Optional[List[Scalar]] = None

    @field_validator("values", mode="before")
    @classmethod
    def ensure_list(cls, v: Any) -> Any:
        if v is None or isinstance(v, list):
            return v
        return [v]

    @field_validator("range", mode="before")
    @classmethod
    def ensure_range(cls, v: Any) -> Any:
        if v is None or isinstance(v, list):
            return v
        if isinstance(v, tuple):
            return list(v)
        return [v]


class AggregationSpec(BaseModel):
    aggregation: Literal["count", "sum", "mean", "median", "nunique"] = "count"
    column: Optional[str] = None
    filter: Optional[FilterCondition] = None


class RatioFormula(BaseModel):
    type: Literal["ratio"] = "ratio"
    numerator: AggregationSpec
    denominator: AggregationSpec
    multiplier: float = 1.0
    format: Literal["percentage", "ratio", "rate", "index"] = "percentage"
    guard_rail: Literal["clamp_zero", "skip_on_zero"] = "clamp_zero"


class SingleValueFormula(BaseModel):
    type: Literal["average", "sum", "count", "median"] = "average"
    aggregation: AggregationSpec
    multiplier: float = 1.0
    format: Literal["number", "currency", "percentage", "rate"] = "number"


KPIFormula = Annotated[Union[RatioFormula, SingleValueFormula], Field(discriminator="type")]


class KPIProposal(BaseModel):
    kpi_id: str
    name: str
    alias: str
    metric_type: Literal["ratio", "efficiency", "quality", "financial", "custom"] = "ratio"
    description: str
    rationale: str
    financial_impact: Optional[str] = None
    confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    recommended_direction: Literal["higher_is_better", "lower_is_better", "target_range"] = "higher_is_better"
    formula: Optional[KPIFormula] = None
    required_columns: List[str] = Field(default_factory=list)
    supporting_evidence: List[str] = Field(default_factory=list)
    why_selected: Optional[str] = None
    expected_outcome: Optional[str] = None
    monitoring_guidance: Optional[str] = None
    tradeoffs: Optional[str] = None
    source: Literal["llm", "system", "custom_slot"] = "llm"
    warnings: List[str] = Field(default_factory=list)
    notes: Optional[str] = None
    editable: bool = True

    @field_validator("alias")
    @classmethod
    def sanitize_alias(cls, value: str) -> str:
        value = value.strip()
        return value or "kpi_candidate"

    @field_validator("required_columns", mode="before")
    @classmethod
    def ensure_required_columns(cls, value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return [str(v) for v in value]
        return [str(value)]


class KPIProposalBundle(BaseModel):
    proposals: List[KPIProposal]
    warnings: List[str] = Field(default_factory=list)
    context_snapshot: Dict[str, Any] = Field(default_factory=dict)
    explanation: Optional[str] = None


class KPIValidationRequest(BaseModel):
    proposals: List[KPIProposal]


class KPIValidationResult(BaseModel):
    proposal: KPIProposal
    status: Literal["pass", "warn", "fail"]
    computed_value: Optional[float] = None
    formatted_value: Optional[str] = None
    reason: Optional[str] = None


class KPIValidationResponse(BaseModel):
    results: List[KPIValidationResult]
    warnings: List[str] = Field(default_factory=list)


class KPIAdoptionRequest(BaseModel):
    proposal: KPIProposal
    adopted_name: Optional[str] = None
    notes: Optional[str] = None
    adopted_at: Optional[datetime] = None


class KPIAdoptionResponse(BaseModel):
    message: str
    entry: Dict[str, Any]
