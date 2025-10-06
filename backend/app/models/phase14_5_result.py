from pydantic import BaseModel
from typing import List, Dict, Optional


class FeatureInsight(BaseModel):
    feature_name: str
    importance_score: float
    importance_rank: int
    explanation: str
    business_impact: str
    recommendation: str


class ModelInsight(BaseModel):
    model_name: str
    strengths: List[str]
    weaknesses: List[str]
    when_to_use: str
    comparison_with_others: str


class ConfusionMatrixInsight(BaseModel):
    true_negatives: int
    false_positives: int
    false_negatives: int
    true_positives: int
    fp_cost_explanation: str
    fn_cost_explanation: str
    which_is_worse: str


class Recommendation(BaseModel):
    id: str
    title: str
    description: str
    rationale: str
    impact: str
    effort: str
    priority: int
    action_type: str
    expected_improvement: str


class DecisionLogEntry(BaseModel):
    timestamp: str
    decision_type: str
    description: str
    made_by: str
    rationale: str


class ChatMessage(BaseModel):
    role: str
    content: str
    timestamp: str
    artifacts_referenced: Optional[List[str]] = None


class Phase14_5Result(BaseModel):
    timestamp: str
    llm_provider: str
    feature_insights: List[FeatureInsight]
    model_insights: List[ModelInsight]
    confusion_matrix_insights: Dict[str, ConfusionMatrixInsight]
    recommendations: List[Recommendation]
    decision_log: List[DecisionLogEntry]
    executive_summary: str
    key_findings: List[str]
    next_steps: List[str]


