from __future__ import annotations
from typing import Any, Dict, List, Optional
from pydantic import BaseModel


class PhaseRequest(BaseModel):
    job_id: Optional[str] = None
    phase: int


class Decision(BaseModel):
    title: str
    impact: str
    options: List[Dict[str, Any]]


class GoalScore(BaseModel):
    score_pct: int
    components: List[Dict[str, Any]] = []


class PhaseNode(BaseModel):
    phase: int
    name: str
    status: str = "READY"
    timer_s: int = 0
    requires: List[Dict[str, Any]] = []
    unlocks: List[Dict[str, Any]] = []
    logic: Dict[str, Any] = {}
    best_practices: List[str] = []
    executed: Dict[str, Any] = {}
    goal_score: GoalScore | None = None
    decisions: List[Decision] = []


class QARequest(BaseModel):
    job_id: Optional[str] = None
    phase: Optional[int] = None
    question: str


class QAResponse(BaseModel):
    answer: str
    sources: List[str]
