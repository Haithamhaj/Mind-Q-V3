"""
Services module for the EDA platform.
"""

from .phase0_quality_control import QualityControlService, QualityControlResult
from .phase1_goal_kpis import Phase1Service, GoalKPIsService, GoalKPIsResult, DomainCompatibilityResult
from .phase2_ingestion import Phase2IngestionService, IngestionService, IngestionResult
from .phase3_schema import Phase3SchemaService, SchemaService, SchemaResult
from .domain_packs import DomainPack, DOMAIN_PACKS, get_domain_pack, suggest_domain

__all__ = [
    "QualityControlService",
    "QualityControlResult",
    "Phase1Service",
    "GoalKPIsService",
    "GoalKPIsResult",
    "DomainCompatibilityResult",
    "Phase2IngestionService",
    "IngestionService",
    "IngestionResult",
    "Phase3SchemaService",
    "SchemaService",
    "SchemaResult",
    "DomainPack",
    "DOMAIN_PACKS",
    "get_domain_pack",
    "suggest_domain"
]

