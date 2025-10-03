"""
Phase 1: Goal & KPIs Definition Service
Handles business goal definition and KPI configuration.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime
import json
from pathlib import Path

from ..models.schemas import (
    GoalDefinition, KPIDefinition, DomainSelection, 
    Phase1Config, Phase1Response, DomainInfo, DomainType
)
from ..config import settings


class Phase1Service:
    """
    Service for managing Phase 1: Goal & KPIs Definition
    """
    
    def __init__(self):
        self.config_file = settings.artifacts_dir / "phase1_config.json"
        self.domains_info = self._load_domains_info()
    
    def _load_domains_info(self) -> Dict[str, DomainInfo]:
        """Load domain information from spec file"""
        try:
            with open(settings.spec_path, 'r') as f:
                spec_data = json.load(f)
            
            domains_info = {}
            for domain in spec_data.get("domains", []):
                domain_info = self._get_domain_info(domain)
                domains_info[domain] = domain_info
            
            return domains_info
        except Exception as e:
            # Fallback to default domain info
            return self._get_default_domains_info()
    
    def _get_domain_info(self, domain: str) -> DomainInfo:
        """Get information for a specific domain"""
        domain_mapping = {
            "finance": DomainInfo(
                domain=DomainType.FINANCE,
                name="Finance",
                description="Financial services, banking, insurance, and investment",
                common_goals=[
                    "Fraud detection",
                    "Credit risk assessment", 
                    "Customer churn prediction",
                    "Portfolio optimization",
                    "Market trend analysis"
                ],
                typical_kpis=[
                    "Accuracy", "Precision", "Recall", "F1-Score",
                    "ROC-AUC", "MAE", "RMSE", "Profit/Loss"
                ],
                data_characteristics=[
                    "Time series data", "High-frequency transactions",
                    "Regulatory compliance", "Sensitive financial data"
                ]
            ),
            "healthcare": DomainInfo(
                domain=DomainType.HEALTHCARE,
                name="Healthcare",
                description="Medical services, patient care, and health analytics",
                common_goals=[
                    "Disease diagnosis",
                    "Treatment outcome prediction",
                    "Patient readmission risk",
                    "Drug effectiveness analysis",
                    "Health trend monitoring"
                ],
                typical_kpis=[
                    "Sensitivity", "Specificity", "Accuracy",
                    "Clinical accuracy", "Patient outcomes"
                ],
                data_characteristics=[
                    "Patient privacy", "Medical imaging", "Clinical trials",
                    "HIPAA compliance", "Longitudinal data"
                ]
            ),
            "retail": DomainInfo(
                domain=DomainType.RETAIL,
                name="Retail",
                description="E-commerce, sales, and customer analytics",
                common_goals=[
                    "Customer segmentation",
                    "Sales forecasting",
                    "Inventory optimization",
                    "Recommendation systems",
                    "Price optimization"
                ],
                typical_kpis=[
                    "Conversion rate", "Customer lifetime value",
                    "Inventory turnover", "Revenue growth"
                ],
                data_characteristics=[
                    "Customer behavior", "Seasonal patterns",
                    "Product catalogs", "Transaction data"
                ]
            ),
            "manufacturing": DomainInfo(
                domain=DomainType.MANUFACTURING,
                name="Manufacturing",
                description="Production, quality control, and supply chain",
                common_goals=[
                    "Quality prediction",
                    "Predictive maintenance",
                    "Supply chain optimization",
                    "Production efficiency",
                    "Defect detection"
                ],
                typical_kpis=[
                    "Quality metrics", "Equipment uptime",
                    "Production yield", "Cost reduction"
                ],
                data_characteristics=[
                    "IoT sensor data", "Production metrics",
                    "Quality measurements", "Supply chain data"
                ]
            ),
            "technology": DomainInfo(
                domain=DomainType.TECHNOLOGY,
                name="Technology",
                description="Software, IT services, and digital platforms",
                common_goals=[
                    "System performance optimization",
                    "User behavior analysis",
                    "Security threat detection",
                    "Resource allocation",
                    "Feature adoption prediction"
                ],
                typical_kpis=[
                    "System uptime", "Response time",
                    "User engagement", "Security metrics"
                ],
                data_characteristics=[
                    "Log data", "User interactions",
                    "Performance metrics", "Security events"
                ]
            ),
            "education": DomainInfo(
                domain=DomainType.EDUCATION,
                name="Education",
                description="Educational institutions and learning analytics",
                common_goals=[
                    "Student performance prediction",
                    "Learning outcome optimization",
                    "Resource allocation",
                    "Curriculum effectiveness",
                    "Dropout prevention"
                ],
                typical_kpis=[
                    "Student success rate", "Learning outcomes",
                    "Engagement metrics", "Retention rate"
                ],
                data_characteristics=[
                    "Student records", "Assessment data",
                    "Learning analytics", "Academic performance"
                ]
            ),
            "government": DomainInfo(
                domain=DomainType.GOVERNMENT,
                name="Government",
                description="Public services and policy analytics",
                common_goals=[
                    "Policy impact assessment",
                    "Resource optimization",
                    "Service delivery improvement",
                    "Citizen satisfaction",
                    "Fraud detection"
                ],
                typical_kpis=[
                    "Service efficiency", "Citizen satisfaction",
                    "Cost effectiveness", "Policy outcomes"
                ],
                data_characteristics=[
                    "Public records", "Service data",
                    "Policy metrics", "Citizen feedback"
                ]
            ),
            "general": DomainInfo(
                domain=DomainType.GENERAL,
                name="General",
                description="General purpose data analysis",
                common_goals=[
                    "Pattern recognition",
                    "Data exploration",
                    "Trend analysis",
                    "Anomaly detection",
                    "Insight generation"
                ],
                typical_kpis=[
                    "Accuracy", "Precision", "Recall",
                    "F1-Score", "Business metrics"
                ],
                data_characteristics=[
                    "Mixed data types", "Various formats",
                    "Flexible requirements", "Custom metrics"
                ]
            )
        }
        
        return domain_mapping.get(domain, domain_mapping["general"])
    
    def _get_default_domains_info(self) -> Dict[str, DomainInfo]:
        """Get default domain information"""
        domains = ["finance", "healthcare", "retail", "manufacturing", 
                  "technology", "education", "government", "general"]
        return {domain: self._get_domain_info(domain) for domain in domains}
    
    def get_available_domains(self) -> List[DomainInfo]:
        """Get list of available domains with their information"""
        return list(self.domains_info.values())
    
    def get_domain_info(self, domain: DomainType) -> Optional[DomainInfo]:
        """Get information for a specific domain"""
        return self.domains_info.get(domain.value)
    
    def save_domain_selection(self, domain_selection: DomainSelection) -> Phase1Response:
        """Save domain selection"""
        try:
            config = self._load_config()
            config.domain_selection = domain_selection
            config.updated_at = datetime.utcnow()
            
            self._save_config(config)
            
            return Phase1Response(
                status="success",
                message=f"Domain '{domain_selection.domain}' selected successfully",
                data={"domain": domain_selection.domain.value}
            )
        except Exception as e:
            return Phase1Response(
                status="error",
                message=f"Failed to save domain selection: {str(e)}"
            )
    
    def add_goal(self, goal: GoalDefinition) -> Phase1Response:
        """Add a new goal"""
        try:
            config = self._load_config()
            
            # Check if goal ID already exists
            existing_goal_ids = [g.goal_id for g in config.goals]
            if goal.goal_id in existing_goal_ids:
                return Phase1Response(
                    status="error",
                    message=f"Goal with ID '{goal.goal_id}' already exists"
                )
            
            config.goals.append(goal)
            config.updated_at = datetime.utcnow()
            
            self._save_config(config)
            
            return Phase1Response(
                status="success",
                message=f"Goal '{goal.title}' added successfully",
                data={"goal_id": goal.goal_id}
            )
        except Exception as e:
            return Phase1Response(
                status="error",
                message=f"Failed to add goal: {str(e)}"
            )
    
    def add_kpi(self, kpi: KPIDefinition) -> Phase1Response:
        """Add a new KPI"""
        try:
            config = self._load_config()
            
            # Check if KPI ID already exists
            existing_kpi_ids = [k.kpi_id for k in config.kpis]
            if kpi.kpi_id in existing_kpi_ids:
                return Phase1Response(
                    status="error",
                    message=f"KPI with ID '{kpi.kpi_id}' already exists"
                )
            
            config.kpis.append(kpi)
            config.updated_at = datetime.utcnow()
            
            self._save_config(config)
            
            return Phase1Response(
                status="success",
                message=f"KPI '{kpi.name}' added successfully",
                data={"kpi_id": kpi.kpi_id}
            )
        except Exception as e:
            return Phase1Response(
                status="error",
                message=f"Failed to add KPI: {str(e)}"
            )
    
    def get_config(self) -> Phase1Response:
        """Get current Phase 1 configuration"""
        try:
            config = self._load_config()
            return Phase1Response(
                status="success",
                message="Phase 1 configuration retrieved successfully",
                data={
                    "domain_selection": config.domain_selection.dict() if config.domain_selection else None,
                    "goals": [goal.dict() for goal in config.goals],
                    "kpis": [kpi.dict() for kpi in config.kpis],
                    "created_at": config.created_at.isoformat(),
                    "updated_at": config.updated_at.isoformat()
                }
            )
        except Exception as e:
            return Phase1Response(
                status="error",
                message=f"Failed to get configuration: {str(e)}"
            )
    
    def validate_config(self) -> Phase1Response:
        """Validate Phase 1 configuration completeness"""
        try:
            config = self._load_config()
            validation_results = {
                "domain_selected": config.domain_selection is not None,
                "goals_count": len(config.goals),
                "kpis_count": len(config.kpis),
                "primary_kpis": len([kpi for kpi in config.kpis if kpi.is_primary]),
                "is_complete": False
            }
            
            # Check completeness criteria
            is_complete = (
                validation_results["domain_selected"] and
                validation_results["goals_count"] > 0 and
                validation_results["kpis_count"] > 0 and
                validation_results["primary_kpis"] > 0
            )
            validation_results["is_complete"] = is_complete
            
            status = "success" if is_complete else "warning"
            message = "Configuration is complete" if is_complete else "Configuration is incomplete"
            
            return Phase1Response(
                status=status,
                message=message,
                data=validation_results
            )
        except Exception as e:
            return Phase1Response(
                status="error",
                message=f"Failed to validate configuration: {str(e)}"
            )
    
    def _load_config(self) -> Phase1Config:
        """Load configuration from file"""
        if self.config_file.exists():
            with open(self.config_file, 'r') as f:
                data = json.load(f)
            return Phase1Config(**data)
        else:
            return Phase1Config(
                domain_selection=None,
                goals=[],
                kpis=[]
            )
    
    def _save_config(self, config: Phase1Config) -> None:
        """Save configuration to file"""
        with open(self.config_file, 'w') as f:
            json.dump(config.dict(), f, indent=2, default=str)
