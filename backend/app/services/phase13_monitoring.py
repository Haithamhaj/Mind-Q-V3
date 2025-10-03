from typing import Dict, List
import pandas as pd
import numpy as np
from pydantic import BaseModel
import json


class DriftConfig(BaseModel):
    feature: str
    baseline_mean: float
    baseline_std: float
    psi_warn_threshold: float
    psi_action_threshold: float
    ks_warn_threshold: float
    ks_action_threshold: float


class MonitoringResult(BaseModel):
    baseline_features: List[str]
    drift_configs: List[DriftConfig]
    baseline_timestamp: str


class MonitoringService:
    def __init__(self, df: pd.DataFrame):
        self.df = df
    
    def run(self) -> MonitoringResult:
        """Execute Phase 13: Monitoring & Drift Setup"""
        
        numeric_features = self.df.select_dtypes(include=[np.number]).columns
        
        drift_configs: List[DriftConfig] = []
        
        for feat in numeric_features:
            data = self.df[feat].dropna()
            
            if len(data) < 10:
                continue
            
            config = DriftConfig(
                feature=feat,
                baseline_mean=float(data.mean()),
                baseline_std=float(data.std()),
                psi_warn_threshold=0.10,
                psi_action_threshold=0.25,
                ks_warn_threshold=0.10,
                ks_action_threshold=0.20
            )
            
            drift_configs.append(config)
        
        result = MonitoringResult(
            baseline_features=[f.feature for f in drift_configs],
            drift_configs=drift_configs,
            baseline_timestamp=pd.Timestamp.now().isoformat()
        )
        
        return result


