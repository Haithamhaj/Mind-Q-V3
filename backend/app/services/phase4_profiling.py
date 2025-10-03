from typing import Dict, List
import pandas as pd
import numpy as np
from scipy import stats
from pydantic import BaseModel
import json
from pathlib import Path

from ..config import settings


class DataQualityIssue(BaseModel):
    severity: str  # "critical", "high", "medium", "low"
    column: str
    issue_type: str
    description: str
    affected_rows: int
    affected_pct: float


class ProfilingResult(BaseModel):
    total_rows: int
    total_columns: int
    memory_usage_mb: float
    numeric_summary: Dict
    categorical_summary: Dict
    missing_summary: Dict
    top_issues: List[DataQualityIssue]
    correlation_preview: Dict


class ProfilingService:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.issues: List[DataQualityIssue] = []
    
    def run(self) -> ProfilingResult:
        """Execute Phase 4: Profiling"""
        # Basic stats
        memory_mb = self.df.memory_usage(deep=True).sum() / (1024**2)
        
        # Numeric summary
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        numeric_summary = self._profile_numeric(numeric_cols)
        
        # Categorical summary
        cat_cols = self.df.select_dtypes(include=['object', 'category', 'string']).columns
        categorical_summary = self._profile_categorical(cat_cols)
        
        # Missing data summary
        missing_summary = self._profile_missing()
        
        # Correlation preview (top 5 pairs)
        correlation_preview = self._correlation_preview(numeric_cols)
        
        # Identify top issues
        self._identify_issues()
        top_issues = sorted(self.issues, key=lambda x: self._severity_score(x.severity), reverse=True)[:10]
        
        # Persist top 10 issues to dq_report.json (spec requirement)
        artifacts = settings.artifacts_dir
        artifacts.mkdir(exist_ok=True)
        dq_path = artifacts / "dq_report.json"
        with open(dq_path, "w", encoding="utf-8") as f:
            json.dump({
                "top_10_issues": [issue.model_dump() for issue in top_issues]
            }, f, indent=2)
        
        return ProfilingResult(
            total_rows=len(self.df),
            total_columns=len(self.df.columns),
            memory_usage_mb=round(memory_mb, 2),
            numeric_summary=numeric_summary,
            categorical_summary=categorical_summary,
            missing_summary=missing_summary,
            top_issues=top_issues,
            correlation_preview=correlation_preview
        )
    
    def _profile_numeric(self, cols) -> Dict:
        """Profile numeric columns"""
        if len(cols) == 0:
            return {}
        
        summary: Dict[str, Dict] = {}
        for col in cols:
            data = self.df[col].dropna()
            if len(data) == 0:
                continue
            
            summary[col] = {
                "count": int(len(data)),
                "mean": float(data.mean()),
                "median": float(data.median()),
                "std": float(data.std()),
                "min": float(data.min()),
                "max": float(data.max()),
                "q25": float(data.quantile(0.25)),
                "q75": float(data.quantile(0.75)),
                "skewness": float(stats.skew(data)),
                "kurtosis": float(stats.kurtosis(data))
            }
        
        return summary
    
    def _profile_categorical(self, cols) -> Dict:
        """Profile categorical columns"""
        if len(cols) == 0:
            return {}
        
        summary: Dict[str, Dict] = {}
        for col in cols:
            value_counts = self.df[col].value_counts(dropna=False)
            
            summary[col] = {
                "unique_count": int(self.df[col].nunique(dropna=False)),
                "most_common": (str(value_counts.index[0]) if len(value_counts) > 0 else None),
                "most_common_freq": int(value_counts.iloc[0]) if len(value_counts) > 0 else 0,
                "top_5": {str(k): int(v) for k, v in value_counts.head(5).items()}
            }
        
        return summary
    
    def _profile_missing(self) -> Dict:
        """Profile missing data"""
        if len(self.df) == 0:
            return {}
        missing_count = self.df.isnull().sum()
        missing_pct = missing_count / len(self.df)
        
        return {
            col: {
                "count": int(missing_count[col]),
                "percentage": float(missing_pct[col])
            }
            for col in self.df.columns if missing_count[col] > 0
        }
    
    def _correlation_preview(self, numeric_cols) -> Dict:
        """Get top correlated pairs"""
        if len(numeric_cols) < 2:
            return {}
        
        corr_matrix = self.df[numeric_cols].corr(numeric_only=True)
        
        # Extract upper triangle
        pairs: List[Dict] = []
        for i in range(len(corr_matrix.columns)):
            for j in range(i+1, len(corr_matrix.columns)):
                pairs.append({
                    "col1": corr_matrix.columns[i],
                    "col2": corr_matrix.columns[j],
                    "correlation": float(corr_matrix.iloc[i, j])
                })
        
        # Sort by absolute correlation
        pairs_sorted = sorted(pairs, key=lambda x: abs(x["correlation"]), reverse=True)
        
        return {"top_5_pairs": pairs_sorted[:5]}
    
    def _identify_issues(self):
        """Identify data quality issues"""
        if len(self.df) == 0:
            return
        
        # High missing %
        for col in self.df.columns:
            missing_pct = self.df[col].isnull().sum() / len(self.df)
            if missing_pct > 0.20:
                self.issues.append(DataQualityIssue(
                    severity="critical",
                    column=col,
                    issue_type="high_missing",
                    description=f"High missing data: {missing_pct:.1%}",
                    affected_rows=int(self.df[col].isnull().sum()),
                    affected_pct=float(missing_pct)
                ))
            elif missing_pct > 0.10:
                self.issues.append(DataQualityIssue(
                    severity="high",
                    column=col,
                    issue_type="moderate_missing",
                    description=f"Moderate missing data: {missing_pct:.1%}",
                    affected_rows=int(self.df[col].isnull().sum()),
                    affected_pct=float(missing_pct)
                ))
        
        # High cardinality in categoricals
        cat_cols = self.df.select_dtypes(include=['object', 'category', 'string']).columns
        for col in cat_cols:
            cardinality = self.df[col].nunique(dropna=False)
            if cardinality > 1000:
                self.issues.append(DataQualityIssue(
                    severity="medium",
                    column=col,
                    issue_type="high_cardinality",
                    description=f"Very high cardinality: {cardinality} unique values",
                    affected_rows=len(self.df),
                    affected_pct=1.0
                ))
        
        # Outliers in numeric columns
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            data = self.df[col].dropna()
            if len(data) < 10:
                continue
            
            q1, q3 = data.quantile([0.25, 0.75])
            iqr = q3 - q1
            lower_bound = q1 - 3 * iqr
            upper_bound = q3 + 3 * iqr
            
            outliers = ((data < lower_bound) | (data > upper_bound)).sum()
            outlier_pct = outliers / len(data)
            
            if outlier_pct > 0.05:
                self.issues.append(DataQualityIssue(
                    severity="medium",
                    column=col,
                    issue_type="outliers",
                    description=f"Extreme outliers detected: {outlier_pct:.1%}",
                    affected_rows=int(outliers),
                    affected_pct=float(outlier_pct)
                ))
    
    def _severity_score(self, severity: str) -> int:
        return {"critical": 4, "high": 3, "medium": 2, "low": 1}.get(severity, 0)


