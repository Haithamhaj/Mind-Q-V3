"""
Phase 4: Data Profiling Service - Clean Implementation
Minimal implementation without any Unicode characters
"""

from typing import Dict, List, Optional, Any
import pandas as pd
import numpy as np
from datetime import datetime
from pydantic import BaseModel


class ProfilingStatistics(BaseModel):
    """Data profiling statistics"""
    numeric_summary: Dict[str, Dict[str, float]]
    categorical_summary: Dict[str, Dict[str, Any]]
    missing_data_summary: Dict[str, float]
    data_types_summary: Dict[str, str]
    memory_usage: float
    row_count: int
    column_count: int


class ProfilingResult(BaseModel):
    """Result model for data profiling"""
    status: str
    message: str
    statistics: ProfilingStatistics
    timestamp: str


class ProfilingService:
    """Clean Data Profiling Service without Unicode characters"""
    
    def __init__(self, df: pd.DataFrame):
        self.df = df
    
    def run(self) -> ProfilingResult:
        """Execute data profiling analysis"""
        try:
            # Generate statistics
            statistics = self._generate_statistics()
            
            return ProfilingResult(
                status="success",
                message="Data profiling completed successfully",
                statistics=statistics,
                timestamp=datetime.utcnow().isoformat()
            )
            
        except Exception as e:
            print(f"Error in ProfilingService: {e}")
            # Return fallback result
            return ProfilingResult(
                status="error",
                message=f"Data profiling failed: {str(e)}",
                statistics=ProfilingStatistics(
                    numeric_summary={},
                    categorical_summary={},
                    missing_data_summary={},
                    data_types_summary={},
                    memory_usage=0.0,
                    row_count=0,
                    column_count=0
                ),
                timestamp=datetime.utcnow().isoformat()
            )
    
    def _generate_statistics(self) -> ProfilingStatistics:
        """Generate comprehensive statistics"""
        try:
            # Basic info
            row_count = len(self.df)
            column_count = len(self.df.columns)
            memory_usage = self.df.memory_usage(deep=True).sum() / (1024 * 1024)  # MB
            
            # Data types summary
            data_types_summary = {col: str(dtype) for col, dtype in self.df.dtypes.items()}
            
            # Missing data summary
            missing_data_summary = {}
            for col in self.df.columns:
                missing_count = self.df[col].isnull().sum()
                missing_percentage = (missing_count / row_count) * 100 if row_count > 0 else 0
                missing_data_summary[col] = round(missing_percentage, 2)
            
            # Numeric columns summary
            numeric_summary = {}
            numeric_cols = self.df.select_dtypes(include=[np.number]).columns
            
            for col in numeric_cols:
                try:
                    if self.df[col].dtype != bool:  # Skip boolean columns
                        numeric_summary[col] = {
                            "mean": round(float(self.df[col].mean()), 4),
                            "median": round(float(self.df[col].median()), 4),
                            "std": round(float(self.df[col].std()), 4),
                            "min": round(float(self.df[col].min()), 4),
                            "max": round(float(self.df[col].max()), 4),
                            "count": int(self.df[col].count()),
                            "null_count": int(self.df[col].isnull().sum())
                        }
                except Exception as e:
                    print(f"Error processing numeric column {col}: {e}")
                    continue
            
            # Categorical columns summary
            categorical_summary = {}
            categorical_cols = self.df.select_dtypes(include=['object', 'category']).columns
            
            for col in categorical_cols:
                try:
                    unique_count = self.df[col].nunique()
                    most_common = self.df[col].mode().iloc[0] if len(self.df[col].mode()) > 0 else None
                    most_common_count = int(self.df[col].value_counts().iloc[0]) if len(self.df[col].value_counts()) > 0 else 0
                    
                    categorical_summary[col] = {
                        "unique_count": int(unique_count),
                        "most_common": str(most_common) if most_common is not None else None,
                        "most_common_count": most_common_count,
                        "null_count": int(self.df[col].isnull().sum())
                    }
                except Exception as e:
                    print(f"Error processing categorical column {col}: {e}")
                    continue
            
            return ProfilingStatistics(
                numeric_summary=numeric_summary,
                categorical_summary=categorical_summary,
                missing_data_summary=missing_data_summary,
                data_types_summary=data_types_summary,
                memory_usage=round(memory_usage, 2),
                row_count=row_count,
                column_count=column_count
            )
            
        except Exception as e:
            print(f"Error generating statistics: {e}")
            return ProfilingStatistics(
                numeric_summary={},
                categorical_summary={},
                missing_data_summary={},
                data_types_summary={},
                memory_usage=0.0,
                row_count=0,
                column_count=0
            )
