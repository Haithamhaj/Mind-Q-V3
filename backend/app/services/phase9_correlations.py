from typing import List
import pandas as pd
import numpy as np
from scipy import stats
from scipy.stats import chi2_contingency
from pydantic import BaseModel
import warnings

warnings.filterwarnings('ignore')


class CorrelationPair(BaseModel):
    feature1: str
    feature2: str
    correlation: float
    p_value: float
    method: str  # "pearson", "spearman", "cramers_v", "chi2"
    n: int


class CorrelationsResult(BaseModel):
    numeric_correlations: List[CorrelationPair]
    categorical_associations: List[CorrelationPair]
    fdr_applied: bool
    total_tests: int


class CorrelationsService:
    def __init__(self, df: pd.DataFrame):
        self.df = df
    
    def run(self) -> CorrelationsResult:
        """Execute Phase 9: Summaries & Correlations with robust error handling"""
        try:
            # Validate and clean dataframe
            if self.df is None or self.df.empty:
                return self._empty_result()
            
            # Convert problematic dtypes safely
            self._convert_problematic_dtypes()
            
            # Calculate correlations with individual error handling
            num_corrs = self._safe_numeric_correlations()
            cat_assocs = self._safe_categorical_associations()
            
            total_tests = len(num_corrs) + len(cat_assocs)
            
            # Apply FDR correction if applicable
            fdr_applied = False
            if total_tests > 20:
                fdr_applied = self._apply_safe_fdr_correction(num_corrs + cat_assocs)
            
            return CorrelationsResult(
                numeric_correlations=num_corrs,
                categorical_associations=cat_assocs,
                fdr_applied=fdr_applied,
                total_tests=total_tests
            )
        except Exception:
            # Graceful degradation - return empty result instead of crashing
            return self._empty_result()
    
    def _empty_result(self) -> CorrelationsResult:
        """Return empty result for error cases"""
        return CorrelationsResult(
            numeric_correlations=[],
            categorical_associations=[],
            fdr_applied=False,
            total_tests=0
        )
    
    def _convert_problematic_dtypes(self):
        """Safely convert problematic pandas dtypes"""
        for col in self.df.columns:
            try:
                dtype_str = str(self.df[col].dtype)
                if dtype_str == 'string[python]':
                    self.df[col] = self.df[col].astype('object')
                elif 'datetime64[ns, UTC]' in dtype_str:
                    self.df[col] = pd.to_datetime(self.df[col]).dt.tz_localize(None)
                elif 'datetime64[ns]' in dtype_str and self.df[col].dtype != 'object':
                    # Handle timezone-naive datetime
                    self.df[col] = pd.to_datetime(self.df[col])
            except Exception:
                # If conversion fails, skip this column
                continue
    
    def _safe_numeric_correlations(self) -> List[CorrelationPair]:
        """Calculate numeric correlations with robust error handling"""
        try:
            # Get numeric columns, excluding boolean types
            numeric_cols = self.df.select_dtypes(include=[np.number]).columns
            numeric_cols = [col for col in numeric_cols if self.df[col].dtype != 'bool']
            
            if len(numeric_cols) < 2:
                return []
            
            correlations: List[CorrelationPair] = []
            
            for i in range(len(numeric_cols)):
                for j in range(i + 1, len(numeric_cols)):
                    col1, col2 = numeric_cols[i], numeric_cols[j]
                    
                    try:
                        # Get clean numeric data
                        data1, data2 = self._get_clean_numeric_pair(col1, col2)
                        
                        if len(data1) < 10:
                            continue
                        
                        # Calculate Pearson correlation
                        r, p = stats.pearsonr(data1, data2)
                        
                        # Validate results
                        if not (np.isfinite(r) and np.isfinite(p)):
                            continue
                        
                        correlations.append(CorrelationPair(
                            feature1=col1,
                            feature2=col2,
                            correlation=round(float(r), 4),
                            p_value=round(float(p), 4),
                            method="pearson",
                            n=int(len(data1))
                        ))
                    except Exception:
                        # Skip this pair if calculation fails
                        continue
            
            return correlations
        except Exception:
            return []
    
    def _safe_categorical_associations(self) -> List[CorrelationPair]:
        """Calculate categorical associations with robust error handling"""
        try:
            # Get categorical columns
            cat_cols = self.df.select_dtypes(include=['object', 'category']).columns
            
            if len(cat_cols) < 2:
                return []
            
            associations: List[CorrelationPair] = []
            
            for i in range(len(cat_cols)):
                for j in range(i + 1, len(cat_cols)):
                    col1, col2 = cat_cols[i], cat_cols[j]
                    
                    try:
                        # Get clean categorical data
                        data1, data2 = self._get_clean_categorical_pair(col1, col2)
                        
                        if len(data1) < 10:
                            continue
                        
                        # Create contingency table
                        contingency = pd.crosstab(data1, data2)
                        
                        if contingency.shape[0] < 2 or contingency.shape[1] < 2:
                            continue
                        
                        # Chi-square test
                        chi2, p, _, _ = chi2_contingency(contingency)
                        
                        # Cramér's V calculation
                        n = contingency.values.sum()
                        denom = n * (min(contingency.shape) - 1)
                        if denom <= 0:
                            continue
                        
                        cramers_v = np.sqrt(chi2 / denom)
                        
                        # Validate results
                        if not (np.isfinite(cramers_v) and np.isfinite(p)):
                            continue
                        
                        associations.append(CorrelationPair(
                            feature1=col1,
                            feature2=col2,
                            correlation=round(float(cramers_v), 4),
                            p_value=round(float(p), 4),
                            method="cramers_v",
                            n=int(n)
                        ))
                    except Exception:
                        # Skip this pair if calculation fails
                        continue
            
            return associations
        except Exception:
            return []
    
    def _get_clean_numeric_pair(self, col1: str, col2: str):
        """Get clean numeric data for correlation calculation"""
        # Remove NaNs and convert to numeric
        mask = self.df[[col1, col2]].notna().all(axis=1)
        data1 = pd.to_numeric(self.df.loc[mask, col1], errors='coerce')
        data2 = pd.to_numeric(self.df.loc[mask, col2], errors='coerce')
        
        # Remove any remaining NaNs after conversion
        mask2 = data1.notna() & data2.notna()
        return data1[mask2], data2[mask2]
    
    def _get_clean_categorical_pair(self, col1: str, col2: str):
        """Get clean categorical data for association calculation"""
        # Remove NaNs and convert to string
        mask = self.df[[col1, col2]].notna().all(axis=1)
        data1 = self.df.loc[mask, col1].astype(str)
        data2 = self.df.loc[mask, col2].astype(str)
        
        return data1, data2
    
    def _apply_safe_fdr_correction(self, correlations: List[CorrelationPair]) -> bool:
        """Apply FDR correction with fallback to Bonferroni"""
        try:
            from statsmodels.stats.multitest import multipletests
            
            p_values = [c.p_value for c in correlations]
            _, p_adjusted, _, _ = multipletests(p_values, method='fdr_bh')
            
            for i, corr in enumerate(correlations):
                corr.p_value = round(float(p_adjusted[i]), 4)
            
            return True
        except ImportError:
            # Fallback: simple Bonferroni correction
            n_tests = len(correlations)
            for corr in correlations:
                corr.p_value = min(1.0, corr.p_value * n_tests)
            return False
        except Exception:
            # If FDR correction fails, leave p-values unchanged
            return False
    










