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
        """Execute Phase 9: Summaries & Correlations"""
        
        # Numeric-Numeric correlations
        num_corrs = self._numeric_correlations()
        
        # Categorical-Categorical associations
        cat_assocs = self._categororical_associations()
        
        total_tests = len(num_corrs) + len(cat_assocs)
        
        # Apply FDR correction if > 20 tests
        fdr_applied = False
        if total_tests > 20:
            self._apply_fdr_correction(num_corrs + cat_assocs)
            fdr_applied = True
        
        result = CorrelationsResult(
            numeric_correlations=num_corrs,
            categorical_associations=cat_assocs,
            fdr_applied=fdr_applied,
            total_tests=total_tests
        )
        
        return result
    
    def _numeric_correlations(self) -> List[CorrelationPair]:
        """Calculate numeric-numeric correlations (Pearson)"""
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        
        if len(numeric_cols) < 2:
            return []
        
        correlations: List[CorrelationPair] = []
        
        for i in range(len(numeric_cols)):
            for j in range(i + 1, len(numeric_cols)):
                col1, col2 = numeric_cols[i], numeric_cols[j]
                
                # Remove NaNs
                mask = self.df[[col1, col2]].notna().all(axis=1)
                data1 = self.df.loc[mask, col1]
                data2 = self.df.loc[mask, col2]
                
                if len(data1) < 10:
                    continue
                
                # Pearson correlation
                r, p = stats.pearsonr(data1, data2)
                
                correlations.append(CorrelationPair(
                    feature1=col1,
                    feature2=col2,
                    correlation=round(float(r), 4),
                    p_value=round(float(p), 4),
                    method="pearson",
                    n=int(len(data1))
                ))
        
        return correlations
    
    def _categororical_associations(self) -> List[CorrelationPair]:
        """Calculate categorical-categorical associations using Cramér's V"""
        cat_cols = self.df.select_dtypes(include=['object', 'category']).columns
        
        if len(cat_cols) < 2:
            return []
        
        associations: List[CorrelationPair] = []
        
        for i in range(len(cat_cols)):
            for j in range(i + 1, len(cat_cols)):
                col1, col2 = cat_cols[i], cat_cols[j]
                
                # Create contingency table
                contingency = pd.crosstab(self.df[col1], self.df[col2])
                
                if contingency.shape[0] < 2 or contingency.shape[1] < 2:
                    continue
                
                # Chi-square test
                chi2, p, _, _ = chi2_contingency(contingency)
                
                # Cramér's V
                n = contingency.values.sum()
                denom = n * (min(contingency.shape) - 1)
                if denom <= 0:
                    continue
                cramers_v = np.sqrt(chi2 / denom)
                
                associations.append(CorrelationPair(
                    feature1=col1,
                    feature2=col2,
                    correlation=round(float(cramers_v), 4),
                    p_value=round(float(p), 4),
                    method="cramers_v",
                    n=int(n)
                ))
        
        return associations
    
    def _apply_fdr_correction(self, correlations: List[CorrelationPair]):
        """Apply Benjamini-Hochberg FDR correction"""
        from statsmodels.stats.multitest import multipletests
        
        p_values = [c.p_value for c in correlations]
        _, p_adjusted, _, _ = multipletests(p_values, method='fdr_bh')
        
        for i, corr in enumerate(correlations):
            corr.p_value = round(float(p_adjusted[i]), 4)






