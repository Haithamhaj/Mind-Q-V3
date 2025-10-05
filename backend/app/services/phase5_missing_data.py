from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
from scipy import stats
from sklearn.impute import KNNImputer
from sklearn.experimental import enable_iterative_imputer  # noqa: F401
from sklearn.impute import IterativeImputer
from pydantic import BaseModel


class ImputationDecision(BaseModel):
    column: str
    method: str
    reason: str
    missing_before: int
    missing_after: int


class ValidationMetrics(BaseModel):
    psi: float
    ks_statistic: float
    passed: bool


class ImputationResult(BaseModel):
    decisions: List[ImputationDecision]
    validation: Dict[str, ValidationMetrics]
    record_completeness: float
    status: str  # "PASS", "WARN", "STOP"
    warnings: List[str]


class MissingDataService:
    def __init__(self, df: pd.DataFrame, group_col: Optional[str] = None):
        self.df = df.copy()
        self.df_original = df.copy()  # For PSI/KS validation
        self.group_col = group_col
        self.decisions: List[ImputationDecision] = []
        self.warnings: List[str] = []
        self.n = len(df)
    
    def run(self) -> Tuple[pd.DataFrame, ImputationResult]:
        """Execute Phase 5: Missing Data Handling"""
        
        # Process each column
        for col in self.df.columns:
            missing_count = self.df[col].isnull().sum()
            if missing_count == 0:
                continue
            
            missing_pct = missing_count / self.n
            
            # Determine imputation method
            method, reason = self._decide_method(col, missing_pct)
            
            # Apply imputation
            self.df = self._apply_imputation(col, method)
            
            # Record decision
            self.decisions.append(ImputationDecision(
                column=col,
                method=method,
                reason=reason,
                missing_before=int(missing_count),
                missing_after=int(self.df[col].isnull().sum())
            ))
        
        # Validate imputation quality
        validation = self._validate_imputation()
        
        # Calculate record completeness
        completeness = 1 - (self.df.isnull().sum().sum() / (self.n * len(self.df.columns)))
        
        # Determine status
        status = self._evaluate_status(validation, completeness)
        
        result = ImputationResult(
            decisions=self.decisions,
            validation=validation,
            record_completeness=round(completeness, 4),
            status=status,
            warnings=self.warnings
        )
        
        return self.df, result
    
    def _decide_method(self, col: str, missing_pct: float) -> Tuple[str, str]:
        """Decide imputation method based on Spec rules"""
        dtype = self.df[col].dtype
        
        # Dates: never impute (Spec rule)
        if pd.api.types.is_datetime64_any_dtype(dtype):
            return "flag_only", "Dates are critical - flag only, no imputation"
        
        # Numeric columns
        if pd.api.types.is_numeric_dtype(dtype):
            # Rule 1: Low missing & MCAR → median
            if missing_pct <= 0.05:
                return "median", f"Low missing ({missing_pct:.1%}) - simple median imputation"
            
            # Rule 2: Group structure present → prefer group median up to 50% missing
            if self.group_col and self.group_col in self.df.columns and missing_pct <= 0.50:
                return "group_median", f"Group structure present (missing {missing_pct:.1%})"
            
            # Rule 3: High correlation & large dataset → MICE
            if self.n >= 50000:
                corr_count = self._count_correlated_features(col, threshold=0.4)
                if corr_count >= 3:
                    return "mice", f"High correlation (n={corr_count} peers) and large dataset (n={self.n:,})"
            
            # Rule 4: Small data guards
            if self.n < 2000:
                # Prefer group median if available even in very small data
                if self.group_col and self.group_col in self.df.columns:
                    return "group_median", f"Small dataset (n={self.n}) - prefer group median"
                return "median", f"Small dataset (n={self.n}) - avoid KNN"
            if self.n < 10000:
                return ("group_median" if self.group_col else "median"), f"Dataset n={self.n} - avoid MICE"
            
            # Default: KNN (dense locality)
            return "knn", f"Dense locality imputation for {missing_pct:.1%} missing"
        
        # Categorical columns
        else:
            if missing_pct <= 0.05:
                return "mode", f"Low missing ({missing_pct:.1%}) - simple mode"
            
            if self.group_col and self.group_col in self.df.columns:
                return "group_mode", f"Moderate missing ({missing_pct:.1%}) with group structure"
            
            return "unknown", f"High missing ({missing_pct:.1%}) - mark as 'Unknown'"
    
    def _count_correlated_features(self, col: str, threshold: float = 0.4) -> int:
        """Count features with correlation >= threshold"""
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        if col not in numeric_cols or len(numeric_cols) < 2:
            return 0
        
        corr = self.df[numeric_cols].corr()[col].abs()
        return int((corr >= threshold).sum() - 1)  # Exclude self-correlation
    
    def _apply_imputation(self, col: str, method: str) -> pd.DataFrame:
        """Apply selected imputation method"""
        df = self.df.copy()
        
        if method == "flag_only":
            df[f"{col}_missing"] = df[col].isnull().astype(int)
            return df
        
        elif method == "median":
            median_val = df[col].median()
            df[col] = df[col].fillna(median_val)
        
        elif method == "group_median":
            df[col] = df.groupby(self.group_col)[col].transform(
                lambda x: x.fillna(x.median())
            )
            # Fill any remaining NaNs with global median
            df[col].fillna(df[col].median(), inplace=True)
        
        elif method == "mode":
            mode_val = df[col].mode()[0] if len(df[col].mode()) > 0 else "Unknown"
            # Handle categorical columns properly
            if df[col].dtype.name == 'category':
                # Add 'Unknown' to categories if not present
                if 'Unknown' not in df[col].cat.categories:
                    df[col] = df[col].cat.add_categories(['Unknown'])
            df[col] = df[col].fillna(mode_val)
        
        elif method == "group_mode":
            # Handle categorical columns properly
            if df[col].dtype.name == 'category':
                if 'Unknown' not in df[col].cat.categories:
                    df[col] = df[col].cat.add_categories(['Unknown'])
            
            df[col] = df.groupby(self.group_col)[col].transform(
                lambda x: x.fillna(x.mode()[0] if len(x.mode()) > 0 else "Unknown")
            )
            global_mode = df[col].mode()[0] if len(df[col].mode()) > 0 else "Unknown"
            df[col].fillna(global_mode, inplace=True)
        
        elif method == "unknown":
            # Handle categorical columns properly
            if df[col].dtype.name == 'category':
                if 'Unknown' not in df[col].cat.categories:
                    df[col] = df[col].cat.add_categories(['Unknown'])
            df[col] = df[col].fillna("Unknown")
        
        elif method == "knn":
            df = self._apply_knn(df, col)
        
        elif method == "mice":
            df = self._apply_mice(df, col)
        
        return df
    
    def _apply_knn(self, df: pd.DataFrame, col: str) -> pd.DataFrame:
        """Apply KNN imputation"""
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        if col not in numeric_cols:
            return df
        
        # Use only numeric columns for KNN
        imputer = KNNImputer(n_neighbors=5)
        df[numeric_cols] = imputer.fit_transform(df[numeric_cols])
        
        return df
    
    def _apply_mice(self, df: pd.DataFrame, col: str) -> pd.DataFrame:
        """Apply MICE imputation"""
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        if col not in numeric_cols:
            return df
        
        imputer = IterativeImputer(max_iter=10, random_state=42)
        df[numeric_cols] = imputer.fit_transform(df[numeric_cols])
        
        return df
    
    def _validate_imputation(self) -> Dict[str, ValidationMetrics]:
        """Validate imputation quality with PSI and KS tests"""
        validation: Dict[str, ValidationMetrics] = {}
        
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            # Skip if no imputation happened
            if col not in [d.column for d in self.decisions]:
                continue
            
            original = self.df_original[col].dropna()
            imputed = self.df[col].dropna()
            
            if len(original) < 10 or len(imputed) < 10:
                continue
            
            # Calculate PSI
            psi = self._calculate_psi(original, imputed)
            
            # Calculate KS statistic
            ks_stat, _ = stats.ks_2samp(original, imputed)
            
            passed = (psi <= 0.10 and ks_stat <= 0.10)
            
            if not passed:
                self.warnings.append(
                    f"Column '{col}': PSI={psi:.3f}, KS={ks_stat:.3f} exceed thresholds"
                )
            
            validation[col] = ValidationMetrics(
                psi=round(psi, 4),
                ks_statistic=round(ks_stat, 4),
                passed=passed
            )
        
        return validation
    
    def _calculate_psi(self, original: pd.Series, imputed: pd.Series, bins: int = 10) -> float:
        """Calculate Population Stability Index"""
        # Create bins based on original distribution
        bin_edges = np.percentile(original, np.linspace(0, 100, bins+1))
        bin_edges = np.unique(bin_edges)  # Remove duplicates
        
        if len(bin_edges) < 2:
            return 0.0
        
        # Calculate distributions
        orig_counts, _ = np.histogram(original, bins=bin_edges)
        new_counts, _ = np.histogram(imputed, bins=bin_edges)
        
        orig_pct = orig_counts / len(original)
        new_pct = new_counts / len(imputed)
        
        # Avoid division by zero
        orig_pct = np.where(orig_pct == 0, 0.0001, orig_pct)
        new_pct = np.where(new_pct == 0, 0.0001, new_pct)
        
        psi = np.sum((new_pct - orig_pct) * np.log(new_pct / orig_pct))
        
        return float(psi)
    
    def _evaluate_status(self, validation: Dict, completeness: float) -> str:
        """Evaluate final status"""
        if completeness < 0.80:
            return "STOP"
        
        failed_validations = sum(1 for v in validation.values() if not v.passed)
        
        if failed_validations > len(validation) * 0.3:  # >30% failed
            return "STOP"
        elif failed_validations > 0:
            return "WARN"
        else:
            return "PASS"


