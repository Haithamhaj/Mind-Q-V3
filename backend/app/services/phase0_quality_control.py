"""
Phase 1: Quality Control Service
Implements structural data quality checks with STOP/WARN decision rules.
"""

from typing import Dict, List, Optional, Any
import pandas as pd
from datetime import datetime
from pydantic import BaseModel


class QualityControlResult(BaseModel):
    """Result model for quality control checks"""
    status: str  # "PASS", "WARN", "STOP"
    missing_report: Dict[str, float]
    date_issues: Dict[str, Any]
    key_issues: Dict[str, Any]
    warnings: List[str]
    errors: List[str]
    fixes_applied: List[str] = []  # CSV and data fixes applied
    timestamp: str


class QualityControlService:
    """
    Quality Control Service implementing Phase 1 decision rules:
    1. critical_missing_pct(field) > 0.20 ⇒ WARN (Phase 5 will handle imputation)
    2. date_inversion_pct > 0.005 ⇒ WARN  
    3. orphans > 0.10 OR duplicates > 0.10 ⇒ STOP
    """
    
    def __init__(self, df: pd.DataFrame, key_columns: Optional[List[str]] = None):
        """
        Initialize quality control service
        
        Args:
            df: DataFrame to analyze
            key_columns: List of key column names for uniqueness checks
        """
        self.df = df
        self.key_columns = key_columns or []
        self.warnings = []
        self.errors = []
        self.fixes_applied = []  # Track automatic fixes applied
    
    def run(self) -> QualityControlResult:
        """
        Execute all quality control checks and apply automatic fixes
        
        Returns:
            QualityControlResult with status and detailed reports
        """
        # Reset warnings, errors, and fixes for each run
        self.warnings = []
        self.errors = []
        self.fixes_applied = []
        
        # Auto-fix common data quality issues
        self._auto_fix_data_issues()
        
        # 1. Missing data scan
        missing_report = self._missing_scan()
        
        # 2. Date order check
        date_issues = self._date_order_check()
        
        # 3. Key checks
        key_issues = self._key_checks()
        
        # Determine final status
        status = self._evaluate_status()
        
        return QualityControlResult(
            status=status,
            missing_report=missing_report,
            date_issues=date_issues,
            key_issues=key_issues,
            warnings=self.warnings,
            errors=self.errors,
            fixes_applied=self.fixes_applied,
            timestamp=datetime.utcnow().isoformat()
        )
    
    def _missing_scan(self) -> Dict[str, float]:
        """
        Calculate missing percentage per column and apply critical threshold
        
        Decision Rule: critical_missing_pct(field) > 0.20 ⇒ WARN (Phase 5 handles imputation)
        
        Returns:
            Dictionary mapping column names to missing percentages
        """
        missing_pct = (self.df.isnull().sum() / len(self.df)).to_dict()
        
        # Check critical threshold (20%)
        for col, pct in missing_pct.items():
            if pct > 0.20:
                self.warnings.append(
                    f"Column '{col}' has {pct:.1%} missing data - will apply advanced imputation in Phase 5"
                )
        
        return {k: round(v, 4) for k, v in missing_pct.items()}
    
    def _date_order_check(self) -> Dict[str, Any]:
        """
        Check for date inversions and future dates
        
        Decision Rule: date_inversion_pct > 0.005 ⇒ WARN
        
        Returns:
            Dictionary with date-related issues
        """
        date_cols = self.df.select_dtypes(include=['datetime64', 'datetime']).columns
        issues = {}
        
        if len(date_cols) == 0:
            issues["message"] = "No date/datetime columns found"
            return issues
        
        for col in date_cols:
            # Check for future dates
            future_count = (self.df[col] > pd.Timestamp.now()).sum()
            future_pct = future_count / len(self.df)
            
            # Check for date inversions (dates that are out of chronological order)
            sorted_dates = self.df[col].dropna().sort_values()
            if len(sorted_dates) > 1:
                # Calculate percentage of dates that are out of order
                original_order = self.df[col].dropna()
                inversions = 0
                for i in range(1, len(original_order)):
                    if original_order.iloc[i] < original_order.iloc[i-1]:
                        inversions += 1
                inversion_pct = inversions / len(original_order) if len(original_order) > 0 else 0
            else:
                inversion_pct = 0
            
            col_issues = {
                "future_dates": int(future_count),
                "future_pct": round(future_pct, 4),
                "inversions": int(inversions) if len(sorted_dates) > 1 else 0,
                "inversion_pct": round(inversion_pct, 4)
            }
            
            # Apply WARN threshold for date inversions
            if inversion_pct > 0.005:
                self.warnings.append(
                    f"Column '{col}' has {inversion_pct:.3%} date inversions (>0.5% threshold) - WARN condition triggered"
                )
            
            if future_pct > 0:
                col_issues["future_warning"] = f"Column '{col}' has {future_pct:.2%} future dates"
            
            issues[col] = col_issues
        
        return issues
    
    def _key_checks(self) -> Dict[str, Any]:
        """
        Check key uniqueness, orphans, and duplicates
        
        Decision Rule: orphans > 0.10 OR duplicates > 0.10 ⇒ STOP
        
        Returns:
            Dictionary with key-related issues
        """
        if not self.key_columns:
            return {"message": "No key columns specified"}
        
        issues = {}
        
        for key_col in self.key_columns:
            if key_col not in self.df.columns:
                self.errors.append(f"Key column '{key_col}' not found in dataset")
                continue
            
            # Duplicates check
            dup_count = self.df[key_col].duplicated().sum()
            dup_pct = dup_count / len(self.df)
            
            # Null keys (orphans) check
            null_count = self.df[key_col].isnull().sum()
            null_pct = null_count / len(self.df)
            
            col_issues = {
                "duplicates": int(dup_count),
                "duplicates_pct": round(dup_pct, 4),
                "nulls": int(null_count),
                "nulls_pct": round(null_pct, 4)
            }
            
            # Apply STOP thresholds (10%)
            if dup_pct > 0.10:
                self.errors.append(
                    f"Key '{key_col}' has {dup_pct:.1%} duplicates (>10% threshold) - STOP condition triggered"
                )
            
            if null_pct > 0.10:
                self.errors.append(
                    f"Key '{key_col}' has {null_pct:.1%} nulls/orphans (>10% threshold) - STOP condition triggered"
                )
            
            issues[key_col] = col_issues
        
        return issues
    
    def _auto_fix_data_issues(self):
        """Automatically fix common data quality issues"""
        original_shape = self.df.shape
        
        # Fix 1: Remove completely empty rows
        empty_rows_before = self.df.isnull().all(axis=1).sum()
        if empty_rows_before > 0:
            self.df = self.df.dropna(how='all')
            self.fixes_applied.append(f"Removed {empty_rows_before} completely empty rows")
        
        # Fix 2: Remove completely empty columns  
        empty_cols_before = self.df.isnull().all(axis=0).sum()
        if empty_cols_before > 0:
            self.df = self.df.dropna(how='all', axis=1)
            self.fixes_applied.append(f"Removed {empty_cols_before} completely empty columns")
        
        # Fix 3: Clean column names
        original_cols = list(self.df.columns)
        self.df.columns = [str(col).strip().replace('\n', ' ').replace('\r', '') for col in self.df.columns]
        if list(self.df.columns) != original_cols:
            self.fixes_applied.append("Cleaned column names (removed newlines and extra spaces)")
        
        # Fix 4: Remove duplicate rows
        dup_rows_before = self.df.duplicated().sum()
        if dup_rows_before > 0:
            self.df = self.df.drop_duplicates()
            self.fixes_applied.append(f"Removed {dup_rows_before} completely duplicate rows")
        
        if self.fixes_applied:
            new_shape = self.df.shape
            self.warnings.append(f"Auto-fixes applied: {original_shape} → {new_shape} ({len(self.fixes_applied)} fixes)")
    
    def _evaluate_status(self) -> str:
        """
        Determine final status based on errors/warnings
        
        Returns:
            "STOP" if errors found, "WARN" if only warnings, "PASS" otherwise
        """
        if self.errors:
            return "STOP"
        elif self.warnings:
            return "WARN"
        else:
            return "PASS"
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the quality control results
        
        Returns:
            Dictionary with summary information
        """
        return {
            "total_columns": len(self.df.columns),
            "total_rows": len(self.df),
            "key_columns_checked": len(self.key_columns),
            "errors_count": len(self.errors),
            "warnings_count": len(self.warnings),
            "status": self._evaluate_status()
        }

