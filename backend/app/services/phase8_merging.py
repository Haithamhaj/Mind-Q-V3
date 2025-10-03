from typing import Dict, List, Tuple, Optional
import pandas as pd
from pydantic import BaseModel
from pathlib import Path


class MergingIssue(BaseModel):
    table: str
    issue_type: str  # "orphans", "duplicates"
    count: int
    percentage: float
    action: str  # "quarantined", "kept_latest", "stopped", "flagged"


class MergingResult(BaseModel):
    tables_merged: List[str]
    total_rows_before: int
    total_rows_after: int
    issues: List[MergingIssue]
    status: str  # "PASS", "WARN", "STOP"


class MergingService:
    def __init__(self, main_df: pd.DataFrame, join_tables: Optional[Dict[str, pd.DataFrame]] = None):
        self.main_df = main_df.copy()
        self.join_tables = join_tables or {}
        self.issues: List[MergingIssue] = []
        self.orphans: Dict[str, pd.DataFrame] = {}
        self.duplicates: Dict[str, pd.DataFrame] = {}
    
    def run(self, artifacts_dir: Path) -> Tuple[pd.DataFrame, MergingResult]:
        """Execute Phase 8: Merging & Keys"""
        
        rows_before = len(self.main_df)
        
        # Check for duplicates in main table
        self._check_duplicates("main", self.main_df)
        
        # Merge additional tables if provided
        merged_tables = ["main"]
        for table_name, df_join in self.join_tables.items():
            self.main_df = self._merge_table(table_name, df_join)
            merged_tables.append(table_name)
        
        rows_after = len(self.main_df)
        
        # Evaluate status
        status = self._evaluate_status()
        
        # Save orphans if any
        if self.orphans:
            self._save_orphans(artifacts_dir)
        
        result = MergingResult(
            tables_merged=merged_tables,
            total_rows_before=rows_before,
            total_rows_after=rows_after,
            issues=self.issues,
            status=status
        )
        
        return self.main_df, result
    
    def _check_duplicates(self, table_name: str, df: pd.DataFrame):
        """Check for duplicate keys"""
        # Try to identify key column(s)
        key_candidates = [col for col in df.columns if 'id' in col.lower()]
        
        if not key_candidates or len(df) == 0:
            return
        
        key_col = key_candidates[0]
        dup_count = df[key_col].duplicated().sum()
        dup_pct = float(dup_count) / float(len(df)) if len(df) > 0 else 0.0
        
        if dup_pct > 0:
            action = "stopped" if dup_pct > 0.10 else "kept_latest" if dup_pct > 0.03 else "flagged"
            
            self.issues.append(MergingIssue(
                table=table_name,
                issue_type="duplicates",
                count=int(dup_count),
                percentage=round(dup_pct, 4),
                action=action
            ))
            
            # Handle duplicates
            if dup_pct <= 0.10:
                # Keep latest (assuming there's a timestamp)
                timestamp_cols = [c for c in df.columns if 'date' in c.lower() or 'time' in c.lower() or 'ts' in c.lower()]
                if timestamp_cols:
                    df.sort_values(timestamp_cols[0], ascending=False, inplace=True)
                    df.drop_duplicates(subset=[key_col], keep='first', inplace=True)
    
    def _merge_table(self, table_name: str, df_join: pd.DataFrame) -> pd.DataFrame:
        """Merge additional table"""
        if len(self.main_df) == 0 or len(df_join) == 0:
            return self.main_df
        
        # Find common key
        common_cols = list(set(self.main_df.columns) & set(df_join.columns))
        key_cols = [col for col in common_cols if 'id' in col.lower()]
        
        if not key_cols:
            # Skip merge if no common key
            return self.main_df
        
        key_col = key_cols[0]
        
        # Check for duplicates in join table
        self._check_duplicates(table_name, df_join)
        
        # Perform left join
        df_merged = self.main_df.merge(df_join, on=key_col, how='left', suffixes=('', f'_{table_name}'))
        
        # Check for orphans (nulls after join)
        join_cols = [c for c in df_join.columns if c != key_col]
        if join_cols:
            null_count = df_merged[join_cols[0]].isnull().sum()
            null_pct = float(null_count) / float(len(df_merged)) if len(df_merged) > 0 else 0.0
            
            if null_pct > 0:
                action = "stopped" if null_pct > 0.10 else "quarantined" if null_pct > 0.02 else "flagged"
                
                self.issues.append(MergingIssue(
                    table=table_name,
                    issue_type="orphans",
                    count=int(null_count),
                    percentage=round(null_pct, 4),
                    action=action
                ))
                
                # Quarantine orphans if threshold exceeded
                if 0.02 < null_pct <= 0.10:
                    orphan_rows = df_merged[df_merged[join_cols[0]].isnull()]
                    self.orphans[table_name] = orphan_rows
                    df_merged = df_merged[df_merged[join_cols[0]].notna()]
        
        return df_merged
    
    def _evaluate_status(self) -> str:
        """Evaluate final status"""
        for issue in self.issues:
            if issue.action == "stopped":
                return "STOP"
        
        if any(issue.action in ["quarantined", "kept_latest"] for issue in self.issues):
            return "WARN"
        
        return "PASS"
    
    def _save_orphans(self, artifacts_dir: Path):
        """Save orphaned records"""
        for table_name, df_orphans in self.orphans.items():
            path = artifacts_dir / f"orphans_{table_name}.parquet"
            df_orphans.to_parquet(path, compression='zstd')



