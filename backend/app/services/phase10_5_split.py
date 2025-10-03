from typing import Tuple
import pandas as pd
from sklearn.model_selection import train_test_split
from pydantic import BaseModel
import json


class SplitResult(BaseModel):
    train_rows: int
    validation_rows: int
    test_rows: int
    split_method: str
    split_ratios: list[float]
    target_distribution: dict


class SplitService:
    def __init__(
        self, 
        df: pd.DataFrame, 
        target_col: str = None,
        time_col: str = None,
        test_size: float = 0.15,
        val_size: float = 0.15,
        random_state: int = 42
    ):
        self.df = df
        self.target_col = target_col
        self.time_col = time_col
        self.test_size = test_size
        self.val_size = val_size
        self.random_state = random_state
    
    def run(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, SplitResult]:
        """Execute Phase 10.5: Train/Validation/Test Split"""
        
        # Determine split method
        if self.time_col and self.time_col in self.df.columns:
            method = "TimeSeriesSplit"
            df_train, df_val, df_test = self._time_series_split()
        else:
            method = "Stratified_KFold"
            df_train, df_val, df_test = self._stratified_split()
        
        # Calculate target distribution
        target_dist = self._calculate_target_distribution(df_train, df_val, df_test)
        
        result = SplitResult(
            train_rows=len(df_train),
            validation_rows=len(df_val),
            test_rows=len(df_test),
            split_method=method,
            split_ratios=[1 - self.test_size - self.val_size, self.val_size, self.test_size],
            target_distribution=target_dist
        )
        
        return df_train, df_val, df_test, result
    
    def _stratified_split(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Stratified split maintaining target distribution"""
        
        # First split: train+val vs test
        if self.target_col and self.target_col in self.df.columns:
            stratify = self.df[self.target_col]
        else:
            stratify = None
        
        train_val, test = train_test_split(
            self.df,
            test_size=self.test_size,
            stratify=stratify,
            random_state=self.random_state
        )
        
        # Second split: train vs val
        val_ratio = self.val_size / (1 - self.test_size)
        
        if self.target_col and self.target_col in train_val.columns:
            stratify_val = train_val[self.target_col]
        else:
            stratify_val = None
        
        train, val = train_test_split(
            train_val,
            test_size=val_ratio,
            stratify=stratify_val,
            random_state=self.random_state
        )
        
        return train, val, test
    
    def _time_series_split(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Time-based split (no shuffling)"""
        
        # Sort by time
        df_sorted = self.df.sort_values(self.time_col).reset_index(drop=True)
        
        n = len(df_sorted)
        test_idx = int(n * (1 - self.test_size))
        val_idx = int(n * (1 - self.test_size - self.val_size))
        
        train = df_sorted.iloc[:val_idx].copy()
        val = df_sorted.iloc[val_idx:test_idx].copy()
        test = df_sorted.iloc[test_idx:].copy()
        
        return train, val, test
    
    def _calculate_target_distribution(
        self, 
        train: pd.DataFrame, 
        val: pd.DataFrame, 
        test: pd.DataFrame
    ) -> dict:
        """Calculate target distribution across splits"""
        
        if not self.target_col or self.target_col not in train.columns:
            return {}
        
        return {
            "train": train[self.target_col].value_counts(normalize=True).to_dict(),
            "validation": val[self.target_col].value_counts(normalize=True).to_dict(),
            "test": test[self.target_col].value_counts(normalize=True).to_dict()
        }


