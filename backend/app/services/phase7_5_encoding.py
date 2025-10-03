from typing import Dict, List, Tuple, Optional
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, RobustScaler
from category_encoders import TargetEncoder
from pydantic import BaseModel
import joblib


class EncodingConfig(BaseModel):
    column: str
    method: str  # "OHE", "Target_KFold", "Ordinal"
    cardinality: int
    reason: str


class ScalingConfig(BaseModel):
    columns: List[str]
    method: str  # "Standard", "Robust"
    reason: str


class EncodingScalingResult(BaseModel):
    encoding_configs: List[EncodingConfig]
    scaling_config: ScalingConfig
    artifacts_saved: List[str]


class EncodingScalingService:
    def __init__(
        self, 
        df_train: pd.DataFrame,
        df_val: Optional[pd.DataFrame] = None,
        df_test: Optional[pd.DataFrame] = None,
        target_col: Optional[str] = None,
        domain: str = "logistics"
    ):
        self.df_train = df_train.copy()
        self.df_val = df_val.copy() if df_val is not None else None
        self.df_test = df_test.copy() if df_test is not None else None
        self.target_col = target_col
        self.domain = domain
        self.encoding_configs: List[EncodingConfig] = []
        self.encoders: Dict = {}
        self.scalers: Dict = {}
    
    def run(self, artifacts_dir) -> Tuple[pd.DataFrame, Optional[pd.DataFrame], Optional[pd.DataFrame], EncodingScalingResult]:
        """Execute Phase 7.5: Encoding & Scaling (TRAIN ONLY)"""
        
        # 1. Categorical encoding (fit on TRAIN only)
        self._encode_categorical()
        
        # 2. Numeric scaling (fit on TRAIN only)
        scaling_config = self._scale_numeric()
        
        # 3. Transform validation and test sets
        if self.df_val is not None:
            self.df_val = self._transform_validation()
        if self.df_test is not None:
            self.df_test = self._transform_test()
        
        # 4. Save artifacts
        artifacts = self._save_artifacts(artifacts_dir)
        
        result = EncodingScalingResult(
            encoding_configs=self.encoding_configs,
            scaling_config=scaling_config,
            artifacts_saved=artifacts
        )
        
        return self.df_train, self.df_val, self.df_test, result
    
    def _encode_categorical(self):
        """Encode categorical features"""
        cat_cols = self.df_train.select_dtypes(include=['object', 'category']).columns
        
        # Remove target if it's categorical
        if self.target_col and self.target_col in cat_cols:
            cat_cols = cat_cols.drop(self.target_col)
        
        for col in cat_cols:
            cardinality = self.df_train[col].nunique()
            
            # Decision: OHE for low cardinality
            if cardinality <= 50:
                self._apply_ohe(col, cardinality)
            
            # Decision: Target encoding for high cardinality (n>50000)
            elif cardinality > 50 and len(self.df_train) > 50000 and self.target_col:
                self._apply_target_encoding(col, cardinality)
            
            # Fallback: Ordinal encoding
            else:
                self._apply_ordinal_encoding(col, cardinality)
    
    def _apply_ohe(self, col: str, cardinality: int):
        """Apply One-Hot Encoding"""
        # Get dummies on train
        train_dummies = pd.get_dummies(self.df_train[col], prefix=col)
        
        # Store column names
        self.encoders[col] = {'method': 'OHE', 'columns': train_dummies.columns.tolist()}
        
        # Replace in train
        self.df_train = pd.concat([
            self.df_train.drop(columns=[col]),
            train_dummies
        ], axis=1)
        
        self.encoding_configs.append(EncodingConfig(
            column=col,
            method="OHE",
            cardinality=cardinality,
            reason=f"Low cardinality ({cardinality}<=50)"
        ))
    
    def _apply_target_encoding(self, col: str, cardinality: int):
        """Apply Target Encoding with K-Fold (fit on TRAIN only)"""
        if not self.target_col or self.target_col not in self.df_train.columns:
            # Fallback to ordinal
            self._apply_ordinal_encoding(col, cardinality)
            return
        
        # Initialize Target Encoder (approximate K-Fold via smoothing)
        encoder = TargetEncoder(cols=[col], smoothing=1.0, min_samples_leaf=20)
        
        # Fit on train only
        self.df_train[col] = encoder.fit_transform(
            self.df_train[[col]], 
            self.df_train[self.target_col]
        )[col]
        
        # Store encoder
        self.encoders[col] = {'method': 'Target_KFold', 'encoder': encoder}
        
        self.encoding_configs.append(EncodingConfig(
            column=col,
            method="Target_KFold",
            cardinality=cardinality,
            reason=f"High cardinality ({cardinality}>50) and n={len(self.df_train):,}"
        ))
    
    def _apply_ordinal_encoding(self, col: str, cardinality: int):
        """Apply Ordinal Encoding"""
        # Create mapping from train
        categories = self.df_train[col].unique()
        mapping = {cat: idx for idx, cat in enumerate(categories)}
        
        self.df_train[col] = self.df_train[col].map(mapping)
        
        # Store mapping
        self.encoders[col] = {'method': 'Ordinal', 'mapping': mapping}
        
        self.encoding_configs.append(EncodingConfig(
            column=col,
            method="Ordinal",
            cardinality=cardinality,
            reason="Fallback encoding"
        ))
    
    def _scale_numeric(self) -> ScalingConfig:
        """Scale numeric features (fit on TRAIN only)"""
        numeric_cols = self.df_train.select_dtypes(include=[np.number]).columns
        
        # Remove target if numeric
        if self.target_col and self.target_col in numeric_cols:
            numeric_cols = numeric_cols.drop(self.target_col)
        
        if len(numeric_cols) == 0:
            return ScalingConfig(columns=[], method="None", reason="No numeric features")
        
        # Choose scaler based on domain
        if self.domain == "finance":
            scaler = RobustScaler()
            method = "Robust"
            reason = "Finance domain - heavy-tailed distributions"
        else:
            scaler = StandardScaler()
            method="Standard"
            reason="Default scaler for domain"
        
        # Fit on train
        self.df_train[numeric_cols] = scaler.fit_transform(self.df_train[numeric_cols])
        
        # Store scaler
        self.scalers['numeric'] = {'scaler': scaler, 'columns': numeric_cols.tolist()}
        
        return ScalingConfig(
            columns=numeric_cols.tolist(),
            method=method,
            reason=reason
        )
    
    def _transform_validation(self) -> pd.DataFrame:
        """Transform validation set using fitted encoders/scalers"""
        df = self.df_val.copy()
        
        # Apply categorical encodings
        for col, config in self.encoders.items():
            if col not in df.columns:
                continue
            
            if config['method'] == 'OHE':
                dummies = pd.get_dummies(df[col], prefix=col)
                # Align columns with train
                for train_col in config['columns']:
                    if train_col not in dummies.columns:
                        dummies[train_col] = 0
                df = pd.concat([df.drop(columns=[col]), dummies[config['columns']]], axis=1)
            
            elif config['method'] == 'Target_KFold':
                df[col] = config['encoder'].transform(df[[col]])[col]
            
            elif config['method'] == 'Ordinal':
                df[col] = df[col].map(config['mapping'])
        
        # Apply scaling
        if 'numeric' in self.scalers:
            scaler_config = self.scalers['numeric']
            cols = [c for c in scaler_config['columns'] if c in df.columns]
            df[cols] = scaler_config['scaler'].transform(df[cols])
        
        return df
    
    def _transform_test(self) -> pd.DataFrame:
        """Transform test set (same as validation)"""
        df = self.df_test.copy()
        
        for col, config in self.encoders.items():
            if col not in df.columns:
                continue
            
            if config['method'] == 'OHE':
                dummies = pd.get_dummies(df[col], prefix=col)
                for train_col in config['columns']:
                    if train_col not in dummies.columns:
                        dummies[train_col] = 0
                df = pd.concat([df.drop(columns=[col]), dummies[config['columns']]], axis=1)
            elif config['method'] == 'Target_KFold':
                df[col] = config['encoder'].transform(df[[col]])[col]
            elif config['method'] == 'Ordinal':
                df[col] = df[col].map(config['mapping'])
        
        if 'numeric' in self.scalers:
            scaler_config = self.scalers['numeric']
            cols = [c for c in scaler_config['columns'] if c in df.columns]
            df[cols] = scaler_config['scaler'].transform(df[cols])
        
        return df
    
    def _save_artifacts(self, artifacts_dir) -> List[str]:
        """Save encoders and scalers"""
        saved: List[str] = []
        # Ensure directory exists
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        
        # Save encoders
        for col, config in self.encoders.items():
            if config['method'] == 'Target_KFold':
                path = artifacts_dir / f"encoder_{col}.joblib"
                joblib.dump(config['encoder'], path)
                saved.append(str(path))
        
        # Save scalers
        if 'numeric' in self.scalers:
            path = artifacts_dir / "scaler_numeric.joblib"
            joblib.dump(self.scalers['numeric']['scaler'], path)
            saved.append(str(path))
        
        return saved


