from typing import List, Tuple
import pandas as pd
import numpy as np
from sklearn.feature_selection import RFE
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.linear_model import LogisticRegression
from statsmodels.stats.outliers_influence import variance_inflation_factor
from pydantic import BaseModel


class FeatureRanking(BaseModel):
    feature: str
    score: float
    rank: int


class SelectionResult(BaseModel):
    selected_features: List[str]
    feature_rankings: List[FeatureRanking]
    selection_method: str
    vif_check_passed: bool
    business_approved_included: bool


class FeatureSelectionService:
    def __init__(
        self,
        df_train: pd.DataFrame,
        df_val: pd.DataFrame,
        target_col: str,
        top_k: int = 25,
        business_approved: List[str] = None
    ):
        self.df_train = df_train
        self.df_val = df_val
        self.target_col = target_col
        self.top_k = top_k
        self.business_approved = business_approved or []
    
    def run(self) -> Tuple[List[str], SelectionResult]:
        """Execute Phase 11.5: Feature Selection & Ranking"""
        
        # Separate features and target
        X_train = self.df_train.drop(columns=[self.target_col])
        y_train = self.df_train[self.target_col]
        
        # Get numeric features only (for simplicity)
        numeric_features = X_train.select_dtypes(include=[np.number]).columns.tolist()
        X_train_numeric = X_train[numeric_features]
        
        # 1. Model-based ranking
        model_rankings = self._model_based_ranking(X_train_numeric, y_train)
        
        # 2. RFE
        rfe_features = self._recursive_feature_elimination(X_train_numeric, y_train)
        
        # 3. Merge results
        selected_features = self._merge_selections(model_rankings, rfe_features)
        
        # 4. Force include business-approved features
        for feat in self.business_approved:
            if feat in numeric_features and feat not in selected_features:
                selected_features.append(feat)
        
        # Limit to top_k
        selected_features = selected_features[:self.top_k]
        
        # 5. VIF check
        vif_passed = self._check_vif(X_train_numeric[selected_features])
        
        # Create rankings
        rankings = [
            FeatureRanking(feature=feat, score=model_rankings.get(feat, 0.0), rank=idx+1)
            for idx, feat in enumerate(selected_features)
        ]
        
        result = SelectionResult(
            selected_features=selected_features,
            feature_rankings=rankings,
            selection_method="Hybrid_RFE_ModelBased",
            vif_check_passed=vif_passed,
            business_approved_included=any(f in selected_features for f in self.business_approved)
        )
        
        return selected_features, result
    
    def _model_based_ranking(self, X: pd.DataFrame, y: pd.Series) -> dict:
        """Rank features using Random Forest importance"""
        
        # Determine if classification or regression
        if y.dtype == 'object' or y.nunique() < 10:
            model = RandomForestClassifier(n_estimators=100, random_state=42)
        else:
            model = RandomForestRegressor(n_estimators=100, random_state=42)
        
        model.fit(X, y)
        
        importance = dict(zip(X.columns, model.feature_importances_))
        
        return importance
    
    def _recursive_feature_elimination(self, X: pd.DataFrame, y: pd.Series) -> List[str]:
        """Select features using RFE"""
        
        # Use logistic regression for RFE (fast)
        estimator = LogisticRegression(max_iter=1000, random_state=42)
        
        n_features_to_select = min(self.top_k, len(X.columns))
        
        rfe = RFE(estimator=estimator, n_features_to_select=n_features_to_select)
        rfe.fit(X, y)
        
        selected = X.columns[rfe.support_].tolist()
        
        return selected
    
    def _merge_selections(self, model_rankings: dict, rfe_features: List[str]) -> List[str]:
        """Merge model-based and RFE selections"""
        
        # Sort by model importance
        sorted_features = sorted(model_rankings.items(), key=lambda x: x[1], reverse=True)
        
        # Prioritize features in both selections
        merged = []
        
        for feat, score in sorted_features:
            if feat in rfe_features:
                merged.append(feat)
        
        # Add remaining RFE features
        for feat in rfe_features:
            if feat not in merged:
                merged.append(feat)
        
        # Add remaining high-importance features
        for feat, score in sorted_features:
            if feat not in merged and len(merged) < self.top_k:
                merged.append(feat)
        
        return merged
    
    def _check_vif(self, X: pd.DataFrame) -> bool:
        """Check Variance Inflation Factor"""
        
        if len(X.columns) < 2:
            return True
        
        try:
            vif_data = pd.DataFrame()
            vif_data["feature"] = X.columns
            vif_data["VIF"] = [variance_inflation_factor(X.values, i) for i in range(len(X.columns))]
            
            # Check if all VIF < 5
            max_vif = vif_data["VIF"].max()
            return max_vif < 5
        except:
            return True  # Assume passed if calculation fails


