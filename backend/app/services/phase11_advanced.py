from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score
from pydantic import BaseModel
import json


class ClusteringResult(BaseModel):
    method: str
    n_clusters: int
    silhouette_score: float
    cluster_sizes: Dict[int, int]


class AdvancedExplorationResult(BaseModel):
    clustering: Optional[ClusteringResult]
    pca_variance_explained: Optional[List[float]]
    n_anomalies: int
    anomaly_percentage: float


class AdvancedExplorationService:
    def __init__(self, df: pd.DataFrame, random_state: int = 42):
        self.df = df  # Always use full dataset for ML accuracy
        self.random_state = random_state
        
        print(f"🔬 Phase 11: Advanced exploration on full dataset ({len(df):,} rows) for ML accuracy")
    
    def run(self, artifacts_dir) -> AdvancedExplorationResult:
        """Execute Phase 11: Advanced Exploration on full dataset"""
        
        # Get numeric features only
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        
        if len(numeric_cols) < 2:
            return AdvancedExplorationResult(
                clustering=None,
                pca_variance_explained=None,
                n_anomalies=0,
                anomaly_percentage=0.0
            )
        
        X = self.df[numeric_cols].fillna(0)
        
        # Scale features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # 1. Clustering
        clustering_result = self._perform_clustering(X_scaled)
        
        # 2. PCA (if features > 20)
        pca_variance = None
        if len(numeric_cols) > 20:
            pca_variance = self._perform_pca(X_scaled, artifacts_dir)
        
        # 3. Anomaly Detection
        n_anomalies, anomaly_pct = self._detect_anomalies(X_scaled, artifacts_dir)
        
        result = AdvancedExplorationResult(
            clustering=clustering_result,
            pca_variance_explained=pca_variance,
            n_anomalies=n_anomalies,
            anomaly_percentage=anomaly_pct
        )
        
        return result
    
    def _perform_clustering(self, X: np.ndarray) -> Optional[ClusteringResult]:
        """Perform K-Means clustering with silhouette evaluation"""
        
        best_k = 2
        best_score = -1
        best_labels = None
        
        # Grid search for best k
        for k in range(2, min(11, len(X) // 10)):
            kmeans = KMeans(n_clusters=k, random_state=self.random_state, n_init=10)
            labels = kmeans.fit_predict(X)
            score = silhouette_score(X, labels)
            
            if score > best_score:
                best_score = score
                best_k = k
                best_labels = labels
        
        # Calculate cluster sizes
        unique, counts = np.unique(best_labels, return_counts=True)
        cluster_sizes = dict(zip([int(u) for u in unique], [int(c) for c in counts]))
        
        return ClusteringResult(
            method="KMeans",
            n_clusters=best_k,
            silhouette_score=round(best_score, 4),
            cluster_sizes=cluster_sizes
        )
    
    def _perform_pca(self, X: np.ndarray, artifacts_dir) -> List[float]:
        """Perform PCA and save components"""
        
        pca = PCA(n_components=0.90, random_state=self.random_state)  # 90% variance
        pca.fit(X)
        
        variance_explained = pca.explained_variance_ratio_.tolist()
        
        # Save PCA variance
        pca_data = {
            "n_components": pca.n_components_,
            "variance_explained": [round(v, 4) for v in variance_explained],
            "cumulative_variance": [round(v, 4) for v in np.cumsum(variance_explained)]
        }
        
        with open(artifacts_dir / "pca_variance.json", "w") as f:
            json.dump(pca_data, f, indent=2)
        
        return [round(v, 4) for v in variance_explained]
    
    def _detect_anomalies(self, X: np.ndarray, artifacts_dir) -> tuple[int, float]:
        """Detect anomalies using Isolation Forest"""
        
        iso_forest = IsolationForest(
            contamination=0.05,
            random_state=self.random_state,
            n_estimators=100
        )
        
        anomaly_labels = iso_forest.fit_predict(X)
        anomalies = (anomaly_labels == -1).sum()
        anomaly_pct = anomalies / len(X)
        
        # Save anomaly flags
        self.df['anomaly_flag'] = anomaly_labels == -1
        
        return int(anomalies), round(anomaly_pct, 4)


