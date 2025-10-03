"""
Phase 12: Text Features Orchestrator

Orchestrates the execution of Phase 12 text analysis pipeline.
"""

from typing import Optional
import pandas as pd
from pathlib import Path
from pydantic import BaseModel
import json

from .detection import TextDetectionService, TextDetectionResult
from .basic_features import BasicTextFeaturesService
from .sentiment_simple import SentimentAnalysisService


class Phase12Result(BaseModel):
    status: str  # "skipped", "completed", "partial"
    detection: Optional[TextDetectionResult]
    basic_features: Optional[dict]
    sentiment: Optional[dict]
    warnings: list[str]


class Phase12Orchestrator:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.warnings = []
    
    def run(self, artifacts_dir: Path) -> Phase12Result:
        """Execute Phase 12: Text Features (MVP)"""
        
        # Step 1: Detect text columns
        detection_service = TextDetectionService(df=self.df)
        detection_result = detection_service.run()
        
        # Early exit if no text
        if not detection_result.text_columns:
            return Phase12Result(
                status="skipped",
                detection=detection_result,
                basic_features=None,
                sentiment=None,
                warnings=["No text columns detected"]
            )
        
        # Check dataset size
        if len(self.df) > 500000:
            self.warnings.append("Dataset too large (>500k rows) - Text analysis limited")
            return Phase12Result(
                status="skipped",
                detection=detection_result,
                basic_features=None,
                sentiment=None,
                warnings=self.warnings
            )
        
        # Step 2: Basic features
        basic_service = BasicTextFeaturesService(
            df=self.df,
            text_columns=detection_result.text_columns
        )
        basic_features = basic_service.run()
        
        # Step 3: Sentiment analysis
        sentiment_service = SentimentAnalysisService(
            df=self.df,
            text_columns=detection_result.text_columns,
            language=detection_result.language_detected
        )
        sentiment_results = sentiment_service.run()
        
        # Save results
        self._save_results(artifacts_dir, detection_result, basic_features, sentiment_results)
        
        return Phase12Result(
            status="completed",
            detection=detection_result,
            basic_features={k: v.dict() for k, v in basic_features.items()},
            sentiment={k: v.dict() for k, v in sentiment_results.items()},
            warnings=self.warnings
        )
    
    def _save_results(self, artifacts_dir: Path, detection, basic, sentiment):
        """Save Phase 12 results to artifacts"""
        
        output = {
            "detection": detection.dict(),
            "basic_features": {k: v.dict() for k, v in basic.items()},
            "sentiment": {k: v.dict() for k, v in sentiment.items()}
        }
        
        with open(artifacts_dir / "text_features_mvp.json", "w", encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
