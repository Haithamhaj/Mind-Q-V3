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
from .text_cleaning import TextCleaningService
from .basic_features import BasicTextFeaturesService
from .keyword_extractor import KeywordExtractionService
from .sentiment_simple import SentimentAnalysisService


class Phase12Result(BaseModel):
    status: str  # "skipped", "completed", "partial"
    detection: Optional[TextDetectionResult]
    basic_features: Optional[dict]
    sentiment: Optional[dict]
    keywords: Optional[dict]
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
                keywords=None,
                warnings=["No text columns detected"]
            )
        
        limit_heavy_analysis = len(self.df) > 500000 or detection_result.total_text_length > 50_000_000
        if limit_heavy_analysis:
            self.warnings.append(
                "Text volume exceeds heuristic limits - running lightweight features only."
            )
        
        cleaning_service = TextCleaningService(
            df=self.df,
            text_columns=detection_result.text_columns,
            language=detection_result.language_detected,
        )
        cleaned_text = cleaning_service.run()
        
        basic_service = BasicTextFeaturesService(
            df=self.df,
            text_columns=detection_result.text_columns,
            cleaned_text=cleaned_text,
            language=detection_result.language_detected,
        )
        basic_features = basic_service.run()
        
        keyword_results = None
        sentiment_results = None

        if not limit_heavy_analysis:
            keyword_service = KeywordExtractionService(
                cleaned_text=cleaned_text,
                language=detection_result.language_detected,
                top_k=10,
            )
            keyword_results = keyword_service.run()
            
            sentiment_service = SentimentAnalysisService(
                df=self.df,
                text_columns=detection_result.text_columns,
                language=detection_result.language_detected,
                cleaned_text=cleaned_text,
            )
            sentiment_results = sentiment_service.run()
            if sentiment_service.warnings:
                self.warnings.extend(sentiment_service.warnings)
        else:
            keyword_results = {}
            sentiment_results = {}
        
        # Save results
        self._save_results(
            artifacts_dir,
            detection_result,
            basic_features,
            sentiment_results,
            keyword_results,
        )
        
        return Phase12Result(
            status="completed" if not limit_heavy_analysis else "partial",
            detection=detection_result,
            basic_features={k: v.model_dump() for k, v in basic_features.items()},
            sentiment={k: v.model_dump() for k, v in sentiment_results.items()} if sentiment_results else {},
            keywords={k: v.model_dump() for k, v in keyword_results.items()} if keyword_results else {},
            warnings=self.warnings
        )
    
    def _save_results(self, artifacts_dir: Path, detection, basic, sentiment, keywords):
        """Save Phase 12 results to artifacts"""
        
        output = {
            "detection": detection.model_dump(),
            "basic_features": {k: v.model_dump() for k, v in basic.items()},
            "sentiment": {k: v.model_dump() for k, v in sentiment.items()} if sentiment else {},
            "keywords": {k: v.model_dump() for k, v in keywords.items()} if keywords else {},
        }
        
        with open(artifacts_dir / "text_features_mvp.json", "w", encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

