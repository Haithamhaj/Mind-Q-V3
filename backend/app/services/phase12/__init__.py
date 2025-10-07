"""
Phase 12: Text Features (MVP Implementation)

This module provides text analysis capabilities including:
- Text column detection
- Basic text features extraction
- Sentiment analysis (English + Arabic)

This is an MVP implementation with future enhancements planned.
"""

from .detection import TextDetectionService, TextDetectionResult
from .text_cleaning import TextCleaningService
from .basic_features import BasicTextFeaturesService, BasicTextFeatures
from .keyword_extractor import KeywordExtractionService, KeywordSummary
from .sentiment_simple import SentimentAnalysisService, SentimentResult
from .orchestrator import Phase12Orchestrator, Phase12Result

__all__ = [
    "TextDetectionService",
    "TextDetectionResult",
    "TextCleaningService",
    "BasicTextFeaturesService",
    "BasicTextFeatures",
    "KeywordExtractionService",
    "KeywordSummary",
    "SentimentAnalysisService",
    "SentimentResult",
    "Phase12Orchestrator",
    "Phase12Result"
]
