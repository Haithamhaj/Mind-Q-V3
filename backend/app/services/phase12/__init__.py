"""
Phase 12: Text Features (MVP Implementation)

This module provides text analysis capabilities including:
- Text column detection
- Basic text features extraction
- Sentiment analysis (English + Arabic)

This is an MVP implementation with future enhancements planned.
"""

from .detection import TextDetectionService, TextDetectionResult
from .basic_features import BasicTextFeaturesService, BasicTextFeatures
from .sentiment_simple import SentimentAnalysisService, SentimentResult
from .orchestrator import Phase12Orchestrator, Phase12Result

__all__ = [
    "TextDetectionService",
    "TextDetectionResult", 
    "BasicTextFeaturesService",
    "BasicTextFeatures",
    "SentimentAnalysisService",
    "SentimentResult",
    "Phase12Orchestrator",
    "Phase12Result"
]
