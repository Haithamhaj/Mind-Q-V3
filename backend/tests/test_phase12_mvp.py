"""
Unit tests for Phase 12: Text Features (enhanced MVP)
"""

import pandas as pd
import numpy as np
import pytest
from pathlib import Path

from app.services.phase12.detection import TextDetectionService
from app.services.phase12.text_cleaning import TextCleaningService
from app.services.phase12.basic_features import BasicTextFeaturesService
from app.services.phase12.keyword_extractor import KeywordExtractionService
from app.services.phase12.sentiment_simple import SentimentAnalysisService
from app.services.phase12.orchestrator import Phase12Orchestrator


def test_text_detection_identifies_descriptive_fields():
    df = pd.DataFrame({
        "order_id": range(12),
        "customer_note": [
            "Customer praised fast delivery and packaging details",
            "Customer reported excellent delivery speed and friendly support",
            "Average experience but appreciated proactive notifications",
            "Delivery delay resolved quickly with an apology",
        ] * 3,
        "status": ["delivered"] * 12,
    })

    result = TextDetectionService(df=df).run()

    assert "customer_note" in result.text_columns
    assert result.language_detected in {"en", "mixed", "unknown"}


def test_text_detection_skips_numeric_fields():
    codes = np.random.choice(["A12", "B33", "Z99", "X10"], size=50)
    df = pd.DataFrame({
        "id": range(50),
        "amount": np.random.random(50),
        "code": codes
    })

    result = TextDetectionService(df=df).run()

    assert result.text_columns == []
    assert result.language_detected == "none"


def test_basic_features_statistics():
    df = pd.DataFrame({
        "feedback": [
            "Shipping was quick and the product worked as expected.",
            "Packaging was damaged but support resolved the issue promptly.",
            "Average experience overall." ,
        ]
    })

    cleaner = TextCleaningService(df=df, text_columns=["feedback"], language="en")
    cleaned = cleaner.run()
    features = BasicTextFeaturesService(
        df=df,
        text_columns=["feedback"],
        cleaned_text=cleaned,
        language="en",
    ).run()

    stats = features["feedback"]
    assert stats.avg_length_chars > 0
    assert stats.unique_token_ratio > 0
    assert 0 <= stats.stopword_ratio <= 1


def test_basic_features_arabic_text():
    df = pd.DataFrame({
        "arabic": [
            "خدمة التوصيل كانت سريعة وممتازة",
            "تأخر بسيط في التوصيل لكن تم حل المشكلة",
            "تجربة متوسطة" ,
        ]
    })

    cleaner = TextCleaningService(df=df, text_columns=["arabic"], language="ar")
    cleaned = cleaner.run()
    features = BasicTextFeaturesService(
        df=df,
        text_columns=["arabic"],
        cleaned_text=cleaned,
        language="ar",
    ).run()

    stats = features["arabic"]
    assert stats.arabic_ratio > 0.3
    assert stats.avg_length_words >= 2


def test_keyword_extraction_returns_tokens():
    df = pd.DataFrame({
        "note": [
            "Customer loves the weekly delivery service",
            "Delivery issues caused customer frustration",
            "Customer satisfied with delivery speed",
        ]
    })
    cleaner = TextCleaningService(df=df, text_columns=["note"], language="en")
    cleaned = cleaner.run()
    keywords = KeywordExtractionService(cleaned_text=cleaned, language="en").run()

    summary = keywords["note"]
    assert summary.total_tokens > 0
    assert len(summary.top_tokens) > 0


def test_sentiment_analysis_runs_without_errors():
    df = pd.DataFrame({
        "review": [
            "Amazing experience and super friendly support!",
            "Terrible quality, would not recommend.",
            "Neutral feelings about this order.",
        ]
    })
    cleaner = TextCleaningService(df=df, text_columns=["review"], language="en")
    cleaned = cleaner.run()
    sentiment_results = SentimentAnalysisService(
        df=df,
        text_columns=["review"],
        language="en",
        cleaned_text=cleaned,
    ).run()

    result = sentiment_results["review"]
    assert result.method_used in {"VADER", "Arabic_RuleBased", "Mixed_VADER_Arabic"}
    assert 0 <= result.positive_ratio <= 1


def test_phase12_orchestrator_completed(tmp_path: Path):
    df = pd.DataFrame({
        "note": [
            "Customer appreciated proactive communication about delivery.",
            "Delivery was late but customer support was helpful.",
            "Average experience overall with timely updates.",
        ]
    })
    orchestrator = Phase12Orchestrator(df=df)
    result = orchestrator.run(tmp_path)

    assert result.status == "completed"
    assert "note" in result.basic_features
    assert "note" in result.keywords
    assert "note" in result.sentiment


def test_phase12_orchestrator_partial_large_dataset(tmp_path: Path):
    df = pd.DataFrame({
        "note": ["Large dataset sample text"] * 600000,
        "id": range(600000),
    })
    orchestrator = Phase12Orchestrator(df=df)
    result = orchestrator.run(tmp_path)

    assert result.status == "partial"
    assert result.sentiment == {}
    assert result.keywords == {}
    assert any("Text volume" in warn for warn in result.warnings)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
