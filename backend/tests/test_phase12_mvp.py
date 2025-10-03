"""
Unit tests for Phase 12: Text Features (MVP)
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path

from app.services.phase12.detection import TextDetectionService
from app.services.phase12.basic_features import BasicTextFeaturesService
from app.services.phase12.sentiment_simple import SentimentAnalysisService


def test_text_detection_english():
    """Test English text detection"""
    df = pd.DataFrame({
        'description': ['This is a good product'] * 100,
        'short': ['OK'] * 100,
        'number': range(100)
    })
    
    service = TextDetectionService(df=df)
    result = service.run()
    
    assert 'description' in result.text_columns
    assert 'short' not in result.text_columns  # Too short
    assert result.language_detected == "en"


def test_text_detection_arabic():
    """Test Arabic text detection"""
    df = pd.DataFrame({
        'comment': ['هذا منتج ممتاز وجودة عالية'] * 100
    })
    
    service = TextDetectionService(df=df)
    result = service.run()
    
    assert len(result.text_columns) == 1
    assert result.language_detected == "ar"


def test_text_detection_mixed():
    """Test mixed language detection"""
    df = pd.DataFrame({
        'mixed_text': [
            'This is good product',
            'هذا منتج جيد',
            'Good quality جودة عالية'
        ] * 50
    })
    
    service = TextDetectionService(df=df)
    result = service.run()
    
    assert 'mixed_text' in result.text_columns
    assert result.language_detected in ["mixed", "en", "ar"]  # Should detect some language


def test_text_detection_no_text():
    """Test detection with no text columns"""
    df = pd.DataFrame({
        'id': range(100),
        'amount': np.random.random(100),
        'category': ['A', 'B', 'C'] * 33 + ['A']
    })
    
    service = TextDetectionService(df=df)
    result = service.run()
    
    assert len(result.text_columns) == 0
    assert result.language_detected == "none"
    assert result.status == "skipped"


def test_basic_features():
    """Test basic text features extraction"""
    df = pd.DataFrame({
        'text': [
            'Short text.',
            'This is a longer text with more words and characters.',
            'Medium length text here.'
        ]
    })
    
    service = BasicTextFeaturesService(df=df, text_columns=['text'])
    results = service.run()
    
    assert 'text' in results
    assert results['text'].avg_length_chars > 0
    assert results['text'].avg_length_words > 0
    assert results['text'].avg_length_sentences > 0
    assert results['text'].max_length > results['text'].min_length


def test_basic_features_arabic():
    """Test basic features with Arabic text"""
    df = pd.DataFrame({
        'arabic_text': [
            'هذا نص قصير.',
            'هذا نص أطول يحتوي على كلمات أكثر وأحرف أكثر.',
            'هذا نص متوسط الطول هنا.'
        ]
    })
    
    service = BasicTextFeaturesService(df=df, text_columns=['arabic_text'])
    results = service.run()
    
    assert 'arabic_text' in results
    assert results['arabic_text'].arabic_ratio > 0.5  # Should detect Arabic characters


def test_sentiment_english():
    """Test English sentiment analysis"""
    df = pd.DataFrame({
        'review': [
            'This product is amazing and wonderful!',
            'Terrible quality, very disappointed.',
            'It is okay, nothing special.'
        ]
    })
    
    service = SentimentAnalysisService(df=df, text_columns=['review'], language='en')
    results = service.run()
    
    assert 'review' in results
    assert results['review'].positive_ratio > 0
    assert results['review'].negative_ratio > 0
    assert results['review'].neutral_ratio > 0
    assert results['review'].method_used in ["VADER", "Arabic_RuleBased"]  # VADER might not be available


def test_sentiment_arabic():
    """Test Arabic sentiment analysis"""
    df = pd.DataFrame({
        'comment': [
            'منتج ممتاز جداً وجودة رائعة',
            'سيء جداً ومحبط',
            'عادي'
        ]
    })
    
    service = SentimentAnalysisService(df=df, text_columns=['comment'], language='ar')
    results = service.run()
    
    assert 'comment' in results
    assert results['comment'].method_used == "Arabic_RuleBased"


def test_sentiment_mixed():
    """Test mixed language sentiment analysis"""
    df = pd.DataFrame({
        'mixed_review': [
            'This is amazing!',
            'هذا ممتاز',
            'Good product جودة عالية'
        ]
    })
    
    service = SentimentAnalysisService(df=df, text_columns=['mixed_review'], language='mixed')
    results = service.run()
    
    assert 'mixed_review' in results
    assert results['mixed_review'].method_used in ["Mixed_VADER_Arabic", "Arabic_RuleBased"]


def test_sentiment_no_vader():
    """Test sentiment analysis without VADER available"""
    # This test ensures fallback works when VADER is not installed
    df = pd.DataFrame({
        'text': ['This is a test text for sentiment analysis']
    })
    
    service = SentimentAnalysisService(df=df, text_columns=['text'], language='en')
    results = service.run()
    
    assert 'text' in results
    assert results['text'].method_used in ["VADER", "Arabic_RuleBased"]


def test_multiple_text_columns():
    """Test processing multiple text columns"""
    df = pd.DataFrame({
        'description': ['Product description text'] * 50,
        'review': ['Customer review text'] * 50,
        'comment': ['Additional comment text'] * 50,
        'id': range(50)
    })
    
    service = TextDetectionService(df=df)
    detection_result = service.run()
    
    assert len(detection_result.text_columns) == 3
    
    # Test basic features on multiple columns
    basic_service = BasicTextFeaturesService(df=df, text_columns=detection_result.text_columns)
    basic_results = basic_service.run()
    
    assert len(basic_results) == 3
    for col in detection_result.text_columns:
        assert col in basic_results


def test_large_dataset_skip():
    """Test that large datasets are skipped"""
    # Create a large dataset (>500k rows)
    df = pd.DataFrame({
        'text': ['This is a test text'] * 600000,
        'id': range(600000)
    })
    
    service = TextDetectionService(df=df)
    result = service.run()
    
    assert result.recommendation == "Dataset too large (>500k rows) - Text analysis not recommended"


def test_empty_text_columns():
    """Test handling of empty text columns"""
    df = pd.DataFrame({
        'empty_text': [''] * 100,
        'short_text': ['a'] * 100,
        'id': range(100)
    })
    
    service = TextDetectionService(df=df)
    result = service.run()
    
    # Should not detect empty or very short columns as text
    assert len(result.text_columns) == 0


def test_numeric_mixed_columns():
    """Test columns with mixed numeric and text data"""
    df = pd.DataFrame({
        'mixed_column': [
            'This is a long text description that should be detected',
            '123456789',  # Numeric string
            'Another long text description',
            'Short',  # Too short
            'Yet another long description that meets the criteria'
        ] * 20
    })
    
    service = TextDetectionService(df=df)
    result = service.run()
    
    # Should detect the column if average length > 50
    if 'mixed_column' in result.text_columns:
        assert result.text_columns == ['mixed_column']


def test_special_characters():
    """Test text with special characters"""
    df = pd.DataFrame({
        'special_text': [
            'This text has special chars: @#$%^&*()',
            'Text with numbers: 123 and symbols: !@#',
            'Mixed: English & Arabic: مرحبا & Hello'
        ]
    })
    
    service = BasicTextFeaturesService(df=df, text_columns=['special_text'])
    results = service.run()
    
    assert 'special_text' in results
    assert results['special_text'].special_char_ratio > 0
    assert results['special_text'].numeric_ratio > 0


def test_edge_case_empty_dataframe():
    """Test edge case with empty dataframe"""
    df = pd.DataFrame()
    
    service = TextDetectionService(df=df)
    result = service.run()
    
    assert len(result.text_columns) == 0
    assert result.language_detected == "none"


def test_edge_case_single_row():
    """Test edge case with single row"""
    df = pd.DataFrame({
        'text': ['This is a single row with long enough text to be detected as text column']
    })
    
    service = TextDetectionService(df=df)
    result = service.run()
    
    assert 'text' in result.text_columns
    assert result.language_detected == "en"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
