"""
Phase 12: Simple Sentiment Analysis Service

Provides sentiment analysis for English (VADER) and Arabic (rule-based).
"""

from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from pydantic import BaseModel

try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    VADER_AVAILABLE = True
except ImportError:
    VADER_AVAILABLE = False


class SentimentResult(BaseModel):
    positive_ratio: float
    neutral_ratio: float
    negative_ratio: float
    avg_sentiment_score: float
    method_used: str


class SentimentAnalysisService:
    def __init__(
        self,
        df: pd.DataFrame,
        text_columns: List[str],
        language: str,
        cleaned_text: Optional[Dict[str, pd.Series]] = None,
    ):
        self.df = df
        self.text_columns = text_columns
        self.language = language
        self.cleaned_text = cleaned_text or {}
        self.warnings: List[str] = []
        
        # Initialize VADER if available
        if VADER_AVAILABLE:
            self.vader = SentimentIntensityAnalyzer()
        else:
            self.vader = None
            if language == "en":
                self.warnings.append(
                    "VADER sentiment library not installed - falling back to rule-based sentiment."
                )

    def run(self) -> Dict[str, SentimentResult]:
        """Analyze sentiment for each text column"""
        
        results = {}
        
        for col in self.text_columns:
            series = self.cleaned_text.get(col, self.df[col])
            if self.language == "en" and self.vader:
                result = self._analyze_english(series)
            elif self.language == "ar":
                result = self._analyze_arabic_simple(series)
            else:
                result = self._analyze_mixed(series)
            
            results[col] = result
        
        return results
    
    def _analyze_english(self, series: pd.Series) -> SentimentResult:
        """English sentiment using VADER"""
        
        scores = series.astype(str).apply(
            lambda text: self.vader.polarity_scores(text)['compound']
        )
        
        positive = (scores > 0.05).sum()
        neutral = ((scores >= -0.05) & (scores <= 0.05)).sum()
        negative = (scores < -0.05).sum()
        
        total = len(scores)
        if total == 0:
            return SentimentResult(
                positive_ratio=0.0,
                neutral_ratio=0.0,
                negative_ratio=0.0,
                avg_sentiment_score=0.0,
                method_used="VADER",
            )
        
        return SentimentResult(
            positive_ratio=round(positive / total, 4),
            neutral_ratio=round(neutral / total, 4),
            negative_ratio=round(negative / total, 4),
            avg_sentiment_score=round(float(scores.mean()), 4),
            method_used="VADER"
        )
    
    def _analyze_arabic_simple(self, series: pd.Series) -> SentimentResult:
        """Simple rule-based Arabic sentiment (FALLBACK)"""
        
        # Arabic positive/negative word lists (basic)
        positive_words = [
            'ممتاز', 'جيد', 'رائع', 'حلو', 'جميل', 'سعيد', 'ناجح', 'سريع',
            'نظيف', 'مريح', 'لطيف', 'احترافي', 'دقيق', 'مفيد', 'جودة', 'عالية',
            'ممتازة', 'مشكور', 'شكراً', 'ممتاز', 'جيد', 'رائع', 'حلو', 'جميل'
        ]
        
        negative_words = [
            'سيء', 'بطيء', 'متأخر', 'سيئ', 'غالي', 'رديء', 'محبط', 'فاشل',
            'قديم', 'متعب', 'صعب', 'مزعج', 'خطأ', 'مشكلة', 'مشاكل', 'مشكلة',
            'سيئ', 'بطيء', 'متأخر', 'غالي', 'رديء', 'محبط', 'فاشل'
        ]
        
        def score_text(text):
            text_lower = str(text).lower()
            pos_count = sum(1 for word in positive_words if word in text_lower)
            neg_count = sum(1 for word in negative_words if word in text_lower)
            
            if pos_count > neg_count:
                return 1
            elif neg_count > pos_count:
                return -1
            else:
                return 0
        
        scores = series.apply(score_text)
        
        positive = (scores > 0).sum()
        neutral = (scores == 0).sum()
        negative = (scores < 0).sum()
        
        total = len(scores)
        if total == 0:
            return SentimentResult(
                positive_ratio=0.0,
                neutral_ratio=0.0,
                negative_ratio=0.0,
                avg_sentiment_score=0.0,
                method_used="Arabic_RuleBased",
            )
        
        return SentimentResult(
            positive_ratio=round(positive / total, 4),
            neutral_ratio=round(neutral / total, 4),
            negative_ratio=round(negative / total, 4),
            avg_sentiment_score=round(float(scores.mean()), 4),
            method_used="Arabic_RuleBased"
        )
    
    def _analyze_mixed(self, series: pd.Series) -> SentimentResult:
        """Mixed language - try both methods and combine"""
        
        if self.vader:
            english_result = self._analyze_english(series)
            arabic_result = self._analyze_arabic_simple(series)
            
            # Average the results
            return SentimentResult(
                positive_ratio=round((english_result.positive_ratio + arabic_result.positive_ratio) / 2, 4),
                neutral_ratio=round((english_result.neutral_ratio + arabic_result.neutral_ratio) / 2, 4),
                negative_ratio=round((english_result.negative_ratio + arabic_result.negative_ratio) / 2, 4),
                avg_sentiment_score=round((english_result.avg_sentiment_score + arabic_result.avg_sentiment_score) / 2, 4),
                method_used="Mixed_VADER_Arabic"
            )
        else:
            return self._analyze_arabic_simple(series)
