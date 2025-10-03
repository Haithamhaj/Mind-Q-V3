"""
Phase 12: Basic Text Features Service

Extracts basic statistical features from text columns.
"""

from typing import Dict
import pandas as pd
import numpy as np
from pydantic import BaseModel


class BasicTextFeatures(BaseModel):
    avg_length_chars: float
    avg_length_words: float
    avg_length_sentences: float
    max_length: int
    min_length: int
    numeric_ratio: float
    special_char_ratio: float
    arabic_ratio: float


class BasicTextFeaturesService:
    def __init__(self, df: pd.DataFrame, text_columns: list):
        self.df = df
        self.text_columns = text_columns
    
    def run(self) -> Dict[str, BasicTextFeatures]:
        """Generate basic text features for each text column"""
        
        results = {}
        
        for col in self.text_columns:
            features = self._extract_features(self.df[col])
            results[col] = features
        
        return results
    
    def _extract_features(self, series: pd.Series) -> BasicTextFeatures:
        """Extract basic features from text series"""
        
        # Convert to string
        texts = series.astype(str)
        
        # Character length
        lengths = texts.str.len()
        avg_len = float(lengths.mean())
        max_len = int(lengths.max())
        min_len = int(lengths.min())
        
        # Word count
        word_counts = texts.str.split().str.len()
        avg_words = float(word_counts.mean())
        
        # Sentence count (rough estimate by periods)
        sentence_counts = texts.str.count(r'[.!?؟]') + 1
        avg_sentences = float(sentence_counts.mean())
        
        # Numeric ratio
        numeric_chars = texts.apply(lambda x: sum(c.isdigit() for c in x))
        numeric_ratio = float(numeric_chars.sum() / lengths.sum()) if lengths.sum() > 0 else 0.0
        
        # Special characters ratio
        special_chars = texts.apply(lambda x: sum(not c.isalnum() and not c.isspace() for c in x))
        special_ratio = float(special_chars.sum() / lengths.sum()) if lengths.sum() > 0 else 0.0
        
        # Arabic ratio
        arabic_chars = texts.apply(lambda x: sum(1 for c in x if '\u0600' <= c <= '\u06FF'))
        arabic_ratio = float(arabic_chars.sum() / lengths.sum()) if lengths.sum() > 0 else 0.0
        
        return BasicTextFeatures(
            avg_length_chars=round(avg_len, 2),
            avg_length_words=round(avg_words, 2),
            avg_length_sentences=round(avg_sentences, 2),
            max_length=max_len,
            min_length=min_len,
            numeric_ratio=round(numeric_ratio, 4),
            special_char_ratio=round(special_ratio, 4),
            arabic_ratio=round(arabic_ratio, 4)
        )
