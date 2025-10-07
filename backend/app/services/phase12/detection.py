"""
Phase 12: Text Column Detection Service

Detects text columns in the dataset and analyzes language.
"""

from typing import List
import pandas as pd
from pydantic import BaseModel


class TextDetectionResult(BaseModel):
    text_columns: List[str]
    language_detected: str  # "en", "ar", "mixed", "unknown", "none"
    total_text_length: int
    recommendation: str


class TextDetectionService:
    def __init__(self, df: pd.DataFrame):
        self.df = df
    
    def run(self) -> TextDetectionResult:
        """Detect text columns and analyze language"""
        
        text_cols = self._detect_text_columns()
        
        if not text_cols:
            return TextDetectionResult(
                text_columns=[],
                language_detected="none",
                total_text_length=0,
                recommendation="No text columns detected - Phase 12 skipped"
            )
        
        # Detect primary language
        language = self._detect_language(text_cols)
        
        # Calculate total text size
        total_length = sum(
            self.df[col].astype(str).str.len().sum() 
            for col in text_cols
        )
        
        # Generate recommendation
        recommendation = self._generate_recommendation(len(self.df), total_length)
        
        return TextDetectionResult(
            text_columns=text_cols,
            language_detected=language,
            total_text_length=int(total_length),
            recommendation=recommendation
        )
    
    def _detect_text_columns(self) -> List[str]:
        """Identify text columns using multiple heuristics."""
        text_cols: List[str] = []
        sample_size = min(len(self.df), 2000)
        sample_df = self.df.head(sample_size)

        for col in sample_df.columns:
            series = sample_df[col]
            dtype_str = str(series.dtype)
            if dtype_str not in {"object", "string"} and "category" not in dtype_str.lower():
                continue

            # Drop nulls for metrics
            values = series.dropna().astype(str)
            if values.empty:
                continue

            unique_values = values.nunique()

            avg_length = values.str.len().mean()
            alpha_ratio = values.apply(lambda x: _alphabetic_ratio(x)).mean()
            unique_tokens = values.apply(lambda x: len(set(x.split()))).mean()
            space_ratio = values.str.count(r"\s").mean()

            if unique_values == 1 and avg_length < 20:
                continue

            # Heuristics: allow shorter qualitative fields but ensure enough alphabetic content
            if (
                avg_length >= 15
                and alpha_ratio >= 0.3
            ) or (
                avg_length >= 8
                and alpha_ratio >= 0.5
                and unique_tokens >= 3
            ) or (
                avg_length >= 12
                and space_ratio >= 0.2
                and alpha_ratio >= 0.25
            ):
                text_cols.append(col)

        return text_cols
    
    def _detect_language(self, text_cols: List[str]) -> str:
        """Detect primary language (Arabic vs English vs Mixed)"""
        if not text_cols:
            return "none"
        
        sample_texts = []
        for col in text_cols[:3]:
            sample_texts.extend(self.df[col].dropna().astype(str).head(200).tolist())
        sample_text = " ".join(sample_texts)

        if not sample_text:
            return "unknown"

        arabic_chars = sum(1 for c in sample_text if '\u0600' <= c <= '\u06FF')
        latin_chars = sum(1 for c in sample_text if 'a' <= c.lower() <= 'z')
        other_alpha = sum(
            1 for c in sample_text if c.isalpha() and c not in (tuple("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ") + tuple(chr(i) for i in range(0x0600, 0x0700)))
        )
        total_alpha = arabic_chars + latin_chars + other_alpha

        if total_alpha == 0:
            return "unknown"

        arabic_ratio = arabic_chars / total_alpha
        latin_ratio = latin_chars / total_alpha

        if arabic_ratio > 0.7:
            return "ar"
        if latin_ratio > 0.7:
            return "en"
        if 0.3 <= arabic_ratio <= 0.7 and 0.3 <= latin_ratio <= 0.7:
            return "mixed"
        return "unknown"
    
    def _generate_recommendation(self, n_rows: int, total_length: int) -> str:
        """Generate processing recommendation"""
        if n_rows > 500000:
            return "Dataset too large (>500k rows) - Text analysis not recommended"
        elif total_length > 50_000_000:  # 50MB of text
            return "Large text volume detected - Use basic features only"
        else:
            return "Text analysis recommended (MVP: Basic + Sentiment)"


def _alphabetic_ratio(value: str) -> float:
    if not value:
        return 0.0
    alpha = sum(1 for c in value if c.isalpha())
    total = len(value)
    if total == 0:
        return 0.0
    return alpha / total

