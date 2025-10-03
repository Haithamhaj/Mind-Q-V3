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
        """Identify text columns (string type with avg length > 50)"""
        text_cols = []
        
        for col in self.df.columns:
            if self.df[col].dtype == 'object':
                # Calculate average length
                avg_len = self.df[col].astype(str).str.len().mean()
                
                # Consider it text if avg length > 50 characters
                if avg_len > 50:
                    text_cols.append(col)
        
        return text_cols
    
    def _detect_language(self, text_cols: List[str]) -> str:
        """Detect primary language (Arabic vs English vs Mixed)"""
        if not text_cols:
            return "none"
        
        # Sample first 100 rows
        sample_text = " ".join(
            self.df[text_cols[0]].head(100).astype(str).tolist()
        )
        
        # Count Arabic characters
        arabic_chars = sum(1 for c in sample_text if '\u0600' <= c <= '\u06FF')
        total_chars = len([c for c in sample_text if c.isalpha()])
        
        if total_chars == 0:
            return "unknown"
        
        arabic_ratio = arabic_chars / total_chars
        
        if arabic_ratio > 0.7:
            return "ar"
        elif arabic_ratio < 0.3:
            return "en"
        else:
            return "mixed"
    
    def _generate_recommendation(self, n_rows: int, total_length: int) -> str:
        """Generate processing recommendation"""
        if n_rows > 500000:
            return "Dataset too large (>500k rows) - Text analysis not recommended"
        elif total_length > 50_000_000:  # 50MB of text
            return "Large text volume detected - Use basic features only"
        else:
            return "Text analysis recommended (MVP: Basic + Sentiment)"
