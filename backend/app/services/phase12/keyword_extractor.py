"""
Phase 12: Keyword extraction service.

Implements lightweight keyword and bigram extraction using frequency analysis
without external dependencies.
"""

from __future__ import annotations

from collections import Counter
from typing import Dict, Iterable, List, Tuple

import pandas as pd
from pydantic import BaseModel

from .basic_features import _flatten, _tokenize, EN_STOPWORDS, AR_STOPWORDS


class KeywordSummary(BaseModel):
    top_tokens: List[Tuple[str, int]]
    top_bigrams: List[Tuple[str, int]]
    total_tokens: int


class KeywordExtractionService:
    def __init__(
        self,
        cleaned_text: Dict[str, pd.Series],
        language: str = "unknown",
        top_k: int = 10,
    ):
        self.cleaned_text = cleaned_text
        self.language = language
        self.top_k = top_k

    def run(self) -> Dict[str, KeywordSummary]:
        results: Dict[str, KeywordSummary] = {}
        stopwords = self._resolve_stopwords()

        for column, series in self.cleaned_text.items():
            tokens_per_row = series.dropna().astype(str).apply(_tokenize)
            all_tokens = [
                token for token in _flatten(tokens_per_row) if token not in stopwords and len(token) > 1
            ]

            if not all_tokens:
                results[column] = KeywordSummary(
                    top_tokens=[],
                    top_bigrams=[],
                    total_tokens=0,
                )
                continue

            token_counts = Counter(all_tokens)
            bigram_counts = Counter(_generate_bigrams(tokens_per_row, stopwords))

            results[column] = KeywordSummary(
                top_tokens=token_counts.most_common(self.top_k),
                top_bigrams=bigram_counts.most_common(self.top_k),
                total_tokens=len(all_tokens),
            )

        return results

    def _resolve_stopwords(self) -> set[str]:
        if self.language == "en":
            return EN_STOPWORDS
        if self.language == "ar":
            return AR_STOPWORDS
        return EN_STOPWORDS | AR_STOPWORDS


def _generate_bigrams(tokens_per_row: Iterable[List[str]], stopwords: set[str]) -> Iterable[str]:
    for tokens in tokens_per_row:
        filtered = [t for t in tokens if t not in stopwords]
        for i in range(len(filtered) - 1):
            yield f"{filtered[i]} {filtered[i+1]}"
