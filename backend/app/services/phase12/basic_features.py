"""
Phase 12: Basic Text Features Service

Extracts deterministic text statistics without requiring GPUs or heavyweight NLP
dependencies.
"""

from __future__ import annotations

import re
from typing import Dict, Iterable, List, Set

import numpy as np
import pandas as pd
from pydantic import BaseModel

_TOKEN_RE = re.compile(r"[^\W_]+", re.UNICODE)
_SENTENCE_RE = re.compile(r"[.!?؟]+")

EN_STOPWORDS: Set[str] = {
    "the",
    "a",
    "is",
    "it",
    "an",
    "in",
    "and",
    "or",
    "for",
    "to",
    "of",
    "on",
    "with",
    "by",
    "at",
    "from",
}

AR_STOPWORDS: Set[str] = {
    "من",
    "على",
    "عن",
    "هذا",
    "هذه",
    "الى",
    "في",
    "او",
    "و",
    "كما",
    "انه",
    "كانت",
    "كان",
    "هناك",
    "حتى",
    "اليوم",
}


class BasicTextFeatures(BaseModel):
    avg_length_chars: float
    avg_length_words: float
    avg_length_sentences: float
    max_length: int
    min_length: int
    numeric_ratio: float
    special_char_ratio: float
    arabic_ratio: float
    unique_token_ratio: float
    avg_word_length: float
    stopword_ratio: float


class BasicTextFeaturesService:
    def __init__(
        self,
        df: pd.DataFrame,
        text_columns: List[str],
        cleaned_text: Dict[str, pd.Series] | None = None,
        language: str = "unknown",
    ):
        self.df = df
        self.text_columns = text_columns
        self.cleaned_text = cleaned_text or {}
        self.language = language

    def run(self) -> Dict[str, BasicTextFeatures]:
        results: Dict[str, BasicTextFeatures] = {}
        for column in self.text_columns:
            series = self.df[column]
            cleaned = self.cleaned_text.get(column)
            results[column] = self._extract_features(series, cleaned)
        return results

    def _extract_features(self, series: pd.Series, cleaned: pd.Series | None) -> BasicTextFeatures:
        texts = series.dropna().astype(str)
        if texts.empty:
            return _empty_features()

        cleaned_texts = cleaned.dropna().astype(str) if cleaned is not None else texts

        lengths = texts.str.len()
        total_chars = float(lengths.sum())

        avg_len = float(lengths.mean()) if not lengths.empty else 0.0
        max_len = int(lengths.max()) if not lengths.empty else 0
        min_len = int(lengths.min()) if not lengths.empty else 0

        tokens_per_row = cleaned_texts.apply(_tokenize)
        word_counts = tokens_per_row.apply(len)
        total_tokens = int(word_counts.sum())

        avg_words = float(word_counts.mean()) if total_tokens else 0.0

        sentence_counts = texts.apply(lambda x: max(1, len(_SENTENCE_RE.findall(x))))
        avg_sentences = float(sentence_counts.mean())

        numeric_chars = texts.apply(lambda x: sum(c.isdigit() for c in x))
        numeric_ratio = float(numeric_chars.sum() / total_chars) if total_chars else 0.0

        special_chars = texts.apply(lambda x: sum(not c.isalnum() and not c.isspace() for c in x))
        special_ratio = float(special_chars.sum() / total_chars) if total_chars else 0.0

        arabic_chars = texts.apply(lambda x: sum(1 for c in x if "\u0600" <= c <= "\u06FF"))
        arabic_ratio = float(arabic_chars.sum() / total_chars) if total_chars else 0.0

        all_tokens = _flatten(tokens_per_row)
        unique_tokens = len(set(all_tokens))
        unique_ratio = float(unique_tokens / total_tokens) if total_tokens else 0.0

        avg_word_len = float(np.mean([len(token) for token in all_tokens])) if all_tokens else 0.0

        stopwords = _resolve_stopwords(self.language)
        stopword_count = sum(1 for token in all_tokens if token in stopwords)
        stopword_ratio = float(stopword_count / total_tokens) if total_tokens else 0.0

        return BasicTextFeatures(
            avg_length_chars=round(avg_len, 2),
            avg_length_words=round(avg_words, 2),
            avg_length_sentences=round(avg_sentences, 2),
            max_length=max_len,
            min_length=min_len,
            numeric_ratio=round(numeric_ratio, 4),
            special_char_ratio=round(special_ratio, 4),
            arabic_ratio=round(arabic_ratio, 4),
            unique_token_ratio=round(unique_ratio, 4),
            avg_word_length=round(avg_word_len, 2),
            stopword_ratio=round(stopword_ratio, 4),
        )


def _tokenize(text: str) -> List[str]:
    return [match.group(0).lower() for match in _TOKEN_RE.finditer(text)]


def _flatten(list_of_lists: Iterable[List[str]]) -> List[str]:
    flattened: List[str] = []
    for tokens in list_of_lists:
        flattened.extend(tokens)
    return flattened


def _resolve_stopwords(language: str) -> Set[str]:
    if language == "en":
        return EN_STOPWORDS
    if language == "ar":
        return AR_STOPWORDS
    return EN_STOPWORDS | AR_STOPWORDS


def _empty_features() -> BasicTextFeatures:
    return BasicTextFeatures(
        avg_length_chars=0.0,
        avg_length_words=0.0,
        avg_length_sentences=0.0,
        max_length=0,
        min_length=0,
        numeric_ratio=0.0,
        special_char_ratio=0.0,
        arabic_ratio=0.0,
        unique_token_ratio=0.0,
        avg_word_length=0.0,
        stopword_ratio=0.0,
    )
