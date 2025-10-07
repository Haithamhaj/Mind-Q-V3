"""
Phase 12: Lightweight text cleaning utilities.

Provides deterministic, infrastructure-free normalization so downstream
feature extraction and sentiment steps work with consistent text.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Dict, List

import pandas as pd

_URL_RE = re.compile(r"https?://\S+|www\.\S+")
_EMAIL_RE = re.compile(r"\b[\w.\-]+@[\w\-.]+\.\w+\b")
_MENTION_RE = re.compile(r"@\w+")
_WHITESPACE_RE = re.compile(r"\s+")
_DIGIT_RE = re.compile(r"\d+")


class TextCleaningService:
    def __init__(self, df: pd.DataFrame, text_columns: List[str], language: str):
        self.df = df
        self.text_columns = text_columns
        self.language = language

    def run(self) -> Dict[str, pd.Series]:
        cleaned: Dict[str, pd.Series] = {}
        for column in self.text_columns:
            series = self.df[column].fillna("").astype(str)
            cleaned[column] = series.apply(self._clean_text)
        return cleaned

    def _clean_text(self, text: str) -> str:
        text = _URL_RE.sub(" ", text)
        text = _EMAIL_RE.sub(" ", text)
        text = _MENTION_RE.sub(" ", text)

        if self.language in {"en", "mixed"}:
            text = text.lower()
        if self.language in {"ar", "mixed"}:
            text = _normalize_arabic(text)

        text = unicodedata.normalize("NFKC", text)
        text = _DIGIT_RE.sub(" ", text)
        text = _WHITESPACE_RE.sub(" ", text)
        return text.strip()


def _normalize_arabic(text: str) -> str:
    substitutions = {
        "أ": "ا",
        "إ": "ا",
        "آ": "ا",
        "ى": "ي",
        "ؤ": "و",
        "ئ": "ي",
        "ة": "ه",
    }
    for src, target in substitutions.items():
        text = text.replace(src, target)

    # Remove diacritics
    decomposed = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in decomposed if unicodedata.category(ch) != "Mn")
    return text
