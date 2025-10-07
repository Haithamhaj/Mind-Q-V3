from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import List, Optional, Dict, Any

import pandas as pd
import numpy as np
import re


@dataclass
class FeatureMetadata:
    name: str
    clean_name: str
    data_type: str
    semantic_type: str
    description: str
    unique_values: int
    uniqueness_ratio: float
    missing_pct: float
    recommended_role: str
    is_identifier: bool
    is_target_candidate: bool

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class FeatureDictionaryService:
    """
    Build a feature dictionary containing clean aliases, semantic typing and role hints.
    This helps BI dashboards and LLM prompts understand the dataset without guessing.
    """

    IDENTIFIER_KEYWORDS = [
        "id", "uuid", "reference", "ref", "awb", "phone", "address", "name", "tracking"
    ]

    TARGET_CANDIDATE_KEYWORDS = [
        "status", "flag", "success", "return", "delivered", "churn", "default", "fraud"
    ]

    def __init__(self, df: pd.DataFrame, domain: str | None = None):
        self.df = df
        self.domain = domain or "general"
        self._alias_registry: Dict[str, int] = {}

    def generate(self) -> List[FeatureMetadata]:
        rows = []
        total_rows = len(self.df) if len(self.df) > 0 else 1

        for column in self.df.columns:
            series = self.df[column]
            nunique = int(series.nunique(dropna=False))
            uniqueness_ratio = float(nunique / total_rows)
            missing_pct = float(series.isna().mean() * 100)
            data_type = str(series.dtype)
            semantic_type = self._infer_semantic_type(series, column)

            is_identifier = self._is_identifier(column, semantic_type, uniqueness_ratio)
            is_constant = nunique <= 1

            is_target_candidate = self._is_target_candidate(
                column, semantic_type, is_identifier, is_constant, nunique
            )

            recommended_role = self._determine_role(
                is_identifier, is_constant, is_target_candidate, semantic_type
            )

            clean_name = self._make_clean_name(column)
            description = self._make_description(column)

            rows.append(
                FeatureMetadata(
                    name=column,
                    clean_name=clean_name,
                    data_type=data_type,
                    semantic_type=semantic_type,
                    description=description,
                    unique_values=nunique,
                    uniqueness_ratio=round(uniqueness_ratio, 4),
                    missing_pct=round(missing_pct, 2),
                    recommended_role=recommended_role,
                    is_identifier=is_identifier,
                    is_target_candidate=is_target_candidate,
                )
            )

        return rows

    # --- helper methods -------------------------------------------------

    def _make_clean_name(self, name: str) -> str:
        slug = re.sub(r"[^0-9a-zA-Z]+", "_", name).strip("_").lower()
        if not slug:
            slug = "feature"
        # ensure uniqueness
        count = self._alias_registry.get(slug, 0)
        self._alias_registry[slug] = count + 1
        if count > 0:
            slug = f"{slug}_{count}"
        return slug

    def _make_description(self, name: str) -> str:
        cleaned = re.sub(r"[_\-]+", " ", name).replace("%", " percent ")
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        if not cleaned:
            cleaned = name
        return cleaned.title()

    def _infer_semantic_type(self, series: pd.Series, name: str) -> str:
        dtype = series.dtype
        if pd.api.types.is_bool_dtype(dtype):
            return "binary"
        if pd.api.types.is_integer_dtype(dtype) or pd.api.types.is_float_dtype(dtype):
            # treat small cardinality numerics as categorical/binary
            nunique = series.nunique(dropna=True)
            if nunique <= 2:
                return "binary"
            if nunique <= 10:
                return "categorical_numeric"
            return "numeric"
        if pd.api.types.is_datetime64_any_dtype(dtype):
            return "datetime"
        if pd.api.types.is_categorical_dtype(dtype):
            return "categorical"
        # detect bool-like strings
        nunique = series.dropna().str.lower().nunique() if series.dropna().map(type).eq(str).all() else series.nunique(dropna=True)
        if nunique == 2:
            return "binary"
        return "categorical"

    def _is_identifier(self, name: str, semantic_type: str, uniqueness_ratio: float) -> bool:
        lname = name.lower()
        keyword_hit = any(keyword in lname for keyword in self.IDENTIFIER_KEYWORDS)
        return uniqueness_ratio >= 0.9 or (keyword_hit and semantic_type in {"categorical", "numeric", "datetime"})

    def _is_target_candidate(
        self,
        name: str,
        semantic_type: str,
        is_identifier: bool,
        is_constant: bool,
        nunique: int,
    ) -> bool:
        if is_identifier or is_constant:
            return False
        if semantic_type == "binary":
            return True
        if semantic_type.startswith("categorical") and nunique <= 5:
            return True
        lname = name.lower()
        return any(keyword in lname for keyword in self.TARGET_CANDIDATE_KEYWORDS)

    def _determine_role(
        self,
        is_identifier: bool,
        is_constant: bool,
        is_target_candidate: bool,
        semantic_type: str,
    ) -> str:
        if is_constant:
            return "constant"
        if is_identifier:
            return "identifier"
        if is_target_candidate:
            return "target_candidate"
        if semantic_type == "datetime":
            return "temporal"
        return "feature"

