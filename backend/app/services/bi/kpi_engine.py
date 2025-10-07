from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import pandas as pd

from app.config import settings
from app.models.kpi import (
    AggregationSpec,
    KPIAdoptionRequest,
    KPIAdoptionResponse,
    KPIProposal,
    KPIProposalBundle,
    KPIValidationRequest,
    KPIValidationResponse,
    KPIValidationResult,
    RatioFormula,
    SingleValueFormula,
)
from app.services.bi.llm_client import call_llm_api


KPI_PROPOSAL_PROMPT = """You are a senior BI strategist. Generate high-impact KPI candidates using the dataset context.

Context:
- Domain: {domain}
- Rows: {row_count}
- Columns (clean alias | original | role | missing%):
{feature_summary}

Existing Business Goals and KPIs:
{existing_targets}

Correlation Highlights (top drivers):
{correlation_highlights}

Business Validation Warnings:
{business_conflicts}

Data Quality Notes:
{quality_summary}

Best practice KPIs in this domain:
{best_practices}

Task:
- Propose {count} KPIs that best measure performance and align with goals.
- Avoid identifier/constant columns and columns with >50% missing data.
- Justify each KPI with financial impact and why it matters now.
- Suggest short alias (snake_case) based on clean column names.
- Use columns exactly as provided (alias or original).
- Each KPI MUST include a machine-readable formula using the schema below.

Formula Schema (JSON):
- ratio: {{"type": "ratio", "numerator": {{"aggregation": "count|sum|mean", "column": "col", "filter": {{"operator": "...", ...}}}}, "denominator": {{"aggregation": "...", "column": "..."}}, "multiplier": 100, "format": "percentage"}}
- average/sum/count/median: {{"type": "average|sum|count|median", "aggregation": {{"aggregation": "mean|sum|count|median|nunique", "column": "col", "filter": {{...}}}}, "multiplier": 1, "format": "number|percentage|currency"}}
- Filters operators allowed: equals, not_equals, in, not_in, gt, gte, lt, lte, between.

Respond ONLY with JSON:
{{
  "proposals": [
    {{
      "kpi_id": "domain_theme_metric",
      "name": "On-Time Delivery Rate",
      "alias": "on_time_delivery_rate",
      "metric_type": "quality",
      "description": "Share of shipments delivered within SLA.",
      "rationale": "Links transit reliability to customer promises using SLA expectations.",
      "financial_impact": "Each +1% reduces penalty fees by ~5%",
      "confidence": 0.85,
      "recommended_direction": "higher_is_better",
      "formula": {{...}},
      "required_columns": ["status", "sla_flag"],
      "supporting_evidence": ["status vs sla_flag corr=0.61"],
      "warnings": []
    }}
  ]
}}
"""


DOMAIN_KPI_PRACTICES: Dict[str, List[str]] = {
    "logistics": [
        "On-time delivery rate",
        "Return to origin (RTO) rate",
        "Average transit time",
        "Failed attempt ratio",
        "First attempt success",
    ],
    "healthcare": [
        "Patient no-show rate",
        "Average length of stay",
        "Readmission within 30 days",
        "Bed occupancy utilization",
        "Procedure success rate",
    ],
    "retail": [
        "Conversion rate",
        "Average order value",
        "Repeat purchase rate",
        "Inventory turnover",
        "Return rate",
    ],
    "emarketing": [
        "Click-through rate",
        "Conversion rate",
        "Cost per acquisition",
        "Return on ad spend",
        "Lead-to-customer ratio",
    ],
    "finance": [
        "Loan default rate",
        "Net interest margin",
        "Customer lifetime value",
        "Delinquency rate",
        "Liquidity coverage ratio",
    ],
}


@dataclass
class FeatureMeta:
    name: str
    alias: str
    role: str
    missing_pct: float
    semantic_type: str


class KPIProposalEngine:
    """Generate KPI proposals based on feature dictionary, correlations, and business context."""

    def __init__(
        self,
        domain: str,
        language: str = "en",
        artifacts_dir: Optional[Path] = None,
        llm_callable: Optional[Callable[[str], str]] = None,
    ):
        self.domain = domain.lower()
        self.language = language
        self.artifacts_dir = Path(artifacts_dir or settings.artifacts_dir)
        self.llm_callable = llm_callable or call_llm_api
        self._alias_lookup: Dict[str, str] = {}
        self._feature_meta: Dict[str, FeatureMeta] = {}
        self._warnings: List[str] = []
        self._dataset_source: Optional[str] = None

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def generate(self, count: int = 3) -> KPIProposalBundle:
        df = self._load_dataset()
        context = self._build_context(df)

        prompt = KPI_PROPOSAL_PROMPT.format(
            domain=self.domain,
            row_count=len(df),
            feature_summary="\n".join(context["feature_summary"]),
            existing_targets=context["existing_targets"] or "None recorded.",
            correlation_highlights=context["correlation_highlights"] or "No strong signals found.",
            business_conflicts=context["business_conflicts"] or "No outstanding conflicts.",
            quality_summary=context["quality_summary"] or "Data quality acceptable.",
            best_practices=", ".join(DOMAIN_KPI_PRACTICES.get(self.domain, [])),
            count=count,
        )

        raw_response = self.llm_callable(prompt)
        proposals = self._parse_llm_response(raw_response)

        sanitized: List[KPIProposal] = []
        for proposal in proposals:
            normalized = self._normalize_proposal(proposal, df)
            if normalized:
                sanitized.append(normalized)

        if len(sanitized) > count:
            sanitized = sanitized[:count]

        # Append custom slot for UI
        sanitized.append(
            KPIProposal(
                kpi_id="custom_entry",
                name="Custom KPI",
                alias="custom_kpi",
                metric_type="custom",
                description="Editable slot for user-defined KPI.",
                rationale="Let the analyst capture bespoke KPI definitions.",
                financial_impact=None,
                confidence=None,
                recommended_direction="higher_is_better",
                formula=None,
                required_columns=[],
                supporting_evidence=[],
                source="custom_slot",
                warnings=[],
                notes="Users can replace this placeholder with their own KPI.",
                editable=True,
            )
        )

        return KPIProposalBundle(
            proposals=sanitized,
            warnings=self._warnings + context["warnings"],
            context_snapshot={
                "dataset_source": self._dataset_source,
                "feature_columns": list(df.columns),
                "alias_lookup": self._alias_lookup,
                "has_feature_dictionary": context["has_feature_dictionary"],
                "has_correlations": context["has_correlations"],
                "has_business_conflicts": context["has_business_conflicts"],
            },
        )

    def validate(self, request: KPIValidationRequest) -> KPIValidationResponse:
        df = self._load_dataset()
        results: List[KPIValidationResult] = []
        warnings: List[str] = []

        for proposal in request.proposals:
            try:
                normalized = self._normalize_proposal(proposal, df, allow_missing_formula=False)
                if not normalized or normalized.formula is None:
                    results.append(
                        KPIValidationResult(
                            proposal=proposal,
                            status="fail",
                            computed_value=None,
                            reason="Missing or invalid formula definition.",
                        )
                    )
                    continue

                value = self._compute_formula(df, normalized.formula)
                if value is None or (isinstance(value, float) and (math.isnan(value) or math.isinf(value))):
                    results.append(
                        KPIValidationResult(
                            proposal=normalized,
                            status="fail",
                            computed_value=None,
                            reason="Formula evaluation returned no result.",
                        )
                    )
                    continue

                formatted = self._format_value(value, normalized.formula)
                status = "pass"
                reason = None
                if isinstance(normalized.formula, RatioFormula) and value == 0:
                    status = "warn"
                    reason = "Numerator is zero; review if KPI carries signal."

                results.append(
                    KPIValidationResult(
                        proposal=normalized,
                        status=status,
                        computed_value=value,
                        formatted_value=formatted,
                        reason=reason,
                    )
                )
            except Exception as exc:  # pragma: no cover - defensive
                warnings.append(f"Validation failed for {proposal.name}: {exc}")
                results.append(
                    KPIValidationResult(
                        proposal=proposal,
                        status="fail",
                        computed_value=None,
                        reason=str(exc),
                    )
                )

        return KPIValidationResponse(results=results, warnings=warnings)

    def adopt(self, request: KPIAdoptionRequest) -> KPIAdoptionResponse:
        entry = {
            "kpi_id": request.proposal.kpi_id,
            "name": request.adopted_name or request.proposal.name,
            "alias": request.proposal.alias,
            "description": request.proposal.description,
            "rationale": request.proposal.rationale,
            "financial_impact": request.proposal.financial_impact,
            "recommended_direction": request.proposal.recommended_direction,
            "formula": request.proposal.formula.model_dump() if request.proposal.formula else None,
            "notes": request.notes,
            "adopted_at": (request.adopted_at or pd.Timestamp.utcnow()).isoformat(),
            "source": request.proposal.source,
        }

        log_path = self.artifacts_dir / "kpi_decision_log.json"
        existing: List[Dict[str, Any]] = []
        if log_path.exists():
            try:
                existing = json.loads(log_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                self._warnings.append("Existing KPI decision log is corrupted; rebuilding.")
                existing = []

        existing.append(entry)
        log_path.write_text(json.dumps(existing, indent=2, ensure_ascii=False), encoding="utf-8")

        return KPIAdoptionResponse(message="KPI adoption recorded.", entry=entry)

    # ------------------------------------------------------------------ #
    # Context gathering
    # ------------------------------------------------------------------ #

    def _load_dataset(self) -> pd.DataFrame:
        candidates = ["merged_data.parquet", "train.parquet", "encoded_data.parquet", "typed_data.parquet"]
        for candidate in candidates:
            path = self.artifacts_dir / candidate
            if path.exists():
                try:
                    df = pd.read_parquet(path)
                    if not df.empty:
                        self._dataset_source = candidate
                        return df
                except Exception as exc:
                    self._warnings.append(f"Failed to load {candidate}: {exc}")
        raise FileNotFoundError("No dataset available. Run Phase 8 or later before requesting KPI proposals.")

    def _build_context(self, df: pd.DataFrame) -> Dict[str, Any]:
        feature_rows, alias_lookup, has_dictionary = self._load_feature_dictionary()
        self._alias_lookup = alias_lookup
        self._feature_meta = feature_rows

        feature_summary = [
            f"- {meta.alias} | {meta.name} | role={meta.role} | missing={meta.missing_pct:.1f}% | type={meta.semantic_type}"
            for meta in feature_rows.values()
        ]
        feature_summary = feature_summary[:40] if feature_summary else ["- No feature dictionary available."]

        existing_targets = self._load_phase1_targets()
        correlation_highlights, has_correlations = self._load_correlation_highlights()
        business_conflicts, has_conflicts = self._load_business_conflicts()

        quality_summary = self._build_quality_summary(df, feature_rows)

        return {
            "feature_summary": feature_summary,
            "existing_targets": existing_targets,
            "correlation_highlights": correlation_highlights,
            "business_conflicts": business_conflicts,
            "quality_summary": quality_summary,
            "warnings": self._warnings.copy(),
            "has_feature_dictionary": has_dictionary,
            "has_correlations": has_correlations,
            "has_business_conflicts": has_conflicts,
        }

    def _load_feature_dictionary(self) -> Tuple[Dict[str, FeatureMeta], Dict[str, str], bool]:
        dictionary_path = self.artifacts_dir / "feature_dictionary.json"
        alias_path = self.artifacts_dir / "feature_aliases.json"

        rows: Dict[str, FeatureMeta] = {}
        alias_lookup: Dict[str, str] = {}
        if not dictionary_path.exists():
            self._warnings.append("Feature dictionary missing; KPI suggestions may be less accurate.")
            return rows, alias_lookup, False

        try:
            records = json.loads(dictionary_path.read_text(encoding="utf-8"))
            for record in records[:200]:
                name = record.get("name")
                alias = record.get("clean_name") or name
                rows[name] = FeatureMeta(
                    name=name,
                    alias=alias,
                    role=record.get("recommended_role", "feature"),
                    missing_pct=float(record.get("missing_pct", 0.0)),
                    semantic_type=record.get("semantic_type", record.get("data_type", "")),
                )
                alias_lookup[alias] = name
                alias_lookup[alias.lower()] = name
                alias_lookup[name.lower()] = name
        except json.JSONDecodeError:
            self._warnings.append("Feature dictionary is corrupted; ignoring.")

        if alias_path.exists():
            try:
                alias_map = json.loads(alias_path.read_text(encoding="utf-8"))
                for original, alias in alias_map.items():
                    alias_lookup[alias] = original
                    alias_lookup[alias.lower()] = original
                    alias_lookup[original.lower()] = original
            except json.JSONDecodeError:
                self._warnings.append("Feature alias file is corrupted; continuing without it.")

        return rows, alias_lookup, bool(rows)

    def _load_phase1_targets(self) -> str:
        cfg_path = self.artifacts_dir / "phase1_config.json"
        if not cfg_path.exists():
            return ""

        try:
            config = json.loads(cfg_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            self._warnings.append("Phase 1 config is corrupted.")
            return ""

        goals = [goal.get("title", goal.get("name")) for goal in config.get("goals", [])]
        kpis = [kpi.get("name") for kpi in config.get("kpis", [])]
        parts = []
        if goals:
            parts.append("Goals: " + ", ".join(goals[:5]))
        if kpis:
            parts.append("Existing KPIs: " + ", ".join(kpis[:6]))
        return "\n".join(parts)

    def _load_correlation_highlights(self) -> Tuple[str, bool]:
        corr_path = self.artifacts_dir / "correlation_matrix.json"
        if not corr_path.exists():
            return "", False

        try:
            payload = json.loads(corr_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            self._warnings.append("Correlation matrix is corrupted.")
            return "", False

        pairs: List[str] = []
        for section in ("numeric_correlations", "categorical_associations"):
            for item in payload.get(section, []):
                corr = abs(item.get("correlation", 0))
                if corr >= 0.25:
                    pairs.append(
                        f"{item.get('feature1')} vs {item.get('feature2')} => {item.get('correlation'):.2f} ({item.get('method')})"
                    )
        pairs.sort(reverse=True)
        return "\n".join(pairs[:8]), bool(pairs)

    def _load_business_conflicts(self) -> Tuple[str, bool]:
        conflicts_path = self.artifacts_dir / "business_veto_report.json"
        if not conflicts_path.exists():
            return "", False

        try:
            payload = json.loads(conflicts_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            self._warnings.append("Business veto report is corrupted.")
            return "", False

        conflicts = []
        for conflict in payload.get("conflicts_detected", []):
            conflicts.append(
                f"{conflict.get('feature1')} vs {conflict.get('feature2')} -> expected {conflict.get('expected_relationship')} but observed {conflict.get('observed_correlation')}"
            )
        return "\n".join(conflicts[:5]), bool(conflicts)

    def _build_quality_summary(self, df: pd.DataFrame, meta: Dict[str, FeatureMeta]) -> str:
        if df.empty:
            return "Dataset is empty."

        summaries: List[str] = []
        missing_alerts = [
            f"{feat.name} ({feat.missing_pct:.1f}%)"
            for feat in meta.values()
            if feat.missing_pct >= 20 and feat.role not in {"identifier", "constant"}
        ]
        if missing_alerts:
            summaries.append("High missing: " + ", ".join(missing_alerts[:8]))

        numeric_cols = df.select_dtypes(include=["number"]).columns
        if numeric_cols.any():
            summaries.append(f"Numeric columns: {len(numeric_cols)}.")

        return "\n".join(summaries)

    # ------------------------------------------------------------------ #
    # Parsing & normalization
    # ------------------------------------------------------------------ #

    def _parse_llm_response(self, text: str) -> List[KPIProposal]:
        if not text:
            raise ValueError("LLM returned empty response.")

        text = text.strip()
        json_text = text
        if not text.startswith("{") and not text.startswith("["):
            match = re.search(r"(\{.*\}|\[.*\])", text, flags=re.DOTALL)
            if match:
                json_text = match.group(1)

        try:
            payload = json.loads(json_text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Failed to parse LLM JSON: {exc}") from exc

        if isinstance(payload, dict):
            proposals_raw = payload.get("proposals") or payload.get("kpis") or []
        elif isinstance(payload, list):
            proposals_raw = payload
        else:
            raise ValueError("Unexpected LLM response structure.")

        proposals: List[KPIProposal] = []
        for item in proposals_raw:
            try:
                proposals.append(KPIProposal.model_validate(item))
            except Exception as exc:
                self._warnings.append(f"Rejected malformed KPI entry: {exc}")
        return proposals

    def _normalize_proposal(
        self,
        proposal: KPIProposal,
        df: pd.DataFrame,
        allow_missing_formula: bool = True,
    ) -> Optional[KPIProposal]:
        required = []
        for col in proposal.required_columns:
            resolved = self._resolve_column(col, df)
            if resolved:
                required.append(resolved)
            else:
                self._warnings.append(f"{proposal.name}: column '{col}' not found; excluding KPI.")
                return None

        formula = proposal.formula
        if formula is None and not allow_missing_formula:
            return None

        if isinstance(formula, RatioFormula):
            if not self._resolve_column(formula.numerator.column, df):
                self._warnings.append(f"{proposal.name}: numerator column not found.")
                return None
            if not self._resolve_column(formula.denominator.column, df):
                self._warnings.append(f"{proposal.name}: denominator column not found.")
                return None
        elif isinstance(formula, SingleValueFormula):
            if not self._resolve_column(formula.aggregation.column, df):
                self._warnings.append(f"{proposal.name}: aggregation column not found.")
                return None

        alias = proposal.alias or self._slugify(proposal.name)
        return proposal.model_copy(update={"alias": alias, "required_columns": required})

    def _resolve_column(self, name: Optional[str], df: pd.DataFrame) -> Optional[str]:
        if not name:
            return None
        if name in df.columns:
            return name
        if name.lower() in self._alias_lookup:
            mapped = self._alias_lookup[name.lower()]
            if mapped in df.columns:
                return mapped
        for column in df.columns:
            if column.lower() == name.lower():
                return column
        return None

    def _slugify(self, name: str) -> str:
        return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_") or "kpi_candidate"

    # ------------------------------------------------------------------ #
    # Formula evaluation helpers
    # ------------------------------------------------------------------ #

    def _compute_formula(self, df: pd.DataFrame, formula: Any) -> Optional[float]:
        if isinstance(formula, RatioFormula):
            numerator = self._evaluate_aggregation(df, formula.numerator)
            denominator = self._evaluate_aggregation(df, formula.denominator)
            if denominator in (0, None):
                if formula.guard_rail == "clamp_zero":
                    return 0.0
                return None
            value = numerator / denominator
            return value * formula.multiplier
        if isinstance(formula, SingleValueFormula):
            value = self._evaluate_aggregation(df, formula.aggregation)
            if value is None:
                return None
            return value * formula.multiplier
        raise ValueError("Unsupported formula type.")

    def _evaluate_aggregation(self, df: pd.DataFrame, spec: AggregationSpec) -> Optional[float]:
        column = spec.column
        if column:
            column = self._resolve_column(column, df)
            if not column:
                raise ValueError(f"Column '{spec.column}' not found for aggregation.")

        series = df[column] if column else pd.Series([1.0] * len(df), index=df.index)
        mask = self._build_filter_mask(df, column, spec.filter)
        filtered = series[mask] if mask is not None else series

        if spec.aggregation == "count":
            return float(filtered.notna().sum()) if column else float(mask.sum()) if mask is not None else float(len(df))
        if spec.aggregation == "sum":
            return float(pd.to_numeric(filtered, errors="coerce").sum())
        if spec.aggregation == "mean":
            return float(pd.to_numeric(filtered, errors="coerce").mean())
        if spec.aggregation == "median":
            return float(pd.to_numeric(filtered, errors="coerce").median())
        if spec.aggregation == "nunique":
            return float(filtered.nunique(dropna=True))
        raise ValueError(f"Unsupported aggregation '{spec.aggregation}'.")

    def _build_filter_mask(
        self, df: pd.DataFrame, column: Optional[str], filter_spec: Optional[Any]
    ) -> Optional[pd.Series]:
        if filter_spec is None:
            return None
        target_column = column or getattr(filter_spec, "column", None)
        if not target_column:
            return None
        if target_column:
            target_column = self._resolve_column(target_column, df)
        if not target_column:
            return None
        series = df[target_column]

        op = getattr(filter_spec, "operator", None)
        if op is None:
            return None

        value = getattr(filter_spec, "value", None)
        values = getattr(filter_spec, "values", None)
        lower = getattr(filter_spec, "lower", None)
        upper = getattr(filter_spec, "upper", None)
        range_values = getattr(filter_spec, "range", None)

        if op == "equals":
            return series == value
        if op == "not_equals":
            return series != value
        if op == "in":
            return series.isin(values or [])
        if op == "not_in":
            return ~series.isin(values or [])
        if op == "gt":
            return pd.to_numeric(series, errors="coerce") > value
        if op == "gte":
            return pd.to_numeric(series, errors="coerce") >= value
        if op == "lt":
            return pd.to_numeric(series, errors="coerce") < value
        if op == "lte":
            return pd.to_numeric(series, errors="coerce") <= value
        if op == "between":
            if values and len(values) == 2:
                lower, upper = values
            elif range_values and len(range_values) == 2:
                lower, upper = range_values
            return (pd.to_numeric(series, errors="coerce") >= lower) & (
                pd.to_numeric(series, errors="coerce") <= upper
            )
        if op == "exists":
            return series.notna()
        if op == "not_exists":
            return series.isna()
        return None

    def _format_value(self, value: float, formula: Any) -> str:
        if isinstance(formula, RatioFormula) or (hasattr(formula, "format") and getattr(formula, "format") == "percentage"):
            return f"{value:.2f}%"
        if hasattr(formula, "format") and getattr(formula, "format") == "currency":
            return f"${value:,.2f}"
        return f"{value:,.2f}"


def generate_kpi_proposals(domain: str, language: str = "en", count: int = 3) -> KPIProposalBundle:
    engine = KPIProposalEngine(domain=domain, language=language)
    return engine.generate(count=count)


def validate_kpi_proposals(domain: str, request: KPIValidationRequest) -> KPIValidationResponse:
    engine = KPIProposalEngine(domain=domain)
    return engine.validate(request)


def adopt_kpi(domain: str, request: KPIAdoptionRequest) -> KPIAdoptionResponse:
    engine = KPIProposalEngine(domain=domain)
    return engine.adopt(request)
