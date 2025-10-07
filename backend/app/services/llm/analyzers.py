import json
from typing import Dict, List, Optional

from app.services.llm.client import LLMConfigurationError, get_llm_client
from app.services.llm.prompts import (
    SYSTEM_PROMPT,
    FEATURE_IMPORTANCE_PROMPT,
    MODEL_COMPARISON_PROMPT,
    CONFUSION_MATRIX_PROMPT,
    RECOMMENDATIONS_PROMPT,
    EXECUTIVE_SUMMARY_PROMPT,
    SUGGEST_TARGET_PROMPT,
)
from app.models.phase14_5_result import (
    FeatureInsight,
    ModelInsight,
    ConfusionMatrixInsight,
    Recommendation,
)


def _call_llm(prompt: str, *, max_tokens: int = 4000, system: Optional[str] = SYSTEM_PROMPT) -> str:
    try:
        client = get_llm_client()
        return client.call(prompt, max_tokens=max_tokens, system=system)
    except LLMConfigurationError as exc:
        raise Exception(f"LLM configuration error: {exc}") from exc


def _describe_feature(name: str, dictionary: Optional[dict]) -> str:
    if not dictionary:
        return name
    meta = dictionary.get(name)
    if not meta:
        return name
    alias = meta.get("clean_name") or name
    description = meta.get("description") or ""
    role = meta.get("recommended_role")
    parts = [alias]
    if alias != name:
        parts.append(f"(original: {name})")
    if description:
        parts.append(description)
    if role and role not in {"feature"}:
        parts.append(f"role: {role}")
    return " - ".join(parts)


class FeatureAnalyzer:
    @staticmethod
    def analyze(
        feature_importance: Dict[str, float],
        problem_spec: dict,
        selected_features_info: dict,
        validation_metrics: dict,
        feature_dictionary: Optional[dict] = None,
    ) -> List[FeatureInsight]:
        sorted_features = sorted(
            feature_importance.items(), key=lambda x: x[1], reverse=True
        )[:10]
        feature_table = "\n".join(
            [
                f"{i+1}. {_describe_feature(feat, feature_dictionary)}: {imp:.4f}"
                for i, (feat, imp) in enumerate(sorted_features)
            ]
        )
        if feature_dictionary:
            metadata_lines = []
            for feat, _ in sorted_features:
                meta = feature_dictionary.get(feat)
                if not meta:
                    continue
                alias = meta.get("clean_name") or feat
                role = meta.get("recommended_role")
                description = meta.get("description")
                pieces = [alias]
                if role and role not in {"feature"}:
                    pieces.append(f"role={role}")
                if description:
                    pieces.append(description)
                metadata_lines.append(f"- {feat}: {' | '.join(pieces)}")
            if metadata_lines:
                feature_table = f"{feature_table}\n\n**Feature Metadata:**\n" + "\n".join(metadata_lines)

        prompt = FEATURE_IMPORTANCE_PROMPT.format(
            problem_type=problem_spec.get("problem_type", "classification"),
            target_column=problem_spec.get("target_column", "unknown"),
            domain=problem_spec.get("domain", "general"),
            feature_importance_table=feature_table,
            n_features_original=selected_features_info.get("n_features_original", "unknown"),
            n_features_selected=selected_features_info.get(
                "n_features_selected", len(feature_importance)
            ),
            recall=validation_metrics.get("recall", 0),
            precision=validation_metrics.get("precision", 0),
            f1=validation_metrics.get("f1", 0),
        )

        response = _call_llm(prompt, system=SYSTEM_PROMPT)
        try:
            response = response.replace("```json", "").replace("```", "").strip()
            insights_data = json.loads(response)
            return [FeatureInsight(**insight) for insight in insights_data]
        except json.JSONDecodeError as e:
            raise Exception(
                f"Failed to parse LLM response as JSON: {str(e)}\nResponse: {response}"
            )


class ModelComparator:
    @staticmethod
    def compare(evaluation_report: dict, problem_spec: dict) -> Dict:
        models_data = evaluation_report.get("validation_results", {})
        comparison_lines = []
        for model_name, metrics in models_data.items():
            line = (
                f"- {model_name}: "
                f"Recall={metrics['recall']:.3f}, "
                f"Precision={metrics['precision']:.3f}, "
                f"F1={metrics['f1']:.3f}, "
                f"Accuracy={metrics['accuracy']:.3f}"
            )
            comparison_lines.append(line)

        comparison_table = "\n".join(comparison_lines)

        prompt = MODEL_COMPARISON_PROMPT.format(
            problem_type=problem_spec.get("problem_type", "classification"),
            target_column=problem_spec.get("target_column", "unknown"),
            domain=problem_spec.get("domain", "general"),
            primary_metric=problem_spec.get("primary_metric", "recall"),
            models_comparison_table=comparison_table,
            fn_cost_description=problem_spec.get(
                "fn_cost", "Missing a positive case"
            ),
            fp_cost_description=problem_spec.get("fp_cost", "False alarm"),
        )

        response = _call_llm(prompt, system=SYSTEM_PROMPT)
        try:
            response = response.replace("```json", "").replace("```", "").strip()
            result = json.loads(response)
            model_insights = [ModelInsight(**ins) for ins in result["model_insights"]]
            return {
                "model_insights": model_insights,
                "recommended_model": result.get("recommended_model"),
                "reasoning": result.get("reasoning"),
            }
        except json.JSONDecodeError as e:
            raise Exception(f"Failed to parse LLM response: {str(e)}")


class ConfusionMatrixAnalyzer:
    @staticmethod
    def analyze(
        model_name: str, confusion_matrix: List[List[int]], metrics: dict, problem_spec: dict
    ) -> ConfusionMatrixInsight:
        tn, fp, fn, tp = (
            confusion_matrix[0][0],
            confusion_matrix[0][1],
            confusion_matrix[1][0],
            confusion_matrix[1][1],
        )

        prompt = CONFUSION_MATRIX_PROMPT.format(
            model_name=model_name,
            domain=problem_spec.get("domain", "general"),
            target_column=problem_spec.get("target_column", "unknown"),
            tn=tn,
            fp=fp,
            fn=fn,
            tp=tp,
            accuracy=metrics.get("accuracy", 0),
            precision=metrics.get("precision", 0),
            recall=metrics.get("recall", 0),
            f1=metrics.get("f1", 0),
        )

        response = _call_llm(prompt, system=SYSTEM_PROMPT)
        try:
            response = response.replace("```json", "").replace("```", "").strip()
            data = json.loads(response)
            defaults = {
                "true_negatives": tn,
                "false_positives": fp,
                "false_negatives": fn,
                "true_positives": tp,
            }

            def _coerce_count(raw_value, fallback):
                """Ensure confusion-matrix counts are numeric even if LLM wraps them in dicts."""
                if isinstance(raw_value, dict):
                    for key in ("count", "value", "total"):
                        if key in raw_value:
                            return _coerce_count(raw_value[key], fallback)
                try:
                    return int(raw_value)
                except (TypeError, ValueError):
                    return fallback

            def _coerce_text(raw_value, fallback: str) -> str:
                if isinstance(raw_value, dict):
                    for key in ("english", "en", "text", "value", "summary"):
                        if key in raw_value and raw_value[key]:
                            return str(raw_value[key])
                    try:
                        return json.dumps(raw_value, ensure_ascii=False)
                    except TypeError:
                        return fallback
                if raw_value is None:
                    return fallback
                return str(raw_value)

            for key, fallback_value in defaults.items():
                if key in data:
                    data[key] = _coerce_count(data[key], fallback_value)
                else:
                    data[key] = fallback_value

            default_fp = "LLM output did not provide a false-positive cost explanation."
            default_fn = "LLM output did not provide a false-negative cost explanation."
            data["fp_cost_explanation"] = _coerce_text(data.get("fp_cost_explanation"), default_fp)
            data["fn_cost_explanation"] = _coerce_text(data.get("fn_cost_explanation"), default_fn)
            data["which_is_worse"] = _coerce_text(data.get("which_is_worse"), "undetermined")
            data.pop("recommendations", None)
            return ConfusionMatrixInsight(**data)
        except json.JSONDecodeError as e:
            raise Exception(f"Failed to parse LLM response: {str(e)}")


class RecommendationGenerator:
    @staticmethod
    def generate(
        evaluation_report: dict,
        feature_insights: List[FeatureInsight],
        problem_spec: dict,
        feature_dictionary: Optional[dict] = None,
    ) -> List[Recommendation]:
        best_model = evaluation_report.get("best_model", {})
        best_model_name = best_model.get("name", "unknown")
        val_metrics = best_model.get("val_metrics", {})

        primary_metric = problem_spec.get("primary_metric", "recall")
        primary_value = val_metrics.get(primary_metric, 0)

        feature_summary = "\n".join(
            [
                f"- {_describe_feature(fi.feature_name, feature_dictionary)} (importance: {fi.importance_score:.3f}): {fi.recommendation}"
                for fi in feature_insights[:5]
            ]
        )

        prompt = RECOMMENDATIONS_PROMPT.format(
            best_model=best_model_name,
            primary_metric=primary_metric,
            primary_metric_value=primary_value,
            weaknesses="Low precision" if val_metrics.get("precision", 1) < 0.7 else "Minor issues",
            feature_summary=feature_summary,
        )

        response = _call_llm(prompt, system=SYSTEM_PROMPT)
        try:
            response = response.replace("```json", "").replace("```", "").strip()
            recommendations_data = json.loads(response)
            return [Recommendation(**rec) for rec in recommendations_data]
        except json.JSONDecodeError as e:
            raise Exception(f"Failed to parse LLM response: {str(e)}")


class ExecutiveSummaryGenerator:
    @staticmethod
    def generate(
        evaluation_report: dict,
        feature_insights: List[FeatureInsight],
        recommendations: List[Recommendation],
        problem_spec: dict,
        feature_dictionary: Optional[dict] = None,
    ) -> Dict[str, object]:
        best_model = evaluation_report.get("best_model", {})
        models_trained = evaluation_report.get("models_evaluated", [])

        key_metrics_str = ", ".join(
            [f"{k}={v:.3f}" for k, v in best_model.get("val_metrics", {}).items()]
        )
        top_features_str = ", ".join(
            [_describe_feature(fi.feature_name, feature_dictionary) for fi in feature_insights[:5]]
        )

        prompt = EXECUTIVE_SUMMARY_PROMPT.format(
            problem_type=problem_spec.get("problem_type", "classification"),
            target_column=problem_spec.get("target_column", "unknown"),
            domain=problem_spec.get("domain", "general"),
            models_trained=", ".join(models_trained),
            best_model=best_model.get("name", "unknown"),
            key_metrics=key_metrics_str,
            top_features=top_features_str,
        )

        response = _call_llm(prompt, system=SYSTEM_PROMPT, max_tokens=2000)

        parts = response.split("#")
        executive_summary = ""
        key_findings: List[str] = []
        next_steps: List[str] = []
        for part in parts:
            if "ملخص تنفيذي" in part or "Executive Summary" in part:
                executive_summary += part.strip() + "\n\n"
            elif "Key Findings" in part:
                findings_text = part.split("Key Findings")[1].strip()
                key_findings = [
                    line.strip("- ").strip()
                    for line in findings_text.split("\n")
                    if line.strip().startswith("-")
                ]
            elif "Next Steps" in part:
                steps_text = part.split("Next Steps")[1].strip()
                next_steps = [
                    line.strip("- ").strip()
                    for line in steps_text.split("\n")
                    if line.strip().startswith("-")
                ]

        return {
            "executive_summary": executive_summary.strip(),
            "key_findings": key_findings,
            "next_steps": next_steps,
        }


class TargetSuggester:
    @staticmethod
    def suggest(
        domain: str,
        df,
        columns_meta: list[dict],
        feature_dictionary: Optional[dict] = None,
    ) -> dict:
        """Use LLM (Gemini/OpenAI/Anthropic) to suggest a target column.
        df: pandas.DataFrame (small preview will be used)
        columns_meta: list of {name, dtype, nunique}
        Returns dict with suggested_target and candidates.
        """
        try:
            preview = df.head(10).to_json(orient="records")
        except Exception:
            preview = "[]"
        columns_summary = "\n".join(
            [f"- {c['name']} | {c['dtype']} | nunique={c['nunique']}" for c in columns_meta]
        )
        metadata_summary = "None"
        if feature_dictionary:
            lines = []
            for col in columns_meta:
                meta = feature_dictionary.get(col["name"])
                if not meta:
                    continue
                alias = meta.get("clean_name") or col["name"]
                role = meta.get("recommended_role")
                description = meta.get("description")
                pieces = [alias]
                if role and role not in {"feature"}:
                    pieces.append(f"role={role}")
                if description:
                    pieces.append(description)
                missing = meta.get("missing_pct")
                if missing is not None:
                    pieces.append(f"missing={missing:.1f}%")
                lines.append(f"- {col['name']}: {' | '.join(pieces)}")
            if lines:
                metadata_summary = "\n".join(lines)
        prompt = SUGGEST_TARGET_PROMPT.format(
            domain=domain,
            columns_summary=columns_summary,
            feature_metadata=metadata_summary,
            data_preview=preview,
        )
        resp = _call_llm(prompt, system=SYSTEM_PROMPT)
        try:
            resp = resp.replace("```json", "").replace("```", "").strip()
            return json.loads(resp)
        except Exception:
            return {"suggested_target": None, "candidates": []}


