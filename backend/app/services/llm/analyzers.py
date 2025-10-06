import json
from typing import Dict, List

from app.services.llm.client import llm_client
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


class FeatureAnalyzer:
    @staticmethod
    def analyze(
        feature_importance: Dict[str, float],
        problem_spec: dict,
        selected_features_info: dict,
        validation_metrics: dict,
    ) -> List[FeatureInsight]:
        sorted_features = sorted(
            feature_importance.items(), key=lambda x: x[1], reverse=True
        )[:10]
        feature_table = "\n".join(
            [f"{i+1}. {feat}: {imp:.4f}" for i, (feat, imp) in enumerate(sorted_features)]
        )

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

        response = llm_client.call(prompt, system=SYSTEM_PROMPT)
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

        response = llm_client.call(prompt, system=SYSTEM_PROMPT)
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

        response = llm_client.call(prompt, system=SYSTEM_PROMPT)
        try:
            response = response.replace("```json", "").replace("```", "").strip()
            data = json.loads(response)
            return ConfusionMatrixInsight(**data)
        except json.JSONDecodeError as e:
            raise Exception(f"Failed to parse LLM response: {str(e)}")


class RecommendationGenerator:
    @staticmethod
    def generate(
        evaluation_report: dict, feature_insights: List[FeatureInsight], problem_spec: dict
    ) -> List[Recommendation]:
        best_model = evaluation_report.get("best_model", {})
        best_model_name = best_model.get("name", "unknown")
        val_metrics = best_model.get("val_metrics", {})

        primary_metric = problem_spec.get("primary_metric", "recall")
        primary_value = val_metrics.get(primary_metric, 0)

        feature_summary = "\n".join(
            [
                f"- {fi.feature_name} (importance: {fi.importance_score:.3f}): {fi.recommendation}"
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

        response = llm_client.call(prompt, system=SYSTEM_PROMPT)
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
    ) -> Dict[str, object]:
        best_model = evaluation_report.get("best_model", {})
        models_trained = evaluation_report.get("models_evaluated", [])

        key_metrics_str = ", ".join(
            [f"{k}={v:.3f}" for k, v in best_model.get("val_metrics", {}).items()]
        )
        top_features_str = ", ".join([fi.feature_name for fi in feature_insights[:5]])

        prompt = EXECUTIVE_SUMMARY_PROMPT.format(
            problem_type=problem_spec.get("problem_type", "classification"),
            target_column=problem_spec.get("target_column", "unknown"),
            domain=problem_spec.get("domain", "general"),
            models_trained=", ".join(models_trained),
            best_model=best_model.get("name", "unknown"),
            key_metrics=key_metrics_str,
            top_features=top_features_str,
        )

        response = llm_client.call(prompt, system=SYSTEM_PROMPT, max_tokens=2000)

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
    def suggest(domain: str, df, columns_meta: list[dict]) -> dict:
        """Use LLM (Gemini/OpenAI/Anthropic) to suggest a target column.
        df: pandas.DataFrame (small preview will be used)
        columns_meta: list of {name, dtype, nunique}
        Returns dict with suggested_target and candidates.
        """
        try:
            preview = df.head(10).to_json(orient="records")
        except Exception:
            preview = "[]"
        columns_summary = "\n".join([f"- {c['name']} | {c['dtype']} | nunique={c['nunique']}" for c in columns_meta])
        prompt = SUGGEST_TARGET_PROMPT.format(
            domain=domain,
            columns_summary=columns_summary,
            data_preview=preview,
        )
        resp = llm_client.call(prompt, system=SYSTEM_PROMPT)
        try:
            resp = resp.replace("```json", "").replace("```", "").strip()
            return json.loads(resp)
        except Exception:
            return {"suggested_target": None, "candidates": []}


