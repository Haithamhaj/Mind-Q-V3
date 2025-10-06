import json
from pathlib import Path
from datetime import datetime

from app.services.llm.analyzers import (
    FeatureAnalyzer,
    ModelComparator,
    ConfusionMatrixAnalyzer,
    RecommendationGenerator,
    ExecutiveSummaryGenerator,
)
from app.models.phase14_5_result import Phase14_5Result, DecisionLogEntry
from app.config import settings


class LLMAnalysisService:
    def __init__(self, artifacts_dir: Path | None = None):
        self.artifacts_dir = artifacts_dir or settings.artifacts_dir

    def run(self) -> Phase14_5Result:
        evaluation_report = self._load_json("evaluation_report.json")
        feature_importance = self._load_json("feature_importance.json")
        selected_features_info = self._load_json("selected_features.json")
        problem_spec = self._load_json("problem_spec.json")

        best_model = evaluation_report.get("best_model", {})
        val_metrics = best_model.get("val_metrics", {})

        feature_insights = FeatureAnalyzer.analyze(
            feature_importance=feature_importance,
            problem_spec=problem_spec,
            selected_features_info=selected_features_info,
            validation_metrics=val_metrics,
        )

        comparison_result = ModelComparator.compare(
            evaluation_report=evaluation_report, problem_spec=problem_spec
        )
        model_insights = comparison_result["model_insights"]

        cm_insights = {}
        for model_name, result_data in evaluation_report.get("validation_results", {}).items():
            cm = result_data.get("confusion_matrix", [[0, 0], [0, 0]])
            metrics = {
                "accuracy": result_data.get("accuracy", 0),
                "precision": result_data.get("precision", 0),
                "recall": result_data.get("recall", 0),
                "f1": result_data.get("f1", 0),
            }
            cm_insights[model_name] = ConfusionMatrixAnalyzer.analyze(
                model_name=model_name,
                confusion_matrix=cm,
                metrics=metrics,
                problem_spec=problem_spec,
            )

        recommendations = RecommendationGenerator.generate(
            evaluation_report=evaluation_report,
            feature_insights=feature_insights,
            problem_spec=problem_spec,
        )
        recommendations_sorted = sorted(recommendations, key=lambda x: x.priority)

        summary_result = ExecutiveSummaryGenerator.generate(
            evaluation_report=evaluation_report,
            feature_insights=feature_insights,
            recommendations=recommendations_sorted,
            problem_spec=problem_spec,
        )

        decision_log = [
            DecisionLogEntry(
                timestamp=datetime.utcnow().isoformat(),
                decision_type="analysis_completed",
                description="Phase 14.5 LLM Analysis completed",
                made_by="system",
                rationale="Automatic analysis of Phase 14 results",
            )
        ]

        result = Phase14_5Result(
            timestamp=datetime.utcnow().isoformat(),
            llm_provider=settings.llm_provider,
            feature_insights=feature_insights,
            model_insights=model_insights,
            confusion_matrix_insights=cm_insights,
            recommendations=recommendations_sorted,
            decision_log=decision_log,
            executive_summary=summary_result["executive_summary"],
            key_findings=summary_result["key_findings"],
            next_steps=summary_result["next_steps"],
        )

        self._save_result(result)
        return result

    def _load_json(self, filename: str) -> dict:
        path = self.artifacts_dir / filename
        if not path.exists():
            raise FileNotFoundError(f"Required file not found: {filename}")
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _save_result(self, result: Phase14_5Result) -> None:
        insights_path = self.artifacts_dir / "llm_insights_report.json"
        with open(insights_path, "w", encoding="utf-8") as f:
            json.dump(result.dict(), f, indent=2, ensure_ascii=False)

        recs_path = self.artifacts_dir / "recommendations.json"
        with open(recs_path, "w", encoding="utf-8") as f:
            json.dump([rec.dict() for rec in result.recommendations], f, indent=2, ensure_ascii=False)

        log_path = self.artifacts_dir / "decision_log.json"
        with open(log_path, "w", encoding="utf-8") as f:
            json.dump([entry.dict() for entry in result.decision_log], f, indent=2, ensure_ascii=False)


