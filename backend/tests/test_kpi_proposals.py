import json
from pathlib import Path

import pandas as pd

from app.models.kpi import (
    KPIValidationRequest,
    KPIProposal,
)
from app.services.bi.kpi_engine import KPIProposalEngine


def _write_artifacts(tmp_path: Path, df: pd.DataFrame) -> None:
    artifacts = tmp_path
    artifacts.mkdir(parents=True, exist_ok=True)
    df.to_parquet(artifacts / "merged_data.parquet")

    dictionary = [
        {
            "name": "status",
            "clean_name": "status",
            "recommended_role": "target_candidate",
            "missing_pct": 0.0,
            "semantic_type": "categorical",
        },
        {
            "name": "sla_flag",
            "clean_name": "sla_flag",
            "recommended_role": "feature",
            "missing_pct": 0.0,
            "semantic_type": "binary",
        },
        {
            "name": "transit_time",
            "clean_name": "transit_time",
            "recommended_role": "feature",
            "missing_pct": 0.0,
            "semantic_type": "numeric",
        },
    ]
    (artifacts / "feature_dictionary.json").write_text(json.dumps(dictionary), encoding="utf-8")

    phase1_config = {
        "goals": [{"title": "Improve SLA compliance"}],
        "kpis": [{"name": "SLA attainment"}],
    }
    (artifacts / "phase1_config.json").write_text(json.dumps(phase1_config), encoding="utf-8")

    correlations = {
        "numeric_correlations": [
            {
                "feature1": "sla_flag",
                "feature2": "transit_time",
                "correlation": -0.45,
                "method": "pearson",
            }
        ]
    }
    (artifacts / "correlation_matrix.json").write_text(json.dumps(correlations), encoding="utf-8")


def test_kpi_proposal_engine_generates_bundle(tmp_path):
    df = pd.DataFrame(
        {
            "status": ["Delivered", "Delivered", "Returned", "Delivered"],
            "sla_flag": [1, 1, 0, 1],
            "transit_time": [24, 26, 40, 30],
        }
    )
    _write_artifacts(tmp_path, df)

    llm_response = json.dumps(
        {
            "proposals": [
                {
                    "kpi_id": "logistics_on_time_rate",
                    "name": "On-Time Delivery Rate",
                    "alias": "on_time_delivery_rate",
                    "metric_type": "quality",
                    "description": "Share of shipments delivered on time.",
                    "rationale": "Directly tracks SLA adherence and customer expectations.",
                    "financial_impact": "Improving this KPI reduces penalty fees.",
                    "confidence": 0.9,
                    "recommended_direction": "higher_is_better",
                    "expected_outcome": "SLA improvements reduce churn and penalties.",
                    "monitoring_guidance": "Check weekly and alert if SLA dips 2% below baseline.",
                    "why_selected": "Direct relationship between delivered status and SLA compliance.",
                    "tradeoffs": "Requires accurate status timestamps.",
                    "formula": {
                        "type": "ratio",
                        "numerator": {
                            "aggregation": "count",
                            "column": "status",
                            "filter": {
                                "operator": "equals",
                                "column": "status",
                                "value": "Delivered",
                            },
                        },
                        "denominator": {
                            "aggregation": "count",
                            "column": "status",
                        },
                        "multiplier": 100,
                        "format": "percentage",
                    },
                    "required_columns": ["status"],
                    "supporting_evidence": ["status vs sla_flag corr=0.61"],
                }
            ],
            "why_options_limited": "Identifier-like fields and high-missing columns were excluded."
        }
    )

    engine = KPIProposalEngine(
        domain="logistics",
        artifacts_dir=tmp_path,
        llm_callable=lambda prompt: llm_response,
    )

    bundle = engine.generate(count=1)
    assert bundle.proposals, "Expected proposals to be returned"
    assert bundle.proposals[0].name == "On-Time Delivery Rate"
    assert bundle.proposals[0].source == "llm"
    assert bundle.proposals[-1].source == "custom_slot"
    assert bundle.proposals[0].expected_outcome is not None
    assert bundle.explanation


def test_kpi_proposal_validation(tmp_path):
    df = pd.DataFrame(
        {
            "status": ["Delivered", "Delivered", "Returned", "Delivered"],
            "sla_flag": [1, 1, 0, 1],
            "transit_time": [24, 26, 40, 30],
        }
    )
    _write_artifacts(tmp_path, df)

    proposal = KPIProposal.model_validate(
        {
            "kpi_id": "logistics_on_time_rate",
            "name": "On-Time Delivery Rate",
            "alias": "on_time_delivery_rate",
            "metric_type": "quality",
            "description": "Share of shipments delivered on time.",
            "rationale": "Directly tracks SLA adherence.",
            "recommended_direction": "higher_is_better",
            "formula": {
                "type": "ratio",
                "numerator": {
                    "aggregation": "count",
                    "column": "status",
                    "filter": {
                        "operator": "equals",
                        "column": "status",
                        "value": "Delivered",
                    },
                },
                "denominator": {
                    "aggregation": "count",
                    "column": "status",
                },
                "multiplier": 100,
                "format": "percentage",
            },
            "required_columns": ["status"],
            "supporting_evidence": [],
        }
    )

    engine = KPIProposalEngine(
        domain="logistics",
        artifacts_dir=tmp_path,
        llm_callable=lambda prompt: "",
    )
    response = engine.validate(KPIValidationRequest(proposals=[proposal]))
    assert response.results[0].status == "pass"
    assert response.results[0].computed_value == 75.0
    assert response.results[0].formatted_value == "75.00%"
