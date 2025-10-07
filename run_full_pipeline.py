"""
Utility script to execute the entire Mind-Q V3 backend pipeline sequentially
using FastAPI's TestClient. This mimics the frontend `FullEDAPipeline` flow
without having to spin up uvicorn.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parent
BACKEND_DIR = ROOT / "backend"

import sys

if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.main import app  # type: ignore


PHASES: Iterable[Tuple[str, str]] = (
    ("phase0", "/api/v1/phases/quality-control"),
    ("phase1", "/api/v1/phases/phase1-goal-kpis-clean"),
    ("phase2", "/api/v1/phases/phase2-ingestion"),
    ("phase3", "/api/v1/phases/phase3-schema"),
    ("phase4", "/api/v1/phases/phase4-profiling-clean"),
    ("phase5", "/api/v1/phases/phase5-missing-data"),
    ("phase6", "/api/v1/phases/phase6-standardization"),
    ("phase7", "/api/v1/phases/phase7-features"),
    ("phase7.5", "/api/v1/phases/phase7-5-encoding"),
    ("phase8", "/api/v1/phases/phase8-merging"),
    ("phase9", "/api/v1/phases/phase9-correlations"),
    ("phase9.5", "/api/v1/phases/phase9-5-business-validation"),
    ("phase10", "/api/v1/phases/phase10-packaging"),
    ("phase10.5", "/api/v1/phases/phase10-5-split"),
    ("phase11", "/api/v1/phases/phase11-advanced"),
    ("phase11.5", "/api/v1/phases/phase11-5-selection"),
    ("phase12", "/api/v1/phases/phase12-text-features"),
    ("phase13", "/api/v1/phases/phase13-monitoring"),
    ("phase14", "/api/v1/phases/phase14-train-models"),
    ("phase14.5", "/api/v1/llm-analysis/run-analysis"),
)


def _safe_text(value: Any) -> str:
    """Return an ASCII-safe representation suitable for Windows consoles."""
    if isinstance(value, str):
        text = value
    else:
        try:
            text = json.dumps(value, ensure_ascii=False, indent=2)
        except TypeError:
            text = str(value)
    return text.encode("ascii", "replace").decode("ascii")


def main() -> None:
    data_path = Path("pipeline_input.csv")
    if not data_path.exists():
        raise SystemExit("Missing pipeline_input.csv. Generate it before running the pipeline.")

    domain = "logistics"
    target_column = "STATUS_Return"
    results: Dict[str, Any] = {}

    with TestClient(app) as client:
        print("Starting full pipeline execution...")

        for phase_id, endpoint in PHASES:
            print(f"\n=== Running {phase_id} -> {endpoint} ===")

            try:
                response = _call_phase(
                    client=client,
                    phase_id=phase_id,
                    endpoint=endpoint,
                    data_path=data_path,
                    domain=domain,
                    target_column=target_column,
                )
            except Exception as exc:  # pragma: no cover - defensive logging
                print(_safe_text(f"[{phase_id}] FAILED: {exc}"))
                results[phase_id] = {"status": "error", "error": str(exc)}
                continue

            if response.status_code >= 400:
                detail = response.json()
                print(_safe_text(f"[{phase_id}] FAILED ({response.status_code}): {detail}"))
                results[phase_id] = {"status": "error", "error": detail}
            else:
                payload = response.json()
                results[phase_id] = {"status": "success", "data": payload}
                summary = json.dumps(payload, ensure_ascii=False, indent=2)[:800]
                print(_safe_text(f"[{phase_id}] success. Preview:\n{summary}"))

        print("\nPipeline execution complete.")
        successes = sum(1 for r in results.values() if r["status"] == "success")
        failures = sum(1 for r in results.values() if r["status"] != "success")
        print(f"Successful phases: {successes}, failed phases: {failures}")


def _call_phase(
    client: TestClient,
    phase_id: str,
    endpoint: str,
    data_path: Path,
    domain: str,
    target_column: str,
):
    if phase_id == "phase0":
        with data_path.open("rb") as f:
            files = {"file": (data_path.name, f, "text/csv")}
            return client.post(endpoint, files=files)

    if phase_id == "phase1":
        payload = {"domain": domain}
        return client.post(endpoint, json=payload)

    if phase_id == "phase6":
        return client.post(endpoint, data={"domain": domain})

    if phase_id == "phase7":
        return client.post(endpoint, data={"domain": domain})

    if phase_id == "phase7.5":
        return client.post(endpoint, data={"domain": domain, "target_column": target_column})

    if phase_id == "phase9.5":
        return client.post(endpoint, data={"domain": domain})

    if phase_id == "phase10.5":
        return client.post(endpoint, data={"target_column": target_column})

    if phase_id == "phase11.5":
        return client.post(endpoint, data={"target_column": target_column, "top_k": "25"})

    if phase_id == "phase14":
        data = {"domain": domain, "primary_metric": "recall", "target_column": target_column}
        return client.post(endpoint, data=data)

    # All other phases are simple POSTs without additional payload
    return client.post(endpoint)


if __name__ == "__main__":
    main()
