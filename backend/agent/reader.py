from __future__ import annotations
from pathlib import Path
import json
import os
from typing import Dict, Any, List

ART_DIR = Path(os.getenv("AGENT_ARTIFACTS_DIR", "backend/artifacts"))


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def phase5_metrics() -> Dict[str, float]:
    kpis: Dict[str, float] = {}
    imp = ART_DIR / "imputation_report.json"
    dq = ART_DIR / "dq_report.json"
    if imp.exists():
        j = _read_json(imp)
        kpis["completeness"] = float(j.get("completeness", 0))
        kpis["psi"] = float(j.get("psi", 0))
        kpis["ks"] = float(j.get("ks", 0))
        kpis["traceability"] = 1.0 if j else 0.0
    elif dq.exists():
        j = _read_json(dq)
        kpis["completeness"] = float(j.get("completeness", 0))
    return kpis


def phase9_metrics() -> Dict[str, float]:
    kpis: Dict[str, float] = {}
    dup = ART_DIR / "duplicate_report.json"
    orph = ART_DIR / "orphan_report.json"
    if dup.exists():
        kpis["duplicates_pct"] = float(_read_json(dup).get("duplicates_pct", 0))
    if orph.exists():
        kpis["orphans_pct"] = float(_read_json(orph).get("orphans_pct", 0))
    kpis["policy_compliance"] = 1.0
    return kpis


def phase19_metrics() -> Dict[str, float]:
    kpis: Dict[str, float] = {}
    drift = ART_DIR / "drift_config.json"
    if drift.exists():
        j = _read_json(drift)
        kpis["feature_coverage"] = float(j.get("feature_coverage", 0))
        req_keys = {"psi_warn", "psi_action", "ks_warn", "ks_action"}
        kpis["thresholds_complete"] = 1.0 if req_keys.issubset(set(j.keys())) else 0.0
        kpis["traceability"] = 1.0
    return kpis


def list_existing(names: List[str]) -> List[str]:
    found: List[str] = []
    for n in names:
        p = ART_DIR / n
        if p.exists():
            found.append(str(p))
    return found
