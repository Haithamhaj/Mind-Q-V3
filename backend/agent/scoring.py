from __future__ import annotations
from typing import Dict, Any, List


def compute_score(phase_id: int, kpis: Dict[str, float], cfg: Dict[str, Any]) -> Dict[str, Any]:
    per = cfg.get("per_phase", {}).get(str(phase_id))
    if not per:
        weights = cfg.get("default_weights", {})
        if not weights:
            return {"score_pct": 0, "components": []}
        # simplistic average with provided weights
        score = 0.0
        total_w = 0.0
        comps: List[Dict[str, Any]] = []
        for k, w in weights.items():
            val = float(kpis.get(k, 0)) * 100 if val_in_0_1(k, kpis) else float(kpis.get(k, 0))
            score += val * float(w)
            total_w += float(w)
            comps.append({"metric": k, "value": val, "weight": w})
        score = score / max(total_w, 1e-9)
        return {"score_pct": int(round(score)), "components": comps}

    weights = per.get("weights", {})

    # derive metrics from formulas if present
    formulas = cfg.get("metric_formulas", {})
    derived: Dict[str, float] = {}
    for name, expr in formulas.items():
        val_expr = expr
        for k, v in kpis.items():
            val_expr = val_expr.replace(k, str(float(v)))
        try:
            derived[name] = float(eval(val_expr, {"__builtins__": {}}, {}))
        except Exception:
            pass
    metrics = {**kpis, **derived}

    score = 0.0
    comps: List[Dict[str, Any]] = []
    for m, w in weights.items():
        val = float(metrics.get(m, 0))
        score += val * float(w)
        comps.append({"metric": m, "value": val, "weight": w})
    return {"score_pct": int(round(score)), "components": comps}


def val_in_0_1(metric: str, kpis: Dict[str, float]) -> bool:
    try:
        v = float(kpis.get(metric, 0))
        return 0.0 <= v <= 1.0
    except Exception:
        return False
