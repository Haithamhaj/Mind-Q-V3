from __future__ import annotations
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from typing import Any, Dict, List
from .dto import PhaseNode, QARequest, QAResponse, GoalScore
from . import policies as pol
from . import reader
from .scoring import compute_score
from .events import event_stream

router = APIRouter(prefix="/agent", tags=["agent"])  # prefix added here; include without extra prefix


@router.get("/graph")
def get_graph() -> Dict[str, Any]:
    graph = pol.load_phase_graph()
    return graph if graph else pol.load_phase_index()


@router.get("/phase", response_model=PhaseNode)
def get_phase(job_id: str | None = None, phase: int = Query(...)) -> PhaseNode:
    idx = pol.load_phase_index()
    phases = {p["id"]: p["name"] for p in idx["phases"]}
    if phase not in phases:
        raise HTTPException(404, "unknown phase")
    name = phases[phase]
    logic_md = pol.load_logic_md(phase)
    best_md = pol.load_best_practices_md(phase)

    # metrics per phase via mapping for extensibility
    metrics_readers = {
        5: reader.phase5_metrics,
        9: reader.phase9_metrics,
        19: reader.phase19_metrics,
    }
    kpis: Dict[str, float] = metrics_readers.get(phase, lambda: {})()

    # executed details and artifacts
    executed: Dict[str, Any] = {}
    if kpis:
        artifacts_by_phase = {
            5: ["dq_report.json", "imputation_report.json"],
            9: ["duplicate_report.json", "orphan_report.json"],
            19: ["drift_config.json"],
        }
        artifacts = reader.list_existing(artifacts_by_phase.get(phase, []))
        details: Dict[str, Any] = {}
        rules = pol.load_rules()
        if phase == 5:
            details = {
                "psi_warn": rules.get("psi_warn"),
                "psi_stop": rules.get("psi_stop"),
                "ks_warn": rules.get("ks_warn"),
                "ks_stop": rules.get("ks_stop"),
            }
        elif phase == 9:
            details = {
                "duplicates_warn_min": rules.get("duplicates_warn_min"),
                "duplicates_stop": rules.get("duplicates_stop"),
                "orphans_warn_min": rules.get("orphans_warn_min"),
                "orphans_stop": rules.get("orphans_stop"),
            }
        elif phase == 19:
            details = {"baseline": "drift thresholds present in artifact if complete"}
        executed = {"kpis": kpis, "details": details, "artifacts": artifacts}

    status = "READY"
    rules = pol.load_rules()
    if phase == 5 and kpis:
        psi, ks = kpis.get("psi", 0.0), kpis.get("ks", 0.0)
        status = (
            "STOP" if (psi >= rules["psi_stop"] or ks >= rules["ks_stop"]) else
            ("WARN" if (psi >= rules["psi_warn"] or ks >= rules["ks_warn"]) else "PASS")
        )
    if phase == 9 and kpis:
        dup, orph = kpis.get("duplicates_pct", 0.0), kpis.get("orphans_pct", 0.0)
        status = (
            "STOP" if (dup >= rules["duplicates_stop"] or orph >= rules["orphans_stop"]) else
            ("WARN" if (dup >= rules["duplicates_warn_min"] or orph >= rules["orphans_warn_min"]) else "PASS")
        )
    if phase == 19 and kpis:
        status = "PASS" if kpis.get("thresholds_complete", 0.0) == 1.0 else "WARN"

    score_dict = compute_score(phase, kpis, pol.load_scores_cfg()) if kpis else None
    score_model = GoalScore(**score_dict) if score_dict else None

    best_lines = [ln for ln in best_md.splitlines() if ln.strip()] if best_md else [
        f"No best practices found for phase {phase}.",
        f"Add a file at policies/best_practices/{phase}.md"
    ]

    return PhaseNode(
        phase=phase,
        name=name,
        status=status,
        timer_s=0,
        requires=[],
        unlocks=[],
        logic={
            "purpose": logic_md.splitlines()[0] if logic_md else "",
            "rules": logic_md or f"No logic file found. Add policies/logic/{phase}.md",
        },
        best_practices=best_lines,
        executed=executed,
        goal_score=score_model,
        decisions=[]
    )


@router.post("/qa", response_model=QAResponse)
def qa(req: QARequest) -> QAResponse:
    # ultra-simple local search over docs/policies/artifacts
    import glob
    import pathlib

    candidates: List[str] = []
    tokens = [t.lower() for t in req.question.split()[:3]]
    for root in ("docs", "policies", "backend/artifacts"):
        for p in glob.glob(f"{root}/**/*.*", recursive=True):
            try:
                text = pathlib.Path(p).read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue
            if any(tok in text.lower() for tok in tokens):
                candidates.append(p)
            if len(candidates) >= 5:
                break
    if not candidates:
        return QAResponse(answer="لم يتم العثور على إجابة من المصادر المحلية. جرّب كلمات مفتاحية أخرى.", sources=[])
    return QAResponse(answer="تم العثور على مصادر متعلقة بالسؤال.", sources=candidates[:5])


@router.get("/events")
async def events(session_id: str) -> StreamingResponse:
    return StreamingResponse(event_stream(session_id), media_type="text/event-stream")
