from __future__ import annotations
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from typing import Any, Dict, List
from .dto import PhaseNode, QARequest, QAResponse
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

    # metrics per phase (samples)
    kpis: Dict[str, float] = {}
    if phase == 5:
        kpis = reader.phase5_metrics()
    elif phase == 9:
        kpis = reader.phase9_metrics()
    elif phase == 19:
        kpis = reader.phase19_metrics()

    executed = {"kpis": kpis, "details": {}, "artifacts": []} if kpis else {}
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

    score = compute_score(phase, kpis, pol.load_scores_cfg()) if kpis else {"score_pct": 0, "components": []}

    return PhaseNode(
        phase=phase,
        name=name,
        status=status,
        timer_s=0,
        requires=[],
        unlocks=[],
        logic={
            "purpose": logic_md.splitlines()[0] if logic_md else "",
            "rules": logic_md
        },
        best_practices=[ln for ln in best_md.splitlines() if ln.strip()],
        executed=executed,
        goal_score=score,
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
        return QAResponse(answer="لم يتم العثور على إجابة من المصادر المحلية.", sources=[])
    return QAResponse(answer="تم العثور على مصادر متعلقة بالسؤال.", sources=candidates[:5])


@router.get("/events")
async def events(session_id: str) -> StreamingResponse:
    return StreamingResponse(event_stream(session_id), media_type="text/event-stream")
