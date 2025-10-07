from backend.agent.scoring import compute_score
from backend.agent import policies as pol


def test_phase5_scoring_increases_with_completeness():
    cfg = pol.load_scores_cfg()
    low = compute_score(5, {"completeness": 0.5, "psi": 0.2, "ks": 0.1, "traceability": 1.0}, cfg)["score_pct"]
    high = compute_score(5, {"completeness": 0.9, "psi": 0.05, "ks": 0.05, "traceability": 1.0}, cfg)["score_pct"]
    assert high >= low
    assert 0 <= low <= 100 and 0 <= high <= 100


def test_phase9_scoring_increases_when_duplicates_drop():
    cfg = pol.load_scores_cfg()
    worse = compute_score(9, {"duplicates_pct": 0.2, "orphans_pct": 0.1, "policy_compliance": 1.0}, cfg)["score_pct"]
    better = compute_score(9, {"duplicates_pct": 0.02, "orphans_pct": 0.01, "policy_compliance": 1.0}, cfg)["score_pct"]
    assert better >= worse


def test_phase19_thresholds_complete_has_score():
    cfg = pol.load_scores_cfg()
    empty = compute_score(19, {"feature_coverage": 0.0, "thresholds_complete": 0.0, "traceability": 0.0}, cfg)["score_pct"]
    filled = compute_score(19, {"feature_coverage": 0.8, "thresholds_complete": 1.0, "traceability": 1.0}, cfg)["score_pct"]
    assert filled >= empty
