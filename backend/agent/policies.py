from __future__ import annotations
from pathlib import Path
import json
from typing import Any, Dict, Optional
from functools import lru_cache

BASE = Path("policies")


def _load_json(p: Path) -> Dict[str, Any]:
    return json.loads(p.read_text(encoding="utf-8"))


@lru_cache(maxsize=1)
def load_phase_index() -> Dict[str, Any]:
    return _load_json(BASE / "phase_index.json")


@lru_cache(maxsize=1)
def load_phase_graph() -> Optional[Dict[str, Any]]:
    p = BASE / "phase_graph.json"
    return _load_json(p) if p.exists() else None


@lru_cache(maxsize=1)
def load_rules() -> Dict[str, Any]:
    return _load_json(BASE / "rules.json")


@lru_cache(maxsize=1)
def load_scores_cfg() -> Dict[str, Any]:
    return _load_json(BASE / "phase_scores.json")


@lru_cache(maxsize=64)
def load_logic_md(phase_id: int) -> str:
    p = BASE / "logic" / f"{phase_id}.md"
    return p.read_text(encoding="utf-8") if p.exists() else ""


@lru_cache(maxsize=64)
def load_best_practices_md(phase_id: int) -> str:
    p = BASE / "best_practices" / f"{phase_id}.md"
    return p.read_text(encoding="utf-8") if p.exists() else ""
