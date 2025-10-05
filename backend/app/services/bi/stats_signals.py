from __future__ import annotations
import pandas as pd
import numpy as np
import json
from typing import Dict, Any, Optional
from .metrics_registry import compute_kpis


def _missing_pct(df: pd.DataFrame) -> Dict[str, float]:
    """Calculate missing percentage per column"""
    return {c: float(df[c].isna().mean() * 100) for c in df.columns}


def _orphans_dup_metrics(df: pd.DataFrame, key_cols: Optional[list] = None) -> Dict[str, float]:
    """Calculate orphans and duplicates metrics"""
    res = {"orphans_pct": np.nan, "duplicates_pct": np.nan}
    if key_cols:
        dup = df.duplicated(subset=key_cols).mean() * 100
        res["duplicates_pct"] = float(dup)
    return res


def _skew_kurtosis(df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    """Calculate skewness and kurtosis for numeric columns"""
    out = {}
    for c in df.select_dtypes(include=[np.number]).columns:
        s = df[c].dropna()
        if len(s) >= 10:
            out[c] = {
                "skew": float(s.skew()),
                "kurtosis": float(s.kurt())
            }
    return out


def _outlier_pct_iqr(df: pd.DataFrame) -> Dict[str, float]:
    """Calculate outlier percentage using IQR method"""
    out = {}
    for c in df.select_dtypes(include=[np.number]).columns:
        s = df[c].dropna()
        if len(s) < 10:
            continue
        q1, q3 = np.percentile(s, [25, 75])
        iqr = q3 - q1
        low, high = q1 - 1.5 * iqr, q3 + 1.5 * iqr
        out[c] = float(((s < low) | (s > high)).mean() * 100)
    return out


def _quantiles(df: pd.DataFrame, qs=(0.9, 0.95)) -> Dict[str, Dict[str, float]]:
    """Calculate quantiles for numeric columns"""
    out = {}
    for c in df.select_dtypes(include=[np.number]).columns:
        s = df[c].dropna()
        if len(s) >= 10:
            out[c] = {f"p{int(q*100)}": float(np.quantile(s, q)) for q in qs}
    return out


def _date_col(df: pd.DataFrame) -> Optional[str]:
    """Find first datetime column"""
    dt_cols = [c for c in df.columns if np.issubdtype(df[c].dtype, np.datetime64)]
    return dt_cols[0] if dt_cols else None


def _trend(df: pd.DataFrame, metric: str, freq: str = "D") -> Optional[Dict[str, float]]:
    """Calculate trend for a metric over time"""
    dt = _date_col(df)
    if not dt or metric not in df.columns:
        return None
    
    temp = df[[dt, metric]].dropna().copy()
    if temp.empty:
        return None
    
    temp["date"] = pd.to_datetime(temp[dt]).dt.to_period(freq).dt.to_timestamp()
    g = temp.groupby("date")[metric].mean().reset_index()
    g["t"] = np.arange(len(g))
    
    if len(g) < 5:
        return None
    
    # Linear regression slope
    x, y = g["t"].values, g[metric].values
    slope = np.polyfit(x, y, 1)[0]
    mean = y.mean() if y.mean() != 0 else 1.0
    
    return {
        "slope_norm_pct": float((slope / mean) * 100),
        "n_points": int(len(g))
    }


def build_signals(
    df: pd.DataFrame,
    domain: str,
    time_window: str,
    key_cols: Optional[list] = None
) -> Dict[str, Any]:
    """
    Build complete signals JSON containing:
    - meta (domain, time_window, n)
    - kpis (domain-specific metrics)
    - quality (missing%, orphans%, duplicates%)
    - distributions (shape, outliers, quantiles)
    - trends (slope analysis)
    """
    n = int(len(df))
    
    signals = {
        "meta": {
            "domain": domain,
            "time_window": time_window,
            "n": n
        },
        "kpis": compute_kpis(df, domain),
        "quality": {
            **_orphans_dup_metrics(df, key_cols),
            "missing_pct": _missing_pct(df)
        },
        "distributions": {
            "shape": _skew_kurtosis(df),
            "outliers_pct_iqr": _outlier_pct_iqr(df),
            "quantiles": _quantiles(df)
        },
        "trends": {},
        "associations": {}  # Can be filled from Phase 9 outputs
    }
    
    # Add trend for primary metric per domain
    candidates = {
        "logistics": "transit_time",
        "healthcare": "los_days",
        "emarketing": "ctr",
        "retail": "order_value",
        "finance": "balance"
    }
    
    metric = candidates.get(domain)
    tr = _trend(df, metric) if metric else None
    if tr:
        signals["trends"][metric] = tr
    
    return signals


def save_signals_json(signals: Dict[str, Any], path: str) -> None:
    """Save signals to JSON file"""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(signals, f, ensure_ascii=False, indent=2)
