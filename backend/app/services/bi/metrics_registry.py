from __future__ import annotations
import pandas as pd
import numpy as np
from typing import Dict, Callable, Any


def _pct(series: pd.Series) -> float:
    """Calculate percentage (0-100)"""
    s = series.dropna()
    return float(s.mean() * 100) if len(s) > 0 else np.nan


def _avg(series: pd.Series) -> float:
    """Calculate average"""
    s = series.dropna()
    return float(s.mean()) if len(s) > 0 else np.nan


def _sum(series: pd.Series) -> float:
    """Calculate sum"""
    s = series.dropna()
    return float(s.sum()) if len(s) > 0 else np.nan


# Registry of KPI functions per domain
REGISTRY: Dict[str, Dict[str, Callable[[pd.DataFrame], Any]]] = {
    "logistics": {
        "sla_pct": lambda df: _pct(df["sla_flag"]) if "sla_flag" in df else np.nan,
        "rto_pct": lambda df: _pct(df["rto_flag"]) if "rto_flag" in df else np.nan,
        "fas_pct": lambda df: _pct(df["fas_flag"]) if "fas_flag" in df else np.nan,
        "avg_transit_h": lambda df: _avg(df["transit_time"]) if "transit_time" in df else np.nan,
        "total_shipments": lambda df: int(len(df))
    },
    "healthcare": {
        "avg_los_days": lambda df: _avg(df["los_days"]) if "los_days" in df else np.nan,
        "readmission_30d_pct": lambda df: _pct(df["readmission_flag"]) if "readmission_flag" in df else np.nan,
        "bed_occupancy_pct": lambda df: _pct(df["bed_occupied_flag"]) if "bed_occupied_flag" in df else np.nan,
        "total_admissions": lambda df: int(len(df))
    },
    "emarketing": {
        "ctr_pct": lambda df: _avg(df["ctr"]) * 100 if "ctr" in df else np.nan,
        "conversion_pct": lambda df: _pct(df["conversion_flag"]) if "conversion_flag" in df else np.nan,
        "cac": lambda df: _avg(df["cac"]) if "cac" in df else np.nan,
        "roas": lambda df: _avg(df["roas"]) if "roas" in df else np.nan,
        "total_campaigns": lambda df: int(len(df))
    },
    "retail": {
        "gmv": lambda df: _sum(df["order_value"]) if "order_value" in df else np.nan,
        "aov": lambda df: _avg(df["order_value"]) if "order_value" in df else np.nan,
        "return_pct": lambda df: _pct(df["return_flag"]) if "return_flag" in df else np.nan,
        "basket_size": lambda df: _avg(df["basket_size"]) if "basket_size" in df else np.nan,
        "total_orders": lambda df: int(len(df))
    },
    "finance": {
        "npl_pct": lambda df: _pct(df["default_flag"]) if "default_flag" in df else np.nan,
        "avg_balance": lambda df: _avg(df["balance"]) if "balance" in df else np.nan,
        "liquidity_ratio": lambda df: _avg(df["liquidity_ratio"]) if "liquidity_ratio" in df else np.nan,
        "total_accounts": lambda df: int(len(df))
    }
}


def compute_kpis(df: pd.DataFrame, domain: str) -> Dict[str, Any]:
    """Compute all KPIs for given domain"""
    fns = REGISTRY.get(domain, REGISTRY["logistics"])
    return {k: v(df) for k, v in fns.items()}
