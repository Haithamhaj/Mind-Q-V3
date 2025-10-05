from __future__ import annotations
from typing import Dict, List


def recommend_from_signals(signals: Dict) -> List[Dict]:
    """
    Generate rule-based recommendations before LLM
    
    Returns list of recommendations with:
    - title: Brief recommendation title
    - description: Detailed explanation
    - severity: high/medium/low
    """
    
    dom = signals["meta"]["domain"]
    k = signals.get("kpis", {})
    tr = signals.get("trends", {})
    recs: List[Dict] = []
    
    # Logistics recommendations
    if dom == "logistics":
        if k.get("sla_pct", 100) < 90 and tr.get("transit_time", {}).get("slope_norm_pct", 0) > 0:
            recs.append({
                "title": "Carrier/HUB performance audit",
                "description": "SLA below 90% while transit time is rising; review carrier mix and hub operations.",
                "severity": "high"
            })
        
        if k.get("rto_pct", 0) > 5:
            recs.append({
                "title": "Reduce RTO",
                "description": "High RTO% suggests address or COD issues; validate NDR reasons and address quality.",
                "severity": "medium"
            })
        
        if k.get("fas_pct", 100) < 85:
            recs.append({
                "title": "First Attempt Success improvement",
                "description": "Low FAS% indicates delivery issues; review address validation and customer communication.",
                "severity": "medium"
            })
    
    # E-marketing recommendations
    if dom == "emarketing":
        if k.get("roas", 1) < 1 and "ctr_pct" in k:
            recs.append({
                "title": "Reallocate spend",
                "description": "Low ROAS; shift budget to high-ROAS channels and pause underperformers.",
                "severity": "high"
            })
        
        if k.get("ctr_pct", 0) < 1:
            recs.append({
                "title": "Creative refresh needed",
                "description": "Low CTR suggests ad fatigue; test new creatives and messaging.",
                "severity": "medium"
            })
    
    # Retail recommendations
    if dom == "retail":
        if k.get("return_pct", 0) > 8:
            recs.append({
                "title": "Quality/size review",
                "description": "Return% above 8%; audit size charts, product descriptions, and QC for top-return categories.",
                "severity": "medium"
            })
        
        if k.get("aov", 0) < k.get("gmv", 0) / k.get("total_orders", 1) * 0.8:
            recs.append({
                "title": "Basket size optimization",
                "description": "Low AOV; implement cross-sell/upsell strategies and bundle offers.",
                "severity": "low"
            })
    
    # Healthcare recommendations
    if dom == "healthcare":
        if k.get("readmission_30d_pct", 0) > 15:
            recs.append({
                "title": "Discharge protocol review",
                "description": "Readmission>15%; audit discharge instructions and follow-up scheduling.",
                "severity": "high"
            })
        
        if k.get("avg_los_days", 0) > 7:
            recs.append({
                "title": "Length of stay optimization",
                "description": "High average LOS; review clinical pathways and discharge planning.",
                "severity": "medium"
            })
    
    # Finance recommendations
    if dom == "finance":
        if k.get("npl_pct", 0) > 6:
            recs.append({
                "title": "Tighten underwriting",
                "description": "Elevated NPL%; review underwriting thresholds and collection processes.",
                "severity": "high"
            })
        
        if k.get("liquidity_ratio", 1) < 1.2:
            recs.append({
                "title": "Liquidity monitoring",
                "description": "Low liquidity ratio; review cash reserves and short-term obligations.",
                "severity": "high"
            })
    
    return recs[:3]  # Return top 3 recommendations
