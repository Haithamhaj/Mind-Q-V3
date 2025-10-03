"""
Domain Pack Definitions for Mind-Q V3
Contains predefined domain packs with business KPIs and expected columns.
"""

from typing import Dict, List
from pydantic import BaseModel


class DomainPack(BaseModel):
    name: str
    kpis: List[str]
    expected_columns: List[str]
    expected_features: List[str]
    description: str


DOMAIN_PACKS: Dict[str, DomainPack] = {
    "logistics": DomainPack(
        name="logistics",
        kpis=["SLA_pct", "TransitTime_avg", "RTO_pct", "FAS_pct", "NDR_pct"],
        expected_columns=[
            "shipment_id", "order_id", "carrier", "origin", "destination",
            "pickup_date", "delivery_date", "status", "transit_time", "dwell_time"
        ],
        expected_features=["transit_time", "dwell_time", "sla_flag", "rto_flag", "fas_flag"],
        description="Logistics and delivery operations"
    ),
    "healthcare": DomainPack(
        name="healthcare",
        kpis=["BedOccupancy_pct", "AvgLOS_days", "Readmission_30d_pct", "ProcedureSuccess_pct"],
        expected_columns=[
            "patient_id", "admission_ts", "discharge_ts", "department",
            "diagnosis", "procedure", "los_days", "age", "gender"
        ],
        expected_features=["los_days", "age_group", "icd_chapter"],
        description="Healthcare and hospital operations"
    ),
    "emarketing": DomainPack(
        name="emarketing",
        kpis=["CTR_pct", "Conversion_pct", "CAC", "ROAS"],
        expected_columns=[
            "campaign_id", "date", "channel", "spend", "impressions",
            "clicks", "conversions", "start_date", "end_date"
        ],
        expected_features=["spend", "impressions", "clicks", "ctr", "conversion_flag"],
        description="E-marketing and digital advertising"
    ),
    "retail": DomainPack(
        name="retail",
        kpis=["GMV", "AOV", "CartAbandon_pct", "Return_pct"],
        expected_columns=[
            "order_id", "customer_id", "order_date", "product_id",
            "quantity", "price", "payment_method", "return_flag"
        ],
        expected_features=["order_value", "basket_size", "return_flag", "seasonality_bucket"],
        description="Retail and e-commerce"
    ),
    "finance": DomainPack(
        name="finance",
        kpis=["NPL_pct", "ROI_pct", "Liquidity_Ratio", "Default_pct"],
        expected_columns=[
            "account_id", "customer_id", "account_type", "balance",
            "open_date", "currency", "interest_rate", "default_flag"
        ],
        expected_features=["loan_duration_days", "interest_yield", "overdue_flag"],
        description="Finance and banking"
    )
}


def get_domain_pack(domain_name: str) -> DomainPack:
    """Get domain pack by name"""
    if domain_name not in DOMAIN_PACKS:
        raise ValueError(f"Domain '{domain_name}' not found. Available: {list(DOMAIN_PACKS.keys())}")
    return DOMAIN_PACKS[domain_name]


def suggest_domain(columns: List[str]) -> Dict[str, float]:
    """Suggest domain based on column names"""
    matches = {}
    
    for domain_name, pack in DOMAIN_PACKS.items():
        expected_lower = [c.lower() for c in pack.expected_columns]
        columns_lower = [c.lower() for c in columns]
        
        match_count = sum(1 for col in expected_lower if col in columns_lower)
        # Balanced coverage: average of coverage over expected and over provided columns
        coverage_expected = match_count / len(pack.expected_columns)
        coverage_provided = match_count / max(len(columns_lower), 1)
        adjusted = 0.5 * (coverage_expected + coverage_provided)
        matches[domain_name] = round(adjusted, 3)
    
    return dict(sorted(matches.items(), key=lambda x: x[1], reverse=True))

