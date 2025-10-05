from fastapi import APIRouter, HTTPException, Form
from typing import List
import pandas as pd
import json

from app.config import settings
from app.services.bi.orchestrator import BIOrchestrator, BIResponse
from app.services.bi.llm_client import call_llm_api
from app.services.bi.stats_signals import save_signals_json

router = APIRouter()


@router.post("/ask", response_model=BIResponse)
async def ask_question(
    question: str = Form(...),
    domain: str = Form("logistics"),
    time_window: str = Form("2024-01-01..2024-12-31")
):
    """
    Natural Language Query Endpoint
    
    Examples:
    - English: "What is the average transit time for DHL?"
    - Arabic: "ما متوسط وقت الشحن لشركة DHL؟"
    
    Args:
        question: Natural language question
        domain: Domain (logistics/healthcare/retail/emarketing/finance)
        time_window: Time period for context
    """
    try:
        # Load data
        data_path = settings.artifacts_dir / "merged_data.parquet"
        if not data_path.exists():
            data_path = settings.artifacts_dir / "train.parquet"
        
        if not data_path.exists():
            raise HTTPException(400, "No data found. Run EDA pipeline first.")
        
        df = pd.read_parquet(data_path)
        # Convert problematic dtypes for compatibility
        for col in df.columns:
            if df[col].dtype == 'string[python]':
                df[col] = df[col].astype('object')
            elif 'datetime64[ns, UTC]' in str(df[col].dtype):
                df[col] = pd.to_datetime(df[col]).dt.tz_localize(None)
        
        orchestrator = BIOrchestrator(
            df=df,
            domain=domain,
            time_window=time_window,
            llm_call=call_llm_api
        )
        
        response = orchestrator.process_question(question)
        
        return response
    
    except Exception as e:
        print(f"❌ Error in ask_question: {e}")
        print(f"❌ Error type: {type(e)}")
        import traceback
        print(f"❌ Traceback: {traceback.format_exc()}")
        raise HTTPException(500, str(e))


@router.get("/debug/llm")
async def debug_llm():
    """Debug LLM client"""
    try:
        test_prompt = '{"intent": "test"}'
        response = call_llm_api(test_prompt)
        return {
            "response": response,
            "type": str(type(response)),
            "is_string": isinstance(response, str),
            "is_none": response is None
        }
    except Exception as e:
        return {"error": str(e), "type": str(type(e))}


@router.get("/signals")
async def get_signals(
    domain: str = "logistics",
    time_window: str = "2024-01-01..2024-12-31"
):
    """
    Get complete signals JSON
    
    Returns:
    - meta (domain, time_window, n)
    - kpis (domain-specific metrics)
    - quality (missing%, outliers%, duplicates%)
    - distributions (shape, quantiles)
    - trends (slope analysis)
    """
    try:
        data_path = settings.artifacts_dir / "merged_data.parquet"
        if not data_path.exists():
            data_path = settings.artifacts_dir / "train.parquet"
        
        if not data_path.exists():
            raise HTTPException(400, "No data found")
        
        df = pd.read_parquet(data_path)
        # Convert problematic dtypes for compatibility
        for col in df.columns:
            if df[col].dtype == 'string[python]':
                df[col] = df[col].astype('object')
            elif 'datetime64[ns, UTC]' in str(df[col].dtype):
                df[col] = pd.to_datetime(df[col]).dt.tz_localize(None)
        
        orchestrator = BIOrchestrator(
            df=df,
            domain=domain,
            time_window=time_window,
            llm_call=call_llm_api
        )
        
        signals = orchestrator.get_signals()
        
        # Save to artifacts
        save_signals_json(
            signals,
            str(settings.artifacts_dir / "signals.json")
        )
        
        return signals
    
    except Exception as e:
        raise HTTPException(500, str(e))


@router.get("/kpis")
async def get_kpis(domain: str = "logistics"):
    """
    Get KPI cards for domain
    
    Returns domain-specific KPIs:
    - Logistics: SLA%, RTO%, avg_transit_h, total_shipments
    - Healthcare: avg_los_days, readmission_30d_pct, total_admissions
    - Retail: GMV, AOV, return_pct, total_orders
    - E-marketing: CTR%, conversion%, CAC, ROAS
    - Finance: NPL%, avg_balance, total_accounts
    """
    try:
        data_path = settings.artifacts_dir / "merged_data.parquet"
        if not data_path.exists():
            data_path = settings.artifacts_dir / "train.parquet"
        
        if not data_path.exists():
            raise HTTPException(400, "No data found")
        
        df = pd.read_parquet(data_path)
        # Convert problematic dtypes for compatibility
        for col in df.columns:
            if df[col].dtype == 'string[python]':
                df[col] = df[col].astype('object')
            elif 'datetime64[ns, UTC]' in str(df[col].dtype):
                df[col] = pd.to_datetime(df[col]).dt.tz_localize(None)
        
        orchestrator = BIOrchestrator(
            df=df,
            domain=domain,
            time_window="",
            llm_call=call_llm_api
        )
        
        kpis = orchestrator.get_kpis()
        
        return kpis
    
    except Exception as e:
        raise HTTPException(500, str(e))


@router.get("/recommendations")
async def get_recommendations(domain: str = "logistics"):
    """
    Get rule-based recommendations (pre-LLM)
    
    Returns top 3 actionable recommendations based on:
    - KPI thresholds
    - Trend analysis
    - Domain-specific rules
    """
    try:
        data_path = settings.artifacts_dir / "merged_data.parquet"
        if not data_path.exists():
            data_path = settings.artifacts_dir / "train.parquet"
        
        if not data_path.exists():
            raise HTTPException(400, "No data found")
        
        df = pd.read_parquet(data_path)
        # Convert problematic dtypes for compatibility
        for col in df.columns:
            if df[col].dtype == 'string[python]':
                df[col] = df[col].astype('object')
            elif 'datetime64[ns, UTC]' in str(df[col].dtype):
                df[col] = pd.to_datetime(df[col]).dt.tz_localize(None)
        
        orchestrator = BIOrchestrator(
            df=df,
            domain=domain,
            time_window="",
            llm_call=call_llm_api
        )
        
        from app.services.bi.rule_recommender import recommend_from_signals
        recommendations = recommend_from_signals(orchestrator.get_signals())
        
        return recommendations
    
    except Exception as e:
        raise HTTPException(500, str(e))
