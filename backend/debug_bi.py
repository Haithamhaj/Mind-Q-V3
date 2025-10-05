#!/usr/bin/env python3
"""
Debug script for BI services
"""

import sys
import os
sys.path.append('.')

import pandas as pd
from app.config import settings
from app.services.bi.orchestrator import BIOrchestrator
from app.services.bi.llm_client import call_llm_api

def test_bi_service():
    """Test BI service with actual data"""
    
    print("Testing BI Service...")
    
    try:
        # Check if data exists
        data_path = settings.artifacts_dir / "merged_data.parquet"
        if not data_path.exists():
            data_path = settings.artifacts_dir / "train.parquet"
        
        if not data_path.exists():
            print("No data files found")
            return
        
        print(f"📁 Using data file: {data_path}")
        
        # Load data with dtype fixes
        df = pd.read_parquet(data_path)
        print(f"Data shape: {df.shape}")
        print(f"Data columns: {list(df.columns)}")
        print(f"Data dtypes: {df.dtypes.to_dict()}")
        
        # Fix dtypes
        for col in df.columns:
            if df[col].dtype == 'string[python]':
                df[col] = df[col].astype('object')
                print(f"Fixed string dtype for column: {col}")
            elif 'datetime64[ns, UTC]' in str(df[col].dtype):
                df[col] = pd.to_datetime(df[col]).dt.tz_localize(None)
                print(f"Fixed datetime dtype for column: {col}")
        
        print(f"Fixed dtypes: {df.dtypes.to_dict()}")
        
        # Test orchestrator
        print("🤖 Creating BI Orchestrator...")
        orchestrator = BIOrchestrator(
            df=df,
            domain="logistics",
            time_window="2024-01-01..2024-12-31",
            llm_call=call_llm_api
        )
        
        print("BI Orchestrator created successfully")
        
        # Test KPIs
        print("Testing KPIs...")
        kpis = orchestrator.get_kpis()
        print(f"KPIs: {kpis}")
        
        # Test signals
        print("Testing signals...")
        signals = orchestrator.get_signals()
        print(f"Signals keys: {list(signals.keys())}")
        
        # Test recommendations
        print("💡 Testing recommendations...")
        from app.services.bi.rule_recommender import recommend_from_signals
        recommendations = recommend_from_signals(signals)
        print(f"Recommendations: {recommendations}")
        
        print("All tests passed!")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_bi_service()
