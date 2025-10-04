import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from app.services.phase8_merging import MergingService
from app.services.phase9_correlations import CorrelationsService
from app.services.phase9_5_business_validation import BusinessValidationService


def test_phase8_duplicate_handling():
    """Test duplicate key handling"""
    df = pd.DataFrame({
        'order_id': [1, 1, 2, 3, 4],  # 20% duplicates
        'timestamp': pd.date_range('2024-01-01', periods=5),
        'value': [10, 20, 30, 40, 50]
    })
    
    service = MergingService(main_df=df)
    df_result, result = service.run(Path('/tmp'))
    
    # Should handle duplicates
    assert result.status in ["WARN", "PASS"]
    assert any(issue.issue_type == "duplicates" for issue in result.issues)


def test_phase8_stop_on_high_duplicates():
    """Test STOP trigger on high duplicate %"""
    df = pd.DataFrame({
        'order_id': [1]*6 + [2,3,4,5],  # 50% duplicates
        'value': range(10)
    })
    
    service = MergingService(main_df=df)
    df_result, result = service.run(Path('/tmp'))
    
    assert result.status == "STOP"


def test_phase9_numeric_correlations():
    """Test numeric correlation calculation"""
    np.random.seed(42)
    df = pd.DataFrame({
        'feature1': np.random.randn(100),
        'feature2': np.random.randn(100)
    })
    # Create correlation
    df['feature3'] = df['feature1'] * 0.8 + np.random.randn(100) * 0.2
    
    service = CorrelationsService(df=df)
    result = service.run()
    
    assert len(result.numeric_correlations) > 0
    # Should find strong correlation between feature1 and feature3
    corr_pair = next(
        (c for c in result.numeric_correlations 
         if set([c.feature1, c.feature2]) == set(['feature1', 'feature3'])),
        None
    )
    assert corr_pair is not None
    assert abs(corr_pair.correlation) > 0.6


def test_phase9_fdr_correction():
    """Test FDR correction when > 20 tests"""
    # Create dataset with many features
    np.random.seed(42)
    df = pd.DataFrame(np.random.randn(100, 25))
    df.columns = [f'feat_{i}' for i in range(25)]
    
    service = CorrelationsService(df=df)
    result = service.run()
    
    # Should apply FDR correction
    assert result.fdr_applied is True
    assert result.total_tests > 20


def test_phase9_5_conflict_detection():
    """Test business conflict detection"""
    from app.services.phase9_correlations import CorrelationPair
    
    # Create correlation that conflicts with logistics domain
    correlations = [
        CorrelationPair(
            feature1="transit_time",
            feature2="sla_flag",
            correlation=0.8,  # Should be negative!
            p_value=0.01,
            method="pearson",
            n=1000
        )
    ]
    
    service = BusinessValidationService(
        correlations=correlations,
        domain="logistics"
    )
    result = service.run()
    
    # Should detect conflict
    assert len(result.conflicts_detected) > 0
    assert result.status in ["WARN", "STOP"]


def test_phase9_5_llm_hypothesis():
    """Test LLM hypothesis generation"""
    from app.services.phase9_correlations import CorrelationPair
    
    correlations = [
        CorrelationPair(
            feature1="spend",
            feature2="conversions",
            correlation=-0.3,  # Should be positive!
            p_value=0.01,
            method="pearson",
            n=5000
        )
    ]
    
    service = BusinessValidationService(
        correlations=correlations,
        domain="emarketing"
    )
    result = service.run()
    
    # Should generate hypothesis
    assert result.llm_hypotheses_generated > 0
    assert result.conflicts_detected[0].llm_hypothesis is not None









