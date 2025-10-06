#!/usr/bin/env python3
import sys
import pandas as pd
import numpy as np
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "backend"))

from app.services.phase8_merging import MergingService
from app.services.phase9_correlations import CorrelationsService
from app.services.phase9_5_business_validation import BusinessValidationService


def test_phase8():
    """Test Phase 8: Merging"""
    print("\n8️⃣ Testing Phase 8: Merging & Keys...")
    
    df = pd.DataFrame({
        'order_id': list(range(1, 96)) + [1, 2, 3, 4, 5],  # 5% duplicates
        'timestamp': pd.date_range('2024-01-01', periods=100),
        'value': np.random.randn(100)
    })
    
    service = MergingService(main_df=df)
    df_result, result = service.run(Path('/tmp'))
    
    assert result.status != "STOP"
    print(f"✅ Merging completed: {result.total_rows_before} → {result.total_rows_after} rows")
    print(f"   Issues detected: {len(result.issues)}")


def test_phase9():
    """Test Phase 9: Correlations"""
    print("\n9️⃣ Testing Phase 9: Correlations...")
    
    np.random.seed(42)
    df = pd.DataFrame({
        'feat1': np.random.randn(100),
        'feat2': np.random.randn(100),
        'cat1': np.random.choice(['A', 'B', 'C'], 100),
        'cat2': np.random.choice(['X', 'Y'], 100)
    })
    # Create correlation
    df['feat3'] = df['feat1'] * 0.7 + np.random.randn(100) * 0.3
    
    service = CorrelationsService(df=df)
    result = service.run()
    
    assert result.total_tests > 0
    print(f"✅ Correlations calculated: {len(result.numeric_correlations)} numeric pairs")
    print(f"   Categorical associations: {len(result.categorical_associations)}")
    print(f"   FDR correction applied: {result.fdr_applied}")


def test_phase9_5():
    """Test Phase 9.5: Business Validation"""
    print("\n9.5️⃣ Testing Phase 9.5: Business Validation...")
    
    from app.services.phase9_correlations import CorrelationPair
    
    # Simulate conflicting correlation
    correlations = [
        CorrelationPair(
            feature1="transit_time",
            feature2="sla_flag",
            correlation=0.75,  # Should be negative in logistics
            p_value=0.001,
            method="pearson",
            n=10000
        )
    ]
    
    service = BusinessValidationService(
        correlations=correlations,
        domain="logistics"
    )
    result = service.run()
    
    print(f"✅ Business validation completed")
    print(f"   Conflicts detected: {len(result.conflicts_detected)}")
    print(f"   LLM hypotheses generated: {result.llm_hypotheses_generated}")
    print(f"   Status: {result.status}")
    
    if result.conflicts_detected:
        conflict = result.conflicts_detected[0]
        print(f"\n   Example conflict:")
        print(f"   - {conflict.feature1} vs {conflict.feature2}")
        print(f"   - Observed: {conflict.observed_correlation:.2f}")
        print(f"   - Expected: {conflict.expected_relationship}")
        print(f"   - Severity: {conflict.conflict_severity}")


if __name__ == "__main__":
    print("🔍 Validating Phase 5 (Merging + Analysis + Business Validation)")
    print("="*70)
    
    try:
        test_phase8()
        test_phase9()
        test_phase9_5()
        
        print("="*70)
        print("✅✅✅ Phase 5 (8-9.5) Validation PASSED ✅✅✅")
        sys.exit(0)
    
    except AssertionError as e:
        print(f"\n❌ Validation failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)












