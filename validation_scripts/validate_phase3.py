#!/usr/bin/env python3
import sys
import pandas as pd
import numpy as np
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "backend"))

from app.services.phase4_profiling import ProfilingService
from app.services.phase5_missing_data import MissingDataService


def test_phase4():
    """Test Phase 4: Profiling"""
    print("\n4️⃣ Testing Phase 4: Profiling...")
    
    df = pd.DataFrame({
        'id': ['A', 'B', 'C', 'D', 'E'],
        'numeric': [10, 20, 30, 40, 50],
        'category': ['X', 'Y', 'X', 'Z', 'Y'],
        'with_missing': [1.0, None, 3.0, None, 5.0]
    })
    
    service = ProfilingService(df=df)
    result = service.run()
    
    assert result.total_rows == 5
    assert result.total_columns == 4
    assert len(result.top_issues) <= 10
    print(f"✅ Profile generated: {result.total_rows}x{result.total_columns}")
    print(f"   Issues found: {len(result.top_issues)}")


def test_phase5():
    """Test Phase 5: Missing Data"""
    print("\n5️⃣ Testing Phase 5: Missing Data Handling...")
    
    # Create test data with various missing patterns
    np.random.seed(42)
    df = pd.DataFrame({
        'low_missing': np.concatenate([np.random.randn(95), [np.nan]*5]),  # 5%
        'med_missing': np.concatenate([np.random.randn(85), [np.nan]*15]),  # 15%
        'group': ['A']*50 + ['B']*50,
        'date_col': pd.to_datetime(['2024-01-01']*80 + [None]*20)
    })
    
    service = MissingDataService(df=df, group_col='group')
    df_result, result = service.run()
    
    # Check imputation decisions
    assert len(result.decisions) >= 2
    print(f"✅ Imputation completed: {len(result.decisions)} columns processed")
    
    # Check validation
    if len(result.validation) > 0:
        psi_values = [v.psi for v in result.validation.values()]
        print(f"   PSI range: {min(psi_values):.3f} - {max(psi_values):.3f}")
    
    # Check completeness
    assert result.record_completeness >= 0.80
    print(f"   Record completeness: {result.record_completeness:.1%}")
    
    # Check date flag
    assert 'date_col_missing' in df_result.columns
    print(f"   Date flagging: ✅")


if __name__ == "__main__":
    print("🔍 Validating Phase 3 (Profiling + Missing Data)")
    print("="*50)
    
    try:
        test_phase4()
        test_phase5()
        print("\n✅✅✅ Phase 3 Validation PASSED ✅✅✅")
        sys.exit(0)
    except AssertionError as e:
        print(f"\n❌ Validation failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


