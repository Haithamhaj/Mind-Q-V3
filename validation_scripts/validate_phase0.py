#!/usr/bin/env python3
"""
Validation script for Phase 0: Quality Control
"""
import sys
import pandas as pd
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent.parent / "backend"))

from app.services.phase0_quality_control import QualityControlService


def test_warn_on_high_missing():
    """Test WARN trigger on >20% missing (Phase 5 will handle imputation)"""
    df = pd.DataFrame({
        'col1': [1, 2, 3, 4, 5],
        'col2': [None, None, None, None, 5]  # 80% missing
    })
    
    service = QualityControlService(df)
    result = service.run()
    
    assert result.status in ["WARN", "PASS"], f"Expected WARN or PASS, got {result.status}"
    assert len(result.warnings) > 0, "Expected warnings for high missing %"
    print("✅ WARN on high missing (Phase 5 handles imputation): PASS")


def test_stop_on_high_duplicates():
    """Test STOP trigger on >10% duplicates"""
    df = pd.DataFrame({
        'id': [1, 1, 1, 1, 5],  # 60% duplicates
        'val': [10, 20, 30, 40, 50]
    })
    
    service = QualityControlService(df, key_columns=['id'])
    result = service.run()
    
    assert result.status == "STOP", f"Expected STOP, got {result.status}"
    print("✅ STOP on high duplicates: PASS")


def test_pass_on_clean_data():
    """Test PASS on clean dataset"""
    df = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'date': pd.date_range('2024-01-01', periods=5),
        'value': [10, 20, 30, 40, 50]
    })
    
    service = QualityControlService(df, key_columns=['id'])
    result = service.run()
    
    assert result.status == "PASS", f"Expected PASS, got {result.status}"
    assert len(result.errors) == 0, "Expected no errors"
    print("✅ PASS on clean data: PASS")


if __name__ == "__main__":
    print("🔍 Validating Phase 0: Quality Control")
    print("-" * 50)
    
    try:
        test_warn_on_high_missing()
        test_stop_on_high_duplicates()
        test_pass_on_clean_data()
        
        print("-" * 50)
        print("✅✅✅ Phase 0 Validation PASSED ✅✅✅")
        sys.exit(0)
    except AssertionError as e:
        print(f"❌ Validation failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)
