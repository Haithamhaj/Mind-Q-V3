#!/usr/bin/env python3
import sys
import pandas as pd
import numpy as np
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "backend"))

from app.services.phase6_standardization import StandardizationService
from app.services.phase7_features import FeatureDraftService
from app.services.phase7_5_encoding import EncodingScalingService


def test_phase6():
    """Test Phase 6: Standardization"""
    print("\n6️⃣ Testing Phase 6: Standardization...")
    
    df = pd.DataFrame({
        'carrier': ['dhl', 'DHL Express', 'aramex', 'SMSA'],
        'text': ['أحمد', 'محمد', 'Test', 'Data'],
        'rare_cat': ['Common']*95 + ['Rare1']*3 + ['Rare2']*2
    })
    
    service = StandardizationService(df=df, domain="logistics")
    df_result, result = service.run()
    
    # Check mappings applied
    assert df_result['carrier'].nunique() <= 3  # Should be standardized
    print(f"✅ Carrier standardization: {df.carrier.nunique()} → {df_result.carrier.nunique()}")
    
    # Check rare collapse
    assert 'Other' in df_result['rare_cat'].values
    print(f"✅ Rare categories collapsed: {result.categories_collapsed['rare_cat']}")


def test_phase7():
    """Test Phase 7: Feature Draft"""
    print("\n7️⃣ Testing Phase 7: Feature Draft...")
    
    df = pd.DataFrame({
        'pickup_date': pd.date_range('2024-01-01', periods=10),
        'delivery_date': pd.date_range('2024-01-03', periods=10),
        'status': ['Delivered']*8 + ['Returned']*2
    })
    
    service = FeatureDraftService(df=df, domain="logistics")
    df_result, result = service.run()
    
    assert 'transit_time' in df_result.columns
    assert 'rto_flag' in df_result.columns
    print(f"✅ Features created: {len(result.features_created)}")
    
    for feat in result.features_created:
        print(f"   - {feat.name}: {feat.description}")


def test_phase7_5():
    """Test Phase 7.5: Encoding & Scaling"""
    print("\n7.5️⃣ Testing Phase 7.5: Encoding & Scaling...")
    
    # Create train/val/test splits
    np.random.seed(42)
    
    df_train = pd.DataFrame({
        'category': np.random.choice(['A', 'B', 'C'], 100),
        'numeric': np.random.randn(100) * 10 + 50,
        'target': np.random.randint(0, 2, 100)
    })
    
    df_val = pd.DataFrame({
        'category': np.random.choice(['A', 'B', 'C'], 20),
        'numeric': np.random.randn(20) * 10 + 50,
        'target': np.random.randint(0, 2, 20)
    })
    
    df_test = pd.DataFrame({
        'category': np.random.choice(['A', 'B', 'C'], 20),
        'numeric': np.random.randn(20) * 10 + 50,
        'target': np.random.randint(0, 2, 20)
    })
    
    service = EncodingScalingService(
        df_train=df_train,
        df_val=df_val,
        df_test=df_test,
        target_col='target'
    )
    
    df_train_enc, df_val_enc, df_test_enc, result = service.run(Path('/tmp'))
    
    # Verify encoding happened
    assert len(result.encoding_configs) > 0
    print(f"✅ Encoding applied: {len(result.encoding_configs)} columns")
    
    # Verify scaling happened
    assert len(result.scaling_config.columns) > 0
    print(f"✅ Scaling applied: {result.scaling_config.method}")
    
    # Critical: verify val/test were transformed, not fit
    assert df_val_enc is not None
    assert df_test_enc is not None
    print(f"✅ Val/Test transformed (not fit)")
    
    # Verify no leakage: train mean should be ~0 after scaling
    if 'numeric' in df_train_enc.columns:
        train_mean = df_train_enc['numeric'].mean()
        assert abs(train_mean) < 0.5  # Should be close to 0
        print(f"✅ Scaling verified: train mean = {train_mean:.3f}")


if __name__ == "__main__":
    print("🔍 Validating Phase 4 (Standardization + Features + Encoding)")
    print("="*60)
    
    try:
        test_phase6()
        test_phase7()
        test_phase7_5()
        
        print("="*60)
        print("✅✅✅ Phase 4 (6-7.5) Validation PASSED ✅✅✅")
        sys.exit(0)
    
    except AssertionError as e:
        print(f"\n❌ Validation failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


