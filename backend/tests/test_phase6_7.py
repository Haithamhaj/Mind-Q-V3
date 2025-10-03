import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from app.services.phase6_standardization import StandardizationService
from app.services.phase7_features import FeatureDraftService
from app.services.phase7_5_encoding import EncodingScalingService


def test_phase6_text_normalization():
    """Test Unicode and Arabic normalization"""
    df = pd.DataFrame({
        'text_col': ['أحمد', 'محمّد', 'Test'],
        'carrier': ['DHL', 'dhl', 'ARAMEX']
    })
    
    service = StandardizationService(df=df, domain="logistics")
    df_result, result = service.run()
    
    assert len(result.text_normalized) > 0
    # Check carrier mapping applied
    assert 'DHL' in df_result['carrier'].values


def test_phase6_rare_collapse():
    """Test rare category collapsing"""
    df = pd.DataFrame({
        'category': ['A']*95 + ['B']*3 + ['C']*2  # B and C are <1%
    })
    
    service = StandardizationService(df=df, rare_threshold=0.01)
    df_result, result = service.run()
    
    assert 'Other' in df_result['category'].values
    assert result.categories_collapsed['category'] == 2


def test_phase7_logistics_features():
    """Test logistics feature derivation"""
    df = pd.DataFrame({
        'pickup_date': pd.date_range('2024-01-01', periods=5),
        'delivery_date': pd.date_range('2024-01-02', periods=5),
        'status': ['Delivered', 'Delivered', 'Returned', 'Delivered', 'In Transit']
    })
    
    service = FeatureDraftService(df=df, domain="logistics")
    df_result, result = service.run()
    
    assert 'transit_time' in df_result.columns
    assert 'rto_flag' in df_result.columns
    assert any(f.name == 'transit_time' for f in result.features_created)


def test_phase7_outlier_capping():
    """Test outlier capping for logistics"""
    df = pd.DataFrame({
        'transit_time': [24, 48, 72, 300, 500]  # Last two exceed 240h cap
    })
    
    service = FeatureDraftService(df=df, domain="logistics")
    df_result, result = service.run()
    
    assert df_result['transit_time'].max() <= 240
    assert 'transit_time' in result.outliers_capped


def test_phase7_5_ohe_encoding():
    """Test One-Hot Encoding for low cardinality"""
    df_train = pd.DataFrame({
        'category': ['A', 'B', 'C', 'A', 'B'],
        'value': [10, 20, 30, 40, 50]
    })
    
    service = EncodingScalingService(df_train=df_train)
    df_train_enc, _, _, result = service.run(Path('/tmp'))
    
    # Should have OHE columns
    assert 'category_A' in df_train_enc.columns
    assert 'category_B' in df_train_enc.columns
    assert any(e.method == 'OHE' for e in result.encoding_configs)


def test_phase7_5_target_encoding():
    """Test Target Encoding for high cardinality"""
    np.random.seed(42)
    categories = [f'cat_{i}' for i in range(100)]  # High cardinality
    
    df_train = pd.DataFrame({
        'category': np.random.choice(categories, 60000),
        'target': np.random.randint(0, 2, 60000)
    })
    
    service = EncodingScalingService(
        df_train=df_train,
        target_col='target'
    )
    df_train_enc, _, _, result = service.run(Path('/tmp'))
    
    # Category should be encoded as numeric
    assert pd.api.types.is_numeric_dtype(df_train_enc['category'])
    assert any(e.method == 'Target_KFold' for e in result.encoding_configs)


def test_phase7_5_train_val_split():
    """Test that encoding fits on train, transforms on val"""
    df_train = pd.DataFrame({
        'category': ['A', 'B', 'C'] * 10,
        'value': range(30)
    })
    
    df_val = pd.DataFrame({
        'category': ['A', 'B', 'D'],  # 'D' is new category
        'value': [100, 200, 300]
    })
    
    service = EncodingScalingService(df_train=df_train, df_val=df_val)
    df_train_enc, df_val_enc, _, result = service.run(Path('/tmp'))
    
    # Validation should be transformed
    assert df_val_enc is not None
    assert len(df_val_enc) == 3


def test_phase7_5_scaling():
    """Test numeric scaling"""
    df_train = pd.DataFrame({
        'feature1': [10, 20, 30, 40, 50],
        'feature2': [100, 200, 300, 400, 500]
    })
    
    service = EncodingScalingService(df_train=df_train, domain="finance")
    df_train_enc, _, _, result = service.run(Path('/tmp'))
    
    # Should be scaled
    assert df_train_enc['feature1'].mean() < 1  # Approximately 0 after scaling
    assert result.scaling_config.method == "Robust"  # Finance uses Robust


