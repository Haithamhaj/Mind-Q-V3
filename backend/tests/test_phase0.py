import pytest
import pandas as pd
from app.services.phase0_quality_control import QualityControlService


def test_missing_threshold_pass():
    """Test that low missing % passes"""
    df = pd.DataFrame({
        'col1': [1, 2, 3, 4, 5],
        'col2': [1, None, 3, 4, 5]  # 20% missing
    })
    
    service = QualityControlService(df)
    result = service.run()
    
    assert result.status in ["PASS", "WARN"]  # Should not STOP at exactly 20%


def test_missing_threshold_stop():
    """Test that high missing % triggers STOP"""
    df = pd.DataFrame({
        'col1': [1, 2, 3, 4, 5],
        'col2': [1, None, None, None, 5]  # 40% missing
    })
    
    service = QualityControlService(df)
    result = service.run()
    
    assert result.status == "STOP"
    assert any("col2" in err for err in result.errors)


def test_duplicate_keys_stop():
    """Test that high duplicate % triggers STOP"""
    df = pd.DataFrame({
        'order_id': [1, 1, 2, 2, 3],  # 40% duplicates
        'value': [10, 20, 30, 40, 50]
    })
    
    service = QualityControlService(df, key_columns=['order_id'])
    result = service.run()
    
    assert result.status == "STOP"
    assert any("duplicates" in err.lower() for err in result.errors)


def test_null_keys_stop():
    """Test that high null key % triggers STOP"""
    df = pd.DataFrame({
        'order_id': [1, None, None, 4, 5],  # 40% nulls
        'value': [10, 20, 30, 40, 50]
    })
    
    service = QualityControlService(df, key_columns=['order_id'])
    result = service.run()
    
    assert result.status == "STOP"
    assert any("nulls" in err.lower() for err in result.errors)


def test_clean_dataset_pass():
    """Test that clean dataset passes all checks"""
    df = pd.DataFrame({
        'order_id': [1, 2, 3, 4, 5],
        'date': pd.date_range('2024-01-01', periods=5),
        'value': [10, 20, 30, 40, 50]
    })
    
    service = QualityControlService(df, key_columns=['order_id'])
    result = service.run()
    
    assert result.status == "PASS"
    assert len(result.errors) == 0
