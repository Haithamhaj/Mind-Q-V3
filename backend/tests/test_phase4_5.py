import pytest
import pandas as pd
import numpy as np
from app.services.phase4_profiling import ProfilingService
from app.services.phase5_missing_data import MissingDataService


def test_phase4_profiling():
    """Test basic profiling"""
    df = pd.DataFrame({
        'numeric': [1, 2, 3, 4, 5],
        'categorical': ['A', 'B', 'A', 'C', 'B'],
        'with_missing': [10, None, 30, None, 50]
    })
    
    service = ProfilingService(df=df)
    result = service.run()
    
    assert result.total_rows == 5
    assert result.total_columns == 3
    assert 'numeric' in result.numeric_summary
    assert 'categorical' in result.categorical_summary
    assert 'with_missing' in result.missing_summary


def test_phase5_median_imputation():
    """Test simple median imputation for low missing%"""
    df = pd.DataFrame({
        'value': [10.0, 20.0, None, 40.0, 50.0]  # 20% missing
    })
    
    service = MissingDataService(df=df)
    df_result, result = service.run()
    
    assert df_result['value'].isnull().sum() == 0
    assert any('median' in d.method for d in result.decisions)
    assert result.status in ["PASS", "WARN"]


def test_phase5_group_median():
    """Test group median imputation"""
    df = pd.DataFrame({
        'group': ['A', 'A', 'B', 'B', 'A'],
        'value': [10.0, None, 30.0, None, 50.0]
    })
    
    service = MissingDataService(df=df, group_col='group')
    df_result, result = service.run()
    
    assert df_result['value'].isnull().sum() == 0
    assert any('group_median' in d.method for d in result.decisions)


def test_phase5_date_flag_only():
    """Test that dates are flagged, not imputed"""
    df = pd.DataFrame({
        'date_col': pd.to_datetime(['2024-01-01', None, '2024-01-03'])
    })
    
    service = MissingDataService(df=df)
    df_result, result = service.run()
    
    assert 'date_col_missing' in df_result.columns
    assert any('flag_only' in d.method for d in result.decisions)


def test_phase5_psi_validation():
    """Test PSI validation catches distribution shifts"""
    # Create data where imputation would shift distribution significantly
    np.random.seed(42)
    df = pd.DataFrame({
        'value': np.concatenate([
            np.random.normal(10, 2, 80),  # Original distribution
            [np.nan] * 20  # 20% missing
        ])
    })
    
    service = MissingDataService(df=df)
    df_result, result = service.run()
    
    # Should have validation metrics
    assert len(result.validation) > 0
    assert 'value' in result.validation


def test_phase5_small_dataset_guard():
    """Test that small datasets avoid MICE/KNN"""
    df = pd.DataFrame({
        'col1': np.concatenate([[1.0, 2.0, 3.0], [np.nan] * 7]),  # n=10
        'col2': np.concatenate([[10.0, 20.0, 30.0], [np.nan] * 7])
    })
    
    service = MissingDataService(df=df)
    _, result = service.run()
    
    # Should use simple methods, not KNN/MICE
    methods = [d.method for d in result.decisions]
    assert 'mice' not in methods
    assert 'knn' not in methods


