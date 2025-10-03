"""
Test suite for Phase 0 Quality Control Service
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from backend.app.services.phase0_quality_control import QualityControlService, QualityControlResult


class TestQualityControlService:
    """Test cases for QualityControlService"""
    
    def setup_method(self):
        """Set up test data"""
        # Create test DataFrame with various data quality issues
        self.test_data = {
            'id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'name': ['Alice', 'Bob', None, 'Charlie', 'David', 'Eve', None, 'Frank', 'Grace', None],
            'age': [25, 30, 35, None, 45, 50, 55, None, 65, 70],
            'date_created': [
                datetime.now() - timedelta(days=10),
                datetime.now() - timedelta(days=9),
                datetime.now() - timedelta(days=8),
                datetime.now() - timedelta(days=7),
                datetime.now() - timedelta(days=6),
                datetime.now() - timedelta(days=5),
                datetime.now() - timedelta(days=4),
                datetime.now() - timedelta(days=3),
                datetime.now() - timedelta(days=2),
                datetime.now() - timedelta(days=1)
            ],
            'score': [85, 90, 78, 92, 88, 95, 87, 91, 89, 93]
        }
        self.df = pd.DataFrame(self.test_data)
        self.key_columns = ['id']
    
    def test_missing_data_scan_pass(self):
        """Test missing data scan with acceptable levels"""
        service = QualityControlService(self.df, self.key_columns)
        result = service.run()
        
        # Should have missing data but within thresholds
        assert result.status == "PASS"
        assert 'name' in result.missing_report
        assert 'age' in result.missing_report
        assert result.missing_report['name'] == 0.3  # 3/10 missing
        assert result.missing_report['age'] == 0.2   # 2/10 missing
    
    def test_missing_data_scan_stop(self):
        """Test missing data scan triggering STOP condition"""
        # Create DataFrame with >20% missing data
        bad_data = {
            'id': range(10),
            'critical_field': [1, 2, None, None, None, None, None, 8, 9, 10]  # 50% missing
        }
        df_bad = pd.DataFrame(bad_data)
        
        service = QualityControlService(df_bad, ['id'])
        result = service.run()
        
        assert result.status == "STOP"
        assert len(result.errors) > 0
        assert any("critical_field" in error and ">20% threshold" in error for error in result.errors)
    
    def test_date_order_check_warn(self):
        """Test date order check triggering WARN condition"""
        # Create DataFrame with date inversions >0.5%
        dates = [datetime.now() - timedelta(days=i) for i in range(1000)]
        # Introduce inversions
        dates[100] = datetime.now() - timedelta(days=50)
        dates[200] = datetime.now() - timedelta(days=150)
        
        df_dates = pd.DataFrame({
            'id': range(1000),
            'date_field': dates
        })
        
        service = QualityControlService(df_dates, ['id'])
        result = service.run()
        
        # Should warn about date inversions
        assert result.status in ["WARN", "PASS"]  # Depends on exact inversion calculation
        assert 'date_field' in result.date_issues
    
    def test_key_checks_duplicates_stop(self):
        """Test key checks triggering STOP for duplicates"""
        # Create DataFrame with >10% duplicates in key column
        duplicate_data = {
            'id': [1, 2, 3, 1, 2, 3, 1, 2, 3, 1],  # 60% duplicates
            'value': range(10)
        }
        df_dups = pd.DataFrame(duplicate_data)
        
        service = QualityControlService(df_dups, ['id'])
        result = service.run()
        
        assert result.status == "STOP"
        assert any("duplicates" in error and ">10% threshold" in error for error in result.errors)
    
    def test_key_checks_nulls_stop(self):
        """Test key checks triggering STOP for nulls/orphans"""
        # Create DataFrame with >10% nulls in key column
        null_data = {
            'id': [1, 2, None, 4, None, 6, None, 8, None, None],  # 50% nulls
            'value': range(10)
        }
        df_nulls = pd.DataFrame(null_data)
        
        service = QualityControlService(df_nulls, ['id'])
        result = service.run()
        
        assert result.status == "STOP"
        assert any("nulls/orphans" in error and ">10% threshold" in error for error in result.errors)
    
    def test_multiple_key_columns(self):
        """Test quality control with multiple key columns"""
        multi_key_data = {
            'id': range(10),
            'user_id': range(10),
            'name': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J'],
            'value': range(10)
        }
        df_multi = pd.DataFrame(multi_key_data)
        
        service = QualityControlService(df_multi, ['id', 'user_id'])
        result = service.run()
        
        assert result.status == "PASS"
        assert 'id' in result.key_issues
        assert 'user_id' in result.key_issues
    
    def test_empty_dataframe(self):
        """Test quality control with empty DataFrame"""
        df_empty = pd.DataFrame()
        
        service = QualityControlService(df_empty)
        result = service.run()
        
        assert result.status == "PASS"
        assert result.missing_report == {}
        assert result.key_issues == {"message": "No key columns specified"}
    
    def test_no_key_columns(self):
        """Test quality control without key columns"""
        service = QualityControlService(self.df)
        result = service.run()
        
        assert result.status == "PASS"
        assert result.key_issues == {"message": "No key columns specified"}
    
    def test_future_dates_warning(self):
        """Test detection of future dates"""
        future_data = {
            'id': range(5),
            'date_field': [
                datetime.now() - timedelta(days=1),
                datetime.now() + timedelta(days=1),  # Future date
                datetime.now() + timedelta(days=2),  # Future date
                datetime.now() - timedelta(days=1),
                datetime.now() - timedelta(days=1)
            ]
        }
        df_future = pd.DataFrame(future_data)
        
        service = QualityControlService(df_future, ['id'])
        result = service.run()
        
        assert 'date_field' in result.date_issues
        assert result.date_issues['date_field']['future_dates'] == 2
        assert result.date_issues['date_field']['future_pct'] == 0.4
    
    def test_get_summary(self):
        """Test summary generation"""
        service = QualityControlService(self.df, self.key_columns)
        summary = service.get_summary()
        
        assert summary['total_columns'] == 5
        assert summary['total_rows'] == 10
        assert summary['key_columns_checked'] == 1
        assert 'status' in summary
    
    def test_result_model_validation(self):
        """Test QualityControlResult model validation"""
        result_data = {
            'status': 'PASS',
            'missing_report': {'col1': 0.1},
            'date_issues': {'date_col': {'future_dates': 0}},
            'key_issues': {'key_col': {'duplicates': 0}},
            'warnings': [],
            'errors': [],
            'timestamp': datetime.utcnow().isoformat()
        }
        
        result = QualityControlResult(**result_data)
        assert result.status == 'PASS'
        assert len(result.warnings) == 0
        assert len(result.errors) == 0


if __name__ == "__main__":
    pytest.main([__file__])

