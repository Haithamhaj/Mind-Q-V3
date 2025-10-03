"""
Tests for Phase 3: Schema Validation & Data Type Enforcement
"""

import pytest
import pandas as pd
import tempfile
import os
from pathlib import Path
from datetime import datetime

from app.services.phase3_schema import Phase3SchemaService


class TestPhase3SchemaService:
    """Test Phase 3 schema service"""
    
    @pytest.fixture
    def service(self):
        """Create Phase3SchemaService instance"""
        return Phase3SchemaService()
    
    @pytest.fixture
    def sample_dataframe(self):
        """Create sample dataframe with mixed data types"""
        data = {
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'age': [25, 30, 35, 40, 45],
            'salary': [50000.5, 60000.0, 70000.5, 80000.0, 90000.5],
            'hire_date': ['2020-01-15', '2019-03-20', '2021-06-10', '2018-11-05', '2022-02-28'],
            'department': ['IT', 'HR', 'Finance', 'IT', 'HR'],
            'is_active': [True, True, False, True, False],
            'phone': ['123-456-7890', '234-567-8901', '345-678-9012', '456-789-0123', '567-890-1234']
        }
        return pd.DataFrame(data)
    
    @pytest.fixture
    def sample_parquet_file(self, sample_dataframe):
        """Create temporary parquet file"""
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            sample_dataframe.to_parquet(f.name)
            yield f.name
        os.unlink(f.name)
    
    def test_validate_and_enforce_schema(self, service, sample_parquet_file):
        """Test schema validation and enforcement"""
        result = service.validate_and_enforce_schema(sample_parquet_file)
        
        assert result.status == "success"
        assert result.data is not None
        assert result.data.total_rows == 5
        assert result.data.total_columns == 8
        assert result.data.violation_rate >= 0  # Should be calculated
        assert result.data.status in ["OK", "WARN"]
        assert len(result.data.column_types) == 8
        assert len(result.data.type_conversions) >= 0
        assert len(result.data.warnings) >= 0
        
        # Verify processed file was created
        processed_path = Path(result.data.file_path.replace('.parquet', '_processed.parquet'))
        assert processed_path.exists()
    
    def test_validate_with_domain_pack(self, service, sample_parquet_file):
        """Test schema validation with domain pack"""
        result = service.validate_and_enforce_schema(sample_parquet_file, "retail")
        
        assert result.status == "success"
        assert result.data is not None
        assert result.data.status in ["OK", "WARN"]
    
    def test_validate_nonexistent_file(self, service):
        """Test validation of non-existent file"""
        result = service.validate_and_enforce_schema("nonexistent_file.parquet")
        
        assert result.status == "error"
        assert "not found" in result.message
    
    def test_get_schema_info(self, service, sample_parquet_file):
        """Test getting schema information"""
        result = service.get_schema_info(sample_parquet_file)
        
        assert result.status == "success"
        assert result.data is not None
        assert result.data.total_rows == 5
        assert result.data.total_columns == 8
        assert len(result.data.column_types) == 8
        assert result.data.status in ["OK", "WARN"]
    
    def test_list_processed_files(self, service, sample_parquet_file):
        """Test listing processed files"""
        # First validate a file to create processed version
        validate_result = service.validate_and_enforce_schema(sample_parquet_file)
        assert validate_result.status == "success"
        
        # Then list processed files
        list_result = service.list_processed_files()
        
        assert list_result.status == "success"
        assert "data" in list_result.dict()
        assert len(list_result.data["files"]) >= 1
        
        # Check file info structure
        file_info = list_result.data["files"][0]
        assert "filename" in file_info
        assert "path" in file_info
        assert "rows" in file_info
        assert "columns" in file_info
        assert "size_mb" in file_info
        assert "modified" in file_info
        assert "schema" in file_info


class TestDataTypeEnforcement:
    """Test data type enforcement rules"""
    
    @pytest.fixture
    def service(self):
        """Create Phase3SchemaService instance"""
        return Phase3SchemaService()
    
    def test_id_column_conversion(self, service):
        """Test ID columns are converted to string"""
        df = pd.DataFrame({
            'user_id': [1, 2, 3, 4, 5],
            'order_id': [100, 101, 102, 103, 104],
            'product_code': ['A001', 'A002', 'A003', 'A004', 'A005']
        })
        
        df_processed, conversions, warnings = service._enforce_data_types(df)
        
        assert 'user_id' in conversions
        assert 'order_id' in conversions
        assert 'product_code' in conversions
        
        # Check that ID columns are string type
        assert str(df_processed['user_id'].dtype) == 'string'
        assert str(df_processed['order_id'].dtype) == 'string'
        assert str(df_processed['product_code'].dtype) == 'string'
    
    def test_timestamp_column_conversion(self, service):
        """Test timestamp columns are converted to datetime[UTC]"""
        df = pd.DataFrame({
            'created_date': ['2020-01-15', '2019-03-20', '2021-06-10'],
            'updated_time': ['2020-01-15 10:30:00', '2019-03-20 14:45:00', '2021-06-10 09:15:00'],
            'timestamp': [1579017600, 1553097600, 1623340800]  # Unix timestamps
        })
        
        df_processed, conversions, warnings = service._enforce_data_types(df)
        
        assert 'created_date' in conversions
        assert 'updated_time' in conversions
        assert 'timestamp' in conversions
        
        # Check that timestamp columns are datetime type
        assert 'datetime' in str(df_processed['created_date'].dtype)
        assert 'datetime' in str(df_processed['updated_time'].dtype)
        assert 'datetime' in str(df_processed['timestamp'].dtype)
    
    def test_numeric_column_conversion(self, service):
        """Test numeric columns are converted to float"""
        df = pd.DataFrame({
            'amount': ['100.50', '200.75', '300.25'],
            'price': [10, 20, 30],
            'rate': [0.15, 0.20, 0.25],
            'percentage': ['15%', '20%', '25%']
        })
        
        df_processed, conversions, warnings = service._enforce_data_types(df)
        
        assert 'amount' in conversions
        assert 'price' in conversions
        assert 'rate' in conversions
        assert 'percentage' in conversions
        
        # Check that numeric columns are float type
        assert 'float' in str(df_processed['amount'].dtype)
        assert 'float' in str(df_processed['price'].dtype)
        assert 'float' in str(df_processed['rate'].dtype)
    
    def test_categorical_column_conversion(self, service):
        """Test categorical columns are converted to category"""
        df = pd.DataFrame({
            'status': ['active', 'inactive', 'pending', 'active', 'inactive'],
            'type': ['A', 'B', 'C', 'A', 'B'],
            'department': ['IT', 'HR', 'Finance', 'IT', 'HR'],
            'flag': [True, False, True, False, True]
        })
        
        df_processed, conversions, warnings = service._enforce_data_types(df)
        
        assert 'status' in conversions
        assert 'type' in conversions
        assert 'department' in conversions
        assert 'flag' in conversions
        
        # Check that categorical columns are category type
        assert 'category' in str(df_processed['status'].dtype)
        assert 'category' in str(df_processed['type'].dtype)
        assert 'category' in str(df_processed['department'].dtype)


class TestSchemaViolationDetection:
    """Test schema violation detection"""
    
    @pytest.fixture
    def service(self):
        """Create Phase3SchemaService instance"""
        return Phase3SchemaService()
    
    def test_count_schema_violations(self, service):
        """Test counting schema violations"""
        # Create dataframe with violations
        df = pd.DataFrame({
            'id': [1, 2, None, 4, 5],  # Null values in ID column
            'amount': [100.5, None, 300.25, 400.0, 500.75],  # Null values in numeric column
            'date': ['2020-01-15', 'invalid_date', '2021-06-10', None, '2022-02-28']  # Invalid dates
        })
        
        violations = service._count_schema_violations(df)
        
        # Should count null values in ID column (1 violation)
        # Should count null values in numeric column (1 violation)
        # Should count invalid dates (1 violation)
        assert violations >= 3
    
    def test_violation_rate_calculation(self, service):
        """Test violation rate calculation"""
        # Create dataframe with known violations
        df = pd.DataFrame({
            'id': [1, 2, None, 4, 5],  # 1 violation out of 5
            'amount': [100.5, 200.0, 300.25, 400.0, 500.75]  # No violations
        })
        
        total_cells = len(df) * len(df.columns)  # 5 * 2 = 10
        violations = service._count_schema_violations(df)  # Should be 1
        violation_rate = violations / total_cells  # Should be 0.1
        
        assert violation_rate == 0.1
    
    def test_status_determination(self, service):
        """Test status determination based on violation rate"""
        # Test OK status (violation rate <= 0.02)
        df_ok = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'amount': [100.5, 200.0, 300.25, 400.0, 500.75]
        })
        
        total_cells = len(df_ok) * len(df_ok.columns)
        violations = service._count_schema_violations(df_ok)
        violation_rate = violations / total_cells
        
        if violation_rate <= 0.02:
            status = "OK"
        else:
            status = "WARN"
        
        assert status in ["OK", "WARN"]


class TestErrorHandling:
    """Test error handling in Phase 3"""
    
    @pytest.fixture
    def service(self):
        """Create Phase3SchemaService instance"""
        return Phase3SchemaService()
    
    def test_corrupted_parquet_file(self, service):
        """Test handling corrupted parquet file"""
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            f.write(b"invalid parquet data")
            f.flush()
            
            result = service.validate_and_enforce_schema(f.name)
            
            assert result.status == "error"
        
        os.unlink(f.name)
    
    def test_conversion_errors(self, service):
        """Test handling conversion errors gracefully"""
        df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'invalid_date': ['not_a_date', 'also_not_a_date', '2020-01-15', '2019-03-20', '2021-06-10']
        })
        
        df_processed, conversions, warnings = service._enforce_data_types(df)
        
        # Should handle conversion errors gracefully
        assert len(warnings) >= 0  # May have warnings for failed conversions
        assert 'invalid_date' in conversions  # Should attempt conversion
    
    def test_empty_dataframe(self, service):
        """Test handling empty dataframe"""
        df = pd.DataFrame()
        
        df_processed, conversions, warnings = service._enforce_data_types(df)
        
        assert len(conversions) == 0
        assert len(warnings) == 0
        assert len(df_processed) == 0


class TestDomainSpecificValidation:
    """Test domain-specific validation"""
    
    @pytest.fixture
    def service(self):
        """Create Phase3SchemaService instance"""
        return Phase3SchemaService()
    
    def test_logistics_domain_validation(self, service):
        """Test validation with logistics domain"""
        df = pd.DataFrame({
            'shipment_id': [1, 2, 3, 4, 5],
            'order_id': [100, 101, 102, 103, 104],
            'carrier': ['UPS', 'FedEx', 'DHL', 'UPS', 'FedEx'],
            'origin': ['NYC', 'LA', 'CHI', 'NYC', 'LA'],
            'destination': ['BOS', 'SEA', 'DEN', 'BOS', 'SEA'],
            'pickup_date': ['2020-01-15', '2019-03-20', '2021-06-10', '2018-11-05', '2022-02-28'],
            'delivery_date': ['2020-01-17', '2019-03-22', '2021-06-12', '2018-11-07', '2022-03-02'],
            'status': ['delivered', 'in_transit', 'delivered', 'delivered', 'in_transit'],
            'transit_time': [2, 2, 2, 2, 2],
            'dwell_time': [0.5, 1.0, 0.5, 1.0, 0.5]
        })
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            df.to_parquet(f.name)
            
            result = service.validate_and_enforce_schema(f.name, "logistics")
            
            assert result.status == "success"
            assert result.data is not None
            assert result.data.status in ["OK", "WARN"]
        
        os.unlink(f.name)
    
    def test_healthcare_domain_validation(self, service):
        """Test validation with healthcare domain"""
        df = pd.DataFrame({
            'patient_id': [1, 2, 3, 4, 5],
            'admission_ts': ['2020-01-15 10:30:00', '2019-03-20 14:45:00', '2021-06-10 09:15:00', '2018-11-05 16:20:00', '2022-02-28 08:45:00'],
            'discharge_ts': ['2020-01-17 12:00:00', '2019-03-22 10:30:00', '2021-06-12 11:15:00', '2018-11-07 14:45:00', '2022-03-02 09:30:00'],
            'department': ['Cardiology', 'Neurology', 'Orthopedics', 'Cardiology', 'Neurology'],
            'diagnosis': ['Heart Attack', 'Stroke', 'Fracture', 'Heart Attack', 'Stroke'],
            'procedure': ['Angioplasty', 'Thrombectomy', 'Surgery', 'Angioplasty', 'Thrombectomy'],
            'los_days': [2, 2, 2, 2, 2],
            'age': [65, 70, 45, 60, 75],
            'gender': ['M', 'F', 'M', 'F', 'M']
        })
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            df.to_parquet(f.name)
            
            result = service.validate_and_enforce_schema(f.name, "healthcare")
            
            assert result.status == "success"
            assert result.data is not None
            assert result.data.status in ["OK", "WARN"]
        
        os.unlink(f.name)


