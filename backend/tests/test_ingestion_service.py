"""
Tests for IngestionService - Phase 2 Ingestion & Landing
"""

import pytest
import pandas as pd
import tempfile
import os
from pathlib import Path

from app.services.phase2_ingestion import IngestionService, IngestionResult


class TestIngestionService:
    """Test IngestionService functionality"""
    
    @pytest.fixture
    def sample_csv_data(self):
        """Create sample CSV data"""
        data = {
            'Shipment ID': [1, 2, 3, 4, 5],
            'Order Number': [100, 101, 102, 103, 104],
            'Carrier Name': ['UPS', 'FedEx', 'DHL', 'UPS', 'FedEx'],
            'Origin City': ['New York', 'Los Angeles', 'Chicago', 'New York', 'Los Angeles'],
            'Destination-City': ['Boston', 'Seattle', 'Denver', 'Boston', 'Seattle'],
            'Pickup Date': ['2020-01-15', '2019-03-20', '2021-06-10', '2018-11-05', '2022-02-28'],
            'Status': ['Delivered', 'In Transit', 'Delivered', 'Delivered', 'In Transit']
        }
        return pd.DataFrame(data)
    
    @pytest.fixture
    def sample_csv_file(self, sample_csv_data):
        """Create temporary CSV file"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            sample_csv_data.to_csv(f.name, index=False)
            yield f.name
        os.unlink(f.name)
    
    @pytest.fixture
    def sample_excel_file(self, sample_csv_data):
        """Create temporary Excel file"""
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
            sample_csv_data.to_excel(f.name, index=False)
            yield f.name
        os.unlink(f.name)
    
    @pytest.fixture
    def artifacts_dir(self):
        """Create temporary artifacts directory"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            yield Path(tmp_dir)
    
    def test_csv_ingestion(self, sample_csv_file, artifacts_dir):
        """Test CSV file ingestion"""
        service = IngestionService(file_path=Path(sample_csv_file), artifacts_dir=artifacts_dir)
        df, result = service.run()
        
        # Check DataFrame
        assert len(df) == 5
        assert len(df.columns) == 7
        
        # Check column sanitization
        expected_columns = [
            'shipment_id', 'order_number', 'carrier_name', 'origin_city',
            'destination_city', 'pickup_date', 'status'
        ]
        assert df.columns.tolist() == expected_columns
        
        # Check IngestionResult
        assert isinstance(result, IngestionResult)
        assert result.rows == 5
        assert result.columns == 7
        assert result.column_names == expected_columns
        assert result.file_size_mb > 0
        assert result.parquet_path.endswith('.parquet')
        assert "Successfully ingested 5 rows × 7 columns" in result.message
        
        # Check parquet file exists
        parquet_path = Path(result.parquet_path)
        assert parquet_path.exists()
        
        # Verify parquet file can be read
        df_parquet = pd.read_parquet(parquet_path)
        assert len(df_parquet) == 5
        assert len(df_parquet.columns) == 7
        assert df_parquet.columns.tolist() == expected_columns
    
    def test_excel_ingestion(self, sample_excel_file, artifacts_dir):
        """Test Excel file ingestion"""
        service = IngestionService(file_path=Path(sample_excel_file), artifacts_dir=artifacts_dir)
        df, result = service.run()
        
        # Check DataFrame
        assert len(df) == 5
        assert len(df.columns) == 7
        
        # Check column sanitization
        expected_columns = [
            'shipment_id', 'order_number', 'carrier_name', 'origin_city',
            'destination_city', 'pickup_date', 'status'
        ]
        assert df.columns.tolist() == expected_columns
        
        # Check IngestionResult
        assert result.rows == 5
        assert result.columns == 7
        assert result.column_names == expected_columns
        assert result.file_size_mb > 0
        assert result.parquet_path.endswith('.parquet')
    
    def test_column_sanitization(self, artifacts_dir):
        """Test column name sanitization"""
        # Create data with problematic column names
        data = {
            'Shipment-ID': [1, 2, 3],
            'Order Number': [100, 101, 102],
            'Carrier@Name': ['UPS', 'FedEx', 'DHL'],
            'Origin City!!!': ['NYC', 'LA', 'CHI'],
            'Destination-City': ['BOS', 'SEA', 'DEN'],
            'Pickup Date': ['2020-01-15', '2019-03-20', '2021-06-10'],
            'Status': ['Delivered', 'In Transit', 'Delivered']
        }
        df = pd.DataFrame(data)
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.to_csv(f.name, index=False)
            
            service = IngestionService(file_path=Path(f.name), artifacts_dir=artifacts_dir)
            df_result, result = service.run()
            
            # Check sanitized column names
            expected_columns = [
                'shipment_id', 'order_number', 'carrier_name', 'origin_city',
                'destination_city', 'pickup_date', 'status'
            ]
            assert df_result.columns.tolist() == expected_columns
            assert result.column_names == expected_columns
        
        os.unlink(f.name)
    
    def test_column_sanitization_edge_cases(self, artifacts_dir):
        """Test column sanitization edge cases"""
        # Create data with edge case column names
        data = {
            '___Special___': [1, 2, 3],
            'Multiple   Spaces': [100, 101, 102],
            'Mixed-Case_Names': ['UPS', 'FedEx', 'DHL'],
            'Numbers123': ['NYC', 'LA', 'CHI'],
            'Special!@#$%Chars': ['BOS', 'SEA', 'DEN'],
            '': ['2020-01-15', '2019-03-20', '2021-06-10'],  # Empty column name
            'Status': ['Delivered', 'In Transit', 'Delivered']
        }
        df = pd.DataFrame(data)
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.to_csv(f.name, index=False)
            
            service = IngestionService(file_path=Path(f.name), artifacts_dir=artifacts_dir)
            df_result, result = service.run()
            
            # Check sanitized column names
            sanitized_columns = df_result.columns.tolist()
            assert len(sanitized_columns) == 7
            
            # All columns should be lowercase with underscores
            for col in sanitized_columns:
                assert col.islower()
                assert ' ' not in col
                assert col.replace('_', '').isalnum() or col == ''
        
        os.unlink(f.name)
    
    def test_parquet_compression(self, sample_csv_file, artifacts_dir):
        """Test parquet file compression"""
        service = IngestionService(file_path=Path(sample_csv_file), artifacts_dir=artifacts_dir)
        df, result = service.run()
        
        # Check that parquet file is compressed
        parquet_path = Path(result.parquet_path)
        assert parquet_path.exists()
        
        # Verify it's a valid parquet file
        df_parquet = pd.read_parquet(parquet_path)
        assert len(df_parquet) == 5
        assert len(df_parquet.columns) == 7
        
        # Check file size is reasonable (should be compressed)
        file_size_mb = parquet_path.stat().st_size / (1024 * 1024)
        assert file_size_mb > 0
        assert file_size_mb < 1.0  # Should be small for this data
    
    def test_unsupported_file_type(self, artifacts_dir):
        """Test unsupported file type"""
        with tempfile.NamedTemporaryFile(suffix='.txt', delete=False) as f:
            f.write(b"test data")
            f.flush()
            
            service = IngestionService(file_path=Path(f.name), artifacts_dir=artifacts_dir)
            
            with pytest.raises(ValueError) as exc_info:
                service.run()
            
            assert "Unsupported file type" in str(exc_info.value)
        
        os.unlink(f.name)
    
    def test_file_not_found(self, artifacts_dir):
        """Test file not found error"""
        service = IngestionService(file_path=Path("nonexistent_file.csv"), artifacts_dir=artifacts_dir)
        
        with pytest.raises(FileNotFoundError):
            service.run()
    
    def test_artifacts_dir_creation(self, sample_csv_file):
        """Test artifacts directory creation"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            artifacts_path = Path(tmp_dir) / "new_subdir"
            
            # Directory shouldn't exist initially
            assert not artifacts_path.exists()
            
            service = IngestionService(file_path=Path(sample_csv_file), artifacts_dir=artifacts_path)
            df, result = service.run()
            
            # Directory should be created
            assert artifacts_path.exists()
            assert artifacts_path.is_dir()
            
            # Parquet file should be in the directory
            parquet_path = Path(result.parquet_path)
            assert parquet_path.parent == artifacts_path


class TestIngestionResult:
    """Test IngestionResult model"""
    
    def test_ingestion_result_structure(self):
        """Test IngestionResult structure"""
        result = IngestionResult(
            rows=100,
            columns=5,
            column_names=['col1', 'col2', 'col3', 'col4', 'col5'],
            file_size_mb=1.25,
            parquet_path="/path/to/file.parquet",
            message="Successfully ingested 100 rows × 5 columns"
        )
        
        assert result.rows == 100
        assert result.columns == 5
        assert len(result.column_names) == 5
        assert result.file_size_mb == 1.25
        assert result.parquet_path == "/path/to/file.parquet"
        assert "Successfully ingested 100 rows × 5 columns" in result.message
    
    def test_ingestion_result_dict_conversion(self):
        """Test IngestionResult dict conversion"""
        result = IngestionResult(
            rows=50,
            columns=3,
            column_names=['id', 'name', 'value'],
            file_size_mb=0.5,
            parquet_path="/tmp/test.parquet",
            message="Successfully ingested 50 rows × 3 columns"
        )
        
        result_dict = result.dict()
        assert isinstance(result_dict, dict)
        assert result_dict['rows'] == 50
        assert result_dict['columns'] == 3
        assert result_dict['column_names'] == ['id', 'name', 'value']
        assert result_dict['file_size_mb'] == 0.5
        assert result_dict['parquet_path'] == "/tmp/test.parquet"


class TestIngestionServiceIntegration:
    """Test IngestionService integration scenarios"""
    
    def test_large_dataset_ingestion(self, artifacts_dir):
        """Test ingestion of larger dataset"""
        # Create larger dataset
        data = {
            'ID': list(range(1, 1001)),
            'Name': [f'Item_{i}' for i in range(1, 1001)],
            'Value': [i * 10.5 for i in range(1, 1001)],
            'Category': ['A', 'B', 'C'] * 334 + ['A']  # 1000 items
        }
        df = pd.DataFrame(data)
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.to_csv(f.name, index=False)
            
            service = IngestionService(file_path=Path(f.name), artifacts_dir=artifacts_dir)
            df_result, result = service.run()
            
            assert len(df_result) == 1000
            assert len(df_result.columns) == 4
            assert result.rows == 1000
            assert result.columns == 4
            assert result.file_size_mb > 0
            
            # Verify parquet file
            parquet_path = Path(result.parquet_path)
            df_parquet = pd.read_parquet(parquet_path)
            assert len(df_parquet) == 1000
            assert len(df_parquet.columns) == 4
        
        os.unlink(f.name)
    
    def test_mixed_data_types(self, artifacts_dir):
        """Test ingestion with mixed data types"""
        data = {
            'ID': [1, 2, 3, 4, 5],
            'Name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'Age': [25, 30, 35, 40, 45],
            'Salary': [50000.5, 60000.0, 70000.5, 80000.0, 90000.5],
            'Active': [True, True, False, True, False],
            'Join Date': ['2020-01-15', '2019-03-20', '2021-06-10', '2018-11-05', '2022-02-28']
        }
        df = pd.DataFrame(data)
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.to_csv(f.name, index=False)
            
            service = IngestionService(file_path=Path(f.name), artifacts_dir=artifacts_dir)
            df_result, result = service.run()
            
            assert len(df_result) == 5
            assert len(df_result.columns) == 6
            assert result.rows == 5
            assert result.columns == 6
            
            # Check data types are preserved
            assert df_result['id'].dtype in ['int64', 'int32']
            assert df_result['name'].dtype == 'object'
            assert df_result['age'].dtype in ['int64', 'int32']
            assert df_result['salary'].dtype in ['float64', 'float32']
            assert df_result['active'].dtype == 'bool'
            assert df_result['join_date'].dtype == 'object'
        
        os.unlink(f.name)
    
    def test_empty_dataset(self, artifacts_dir):
        """Test ingestion of empty dataset"""
        df = pd.DataFrame()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.to_csv(f.name, index=False)
            
            service = IngestionService(file_path=Path(f.name), artifacts_dir=artifacts_dir)
            df_result, result = service.run()
            
            assert len(df_result) == 0
            assert len(df_result.columns) == 0
            assert result.rows == 0
            assert result.columns == 0
            assert result.column_names == []
            assert "Successfully ingested 0 rows × 0 columns" in result.message
        
        os.unlink(f.name)

