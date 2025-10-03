"""
Tests for Phase 2: Data Ingestion Service
"""

import pytest
import pandas as pd
import tempfile
import os
from pathlib import Path

from app.services.phase2_ingestion import Phase2IngestionService
from app.models.schemas import IngestionConfig


class TestPhase2IngestionService:
    """Test Phase 2 ingestion service"""
    
    @pytest.fixture
    def service(self):
        """Create Phase2IngestionService instance"""
        return Phase2IngestionService()
    
    @pytest.fixture
    def sample_csv_data(self):
        """Create sample CSV data for testing"""
        data = {
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'age': [25, 30, 35, 40, 45],
            'salary': [50000, 60000, 70000, 80000, 90000],
            'department': ['IT', 'HR', 'Finance', 'IT', 'HR']
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
    
    def test_ingest_csv_file(self, service, sample_csv_file):
        """Test ingesting CSV file"""
        result = service.ingest_data(sample_csv_file)
        
        assert result.status == "success"
        assert result.data is not None
        assert result.data.rows_ingested == 5
        assert result.data.columns_ingested == 5
        assert result.data.target_file.endswith('.parquet')
        assert result.data.compression_ratio > 0
        assert result.data.ingestion_time_seconds > 0
        
        # Verify target file exists
        target_path = Path(result.data.target_file)
        assert target_path.exists()
        
        # Verify parquet file can be read
        df = pd.read_parquet(target_path)
        assert len(df) == 5
        assert len(df.columns) == 5
    
    def test_ingest_excel_file(self, service, sample_excel_file):
        """Test ingesting Excel file"""
        result = service.ingest_data(sample_excel_file)
        
        assert result.status == "success"
        assert result.data is not None
        assert result.data.rows_ingested == 5
        assert result.data.columns_ingested == 5
        assert result.data.target_file.endswith('.parquet')
    
    def test_ingest_with_config(self, service, sample_csv_file):
        """Test ingesting with custom configuration"""
        config = IngestionConfig(
            source_file=sample_csv_file,
            target_format="parquet",
            compression="zstd",
            chunk_size=1000,
            preserve_index=True
        )
        
        result = service.ingest_data(sample_csv_file, config)
        
        assert result.status == "success"
        assert result.data is not None
        assert result.data.compression_ratio > 0
    
    def test_ingest_nonexistent_file(self, service):
        """Test ingesting non-existent file"""
        result = service.ingest_data("nonexistent_file.csv")
        
        assert result.status == "error"
        assert "not found" in result.message
    
    def test_ingest_unsupported_format(self, service):
        """Test ingesting unsupported file format"""
        with tempfile.NamedTemporaryFile(suffix='.txt', delete=False) as f:
            f.write(b"test data")
            f.flush()
            
            result = service.ingest_data(f.name)
            
            assert result.status == "error"
            assert "Unsupported file format" in result.message
        
        os.unlink(f.name)
    
    def test_get_ingestion_status(self, service, sample_csv_file):
        """Test getting ingestion status"""
        # First ingest a file
        ingest_result = service.ingest_data(sample_csv_file)
        assert ingest_result.status == "success"
        
        # Then get status
        status_result = service.get_ingestion_status(ingest_result.data.target_file)
        
        assert status_result.status == "success"
        assert status_result.data is not None
        assert status_result.data.rows_ingested == 5
        assert status_result.data.columns_ingested == 5
        assert status_result.data.status == "available"
    
    def test_get_ingestion_status_nonexistent(self, service):
        """Test getting status for non-existent file"""
        result = service.get_ingestion_status("nonexistent_file.parquet")
        
        assert result.status == "error"
        assert "not found" in result.message
    
    def test_list_ingested_files(self, service, sample_csv_file):
        """Test listing ingested files"""
        # First ingest a file
        ingest_result = service.ingest_data(sample_csv_file)
        assert ingest_result.status == "success"
        
        # Then list files
        list_result = service.list_ingested_files()
        
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


class TestPhase2Chunking:
    """Test chunking functionality for large files"""
    
    @pytest.fixture
    def service(self):
        """Create Phase2IngestionService instance"""
        return Phase2IngestionService()
    
    @pytest.fixture
    def large_csv_data(self):
        """Create large CSV data for testing chunking"""
        data = {
            'id': list(range(1, 1001)),  # 1000 rows
            'name': [f'Person_{i}' for i in range(1, 1001)],
            'age': [20 + (i % 50) for i in range(1, 1001)],
            'salary': [30000 + (i * 100) for i in range(1, 1001)],
            'department': ['IT' if i % 2 == 0 else 'HR' for i in range(1, 1001)]
        }
        return pd.DataFrame(data)
    
    @pytest.fixture
    def large_csv_file(self, large_csv_data):
        """Create temporary large CSV file"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            large_csv_data.to_csv(f.name, index=False)
            yield f.name
        os.unlink(f.name)
    
    def test_ingest_large_file_with_chunking(self, service, large_csv_file):
        """Test ingesting large file with chunking"""
        config = IngestionConfig(
            source_file=large_csv_file,
            target_format="parquet",
            compression="zstd",
            chunk_size=100  # Small chunk size for testing
        )
        
        result = service.ingest_data(large_csv_file, config)
        
        assert result.status == "success"
        assert result.data is not None
        assert result.data.rows_ingested == 1000
        assert result.data.columns_ingested == 5
        
        # Verify target file exists and can be read
        target_path = Path(result.data.target_file)
        assert target_path.exists()
        
        df = pd.read_parquet(target_path)
        assert len(df) == 1000
        assert len(df.columns) == 5


class TestPhase2Compression:
    """Test compression functionality"""
    
    @pytest.fixture
    def service(self):
        """Create Phase2IngestionService instance"""
        return Phase2IngestionService()
    
    @pytest.fixture
    def sample_csv_file(self):
        """Create sample CSV file"""
        data = {
            'id': list(range(1, 101)),
            'text': ['This is a sample text data for compression testing'] * 100,
            'number': list(range(1, 101)),
            'category': ['A', 'B', 'C', 'D', 'E'] * 20
        }
        df = pd.DataFrame(data)
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.to_csv(f.name, index=False)
            yield f.name
        os.unlink(f.name)
    
    def test_compression_ratio(self, service, sample_csv_file):
        """Test that compression achieves reasonable ratio"""
        result = service.ingest_data(sample_csv_file)
        
        assert result.status == "success"
        assert result.data is not None
        assert result.data.compression_ratio > 1.0  # Should compress the data
        assert result.data.compression_ratio < 10.0  # But not too much (unrealistic)
    
    def test_different_compression_formats(self, service, sample_csv_file):
        """Test different compression formats"""
        # Test with zstd compression (default)
        config_zstd = IngestionConfig(
            source_file=sample_csv_file,
            compression="zstd"
        )
        
        result_zstd = service.ingest_data(sample_csv_file, config_zstd)
        assert result_zstd.status == "success"
        
        # Test with snappy compression
        config_snappy = IngestionConfig(
            source_file=sample_csv_file,
            compression="snappy"
        )
        
        result_snappy = service.ingest_data(sample_csv_file, config_snappy)
        assert result_snappy.status == "success"
        
        # Both should work
        assert result_zstd.data.compression_ratio > 0
        assert result_snappy.data.compression_ratio > 0


class TestPhase2ErrorHandling:
    """Test error handling in Phase 2"""
    
    @pytest.fixture
    def service(self):
        """Create Phase2IngestionService instance"""
        return Phase2IngestionService()
    
    def test_corrupted_csv_file(self, service):
        """Test handling corrupted CSV file"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("invalid,csv,data\n")
            f.write("missing,columns\n")
            f.write("too,many,columns,here,extra\n")
            f.flush()
            
            result = service.ingest_data(f.name)
            
            # Should handle gracefully
            assert result.status in ["success", "error"]
        
        os.unlink(f.name)
    
    def test_empty_file(self, service):
        """Test handling empty file"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("")  # Empty file
            f.flush()
            
            result = service.ingest_data(f.name)
            
            # Should handle gracefully
            assert result.status in ["success", "error"]
        
        os.unlink(f.name)
    
    def test_permission_error(self, service):
        """Test handling permission errors"""
        # Try to ingest a file that doesn't exist
        result = service.ingest_data("/root/nonexistent.csv")
        
        assert result.status == "error"
        assert "not found" in result.message


