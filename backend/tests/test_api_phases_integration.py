"""
Integration tests for Phase 1, 2, and 3 API endpoints
"""

import pytest
import pandas as pd
import tempfile
import os
from fastapi.testclient import TestClient
from unittest.mock import patch

from app.main import app


class TestPhaseAPIIntegration:
    """Test Phase API integration"""
    
    @pytest.fixture
    def client(self):
        """Create test client"""
        return TestClient(app)
    
    @pytest.fixture
    def sample_csv_data(self):
        """Create sample CSV data"""
        data = {
            'shipment_id': [1, 2, 3, 4, 5],
            'order_id': [100, 101, 102, 103, 104],
            'carrier': ['UPS', 'FedEx', 'DHL', 'UPS', 'FedEx'],
            'origin': ['NYC', 'LA', 'CHI', 'NYC', 'LA'],
            'destination': ['BOS', 'SEA', 'DEN', 'BOS', 'SEA'],
            'pickup_date': ['2020-01-15', '2019-03-20', '2021-06-10', '2018-11-05', '2022-02-28'],
            'status': ['delivered', 'in_transit', 'delivered', 'delivered', 'in_transit']
        }
        return pd.DataFrame(data)
    
    @pytest.fixture
    def sample_csv_file(self, sample_csv_data):
        """Create temporary CSV file"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            sample_csv_data.to_csv(f.name, index=False)
            yield f.name
        os.unlink(f.name)
    
    def test_domain_packs_endpoint(self, client):
        """Test domain packs endpoint"""
        response = client.get("/api/v1/phases/domain-packs")
        
        assert response.status_code == 200
        data = response.json()
        assert "domain_packs" in data
        assert len(data["domain_packs"]) == 5
        assert "logistics" in data["domain_packs"]
        assert "healthcare" in data["domain_packs"]
    
    def test_domain_compatibility_endpoint(self, client):
        """Test domain compatibility endpoint"""
        columns = ["shipment_id", "order_id", "carrier", "origin", "destination"]
        
        response = client.post(
            "/api/v1/phases/domain-compatibility",
            json={"domain": "logistics", "columns": columns}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert data["data"]["domain"] == "logistics"
        assert data["data"]["status"] == "OK"
        assert data["data"]["match_percentage"] >= 0.7
    
    def test_domain_compatibility_warn(self, client):
        """Test domain compatibility with warning"""
        columns = ["shipment_id", "carrier"]  # Partial match
        
        response = client.post(
            "/api/v1/phases/domain-compatibility",
            json={"domain": "logistics", "columns": columns}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert data["data"]["status"] == "WARN"
        assert len(data["data"]["suggestions"]) > 0
    
    def test_goal_kpis_endpoint(self, client):
        """Test GoalKPIs service endpoint"""
        columns = ["shipment_id", "order_id", "carrier", "origin", "destination"]
        
        response = client.post(
            "/api/v1/phases/goal-kpis",
            json={"columns": columns, "domain": "logistics"}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["domain"] == "logistics"
        assert len(data["kpis"]) > 0
        assert data["compatibility"]["status"] == "OK"
        assert data["compatibility"]["match_percentage"] >= 0.7
    
    def test_goal_kpis_auto_suggestion(self, client):
        """Test GoalKPIs service with auto-suggestion"""
        columns = ["campaign_id", "date", "channel", "spend", "impressions", "clicks"]
        
        response = client.post(
            "/api/v1/phases/goal-kpis",
            json={"columns": columns}  # No domain specified
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["domain"] == "emarketing"  # Should auto-suggest
        assert len(data["kpis"]) > 0
        assert data["compatibility"]["status"] == "OK"
        assert data["compatibility"]["match_percentage"] >= 0.7
    
    def test_goal_kpis_warning(self, client):
        """Test GoalKPIs service with warning"""
        columns = ["shipment_id", "carrier"]  # Partial match
        
        response = client.post(
            "/api/v1/phases/goal-kpis",
            json={"columns": columns, "domain": "logistics"}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["domain"] == "logistics"
        assert data["compatibility"]["status"] == "WARN"
        assert 0.3 <= data["compatibility"]["match_percentage"] < 0.7
        assert len(data["compatibility"]["suggestions"]) > 0
    
    def test_ingest_data_endpoint(self, client, sample_csv_file):
        """Test data ingestion endpoint"""
        response = client.post(
            "/api/v1/phases/ingest",
            json={"source_file": sample_csv_file}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert data["data"]["rows_ingested"] == 5
        assert data["data"]["columns_ingested"] == 7
        assert data["data"]["target_file"].endswith('.parquet')
    
    def test_ingest_simple_endpoint(self, client, sample_csv_file):
        """Test simple ingestion endpoint"""
        response = client.post(
            "/api/v1/phases/ingest-simple",
            json={"file_path": sample_csv_file}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "dataframe_info" in data
        assert "ingestion_result" in data
        
        # Check dataframe info
        df_info = data["dataframe_info"]
        assert df_info["shape"] == [5, 7]
        assert len(df_info["columns"]) == 7
        assert len(df_info["dtypes"]) == 7
        
        # Check ingestion result
        result = data["ingestion_result"]
        assert result["rows"] == 5
        assert result["columns"] == 7
        assert len(result["column_names"]) == 7
        assert result["file_size_mb"] > 0
        assert result["parquet_path"].endswith('.parquet')
        assert "Successfully ingested 5 rows × 7 columns" in result["message"]
    
    def test_ingest_with_config(self, client, sample_csv_file):
        """Test data ingestion with custom config"""
        config = {
            "source_file": sample_csv_file,
            "target_format": "parquet",
            "compression": "zstd",
            "chunk_size": 1000,
            "preserve_index": False
        }
        
        response = client.post(
            "/api/v1/phases/ingest",
            json=config
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert data["data"]["compression_ratio"] > 0
    
    def test_schema_validation_endpoint(self, client, sample_csv_file):
        """Test schema validation endpoint"""
        # First ingest the file
        ingest_response = client.post(
            "/api/v1/phases/ingest",
            json={"source_file": sample_csv_file}
        )
        assert ingest_response.status_code == 200
        ingest_data = ingest_response.json()
        target_file = ingest_data["data"]["target_file"]
        
        # Then validate schema
        response = client.post(
            "/api/v1/phases/schema/validate",
            json={"file_path": target_file, "domain_pack": "logistics"}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert data["data"]["total_rows"] == 5
        assert data["data"]["total_columns"] == 7
        assert data["data"]["status"] in ["OK", "WARN"]
    
    def test_schema_simple_endpoint(self, client, sample_csv_data):
        """Test simple schema endpoint"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            sample_csv_data.to_csv(f.name, index=False)
            
            with open(f.name, 'rb') as file:
                response = client.post(
                    "/api/v1/phases/schema-simple",
                    files={"file": ("test.csv", file, "text/csv")}
                )
            
            assert response.status_code == 200
            data = response.json()
            assert "file_info" in data
            assert "schema_result" in data
            
            # Check file info
            file_info = data["file_info"]
            assert file_info["filename"] == "test.csv"
            assert file_info["original_shape"] == [5, 7]
            assert file_info["typed_shape"] == [5, 7]
            assert len(file_info["original_dtypes"]) == 7
            assert len(file_info["typed_dtypes"]) == 7
            
            # Check schema result
            schema_result = data["schema_result"]
            assert len(schema_result["dtypes"]) == 7
            assert isinstance(schema_result["id_columns"], list)
            assert isinstance(schema_result["datetime_columns"], list)
            assert isinstance(schema_result["numeric_columns"], list)
            assert isinstance(schema_result["categorical_columns"], list)
            assert isinstance(schema_result["warnings"], list)
            assert isinstance(schema_result["schema_json"], dict)
        
        os.unlink(f.name)
    
    def test_schema_info_endpoint(self, client, sample_csv_file):
        """Test schema info endpoint"""
        # First ingest the file
        ingest_response = client.post(
            "/api/v1/phases/ingest",
            json={"source_file": sample_csv_file}
        )
        assert ingest_response.status_code == 200
        ingest_data = ingest_response.json()
        target_file = ingest_data["data"]["target_file"]
        
        # Then get schema info
        response = client.get(f"/api/v1/phases/schema/info/{target_file}")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert data["data"]["total_rows"] == 5
        assert data["data"]["total_columns"] == 7
        assert len(data["data"]["column_types"]) == 7
    
    def test_list_ingested_files_endpoint(self, client, sample_csv_file):
        """Test list ingested files endpoint"""
        # First ingest a file
        ingest_response = client.post(
            "/api/v1/phases/ingest",
            json={"source_file": sample_csv_file}
        )
        assert ingest_response.status_code == 200
        
        # Then list files
        response = client.get("/api/v1/phases/ingest/files")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert "files" in data["data"]
        assert len(data["data"]["files"]) >= 1
    
    def test_list_processed_files_endpoint(self, client, sample_csv_file):
        """Test list processed files endpoint"""
        # First ingest and validate
        ingest_response = client.post(
            "/api/v1/phases/ingest",
            json={"source_file": sample_csv_file}
        )
        assert ingest_response.status_code == 200
        ingest_data = ingest_response.json()
        target_file = ingest_data["data"]["target_file"]
        
        validate_response = client.post(
            "/api/v1/phases/schema/validate",
            json={"file_path": target_file}
        )
        assert validate_response.status_code == 200
        
        # Then list processed files
        response = client.get("/api/v1/phases/schema/files")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert "files" in data["data"]
        assert len(data["data"]["files"]) >= 1


class TestWorkflowEndpoints:
    """Test workflow endpoints"""
    
    @pytest.fixture
    def client(self):
        """Create test client"""
        return TestClient(app)
    
    @pytest.fixture
    def sample_csv_data(self):
        """Create sample CSV data"""
        data = {
            'shipment_id': [1, 2, 3, 4, 5],
            'order_id': [100, 101, 102, 103, 104],
            'carrier': ['UPS', 'FedEx', 'DHL', 'UPS', 'FedEx'],
            'origin': ['NYC', 'LA', 'CHI', 'NYC', 'LA'],
            'destination': ['BOS', 'SEA', 'DEN', 'BOS', 'SEA']
        }
        return pd.DataFrame(data)
    
    def test_workflow_domain_check(self, client, sample_csv_data):
        """Test domain check workflow"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            sample_csv_data.to_csv(f.name, index=False)
            
            with open(f.name, 'rb') as file:
                response = client.post(
                    "/api/v1/phases/workflow/domain-check",
                    files={"file": ("test.csv", file, "text/csv")},
                    data={"domain": "logistics"}
                )
            
            assert response.status_code == 200
            data = response.json()
            assert "file_info" in data
            assert "compatibility" in data
            assert data["file_info"]["rows"] == 5
            assert data["file_info"]["columns"] == 5
            
            if data["compatibility"]:
                assert data["compatibility"]["domain"] == "logistics"
                assert data["compatibility"]["status"] in ["OK", "WARN", "STOP"]
        
        os.unlink(f.name)
    
    def test_workflow_full_pipeline(self, client, sample_csv_data):
        """Test full pipeline workflow"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            sample_csv_data.to_csv(f.name, index=False)
            
            with open(f.name, 'rb') as file:
                response = client.post(
                    "/api/v1/phases/workflow/full-pipeline",
                    files={"file": ("test.csv", file, "text/csv")},
                    data={
                        "domain": "logistics",
                        "auto_ingest": "true",
                        "auto_validate": "true"
                    }
                )
            
            assert response.status_code == 200
            data = response.json()
            assert "file_info" in data
            assert "steps" in data
            assert len(data["steps"]) >= 1
            
            # Check first step (domain compatibility)
            assert data["steps"][0]["step"] == "domain_compatibility"
            assert data["steps"][0]["status"] in ["success", "error"]
            
            # If domain compatibility is OK/WARN, should have more steps
            if len(data["steps"]) > 1:
                assert data["steps"][1]["step"] == "ingestion"
                assert data["steps"][1]["status"] in ["success", "error"]
                
                if len(data["steps"]) > 2:
                    assert data["steps"][2]["step"] == "schema_validation"
                    assert data["steps"][2]["status"] in ["success", "error"]
        
        os.unlink(f.name)
    
    def test_workflow_with_excel_file(self, client):
        """Test workflow with Excel file"""
        data = {
            'patient_id': [1, 2, 3, 4, 5],
            'admission_ts': ['2020-01-15', '2019-03-20', '2021-06-10', '2018-11-05', '2022-02-28'],
            'department': ['Cardiology', 'Neurology', 'Orthopedics', 'Cardiology', 'Neurology'],
            'age': [65, 70, 45, 60, 75],
            'gender': ['M', 'F', 'M', 'F', 'M']
        }
        df = pd.DataFrame(data)
        
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
            df.to_excel(f.name, index=False)
            
            with open(f.name, 'rb') as file:
                response = client.post(
                    "/api/v1/phases/workflow/domain-check",
                    files={"file": ("test.xlsx", file, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
                    data={"domain": "healthcare"}
                )
            
            assert response.status_code == 200
            data = response.json()
            assert data["file_info"]["rows"] == 5
            assert data["file_info"]["columns"] == 5
        
        os.unlink(f.name)
    
    def test_workflow_unsupported_file_format(self, client):
        """Test workflow with unsupported file format"""
        with tempfile.NamedTemporaryFile(suffix='.txt', delete=False) as f:
            f.write(b"test data")
            f.flush()
            
            with open(f.name, 'rb') as file:
                response = client.post(
                    "/api/v1/phases/workflow/domain-check",
                    files={"file": ("test.txt", file, "text/plain")},
                    data={"domain": "general"}
                )
            
            assert response.status_code == 400
            assert "Only CSV and Excel files supported" in response.json()["detail"]
        
        os.unlink(f.name)


class TestErrorHandling:
    """Test error handling in API endpoints"""
    
    @pytest.fixture
    def client(self):
        """Create test client"""
        return TestClient(app)
    
    def test_domain_compatibility_invalid_domain(self, client):
        """Test domain compatibility with invalid domain"""
        response = client.post(
            "/api/v1/phases/domain-compatibility",
            json={"domain": "nonexistent", "columns": ["col1", "col2"]}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "error"
        assert "not found" in data["message"]
    
    def test_ingest_nonexistent_file(self, client):
        """Test ingesting non-existent file"""
        response = client.post(
            "/api/v1/phases/ingest",
            json={"source_file": "nonexistent_file.csv"}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "error"
        assert "not found" in data["message"]
    
    def test_schema_validation_nonexistent_file(self, client):
        """Test schema validation with non-existent file"""
        response = client.post(
            "/api/v1/phases/schema/validate",
            json={"file_path": "nonexistent_file.parquet"}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "error"
        assert "not found" in data["message"]
    
    def test_schema_info_nonexistent_file(self, client):
        """Test schema info with non-existent file"""
        response = client.get("/api/v1/phases/schema/info/nonexistent_file.parquet")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "error"
        assert "not found" in data["message"]


class TestAPIStatus:
    """Test API status endpoints"""
    
    @pytest.fixture
    def client(self):
        """Create test client"""
        return TestClient(app)
    
    def test_main_status_endpoint(self, client):
        """Test main status endpoint"""
        response = client.get("/api/v1/status")
        
        assert response.status_code == 200
        data = response.json()
        assert "phases_implemented" in data
        assert "total_phases" in data
        assert data["phases_implemented"] >= 4  # Should include phases 0, 1, 2, 3
        assert data["total_phases"] == 14
    
    def test_phase1_status_endpoint(self, client):
        """Test Phase 1 status endpoint"""
        response = client.get("/api/v1/phases/status")
        
        assert response.status_code == 200
        data = response.json()
        assert data["phase"] == 1
        assert data["name"] == "Goal & KPIs Definition"
        assert "progress" in data
        assert "validation" in data
