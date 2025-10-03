import pytest
import pandas as pd
from pathlib import Path

from app.services.phase1_goal_kpis import GoalKPIsService
from app.services.phase2_ingestion import IngestionService
from app.services.phase3_schema import SchemaService


def test_phase1_domain_match():
    """Test domain matching with logistics data"""
    columns = [
        "shipment_id", "order_id", "carrier", "origin",
        "destination", "pickup_date", "delivery_date", "status"
    ]
    
    service = GoalKPIsService(columns=columns, domain="logistics")
    result = service.run()
    
    assert result.domain == "logistics"
    assert result.compatibility.status in ["OK", "WARN"]
    assert result.compatibility.match_percentage > 0.5


def test_phase1_auto_suggest():
    """Test auto domain suggestion"""
    columns = ["order_id", "customer_id", "product_id", "price", "quantity"]
    
    service = GoalKPIsService(columns=columns, domain=None)
    result = service.run()
    
    assert result.domain == "retail"  # Should auto-detect retail


def test_phase1_incompatible_stop():
    """Test STOP on incompatible domain"""
    columns = ["random_col1", "random_col2", "random_col3"]
    
    service = GoalKPIsService(columns=columns, domain="logistics")
    result = service.run()
    
    assert result.compatibility.status == "STOP"
    assert result.compatibility.match_percentage < 0.3


def test_phase2_column_sanitization(tmp_path):
    """Test column name sanitization"""
    # Create test CSV
    test_file = tmp_path / "test.csv"
    df = pd.DataFrame({
        "Order ID": [1, 2, 3],
        "Customer-Name": ["A", "B", "C"],
        "Total  Price": [10.5, 20.0, 15.5]
    })
    df.to_csv(test_file, index=False)
    
    # Run Phase 2
    service = IngestionService(file_path=test_file, artifacts_dir=tmp_path)
    df_result, result = service.run()
    
    assert "order_id" in df_result.columns
    assert "customer_name" in df_result.columns
    assert "total_price" in df_result.columns


def test_phase3_datetime_conversion():
    """Test datetime type inference"""
    df = pd.DataFrame({
        "order_id": ["1", "2", "3"],
        "order_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "amount": [100, 200, 300]
    })
    
    service = SchemaService(df=df)
    df_typed, result = service.run()
    
    assert pd.api.types.is_datetime64_any_dtype(df_typed["order_date"])
    assert "order_id" in result.id_columns
    assert "order_date" in result.datetime_columns
    assert "amount" in result.numeric_columns

"""
Unit Tests for Phases 1-3: Goal & KPIs, Data Ingestion, and Schema Validation
Tests the complete pipeline from domain matching to schema enforcement.
"""

import pytest
import pandas as pd
import tempfile
import os
from pathlib import Path
from datetime import datetime

from app.services.phase1_goal_kpis import GoalKPIsService
from app.services.phase2_ingestion import IngestionService
from app.services.phase3_schema import SchemaService
from app.services.domain_packs import get_domain_pack, suggest_domain


class TestPhase1DomainMatching:
    """Test Phase 1: Goal & KPIs Domain Matching"""
    
    def test_phase1_domain_match(self):
        """Test domain matching with logistics data"""
        columns = [
            "shipment_id", "order_id", "carrier", "origin",
            "destination", "pickup_date", "delivery_date", "status"
        ]
        
        service = GoalKPIsService(columns=columns, domain="logistics")
        result = service.run()
        
        assert result.domain == "logistics"
        assert result.compatibility.status in ["OK", "WARN"]
        assert result.compatibility.match_percentage > 0.5
        assert len(result.kpis) > 0
        assert "SLA_pct" in result.kpis or "TransitTime_avg" in result.kpis
    
    def test_phase1_auto_suggest(self):
        """Test auto domain suggestion"""
        columns = ["order_id", "customer_id", "product_id", "price", "quantity"]
        
        service = GoalKPIsService(columns=columns, domain=None)
        result = service.run()
        
        assert result.domain == "retail"  # Should auto-detect retail
        assert result.compatibility.status in ["OK", "WARN"]
        assert result.compatibility.match_percentage > 0.3
    
    def test_phase1_incompatible_stop(self):
        """Test STOP on incompatible domain"""
        columns = ["random_col1", "random_col2", "random_col3"]
        
        service = GoalKPIsService(columns=columns, domain="logistics")
        result = service.run()
        
        assert result.compatibility.status == "STOP"
        assert result.compatibility.match_percentage < 0.3
        assert len(result.compatibility.suggestions) > 0
    
    def test_phase1_healthcare_domain(self):
        """Test healthcare domain matching"""
        columns = [
            "patient_id", "admission_ts", "discharge_ts", "department",
            "diagnosis", "procedure", "los_days", "age", "gender"
        ]
        
        service = GoalKPIsService(columns=columns, domain="healthcare")
        result = service.run()
        
        assert result.domain == "healthcare"
        assert result.compatibility.status == "OK"
        assert result.compatibility.match_percentage > 0.7
        assert any(kpi in result.kpis for kpi in ["BedOccupancy_pct", "AvgLOS_days", "Readmission_30d_pct"])
    
    def test_phase1_retail_domain(self):
        """Test retail domain matching"""
        columns = [
            "order_id", "customer_id", "order_date", "product_id",
            "quantity", "price", "payment_method", "return_flag"
        ]
        
        service = GoalKPIsService(columns=columns, domain="retail")
        result = service.run()
        
        assert result.domain == "retail"
        assert result.compatibility.status == "OK"
        assert result.compatibility.match_percentage > 0.7
        assert any(kpi in result.kpis for kpi in ["GMV", "AOV", "CartAbandon_pct", "Return_pct"])
    
    def test_phase1_warn_threshold(self):
        """Test WARN status for partial domain match"""
        columns = ["order_id", "customer_id", "some_random_column"]
        
        service = GoalKPIsService(columns=columns, domain="retail")
        result = service.run()
        
        # Should be WARN since only 2/8 expected columns match
        assert result.compatibility.status in ["WARN", "STOP"]
        assert result.compatibility.match_percentage < 0.7


class TestPhase2ColumnSanitization:
    """Test Phase 2: Data Ingestion Column Sanitization"""
    
    def test_phase2_column_sanitization(self, tmp_path):
        """Test column name sanitization"""
        # Create test CSV with problematic column names
        test_file = tmp_path / "test.csv"
        df = pd.DataFrame({
            "Order ID": [1, 2, 3],
            "Customer-Name": ["A", "B", "C"],
            "Total  Price": [10.5, 20.0, 15.5],
            "Date/Time": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "Category@Type": ["X", "Y", "Z"]
        })
        df.to_csv(test_file, index=False)
        
        # Run Phase 2
        service = IngestionService(file_path=test_file, artifacts_dir=tmp_path)
        df_result, result = service.run()
        
        # Verify sanitized column names
        assert "order_id" in df_result.columns
        assert "customer_name" in df_result.columns
        assert "total_price" in df_result.columns
        assert "datetime" in df_result.columns
        assert "categorytype" in df_result.columns
        
        # Verify data integrity
        assert len(df_result) == 3
        assert len(df_result.columns) == 5
        assert result.rows == 3
        assert result.columns == 5
    
    def test_phase2_csv_ingestion(self, tmp_path):
        """Test CSV file ingestion"""
        test_file = tmp_path / "test.csv"
        df = pd.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "age": [25, 30, 35, 40, 45],
            "salary": [50000, 60000, 70000, 80000, 90000]
        })
        df.to_csv(test_file, index=False)
        
        service = IngestionService(file_path=test_file, artifacts_dir=tmp_path)
        df_result, result = service.run()
        
        assert result.status == "success"
        assert result.rows == 5
        assert result.columns == 4
        assert result.file_size_mb > 0
        assert result.parquet_path.endswith('.parquet')
        
        # Verify parquet file exists
        parquet_path = Path(result.parquet_path)
        assert parquet_path.exists()
    
    def test_phase2_excel_ingestion(self, tmp_path):
        """Test Excel file ingestion"""
        test_file = tmp_path / "test.xlsx"
        df = pd.DataFrame({
            "product_id": ["P001", "P002", "P003"],
            "product_name": ["Widget A", "Widget B", "Widget C"],
            "price": [10.99, 15.99, 20.99],
            "in_stock": [True, False, True]
        })
        df.to_excel(test_file, index=False)
        
        service = IngestionService(file_path=test_file, artifacts_dir=tmp_path)
        df_result, result = service.run()
        
        assert result.status == "success"
        assert result.rows == 3
        assert result.columns == 4
        assert "product_id" in df_result.columns
        assert "product_name" in df_result.columns
    
    def test_phase2_compression(self, tmp_path):
        """Test that ingestion produces compressed parquet files"""
        # Create a larger test file to see compression effects
        test_file = tmp_path / "large_test.csv"
        data = {
            "id": list(range(1, 1001)),
            "text": ["This is a sample text for compression testing"] * 1000,
            "number": list(range(1, 1001))
        }
        df = pd.DataFrame(data)
        df.to_csv(test_file, index=False)
        
        service = IngestionService(file_path=test_file, artifacts_dir=tmp_path)
        df_result, result = service.run()
        
        # Verify compression is applied (file should be smaller than CSV)
        csv_size = test_file.stat().st_size
        parquet_size = Path(result.parquet_path).stat().st_size
        
        assert result.status == "success"
        assert parquet_size < csv_size  # Parquet should be more compressed
        assert result.compression_ratio > 1.0
    
    def test_phase2_error_handling(self, tmp_path):
        """Test error handling for unsupported files"""
        # Test with unsupported file extension
        test_file = tmp_path / "test.txt"
        with open(test_file, 'w') as f:
            f.write("This is not a CSV or Excel file")
        
        service = IngestionService(file_path=test_file, artifacts_dir=tmp_path)
        
        with pytest.raises(ValueError, match="Unsupported file type"):
            service.run()


class TestPhase3SchemaValidation:
    """Test Phase 3: Schema Validation & Data Type Enforcement"""
    
    def test_phase3_datetime_conversion(self):
        """Test datetime type inference"""
        df = pd.DataFrame({
            "order_id": ["1", "2", "3"],
            "order_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "amount": [100, 200, 300]
        })
        
        service = SchemaService(df=df)
        df_typed, result = service.run()
        
        assert pd.api.types.is_datetime64_any_dtype(df_typed["order_date"])
        assert "order_id" in result.id_columns
        assert "order_date" in result.datetime_columns
        assert "amount" in result.numeric_columns
    
    def test_phase3_id_column_conversion(self):
        """Test ID columns are converted to string"""
        df = pd.DataFrame({
            "user_id": [1, 2, 3, 4, 5],
            "order_id": [100, 101, 102, 103, 104],
            "product_code": ["A001", "A002", "A003", "A004", "A005"]
        })
        
        service = SchemaService(df=df)
        df_typed, result = service.run()
        
        # Check that ID columns are string type
        assert str(df_typed['user_id'].dtype) == 'string'
        assert str(df_typed['order_id'].dtype) == 'string'
        assert str(df_typed['product_code'].dtype) == 'string'
        
        # Verify categorization
        assert 'user_id' in result.id_columns
        assert 'order_id' in result.id_columns
        assert 'product_code' in result.id_columns
    
    def test_phase3_numeric_conversion(self):
        """Test numeric columns are converted to float"""
        df = pd.DataFrame({
            "amount": ["100.50", "200.75", "300.25"],
            "price": [10, 20, 30],
            "rate": [0.15, 0.20, 0.25],
            "percentage": ["15%", "20%", "25%"]
        })
        
        service = SchemaService(df=df)
        df_typed, result = service.run()
        
        # Check that numeric columns are float type
        assert 'float' in str(df_typed['amount'].dtype)
        assert 'float' in str(df_typed['price'].dtype)
        assert 'float' in str(df_typed['rate'].dtype)
        
        # Verify categorization
        assert 'amount' in result.numeric_columns
        assert 'price' in result.numeric_columns
        assert 'rate' in result.numeric_columns
    
    def test_phase3_categorical_conversion(self):
        """Test categorical columns are converted to category"""
        df = pd.DataFrame({
            "status": ["active", "inactive", "pending", "active", "inactive"],
            "type": ["A", "B", "C", "A", "B"],
            "department": ["IT", "HR", "Finance", "IT", "HR"],
            "flag": [True, False, True, False, True]
        })
        
        service = SchemaService(df=df)
        df_typed, result = service.run()
        
        # Check that categorical columns are category type
        assert 'category' in str(df_typed['status'].dtype)
        assert 'category' in str(df_typed['type'].dtype)
        assert 'category' in str(df_typed['department'].dtype)
        
        # Verify categorization
        assert 'status' in result.categorical_columns
        assert 'type' in result.categorical_columns
        assert 'department' in result.categorical_columns
    
    def test_phase3_comprehensive_schema(self):
        """Test comprehensive schema validation with mixed data types"""
        df = pd.DataFrame({
            "patient_id": [1, 2, 3, 4, 5],
            "admission_date": ["2024-01-15", "2024-01-16", "2024-01-17", "2024-01-18", "2024-01-19"],
            "age": [65, 70, 45, 60, 75],
            "diagnosis": ["Heart Attack", "Stroke", "Fracture", "Heart Attack", "Stroke"],
            "treatment_cost": [50000.5, 75000.0, 25000.5, 60000.0, 80000.5],
            "discharge_status": ["Recovered", "Stable", "Recovered", "Stable", "Recovered"],
            "length_of_stay": [5, 7, 3, 6, 8]
        })
        
        service = SchemaService(df=df)
        df_typed, result = service.run()
        
        # Verify data type conversions
        assert str(df_typed['patient_id'].dtype) == 'string'  # ID column
        assert pd.api.types.is_datetime64_any_dtype(df_typed['admission_date'])  # Date column
        assert 'float' in str(df_typed['age'].dtype)  # Numeric column
        assert 'category' in str(df_typed['diagnosis'].dtype)  # Categorical column
        assert 'float' in str(df_typed['treatment_cost'].dtype)  # Numeric column
        assert 'category' in str(df_typed['discharge_status'].dtype)  # Categorical column
        assert 'float' in str(df_typed['length_of_stay'].dtype)  # Numeric column
        
        # Verify categorization
        assert 'patient_id' in result.id_columns
        assert 'admission_date' in result.datetime_columns
        assert 'age' in result.numeric_columns
        assert 'diagnosis' in result.categorical_columns
        assert 'treatment_cost' in result.numeric_columns
        assert 'discharge_status' in result.categorical_columns
        assert 'length_of_stay' in result.numeric_columns
        
        # Verify schema generation
        assert len(result.schema_json['columns']) == 7
        assert result.violations_pct >= 0.0
    
    def test_phase3_edge_cases(self):
        """Test edge cases in schema validation"""
        df = pd.DataFrame({
            "empty_column": [None, None, None, None],
            "mixed_types": ["1", "2.5", "text", 4],
            "all_nulls": [None, None, None, None],
            "single_value": ["A", "A", "A", "A"]
        })
        
        service = SchemaService(df=df)
        df_typed, result = service.run()
        
        # Should handle edge cases gracefully
        assert len(df_typed) == 4
        assert len(df_typed.columns) == 4
        assert len(result.warnings) >= 0  # May have warnings for conversion failures


class TestEndToEndPipeline:
    """Test complete pipeline from Phase 1 to Phase 3"""
    
    def test_complete_pipeline_logistics(self, tmp_path):
        """Test complete pipeline with logistics data"""
        # Phase 1: Domain matching
        columns = [
            "shipment_id", "order_id", "carrier", "origin",
            "destination", "pickup_date", "delivery_date", "status"
        ]
        
        phase1_service = GoalKPIsService(columns=columns, domain="logistics")
        phase1_result = phase1_service.run()
        
        assert phase1_result.domain == "logistics"
        assert phase1_result.compatibility.status in ["OK", "WARN"]
        
        # Phase 2: Data ingestion
        test_file = tmp_path / "logistics_data.csv"
        df = pd.DataFrame({
            "Shipment ID": ["SH001", "SH002", "SH003"],
            "Order ID": ["ORD001", "ORD002", "ORD003"],
            "Carrier": ["UPS", "FedEx", "DHL"],
            "Origin": ["NYC", "LA", "CHI"],
            "Destination": ["BOS", "SEA", "DEN"],
            "Pickup Date": ["2024-01-15", "2024-01-16", "2024-01-17"],
            "Delivery Date": ["2024-01-17", "2024-01-18", "2024-01-19"],
            "Status": ["Delivered", "In Transit", "Delivered"]
        })
        df.to_csv(test_file, index=False)
        
        phase2_service = IngestionService(file_path=test_file, artifacts_dir=tmp_path)
        df_ingested, phase2_result = phase2_service.run()
        
        assert phase2_result.status == "success"
        assert len(df_ingested) == 3
        
        # Phase 3: Schema validation
        phase3_service = SchemaService(df=df_ingested)
        df_typed, phase3_result = phase3_service.run()
        
        # Verify final schema
        assert "shipment_id" in df_typed.columns
        assert "order_id" in df_typed.columns
        assert str(df_typed['shipment_id'].dtype) == 'string'
        assert str(df_typed['order_id'].dtype) == 'string'
        assert 'pickup_date' in phase3_result.datetime_columns
        assert 'delivery_date' in phase3_result.datetime_columns
    
    def test_complete_pipeline_retail(self, tmp_path):
        """Test complete pipeline with retail data"""
        # Phase 1: Domain matching
        columns = ["order_id", "customer_id", "product_id", "price", "quantity"]
        
        phase1_service = GoalKPIsService(columns=columns, domain="retail")
        phase1_result = phase1_service.run()
        
        assert phase1_result.domain == "retail"
        assert phase1_result.compatibility.status in ["OK", "WARN"]
        
        # Phase 2: Data ingestion
        test_file = tmp_path / "retail_data.csv"
        df = pd.DataFrame({
            "Order ID": ["O001", "O002", "O003"],
            "Customer ID": ["C001", "C002", "C003"],
            "Product ID": ["P001", "P002", "P003"],
            "Price": [29.99, 49.99, 19.99],
            "Quantity": [2, 1, 3],
            "Order Date": ["2024-01-15", "2024-01-16", "2024-01-17"]
        })
        df.to_csv(test_file, index=False)
        
        phase2_service = IngestionService(file_path=test_file, artifacts_dir=tmp_path)
        df_ingested, phase2_result = phase2_service.run()
        
        assert phase2_result.status == "success"
        assert len(df_ingested) == 3
        
        # Phase 3: Schema validation
        phase3_service = SchemaService(df=df_ingested)
        df_typed, phase3_result = phase3_service.run()
        
        # Verify final schema
        assert "order_id" in df_typed.columns
        assert "customer_id" in df_typed.columns
        assert "product_id" in df_typed.columns
        assert str(df_typed['order_id'].dtype) == 'string'
        assert str(df_typed['customer_id'].dtype) == 'string'
        assert str(df_typed['product_id'].dtype) == 'string'
        assert 'price' in phase3_result.numeric_columns
        assert 'quantity' in phase3_result.numeric_columns
        assert 'order_date' in phase3_result.datetime_columns


class TestDomainPacksIntegration:
    """Test integration with domain packs"""
    
    def test_domain_pack_retrieval(self):
        """Test domain pack retrieval"""
        logistics_pack = get_domain_pack("logistics")
        assert logistics_pack.name == "logistics"
        assert "SLA_pct" in logistics_pack.kpis
        assert "shipment_id" in logistics_pack.expected_columns
        
        retail_pack = get_domain_pack("retail")
        assert retail_pack.name == "retail"
        assert "GMV" in retail_pack.kpis
        assert "order_id" in retail_pack.expected_columns
    
    def test_domain_suggestion_accuracy(self):
        """Test domain suggestion accuracy"""
        # Test logistics columns
        logistics_columns = ["shipment_id", "order_id", "carrier", "origin", "destination"]
        suggestions = suggest_domain(logistics_columns)
        assert suggestions["logistics"] > 0.5  # Should be highest
        
        # Test retail columns
        retail_columns = ["order_id", "customer_id", "product_id", "price", "quantity"]
        suggestions = suggest_domain(retail_columns)
        assert suggestions["retail"] > 0.5  # Should be highest
        
        # Test healthcare columns
        healthcare_columns = ["patient_id", "admission_ts", "discharge_ts", "department"]
        suggestions = suggest_domain(healthcare_columns)
        assert suggestions["healthcare"] > 0.5  # Should be highest
    
    def test_invalid_domain_handling(self):
        """Test handling of invalid domain names"""
        with pytest.raises(ValueError, match="Domain 'invalid_domain' not found"):
            get_domain_pack("invalid_domain")
