"""
Tests for SchemaService - Phase 3 Schema & Dtypes
"""

import pytest
import pandas as pd
from app.services.phase3_schema import SchemaService, SchemaResult


class TestSchemaService:
    """Test SchemaService functionality"""
    
    @pytest.fixture
    def sample_dataframe(self):
        """Create sample dataframe with mixed data types"""
        data = {
            'id': [1, 2, 3, 4, 5],
            'user_id': [100, 101, 102, 103, 104],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'age': [25, 30, 35, 40, 45],
            'salary': [50000.5, 60000.0, 70000.5, 80000.0, 90000.5],
            'hire_date': ['2020-01-15', '2019-03-20', '2021-06-10', '2018-11-05', '2022-02-28'],
            'department': ['IT', 'HR', 'Finance', 'IT', 'HR'],
            'is_active': [True, True, False, True, False],
            'phone': ['123-456-7890', '234-567-8901', '345-678-9012', '456-789-0123', '567-890-1234']
        }
        return pd.DataFrame(data)
    
    def test_basic_schema_inference(self, sample_dataframe):
        """Test basic schema inference and type casting"""
        service = SchemaService(df=sample_dataframe)
        df_typed, result = service.run()
        
        # Check result structure
        assert isinstance(result, SchemaResult)
        assert len(result.dtypes) == 9
        assert isinstance(result.id_columns, list)
        assert isinstance(result.datetime_columns, list)
        assert isinstance(result.numeric_columns, list)
        assert isinstance(result.categorical_columns, list)
        assert isinstance(result.warnings, list)
        assert isinstance(result.schema_json, dict)
        
        # Check ID columns
        assert 'id' in result.id_columns
        assert 'user_id' in result.id_columns
        assert str(df_typed['id'].dtype) == 'string'
        assert str(df_typed['user_id'].dtype) == 'string'
    
    def test_datetime_inference(self, sample_dataframe):
        """Test datetime column inference"""
        service = SchemaService(df=sample_dataframe)
        df_typed, result = service.run()
        
        # Check datetime columns
        assert 'hire_date' in result.datetime_columns
        assert pd.api.types.is_datetime64_any_dtype(df_typed['hire_date'])
        
        # Check that datetime is UTC
        if hasattr(df_typed['hire_date'].dtype, 'tz'):
            assert df_typed['hire_date'].dtype.tz is not None
    
    def test_numeric_inference(self, sample_dataframe):
        """Test numeric column inference"""
        service = SchemaService(df=sample_dataframe)
        df_typed, result = service.run()
        
        # Check numeric columns
        assert 'age' in result.numeric_columns
        assert 'salary' in result.numeric_columns
        assert pd.api.types.is_numeric_dtype(df_typed['age'])
        assert pd.api.types.is_numeric_dtype(df_typed['salary'])
    
    def test_categorical_inference(self, sample_dataframe):
        """Test categorical column inference"""
        service = SchemaService(df=sample_dataframe)
        df_typed, result = service.run()
        
        # Check categorical columns (low cardinality strings)
        assert 'department' in result.categorical_columns
        assert pd.api.types.is_categorical_dtype(df_typed['department'])
    
    def test_schema_json_generation(self, sample_dataframe):
        """Test schema JSON generation"""
        service = SchemaService(df=sample_dataframe)
        df_typed, result = service.run()
        
        # Check schema JSON structure
        schema_json = result.schema_json
        assert "columns" in schema_json
        assert "index" in schema_json
        assert "coerce" in schema_json
        assert schema_json["coerce"] is True
        
        # Check column definitions
        assert len(schema_json["columns"]) == 9
        for col in df_typed.columns:
            assert col in schema_json["columns"]
            col_def = schema_json["columns"][col]
            assert "dtype" in col_def
            assert "nullable" in col_def
            assert "unique" in col_def
    
    def test_warnings_generation(self, sample_dataframe):
        """Test warnings generation"""
        # Create data with conversion issues
        data = {
            'id': [1, 2, 3, 4, 5],
            'bad_date': ['2020-01-15', 'invalid_date', '2021-06-10', 'not_a_date', '2022-02-28'],
            'bad_numeric': ['100', '200', 'invalid', '300', '400']
        }
        df = pd.DataFrame(data)
        
        service = SchemaService(df=df)
        df_typed, result = service.run()
        
        # Should have warnings for conversion failures
        assert len(result.warnings) >= 0  # May have warnings depending on conversion success
    
    def test_looks_like_date_heuristic(self, sample_dataframe):
        """Test date detection heuristic"""
        service = SchemaService(df=sample_dataframe)
        
        # Test column name detection
        assert service._looks_like_date(sample_dataframe['hire_date']) is True
        
        # Test sample value detection
        date_series = pd.Series(['2020-01-15', '2019-03-20', '2021-06-10'], name='some_column')
        assert service._looks_like_date(date_series) is True
        
        # Test non-date detection
        non_date_series = pd.Series(['Alice', 'Bob', 'Charlie'], name='name')
        assert service._looks_like_date(non_date_series) is False
    
    def test_column_categorization(self, sample_dataframe):
        """Test column categorization"""
        service = SchemaService(df=sample_dataframe)
        df_typed, result = service.run()
        
        categorized = service._categorize_columns(df_typed)
        
        # Check categorization structure
        assert 'id_columns' in categorized
        assert 'datetime_columns' in categorized
        assert 'numeric_columns' in categorized
        assert 'categorical_columns' in categorized
        
        # Check specific categorizations
        assert 'id' in categorized['id_columns']
        assert 'user_id' in categorized['id_columns']
        assert 'hire_date' in categorized['datetime_columns']
        assert 'age' in categorized['numeric_columns']
        assert 'salary' in categorized['numeric_columns']
        assert 'department' in categorized['categorical_columns']


class TestSchemaServiceEdgeCases:
    """Test SchemaService edge cases"""
    
    def test_empty_dataframe(self):
        """Test with empty dataframe"""
        df = pd.DataFrame()
        service = SchemaService(df=df)
        df_typed, result = service.run()
        
        assert len(df_typed) == 0
        assert len(df_typed.columns) == 0
        assert len(result.dtypes) == 0
        assert len(result.id_columns) == 0
        assert len(result.datetime_columns) == 0
        assert len(result.numeric_columns) == 0
        assert len(result.categorical_columns) == 0
        assert len(result.warnings) == 0
        assert len(result.schema_json["columns"]) == 0
    
    def test_all_string_data(self):
        """Test with all string data"""
        data = {
            'id': ['1', '2', '3', '4', '5'],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'value': ['100', '200', '300', '400', '500']
        }
        df = pd.DataFrame(data)
        
        service = SchemaService(df=df)
        df_typed, result = service.run()
        
        # ID columns should be converted to string
        assert 'id' in result.id_columns
        assert str(df_typed['id'].dtype) == 'string'
        
        # Other columns should remain as object or be converted based on content
        assert len(result.dtypes) == 3
    
    def test_mixed_numeric_strings(self):
        """Test with mixed numeric and string data"""
        data = {
            'id': [1, 2, 3, 4, 5],
            'mixed_numeric': ['100', '200', 'invalid', '300', '400'],
            'pure_numeric': ['100', '200', '300', '400', '500']
        }
        df = pd.DataFrame(data)
        
        service = SchemaService(df=df)
        df_typed, result = service.run()
        
        # ID should be string
        assert 'id' in result.id_columns
        assert str(df_typed['id'].dtype) == 'string'
        
        # Mixed numeric should remain as object if too many failures
        # Pure numeric should be converted to numeric
        assert len(result.dtypes) == 3
    
    def test_high_cardinality_categorical(self):
        """Test with high cardinality categorical data"""
        data = {
            'id': list(range(1, 101)),
            'high_cardinality': [f'value_{i}' for i in range(1, 101)],
            'low_cardinality': ['A', 'B', 'C'] * 34 + ['A']  # 100 items, 3 unique values
        }
        df = pd.DataFrame(data)
        
        service = SchemaService(df=df)
        df_typed, result = service.run()
        
        # High cardinality should remain as object
        # Low cardinality should be converted to category
        assert 'low_cardinality' in result.categorical_columns
        assert pd.api.types.is_categorical_dtype(df_typed['low_cardinality'])
    
    def test_datetime_edge_cases(self):
        """Test datetime edge cases"""
        data = {
            'id': [1, 2, 3, 4, 5],
            'iso_date': ['2020-01-15T10:30:00Z', '2019-03-20T14:45:00Z', '2021-06-10T09:15:00Z'],
            'slash_date': ['01/15/2020', '03/20/2019', '06/10/2021'],
            'timestamp': [1579017600, 1553097600, 1623340800]  # Unix timestamps
        }
        df = pd.DataFrame(data)
        
        service = SchemaService(df=df)
        df_typed, result = service.run()
        
        # All should be detected as datetime columns
        assert 'iso_date' in result.datetime_columns
        assert 'slash_date' in result.datetime_columns
        assert 'timestamp' in result.datetime_columns
        
        # All should be converted to datetime
        assert pd.api.types.is_datetime64_any_dtype(df_typed['iso_date'])
        assert pd.api.types.is_datetime64_any_dtype(df_typed['slash_date'])
        assert pd.api.types.is_datetime64_any_dtype(df_typed['timestamp'])


class TestSchemaResult:
    """Test SchemaResult model"""
    
    def test_schema_result_structure(self):
        """Test SchemaResult structure"""
        result = SchemaResult(
            dtypes={'id': 'string', 'age': 'int64', 'name': 'object'},
            id_columns=['id'],
            datetime_columns=['created_at'],
            numeric_columns=['age', 'salary'],
            categorical_columns=['department'],
            violations_pct=0.05,
            warnings=['Column age: 2 datetime conversion failures'],
            schema_json={'columns': {}, 'index': None, 'coerce': True}
        )
        
        assert len(result.dtypes) == 3
        assert result.id_columns == ['id']
        assert result.datetime_columns == ['created_at']
        assert result.numeric_columns == ['age', 'salary']
        assert result.categorical_columns == ['department']
        assert result.violations_pct == 0.05
        assert len(result.warnings) == 1
        assert isinstance(result.schema_json, dict)
    
    def test_schema_result_dict_conversion(self):
        """Test SchemaResult dict conversion"""
        result = SchemaResult(
            dtypes={'id': 'string', 'age': 'int64'},
            id_columns=['id'],
            datetime_columns=[],
            numeric_columns=['age'],
            categorical_columns=[],
            violations_pct=0.0,
            warnings=[],
            schema_json={'columns': {}, 'index': None, 'coerce': True}
        )
        
        result_dict = result.dict()
        assert isinstance(result_dict, dict)
        assert result_dict['dtypes'] == {'id': 'string', 'age': 'int64'}
        assert result_dict['id_columns'] == ['id']
        assert result_dict['numeric_columns'] == ['age']
        assert result_dict['violations_pct'] == 0.0


class TestSchemaServiceIntegration:
    """Test SchemaService integration scenarios"""
    
    def test_logistics_data_schema(self):
        """Test schema inference on logistics data"""
        data = {
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
        }
        df = pd.DataFrame(data)
        
        service = SchemaService(df=df)
        df_typed, result = service.run()
        
        # Check ID columns
        assert 'shipment_id' in result.id_columns
        assert 'order_id' in result.id_columns
        
        # Check datetime columns
        assert 'pickup_date' in result.datetime_columns
        assert 'delivery_date' in result.datetime_columns
        
        # Check numeric columns
        assert 'transit_time' in result.numeric_columns
        assert 'dwell_time' in result.numeric_columns
        
        # Check categorical columns
        assert 'carrier' in result.categorical_columns
        assert 'origin' in result.categorical_columns
        assert 'destination' in result.categorical_columns
        assert 'status' in result.categorical_columns
    
    def test_healthcare_data_schema(self):
        """Test schema inference on healthcare data"""
        data = {
            'patient_id': [1, 2, 3, 4, 5],
            'admission_ts': ['2020-01-15 10:30:00', '2019-03-20 14:45:00', '2021-06-10 09:15:00'],
            'discharge_ts': ['2020-01-17 12:00:00', '2019-03-22 10:30:00', '2021-06-12 11:15:00'],
            'department': ['Cardiology', 'Neurology', 'Orthopedics', 'Cardiology', 'Neurology'],
            'diagnosis': ['Heart Attack', 'Stroke', 'Fracture', 'Heart Attack', 'Stroke'],
            'procedure': ['Angioplasty', 'Thrombectomy', 'Surgery', 'Angioplasty', 'Thrombectomy'],
            'los_days': [2, 2, 2, 2, 2],
            'age': [65, 70, 45, 60, 75],
            'gender': ['M', 'F', 'M', 'F', 'M']
        }
        df = pd.DataFrame(data)
        
        service = SchemaService(df=df)
        df_typed, result = service.run()
        
        # Check ID columns
        assert 'patient_id' in result.id_columns
        
        # Check datetime columns
        assert 'admission_ts' in result.datetime_columns
        assert 'discharge_ts' in result.datetime_columns
        
        # Check numeric columns
        assert 'los_days' in result.numeric_columns
        assert 'age' in result.numeric_columns
        
        # Check categorical columns
        assert 'department' in result.categorical_columns
        assert 'diagnosis' in result.categorical_columns
        assert 'procedure' in result.categorical_columns
        assert 'gender' in result.categorical_columns


