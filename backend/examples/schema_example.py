"""
Example usage of SchemaService - Phase 3 Schema & Dtypes
"""

import pandas as pd
from app.services.phase3_schema import SchemaService


def example_mixed_data_types():
    """Example with mixed data types"""
    print("=== Mixed Data Types Example ===")
    
    # Create data with mixed types
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
    df = pd.DataFrame(data)
    
    print("Original data types:")
    for col, dtype in df.dtypes.items():
        print(f"  {col}: {dtype}")
    
    # Run schema service
    service = SchemaService(df=df)
    df_typed, result = service.run()
    
    print("\nInferred data types:")
    for col, dtype in result.dtypes.items():
        print(f"  {col}: {dtype}")
    
    print(f"\nColumn categorization:")
    print(f"  ID columns: {result.id_columns}")
    print(f"  Datetime columns: {result.datetime_columns}")
    print(f"  Numeric columns: {result.numeric_columns}")
    print(f"  Categorical columns: {result.categorical_columns}")
    
    if result.warnings:
        print(f"\nWarnings:")
        for warning in result.warnings:
            print(f"  {warning}")
    
    print(f"\nSchema JSON structure:")
    print(f"  Columns: {len(result.schema_json['columns'])}")
    print(f"  Coerce: {result.schema_json['coerce']}")


def example_logistics_data():
    """Example with logistics data"""
    print("\n=== Logistics Data Example ===")
    
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
    
    print("Logistics data schema inference:")
    print(f"  ID columns: {result.id_columns}")
    print(f"  Datetime columns: {result.datetime_columns}")
    print(f"  Numeric columns: {result.numeric_columns}")
    print(f"  Categorical columns: {result.categorical_columns}")
    
    # Show specific type conversions
    print("\nType conversions:")
    for col in df.columns:
        original_type = str(df[col].dtype)
        inferred_type = result.dtypes[col]
        if original_type != inferred_type:
            print(f"  {col}: {original_type} → {inferred_type}")


def example_healthcare_data():
    """Example with healthcare data"""
    print("\n=== Healthcare Data Example ===")
    
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
    
    print("Healthcare data schema inference:")
    print(f"  ID columns: {result.id_columns}")
    print(f"  Datetime columns: {result.datetime_columns}")
    print(f"  Numeric columns: {result.numeric_columns}")
    print(f"  Categorical columns: {result.categorical_columns}")
    
    # Show schema JSON for a few columns
    print("\nSchema JSON sample:")
    for col in list(result.schema_json['columns'].keys())[:3]:
        col_schema = result.schema_json['columns'][col]
        print(f"  {col}: {col_schema}")


def example_edge_cases():
    """Example with edge cases"""
    print("\n=== Edge Cases Example ===")
    
    # Create data with edge cases
    data = {
        'id': ['1', '2', '3', '4', '5'],  # String IDs
        'mixed_numeric': ['100', '200', 'invalid', '300', '400'],  # Mixed numeric/string
        'high_cardinality': [f'value_{i}' for i in range(1, 6)],  # High cardinality
        'low_cardinality': ['A', 'B', 'A', 'B', 'A'],  # Low cardinality
        'iso_date': ['2020-01-15T10:30:00Z', '2019-03-20T14:45:00Z', '2021-06-10T09:15:00Z'],
        'slash_date': ['01/15/2020', '03/20/2019', '06/10/2021'],
        'timestamp': [1579017600, 1553097600, 1623340800]  # Unix timestamps
    }
    df = pd.DataFrame(data)
    
    service = SchemaService(df=df)
    df_typed, result = service.run()
    
    print("Edge cases schema inference:")
    print(f"  ID columns: {result.id_columns}")
    print(f"  Datetime columns: {result.datetime_columns}")
    print(f"  Numeric columns: {result.numeric_columns}")
    print(f"  Categorical columns: {result.categorical_columns}")
    
    if result.warnings:
        print(f"\nWarnings:")
        for warning in result.warnings:
            print(f"  {warning}")
    
    # Show type inference results
    print("\nType inference results:")
    for col in df.columns:
        original_type = str(df[col].dtype)
        inferred_type = result.dtypes[col]
        print(f"  {col}: {original_type} → {inferred_type}")


def example_schema_json():
    """Example showing schema JSON generation"""
    print("\n=== Schema JSON Generation Example ===")
    
    data = {
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35],
        'active': [True, True, False]
    }
    df = pd.DataFrame(data)
    
    service = SchemaService(df=df)
    df_typed, result = service.run()
    
    print("Generated schema JSON:")
    print(f"  Structure: {list(result.schema_json.keys())}")
    print(f"  Coerce: {result.schema_json['coerce']}")
    print(f"  Index: {result.schema_json['index']}")
    
    print("\nColumn definitions:")
    for col, col_schema in result.schema_json['columns'].items():
        print(f"  {col}:")
        print(f"    dtype: {col_schema['dtype']}")
        print(f"    nullable: {col_schema['nullable']}")
        print(f"    unique: {col_schema['unique']}")


if __name__ == "__main__":
    print("SchemaService Examples")
    print("=" * 50)
    
    example_mixed_data_types()
    example_logistics_data()
    example_healthcare_data()
    example_edge_cases()
    example_schema_json()
    
    print("\nAll examples completed!")


