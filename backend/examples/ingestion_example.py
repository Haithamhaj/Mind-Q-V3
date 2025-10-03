"""
Example usage of IngestionService - Phase 2 Ingestion & Landing
"""

import pandas as pd
import tempfile
import os
from pathlib import Path
from app.services.phase2_ingestion import IngestionService


def example_csv_ingestion():
    """Example with CSV file ingestion"""
    print("=== CSV Ingestion Example ===")
    
    # Create sample data with problematic column names
    data = {
        'Shipment ID': [1, 2, 3, 4, 5],
        'Order Number': [100, 101, 102, 103, 104],
        'Carrier Name': ['UPS', 'FedEx', 'DHL', 'UPS', 'FedEx'],
        'Origin City': ['New York', 'Los Angeles', 'Chicago', 'New York', 'Los Angeles'],
        'Destination-City': ['Boston', 'Seattle', 'Denver', 'Boston', 'Seattle'],
        'Pickup Date': ['2020-01-15', '2019-03-20', '2021-06-10', '2018-11-05', '2022-02-28'],
        'Status': ['Delivered', 'In Transit', 'Delivered', 'Delivered', 'In Transit']
    }
    df = pd.DataFrame(data)
    
    # Create temporary CSV file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        df.to_csv(f.name, index=False)
        csv_path = f.name
    
    try:
        # Create artifacts directory
        artifacts_dir = Path(tempfile.mkdtemp())
        
        # Run ingestion
        service = IngestionService(file_path=Path(csv_path), artifacts_dir=artifacts_dir)
        df_result, result = service.run()
        
        print(f"Original columns: {df.columns.tolist()}")
        print(f"Sanitized columns: {df_result.columns.tolist()}")
        print(f"Rows: {result.rows}")
        print(f"Columns: {result.columns}")
        print(f"File size: {result.file_size_mb} MB")
        print(f"Parquet path: {result.parquet_path}")
        print(f"Message: {result.message}")
        
        # Verify parquet file
        parquet_path = Path(result.parquet_path)
        if parquet_path.exists():
            df_parquet = pd.read_parquet(parquet_path)
            print(f"Parquet file verified: {len(df_parquet)} rows × {len(df_parquet.columns)} columns")
        
    finally:
        # Clean up
        os.unlink(csv_path)
        import shutil
        shutil.rmtree(artifacts_dir)


def example_excel_ingestion():
    """Example with Excel file ingestion"""
    print("\n=== Excel Ingestion Example ===")
    
    # Create sample data
    data = {
        'Patient ID': [1, 2, 3, 4, 5],
        'Admission Date': ['2020-01-15', '2019-03-20', '2021-06-10', '2018-11-05', '2022-02-28'],
        'Department': ['Cardiology', 'Neurology', 'Orthopedics', 'Cardiology', 'Neurology'],
        'Diagnosis Code': ['I21.9', 'I63.9', 'S72.001A', 'I21.9', 'I63.9'],
        'Length of Stay': [3, 5, 2, 4, 6],
        'Age': [65, 70, 45, 60, 75],
        'Gender': ['M', 'F', 'M', 'F', 'M']
    }
    df = pd.DataFrame(data)
    
    # Create temporary Excel file
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
        df.to_excel(f.name, index=False)
        excel_path = f.name
    
    try:
        # Create artifacts directory
        artifacts_dir = Path(tempfile.mkdtemp())
        
        # Run ingestion
        service = IngestionService(file_path=Path(excel_path), artifacts_dir=artifacts_dir)
        df_result, result = service.run()
        
        print(f"Original columns: {df.columns.tolist()}")
        print(f"Sanitized columns: {df_result.columns.tolist()}")
        print(f"Rows: {result.rows}")
        print(f"Columns: {result.columns}")
        print(f"File size: {result.file_size_mb} MB")
        print(f"Parquet path: {result.parquet_path}")
        print(f"Message: {result.message}")
        
    finally:
        # Clean up
        os.unlink(excel_path)
        import shutil
        shutil.rmtree(artifacts_dir)


def example_column_sanitization():
    """Example showing column sanitization"""
    print("\n=== Column Sanitization Example ===")
    
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
    
    # Create temporary CSV file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        df.to_csv(f.name, index=False)
        csv_path = f.name
    
    try:
        # Create artifacts directory
        artifacts_dir = Path(tempfile.mkdtemp())
        
        # Run ingestion
        service = IngestionService(file_path=Path(csv_path), artifacts_dir=artifacts_dir)
        df_result, result = service.run()
        
        print("Column sanitization results:")
        for original, sanitized in zip(df.columns, df_result.columns):
            print(f"  '{original}' → '{sanitized}'")
        
        print(f"\nSanitized columns: {df_result.columns.tolist()}")
        
    finally:
        # Clean up
        os.unlink(csv_path)
        import shutil
        shutil.rmtree(artifacts_dir)


def example_large_dataset():
    """Example with larger dataset"""
    print("\n=== Large Dataset Example ===")
    
    # Create larger dataset
    data = {
        'ID': list(range(1, 1001)),
        'Name': [f'Item_{i}' for i in range(1, 1001)],
        'Value': [i * 10.5 for i in range(1, 1001)],
        'Category': ['A', 'B', 'C'] * 334 + ['A']  # 1000 items
    }
    df = pd.DataFrame(data)
    
    # Create temporary CSV file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        df.to_csv(f.name, index=False)
        csv_path = f.name
    
    try:
        # Create artifacts directory
        artifacts_dir = Path(tempfile.mkdtemp())
        
        # Run ingestion
        service = IngestionService(file_path=Path(csv_path), artifacts_dir=artifacts_dir)
        df_result, result = service.run()
        
        print(f"Rows: {result.rows:,}")
        print(f"Columns: {result.columns}")
        print(f"File size: {result.file_size_mb} MB")
        print(f"Compression ratio: {result.file_size_mb / (Path(csv_path).stat().st_size / (1024 * 1024)):.2f}x")
        print(f"Message: {result.message}")
        
    finally:
        # Clean up
        os.unlink(csv_path)
        import shutil
        shutil.rmtree(artifacts_dir)


if __name__ == "__main__":
    print("IngestionService Examples")
    print("=" * 50)
    
    example_csv_ingestion()
    example_excel_ingestion()
    example_column_sanitization()
    example_large_dataset()
    
    print("\nAll examples completed!")


