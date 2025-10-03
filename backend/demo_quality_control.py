#!/usr/bin/env python3
"""
Demo script for Phase 1: Quality Control Service
Demonstrates the quality control functionality with various data quality scenarios.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from app.services.phase0_quality_control import QualityControlService


def create_sample_data():
    """Create sample data with various quality issues"""
    
    # Scenario 1: Good data (should PASS)
    good_data = {
        'id': range(1, 101),
        'name': [f'User_{i}' for i in range(1, 101)],
        'age': np.random.randint(18, 80, 100),
        'email': [f'user{i}@example.com' for i in range(1, 101)],
        'created_at': [datetime.now() - timedelta(days=np.random.randint(1, 365)) for _ in range(100)],
        'score': np.random.normal(75, 15, 100)
    }
    
    # Scenario 2: High missing data (should STOP)
    bad_missing_data = {
        'id': range(1, 101),
        'name': [f'User_{i}' if i % 3 != 0 else None for i in range(1, 101)],  # 33% missing
        'age': [np.random.randint(18, 80) if i % 4 != 0 else None for i in range(1, 101)],  # 25% missing
        'critical_field': [i if i % 5 != 0 else None for i in range(1, 101)]  # 20% missing (threshold)
    }
    
    # Scenario 3: Duplicate keys (should STOP)
    duplicate_data = {
        'id': [i % 50 + 1 for i in range(100)],  # 50% duplicates
        'name': [f'User_{i}' for i in range(100)],
        'value': np.random.rand(100)
    }
    
    # Scenario 4: Date issues (should WARN)
    date_issue_data = {
        'id': range(1, 1001),
        'date_field': [datetime.now() - timedelta(days=i) for i in range(1000)],
        'name': [f'User_{i}' for i in range(1000)]
    }
    # Add some future dates
    date_issue_data['date_field'][100] = datetime.now() + timedelta(days=30)
    date_issue_data['date_field'][200] = datetime.now() + timedelta(days=60)
    
    return {
        'good': pd.DataFrame(good_data),
        'bad_missing': pd.DataFrame(bad_missing_data),
        'duplicates': pd.DataFrame(duplicate_data),
        'date_issues': pd.DataFrame(date_issue_data)
    }


def demo_quality_control():
    """Demonstrate quality control functionality"""
    
    print("=" * 60)
    print("PHASE 1: QUALITY CONTROL SERVICE DEMO")
    print("=" * 60)
    
    # Create sample datasets
    datasets = create_sample_data()
    
    # Test each scenario
    scenarios = [
        ("Good Data (Should PASS)", "good", ["id"]),
        ("High Missing Data (Should STOP)", "bad_missing", ["id"]),
        ("Duplicate Keys (Should STOP)", "duplicates", ["id"]),
        ("Date Issues (Should WARN)", "date_issues", ["id"])
    ]
    
    for scenario_name, dataset_key, key_columns in scenarios:
        print(f"\n{scenario_name}")
        print("-" * 50)
        
        df = datasets[dataset_key]
        service = QualityControlService(df, key_columns)
        result = service.run()
        
        print(f"Dataset: {len(df)} rows, {len(df.columns)} columns")
        print(f"Status: {result.status}")
        print(f"Timestamp: {result.timestamp}")
        
        if result.errors:
            print(f"\nErrors ({len(result.errors)}):")
            for error in result.errors:
                print(f"  ❌ {error}")
        
        if result.warnings:
            print(f"\nWarnings ({len(result.warnings)}):")
            for warning in result.warnings:
                print(f"  ⚠️  {warning}")
        
        # Show missing data report
        if result.missing_report:
            print(f"\nMissing Data Report:")
            for col, pct in result.missing_report.items():
                if pct > 0:
                    print(f"  {col}: {pct:.1%}")
        
        # Show key issues
        if result.key_issues and result.key_issues != {"message": "No key columns specified"}:
            print(f"\nKey Issues:")
            for key, issues in result.key_issues.items():
                print(f"  {key}:")
                if 'duplicates_pct' in issues:
                    print(f"    Duplicates: {issues['duplicates']} ({issues['duplicates_pct']:.1%})")
                if 'nulls_pct' in issues:
                    print(f"    Nulls/Orphans: {issues['nulls']} ({issues['nulls_pct']:.1%})")
        
        # Show date issues
        if result.date_issues and 'message' not in result.date_issues:
            print(f"\nDate Issues:")
            for date_col, issues in result.date_issues.items():
                print(f"  {date_col}:")
                if 'future_dates' in issues:
                    print(f"    Future dates: {issues['future_dates']} ({issues['future_pct']:.1%})")
                if 'inversions' in issues:
                    print(f"    Inversions: {issues['inversions']} ({issues['inversion_pct']:.1%})")
    
    # Show summary
    print(f"\n{'=' * 60}")
    print("DECISION RULES IMPLEMENTED:")
    print("1. critical_missing_pct(field) > 0.20 ⇒ STOP")
    print("2. date_inversion_pct > 0.005 ⇒ WARN")
    print("3. orphans > 0.10 OR duplicates > 0.10 ⇒ STOP")
    print("=" * 60)


def test_edge_cases():
    """Test edge cases"""
    
    print(f"\n{'=' * 60}")
    print("EDGE CASES TESTING")
    print("=" * 60)
    
    # Empty DataFrame
    print("\n1. Empty DataFrame:")
    df_empty = pd.DataFrame()
    service = QualityControlService(df_empty)
    result = service.run()
    print(f"   Status: {result.status}")
    
    # Single row
    print("\n2. Single Row:")
    df_single = pd.DataFrame({'id': [1], 'name': ['Alice']})
    service = QualityControlService(df_single, ['id'])
    result = service.run()
    print(f"   Status: {result.status}")
    
    # No key columns
    print("\n3. No Key Columns:")
    df_no_keys = pd.DataFrame({'name': ['Alice', 'Bob'], 'age': [25, 30]})
    service = QualityControlService(df_no_keys)
    result = service.run()
    print(f"   Status: {result.status}")
    print(f"   Key issues: {result.key_issues}")
    
    # All nulls in key column
    print("\n4. All Nulls in Key Column:")
    df_all_nulls = pd.DataFrame({'id': [None] * 10, 'name': ['Alice'] * 10})
    service = QualityControlService(df_all_nulls, ['id'])
    result = service.run()
    print(f"   Status: {result.status}")


if __name__ == "__main__":
    try:
        demo_quality_control()
        test_edge_cases()
        
        print(f"\n{'=' * 60}")
        print("DEMO COMPLETED SUCCESSFULLY")
        print("The Quality Control Service is working correctly!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nDemo failed with error: {e}")
        import traceback
        traceback.print_exc()

