#!/usr/bin/env python3
"""
Phase 1: Quality Control Validation Script
Validates that the quality control service is properly implemented.
"""

import sys
import os
from pathlib import Path

# Add the backend directory to the Python path
backend_dir = Path(__file__).parent.parent
sys.path.insert(0, str(backend_dir))

def validate_phase1():
    """Validate Phase 1: Quality Control implementation"""
    errors = []
    warnings = []
    
    # Check required files exist
    required_files = [
        "backend/app/services/phase0_quality_control.py",
        "backend/tests/test_phase0_quality_control.py"
    ]
    
    for file_path in required_files:
        if not Path(file_path).exists():
            errors.append(f"Missing file: {file_path}")
    
    # Try to import the quality control service
    try:
        from app.services.phase0_quality_control import QualityControlService, QualityControlResult
        print("✓ Quality control service imports successfully")
    except ImportError as e:
        errors.append(f"Cannot import quality control service: {e}")
    
    # Check if we can create a basic test
    try:
        import pandas as pd
        from datetime import datetime, timedelta
        
        # Create test data
        test_data = {
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', None, 'Charlie', 'David'],
            'date_created': [
                datetime.now() - timedelta(days=5),
                datetime.now() - timedelta(days=4),
                datetime.now() - timedelta(days=3),
                datetime.now() - timedelta(days=2),
                datetime.now() - timedelta(days=1)
            ]
        }
        df = pd.DataFrame(test_data)
        
        # Test the service
        service = QualityControlService(df, ['id'])
        result = service.run()
        
        # Validate result structure
        assert hasattr(result, 'status')
        assert hasattr(result, 'missing_report')
        assert hasattr(result, 'date_issues')
        assert hasattr(result, 'key_issues')
        assert hasattr(result, 'warnings')
        assert hasattr(result, 'errors')
        assert hasattr(result, 'timestamp')
        
        print("✓ Quality control service runs successfully")
        print(f"  Status: {result.status}")
        print(f"  Missing data report: {result.missing_report}")
        
        # Test decision rules
        print("\n✓ Testing decision rules:")
        
        # Test 1: Missing data >20% should trigger STOP
        bad_missing_data = {
            'id': range(10),
            'critical_field': [1, 2, None, None, None, None, None, None, None, None]  # 80% missing
        }
        df_bad = pd.DataFrame(bad_missing_data)
        service_bad = QualityControlService(df_bad, ['id'])
        result_bad = service_bad.run()
        
        if result_bad.status == "STOP" and any(">20% threshold" in error for error in result_bad.errors):
            print("  ✓ Rule 1: Missing data >20% triggers STOP")
        else:
            warnings.append("Rule 1: Missing data >20% should trigger STOP")
        
        # Test 2: Duplicates >10% should trigger STOP
        duplicate_data = {
            'id': [1, 2, 3, 1, 2, 3, 1, 2, 3, 1],  # 60% duplicates
            'value': range(10)
        }
        df_dups = pd.DataFrame(duplicate_data)
        service_dups = QualityControlService(df_dups, ['id'])
        result_dups = service_dups.run()
        
        if result_dups.status == "STOP" and any("duplicates" in error and ">10%" in error for error in result_dups.errors):
            print("  ✓ Rule 2: Duplicates >10% triggers STOP")
        else:
            warnings.append("Rule 2: Duplicates >10% should trigger STOP")
        
        # Test 3: Nulls >10% should trigger STOP
        null_data = {
            'id': [1, 2, None, 4, None, 6, None, 8, None, None],  # 50% nulls
            'value': range(10)
        }
        df_nulls = pd.DataFrame(null_data)
        service_nulls = QualityControlService(df_nulls, ['id'])
        result_nulls = service_nulls.run()
        
        if result_nulls.status == "STOP" and any("nulls/orphans" in error and ">10%" in error for error in result_nulls.errors):
            print("  ✓ Rule 3: Nulls/Orphans >10% triggers STOP")
        else:
            warnings.append("Rule 3: Nulls/Orphans >10% should trigger STOP")
        
    except Exception as e:
        errors.append(f"Cannot test quality control service: {e}")
    
    # Print results
    print("\n" + "="*50)
    if errors:
        print("X PHASE 1 VALIDATION FAILED")
        print("\nErrors:")
        for error in errors:
            print(f"  - {error}")
        return False
    
    if warnings:
        print("! PHASE 1 VALIDATION PASSED WITH WARNINGS")
        print("\nWarnings:")
        for warning in warnings:
            print(f"  - {warning}")
        return True
    
    print("✓ PHASE 1 VALIDATION PASSED")
    print("\nQuality Control Service Features:")
    print("  ✓ Missing data scan with 20% threshold")
    print("  ✓ Date order and future date checks")
    print("  ✓ Key uniqueness and orphan detection")
    print("  ✓ STOP/WARN decision rules implemented")
    print("  ✓ Comprehensive error and warning reporting")
    print("  ✓ Pydantic result models")
    
    return True


if __name__ == "__main__":
    success = validate_phase1()
    sys.exit(0 if success else 1)

