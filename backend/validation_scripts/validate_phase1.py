#!/usr/bin/env python3
"""
Phase 1 Validation Script
Validates that Goal & KPIs Definition phase is properly implemented.
"""

import sys
import json
import requests
from pathlib import Path

def validate_phase1():
    """Validate Phase 1 implementation"""
    errors = []
    warnings = []
    
    # Check required files exist
    required_files = [
        "backend/app/models/schemas.py",
        "backend/app/services/phase1_goal_kpis.py",
        "backend/app/api/v1/phases.py"
    ]
    
    for file_path in required_files:
        if not Path(file_path).exists():
            errors.append(f"Missing file: {file_path}")
    
    # Check if backend is running
    try:
        response = requests.get("http://localhost:8000/health", timeout=5)
        if response.status_code != 200:
            errors.append("Backend health check failed")
    except requests.exceptions.RequestException:
        errors.append("Backend is not running on http://localhost:8000")
    
    # Check Phase 1 API endpoints
    if not errors:  # Only test API if backend is running
        try:
            # Test domains endpoint
            response = requests.get("http://localhost:8000/api/v1/phases/domains", timeout=5)
            if response.status_code != 200:
                errors.append("Domains endpoint not working")
            else:
                domains_data = response.json()
                if len(domains_data) < 8:  # Should have 8 domains
                    warnings.append(f"Expected 8 domains, got {len(domains_data)}")
            
            # Test status endpoint
            response = requests.get("http://localhost:8000/api/v1/phases/status", timeout=5)
            if response.status_code != 200:
                errors.append("Phase status endpoint not working")
            
            # Test config endpoint
            response = requests.get("http://localhost:8000/api/v1/phases/config", timeout=5)
            if response.status_code != 200:
                errors.append("Config endpoint not working")
            
            # Test validation endpoint
            response = requests.get("http://localhost:8000/api/v1/phases/validate", timeout=5)
            if response.status_code != 200:
                errors.append("Validation endpoint not working")
                
        except requests.exceptions.RequestException as e:
            errors.append(f"API testing failed: {e}")
    
    # Check artifacts directory is writable
    artifacts_dir = Path("backend/artifacts")
    if artifacts_dir.exists():
        try:
            test_file = artifacts_dir / "test_write.tmp"
            test_file.write_text("test")
            test_file.unlink()
        except Exception as e:
            errors.append(f"Artifacts directory not writable: {e}")
    
    # Print results
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
    else:
        print("OK PHASE 1 VALIDATION PASSED")
    
    print("\nPhase 1 Features:")
    print("  - Domain selection and information")
    print("  - Goal definition and management")
    print("  - KPI definition and management")
    print("  - Configuration validation")
    print("  - API endpoints for all operations")
    
    return True

if __name__ == "__main__":
    success = validate_phase1()
    sys.exit(0 if success else 1)
