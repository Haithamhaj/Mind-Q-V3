#!/usr/bin/env python3
"""
Phase 0 Validation Script
Validates that the foundation and architecture setup is complete.
"""

import sys
from pathlib import Path

def validate_phase0():
    """Validate Phase 0 implementation"""
    errors = []
    warnings = []
    
    # Check required directories exist
    required_dirs = [
        "backend/app",
        "backend/app/api/v1",
        "backend/app/services",
        "backend/app/models",
        "backend/app/utils",
        "backend/tests",
        "backend/validation_scripts",
        "backend/artifacts",
        "backend/spec"
    ]
    
    for dir_path in required_dirs:
        if not Path(dir_path).exists():
            errors.append(f"Missing directory: {dir_path}")
    
    # Check required files exist
    required_files = [
        "backend/requirements.txt",
        "backend/app/__init__.py",
        "backend/app/config.py",
        "backend/app/main.py",
        "backend/app/api/__init__.py",
        "backend/app/api/v1/__init__.py",
        "backend/app/api/v1/router.py",
        "backend/Dockerfile"
    ]
    
    for file_path in required_files:
        if not Path(file_path).exists():
            errors.append(f"Missing file: {file_path}")
    
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
        print("X PHASE 0 VALIDATION FAILED")
        print("\nErrors:")
        for error in errors:
            print(f"  - {error}")
        return False
    
    if warnings:
        print("! PHASE 0 VALIDATION PASSED WITH WARNINGS")
        print("\nWarnings:")
        for warning in warnings:
            print(f"  - {warning}")
    else:
        print("OK PHASE 0 VALIDATION PASSED")
    
    return True

if __name__ == "__main__":
    success = validate_phase0()
    sys.exit(0 if success else 1)
