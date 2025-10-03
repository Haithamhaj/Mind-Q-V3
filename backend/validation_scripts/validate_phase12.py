#!/usr/bin/env python3
"""
Phase 12 Validation Script

Validates Phase 12: Text Features (MVP) implementation
"""

import sys
import os
from pathlib import Path

# Add backend to path
backend_dir = Path(__file__).parent.parent
sys.path.insert(0, str(backend_dir))

def test_imports():
    """Test that all Phase 12 modules can be imported"""
    print("Testing Phase 12 imports...")
    
    try:
        from app.services.phase12.detection import TextDetectionService, TextDetectionResult
        print("[OK] TextDetectionService imported successfully")
    except ImportError as e:
        print(f"[FAIL] Failed to import TextDetectionService: {e}")
        return False
    
    try:
        from app.services.phase12.basic_features import BasicTextFeaturesService, BasicTextFeatures
        print("[OK] BasicTextFeaturesService imported successfully")
    except ImportError as e:
        print(f"[FAIL] Failed to import BasicTextFeaturesService: {e}")
        return False
    
    try:
        from app.services.phase12.sentiment_simple import SentimentAnalysisService, SentimentResult
        print("[OK] SentimentAnalysisService imported successfully")
    except ImportError as e:
        print(f"[FAIL] Failed to import SentimentAnalysisService: {e}")
        return False
    
    try:
        from app.services.phase12.orchestrator import Phase12Orchestrator, Phase12Result
        print("[OK] Phase12Orchestrator imported successfully")
    except ImportError as e:
        print(f"[FAIL] Failed to import Phase12Orchestrator: {e}")
        return False
    
    return True


def test_basic_functionality():
    """Test basic functionality with mock data"""
    print("\nTesting basic functionality...")
    
    try:
        import pandas as pd
        from app.services.phase12.detection import TextDetectionService
        
        # Create test data
        df = pd.DataFrame({
            'description': ['This is a good product'] * 100,
            'short': ['OK'] * 100,
            'number': range(100)
        })
        
        # Test detection
        service = TextDetectionService(df=df)
        result = service.run()
        
        print(f"[OK] Text detection completed: {len(result.text_columns)} text columns found")
        print(f"[OK] Language detected: {result.language_detected}")
        print(f"[OK] Recommendation: {result.recommendation}")
        
        return True
        
    except Exception as e:
        print(f"[FAIL] Basic functionality test failed: {e}")
        return False


def test_api_endpoint():
    """Test that API endpoint is properly configured"""
    print("\nTesting API endpoint configuration...")
    
    try:
        # Check if phases.py contains Phase 12 endpoint
        phases_file = backend_dir / "app" / "api" / "v1" / "phases.py"
        
        if not phases_file.exists():
            print("[FAIL] phases.py file not found")
            return False
        
        with open(phases_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        if "/phase12-text-features" in content:
            print("[OK] Phase 12 API endpoint found in phases.py")
        else:
            print("[FAIL] Phase 12 API endpoint not found in phases.py")
            return False
        
        if "Phase12Orchestrator" in content:
            print("[OK] Phase12Orchestrator import found in phases.py")
        else:
            print("[FAIL] Phase12Orchestrator import not found in phases.py")
            return False
        
        return True
        
    except Exception as e:
        print(f"[FAIL] API endpoint test failed: {e}")
        return False


def test_requirements():
    """Test that requirements.txt includes Phase 12 dependencies"""
    print("\nTesting requirements.txt...")
    
    try:
        requirements_file = backend_dir / "requirements.txt"
        
        if not requirements_file.exists():
            print("[FAIL] requirements.txt file not found")
            return False
        
        with open(requirements_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        if "vaderSentiment" in content:
            print("[OK] vaderSentiment dependency found in requirements.txt")
        else:
            print("[FAIL] vaderSentiment dependency not found in requirements.txt")
            return False
        
        return True
        
    except Exception as e:
        print(f"[FAIL] Requirements test failed: {e}")
        return False


def test_file_structure():
    """Test that all required files exist"""
    print("\nTesting file structure...")
    
    required_files = [
        "app/services/phase12/__init__.py",
        "app/services/phase12/detection.py",
        "app/services/phase12/basic_features.py",
        "app/services/phase12/sentiment_simple.py",
        "app/services/phase12/orchestrator.py",
        "app/services/phase12/FUTURE_ENHANCEMENTS.md",
        "tests/test_phase12_mvp.py"
    ]
    
    all_exist = True
    
    for file_path in required_files:
        full_path = backend_dir / file_path
        if full_path.exists():
            print(f"[OK] {file_path} exists")
        else:
            print(f"[FAIL] {file_path} missing")
            all_exist = False
    
    return all_exist


def main():
    """Run all validation tests"""
    print("=" * 60)
    print("Phase 12: Text Features (MVP) - Validation Script")
    print("=" * 60)
    
    tests = [
        ("File Structure", test_file_structure),
        ("Imports", test_imports),
        ("Requirements", test_requirements),
        ("API Endpoint", test_api_endpoint),
        ("Basic Functionality", test_basic_functionality),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n{'-' * 40}")
        print(f"Running: {test_name}")
        print(f"{'-' * 40}")
        
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"[FAIL] {test_name} test crashed: {e}")
            results.append((test_name, False))
    
    # Summary
    print(f"\n{'=' * 60}")
    print("VALIDATION SUMMARY")
    print(f"{'=' * 60}")
    
    passed = 0
    total = len(results)
    
    for test_name, result in results:
        status = "PASS" if result else "FAIL"
        print(f"{test_name:20} : {status}")
        if result:
            passed += 1
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("[SUCCESS] All tests passed! Phase 12 is ready for use.")
        return 0
    else:
        print("[ERROR] Some tests failed. Please review the output above.")
        return 1


if __name__ == "__main__":
    exit(main())
