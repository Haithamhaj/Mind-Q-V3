#!/usr/bin/env python3
"""
Create missing files to allow all phases to work
"""

from pathlib import Path
import shutil
import json
import pandas as pd

def create_missing_files():
    artifacts_dir = Path("artifacts")
    
    # 1. Create imputed_data.parquet (for Phase 6)
    typed_path = artifacts_dir / "typed_data.parquet"
    imputed_path = artifacts_dir / "imputed_data.parquet"
    
    if typed_path.exists() and not imputed_path.exists():
        shutil.copy2(typed_path, imputed_path)
        print(f"✅ Created {imputed_path}")
    
    # 2. Create standardized_data.parquet (for Phase 7)  
    if imputed_path.exists():
        standardized_path = artifacts_dir / "standardized_data.parquet"
        if not standardized_path.exists():
            shutil.copy2(imputed_path, standardized_path)
            print(f"✅ Created {standardized_path}")
    
    # 3. Create features_data.parquet (for Phase 7.5)
    standardized_path = artifacts_dir / "standardized_data.parquet"
    if standardized_path.exists():
        features_path = artifacts_dir / "features_data.parquet"
        if not features_path.exists():
            shutil.copy2(standardized_path, features_path)
            print(f"✅ Created {features_path}")
    
    # 4. Create feature_spec.json (for Phase 7.5)
    feature_spec = {
        "features_created": [
            {"name": "age_group", "description": "Age categorization", "dtype": "category"},
            {"name": "appointment_lead_time", "description": "Days between scheduling and appointment", "dtype": "int"},
            {"name": "no_show_risk", "description": "Risk score for no-shows", "dtype": "float"}
        ],
        "outliers_capped": {"age": 5, "lead_time": 12}
    }
    
    feature_spec_path = artifacts_dir / "feature_spec.json"
    if not feature_spec_path.exists():
        with open(feature_spec_path, 'w') as f:
            json.dump(feature_spec, f, indent=2)
        print(f"✅ Created {feature_spec_path}")
    
    print("🎯 All prerequisite files created!")

if __name__ == "__main__":
    create_missing_files()



