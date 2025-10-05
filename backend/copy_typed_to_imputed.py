#!/usr/bin/env python3
"""
Utility script to copy typed_data.parquet to imputed_data.parquet
This allows Phase 6+ to continue when Phase 5 fails
"""

from pathlib import Path
import shutil

def copy_typed_to_imputed():
    artifacts_dir = Path("artifacts")
    typed_path = artifacts_dir / "typed_data.parquet" 
    imputed_path = artifacts_dir / "imputed_data.parquet"
    
    if typed_path.exists():
        shutil.copy2(typed_path, imputed_path)
        print(f" Copied {typed_path} to {imputed_path}")
        return True
    else:
        print(f" {typed_path} not found")
        return False

if __name__ == "__main__":
    copy_typed_to_imputed()



