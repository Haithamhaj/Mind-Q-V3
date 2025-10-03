#!/usr/bin/env python3
import sys
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "backend"))

from app.services.phase1_goal_kpis import GoalKPIsService
from app.services.phase2_ingestion import IngestionService
from app.services.phase3_schema import SchemaService


def test_full_pipeline():
    """Test Phases 1-3 together"""
    print("Creating test dataset...")
    
    # Create test CSV
    df = pd.DataFrame({
        "shipment_id": ["S001", "S002", "S003"],
        "carrier": ["DHL", "Aramex", "SMSA"],
        "pickup_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "transit_time": [24.5, 48.0, 36.5]
    })
    
    test_path = Path("/tmp/test_logistics.csv")
    df.to_csv(test_path, index=False)
    
    # Phase 1
    print("\n1️⃣ Testing Phase 1: Goal & KPIs...")
    phase1 = GoalKPIsService(columns=df.columns.tolist(), domain="logistics")
    result1 = phase1.run()
    assert result1.domain == "logistics"
    assert result1.compatibility.status in ["OK", "WARN"]
    print(f"✅ Domain: {result1.domain}, Match: {result1.compatibility.match_percentage:.1%}")
    
    # Phase 2
    print("\n2️⃣ Testing Phase 2: Ingestion...")
    phase2 = IngestionService(file_path=test_path, artifacts_dir=Path("/tmp"))
    df_ingested, result2 = phase2.run()
    assert result2.rows == 3
    assert result2.columns == 4
    print(f"✅ Ingested: {result2.rows} rows × {result2.columns} columns")
    
    # Phase 3
    print("\n3️⃣ Testing Phase 3: Schema...")
    phase3 = SchemaService(df=df_ingested)
    df_typed, result3 = phase3.run()
    assert "shipment_id" in result3.id_columns
    assert "pickup_date" in result3.datetime_columns
    assert "transit_time" in result3.numeric_columns
    print(f"✅ Schema validated: {len(result3.dtypes)} columns typed")
    
    # Cleanup
    try:
        test_path.unlink()
    except Exception:
        pass
    
    print("\n" + "="*50)
    print("✅✅✅ Phase 2 (1-3) Validation PASSED ✅✅✅")


if __name__ == "__main__":
    try:
        test_full_pipeline()
        sys.exit(0)
    except AssertionError as e:
        print(f"\n❌ Validation failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


