import pandas as pd
from pathlib import Path

from app.services.text_dataset_registry import TextDatasetRegistry

def test_text_dataset_registry_roundtrip(tmp_path: Path):
    df = pd.DataFrame({
        "AWB_NO": ["A1", "A2", "A3"],
        "customer_feedback": ["Great", "Average", "Slow delivery"],
    })

    registry = TextDatasetRegistry(tmp_path)
    meta = registry.register("Customer Feedback", "AWB_NO", df)

    assert meta["name"] == "Customer Feedback"
    assert meta["key_column"] == "AWB_NO"

    loaded = registry.load_tables()
    assert "Customer Feedback" in loaded
    loaded_df = loaded["Customer Feedback"]["dataframe"]
    assert list(loaded_df.columns) == ["AWB_NO", "customer_feedback"]

    registry.clear()
    assert registry.list_datasets() == {}
