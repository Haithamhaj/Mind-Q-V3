from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Optional

import pandas as pd


class TextDatasetRegistry:
    """
    Stores additional text datasets so they can be merged during Phase 8
    without relying on external infrastructure.
    """

    def __init__(self, artifacts_dir: Path):
        self.base_dir = artifacts_dir / "text_datasets"
        self.base_dir.mkdir(exist_ok=True)
        self.registry_path = self.base_dir / "registry.json"
        self._registry = self._load_registry()

    def register(self, name: str, key_column: str, df: pd.DataFrame) -> Dict[str, str]:
        slug = _slugify(name)
        file_path = self.base_dir / f"{slug}.parquet"
        df.to_parquet(file_path, compression="zstd")

        self._registry[slug] = {
            "name": name,
            "key_column": key_column,
            "path": file_path.name,
            "row_count": len(df),
            "columns": list(df.columns),
        }
        self._save_registry()
        return self._registry[slug]

    def delete(self, slug: str) -> None:
        meta = self._registry.pop(slug, None)
        if meta:
            file_path = self.base_dir / meta["path"]
            if file_path.exists():
                file_path.unlink()
            self._save_registry()

    def clear(self) -> None:
        for slug in list(self._registry.keys()):
            self.delete(slug)

    def list_datasets(self) -> Dict[str, Dict[str, str]]:
        return self._registry

    def load_tables(self) -> Dict[str, Dict[str, object]]:
        tables: Dict[str, Dict[str, object]] = {}
        for slug, meta in self._registry.items():
            file_path = self.base_dir / meta["path"]
            if file_path.exists():
                df = pd.read_parquet(file_path)
                tables[meta["name"]] = {"dataframe": df, "key_column": meta["key_column"]}
        return tables

    def _load_registry(self) -> Dict[str, Dict[str, str]]:
        if self.registry_path.exists():
            try:
                return json.loads(self.registry_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                return {}
        return {}

    def _save_registry(self) -> None:
        self.registry_path.write_text(json.dumps(self._registry, indent=2, ensure_ascii=False), encoding="utf-8")


def _slugify(value: str) -> str:
    value = value.strip().lower().replace(" ", "_")
    return "".join(ch for ch in value if ch.isalnum() or ch in {"_", "-"}).strip("_") or "text_dataset"
