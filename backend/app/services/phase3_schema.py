"""
Phase 3: Schema Validation & Data Type Enforcement Service
Handles schema validation and enforces correct data types.
"""

import pandas as pd
import numpy as np
import re
from pathlib import Path
from typing import Dict, List, Optional, Union
from datetime import datetime
import pandera as pa
from pandera import Column, DataFrameSchema
from pydantic import BaseModel

from ..models.schemas import (
    SchemaValidationResult, Phase3SchemaResponse
)
from ..config import settings


class SchemaResult(BaseModel):
    dtypes: Dict[str, str]
    id_columns: List[str]
    datetime_columns: List[str]
    numeric_columns: List[str]
    categorical_columns: List[str]
    violations_pct: float
    warnings: List[str]
    schema_json: Dict


class SchemaService:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.warnings = []
    
    def run(self) -> tuple[pd.DataFrame, SchemaResult]:
        """Execute Phase 3: Schema & Dtypes"""
        # Infer and cast dtypes
        df_typed = self._infer_and_cast()
        
        # Generate schema
        schema_dict = self._generate_schema(df_typed)
        
        # Categorize columns
        categorized = self._categorize_columns(df_typed)
        
        result = SchemaResult(
            dtypes={col: str(dtype) for col, dtype in df_typed.dtypes.items()},
            id_columns=categorized['id_columns'],
            datetime_columns=categorized['datetime_columns'],
            numeric_columns=categorized['numeric_columns'],
            categorical_columns=categorized['categorical_columns'],
            violations_pct=0.0,  # Placeholder
            warnings=self.warnings,
            schema_json=schema_dict
        )
        
        return df_typed, result
    
    def _infer_and_cast(self) -> pd.DataFrame:
        """Infer types and cast appropriately"""
        df = self.df.copy()
        
        for col in df.columns:
            # Detect ID columns (contain 'id' in name)
            col_lower = col.lower()
            if ('id' in col_lower) or ('code' in col_lower):
                # Use pandas string dtype to satisfy tests expecting 'string'
                df[col] = df[col].astype('string')
                continue
            
            # Try datetime conversion
            if self._looks_like_date(df[col]):
                try:
                    df[col] = pd.to_datetime(df[col], utc=True, errors='coerce')
                    null_after = df[col].isnull().sum()
                    if null_after / len(df) > 0.02:
                        self.warnings.append(f"Column '{col}': {null_after} datetime conversion failures")
                    continue
                except:
                    pass
            
            # Try numeric conversion
            if df[col].dtype == 'object' or str(df[col].dtype) == 'string':
                try:
                    # Handle percentages like '15%'
                    series_clean = df[col].astype(str).str.replace('%', '', regex=False)
                    df[col] = pd.to_numeric(series_clean, errors='coerce').astype(float)
                    null_after = df[col].isnull().sum()
                    if null_after / len(df) > 0.02:
                        # Revert if too many failures
                        df[col] = self.df[col]
                    else:
                        continue
                except:
                    pass
            
            # Categorical for low cardinality strings
            if df[col].dtype == 'object' or str(df[col].dtype) == 'string':
                cardinality = df[col].nunique()
                # Relax ratio threshold to classify typical status/type columns
                if cardinality < 50 and cardinality / len(df) <= 0.8:
                    df[col] = df[col].astype('category')
                else:
                    # Ensure non-categorical text columns use pandas string dtype
                    df[col] = df[col].astype('string')
                continue

            # Ensure integer numeric columns are cast to float for consistency
            if pd.api.types.is_integer_dtype(df[col]):
                df[col] = df[col].astype(float)
        
        return df
    
    def _looks_like_date(self, series: pd.Series) -> bool:
        """Heuristic to detect date columns"""
        if series.dtype != 'object':
            return False
        
        # Check column name
        col_name = series.name.lower()
        date_keywords = ['date', 'time', 'timestamp', 'ts', 'dt']
        if any(kw in col_name for kw in date_keywords):
            return True
        
        # Check sample values - require at least one digit to avoid matching plain text like 'IT'
        sample = series.dropna().head(5).astype(str)
        date_patterns = ['-', '/', ':', 'T', 'Z']
        for val in sample:
            if any(ch.isdigit() for ch in val) and any(p in val for p in date_patterns):
                return True
        
        return False
    
    def _categorize_columns(self, df: pd.DataFrame) -> Dict[str, List[str]]:
        """Categorize columns by type"""
        categorized = {
            'id_columns': [],
            'datetime_columns': [],
            'numeric_columns': [],
            'categorical_columns': []
        }
        
        for col in df.columns:
            dtype = df[col].dtype
            
            col_lower = col.lower()
            if ('id' in col_lower) or ('code' in col_lower):
                categorized['id_columns'].append(col)
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                categorized['datetime_columns'].append(col)
            elif pd.api.types.is_numeric_dtype(dtype):
                categorized['numeric_columns'].append(col)
            elif pd.api.types.is_categorical_dtype(dtype):
                categorized['categorical_columns'].append(col)
            elif dtype == 'string':
                # Treat string dtype as categorical candidate if low cardinality
                cardinality = df[col].nunique()
                if cardinality < 50 and cardinality / len(df) < 0.5:
                    categorized['categorical_columns'].append(col)
        
        return categorized
    
    def _generate_schema(self, df: pd.DataFrame) -> Dict:
        """Generate Pandera schema as JSON"""
        schema_dict = {
            "columns": {},
            "index": None,
            "coerce": True
        }
        
        for col in df.columns:
            schema_dict["columns"][col] = {
                "dtype": str(df[col].dtype),
                "nullable": bool(df[col].isnull().any()),
                "unique": False
            }
        
        return schema_dict


class Phase3SchemaService:
    """
    Service for managing Phase 3: Schema Validation & Data Type Enforcement
    
    Decision Rules:
    - IDs→string; timestamps→datetime[UTC]; numeric→float/int; categorical→category
    - schema_violations>0.02 ⇒ WARN
    """
    
    def __init__(self):
        self.processed_dir = settings.artifacts_dir / "processed"
        self.processed_dir.mkdir(exist_ok=True)
    
    def validate_and_enforce_schema(
        self, 
        file_path: Union[str, Path],
        domain_pack: Optional[str] = None
    ) -> Phase3SchemaResponse:
        """
        Validate and enforce schema on ingested data.
        
        Decision Rules:
        - IDs→string; timestamps→datetime[UTC]; numeric→float/int; categorical→category
        - schema_violations>0.02 ⇒ WARN
        """
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                return Phase3SchemaResponse(
                    status="error",
                    message=f"File not found: {file_path}"
                )
            
            # Read the data
            df = pd.read_parquet(file_path)
            
            # Apply schema enforcement
            df_processed, conversions, warnings = self._enforce_data_types(df, domain_pack)
            
            # Calculate violation metrics
            total_cells = len(df) * len(df.columns)
            schema_violations = self._count_schema_violations(df_processed)
            violation_rate = schema_violations / total_cells if total_cells > 0 else 0
            
            # Determine status
            if violation_rate > 0.02:
                status = "WARN"
                message = f"Schema validation completed with warnings (violation rate: {violation_rate:.1%})"
            else:
                status = "OK"
                message = f"Schema validation completed successfully (violation rate: {violation_rate:.1%})"
            
            # Save processed data
            processed_filename = f"{file_path.stem}_processed.parquet"
            processed_path = self.processed_dir / processed_filename
            df_processed.to_parquet(processed_path, compression="zstd")
            
            # Get final column types
            column_types = {col: str(dtype) for col, dtype in df_processed.dtypes.items()}
            
            result = SchemaValidationResult(
                file_path=str(file_path),
                total_rows=len(df_processed),
                total_columns=len(df_processed.columns),
                schema_violations=schema_violations,
                violation_rate=violation_rate,
                status=status,
                column_types=column_types,
                type_conversions=conversions,
                warnings=warnings,
                message=message
            )
            
            return Phase3SchemaResponse(
                status="success",
                message="Schema validation and enforcement completed",
                data=result
            )
            
        except Exception as e:
            return Phase3SchemaResponse(
                status="error",
                message=f"Failed to validate schema: {str(e)}"
            )
    
    def _enforce_data_types(self, df: pd.DataFrame, domain_pack: Optional[str] = None) -> tuple:
        """
        Enforce data types based on column patterns and domain pack.
        
        Rules:
        - IDs→string
        - timestamps→datetime[UTC]
        - numeric→float/int
        - categorical→category
        """
        df_processed = df.copy()
        conversions = {}
        warnings = []
        
        for column in df_processed.columns:
            original_dtype = str(df_processed[column].dtype)
            
            # Determine target type based on column name patterns
            target_type = self._determine_target_type(column, df_processed[column], domain_pack)
            
            if target_type != original_dtype:
                try:
                    df_processed[column] = self._convert_column_type(
                        df_processed[column], target_type
                    )
                    conversions[column] = f"{original_dtype} → {target_type}"
                except Exception as e:
                    warnings.append(f"Failed to convert {column}: {str(e)}")
                    conversions[column] = f"{original_dtype} → {original_dtype} (failed)"
        
        return df_processed, conversions, warnings
    
    def _determine_target_type(self, column_name: str, series: pd.Series, domain_pack: Optional[str] = None) -> str:
        """Determine target data type for a column"""
        column_lower = column_name.lower()
        
        # ID columns → string
        if any(pattern in column_lower for pattern in ['id', 'key', 'code', 'ref']):
            return 'string'
        
        # Timestamp columns → datetime
        if any(pattern in column_lower for pattern in ['date', 'time', 'ts', 'created', 'updated']):
            return 'datetime64[ns, UTC]'
        
        # Numeric columns → float/int
        if any(pattern in column_lower for pattern in ['amount', 'price', 'value', 'count', 'rate', 'pct', 'ratio']):
            return 'float64'
        
        # Categorical columns → category
        if any(pattern in column_lower for pattern in ['status', 'type', 'category', 'flag', 'gender', 'department']):
            return 'category'
        
        # Default based on current data
        if pd.api.types.is_numeric_dtype(series):
            return 'float64'
        elif pd.api.types.is_datetime64_any_dtype(series):
            return 'datetime64[ns, UTC]'
        else:
            return 'string'
    
    def _convert_column_type(self, series: pd.Series, target_type: str) -> pd.Series:
        """Convert series to target type"""
        if target_type == 'string':
            return series.astype('string')
        
        elif target_type == 'datetime64[ns, UTC]':
            if pd.api.types.is_datetime64_any_dtype(series):
                return series.dt.tz_localize('UTC') if series.dt.tz is None else series
            else:
                return pd.to_datetime(series, errors='coerce').dt.tz_localize('UTC')
        
        elif target_type == 'float64':
            return pd.to_numeric(series, errors='coerce')
        
        elif target_type == 'category':
            return series.astype('category')
        
        else:
            return series
    
    def _count_schema_violations(self, df: pd.DataFrame) -> int:
        """Count schema violations in the dataframe"""
        violations = 0
        
        for column in df.columns:
            series = df[column]
            
            # Count null values in non-nullable columns
            if 'id' in column.lower() and series.isnull().any():
                violations += series.isnull().sum()
            
            # Count invalid datetime values
            if pd.api.types.is_datetime64_any_dtype(series):
                violations += pd.isnull(series).sum()
            
            # Count invalid numeric values
            if pd.api.types.is_numeric_dtype(series):
                violations += pd.isnull(series).sum()
        
        return violations
    
    def get_schema_info(self, file_path: Union[str, Path]) -> Phase3SchemaResponse:
        """Get schema information for a file"""
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                return Phase3SchemaResponse(
                    status="error",
                    message=f"File not found: {file_path}"
                )
            
            df = pd.read_parquet(file_path)
            
            # Basic schema info
            column_types = {col: str(dtype) for col, dtype in df.dtypes.items()}
            schema_violations = self._count_schema_violations(df)
            total_cells = len(df) * len(df.columns)
            violation_rate = schema_violations / total_cells if total_cells > 0 else 0
            
            result = SchemaValidationResult(
                file_path=str(file_path),
                total_rows=len(df),
                total_columns=len(df.columns),
                schema_violations=schema_violations,
                violation_rate=violation_rate,
                status="OK" if violation_rate <= 0.02 else "WARN",
                column_types=column_types,
                type_conversions={},
                warnings=[],
                message=f"Schema info for {len(df):,} rows and {len(df.columns)} columns"
            )
            
            return Phase3SchemaResponse(
                status="success",
                message="Schema information retrieved successfully",
                data=result
            )
            
        except Exception as e:
            return Phase3SchemaResponse(
                status="error",
                message=f"Failed to get schema info: {str(e)}"
            )
    
    def list_processed_files(self) -> Phase3SchemaResponse:
        """List all processed files"""
        try:
            processed_files = []
            
            for file_path in self.processed_dir.glob("*.parquet"):
                try:
                    df = pd.read_parquet(file_path)
                    file_size_mb = file_path.stat().st_size / (1024 * 1024)
                    
                    file_info = {
                        "filename": file_path.name,
                        "path": str(file_path),
                        "rows": len(df),
                        "columns": len(df.columns),
                        "size_mb": round(file_size_mb, 2),
                        "modified": datetime.fromtimestamp(file_path.stat().st_mtime).isoformat(),
                        "schema": {col: str(dtype) for col, dtype in df.dtypes.items()}
                    }
                    processed_files.append(file_info)
                    
                except Exception as e:
                    # Skip corrupted files
                    continue
            
            return Phase3SchemaResponse(
                status="success",
                message=f"Found {len(processed_files)} processed files",
                data={"files": processed_files}
            )
            
        except Exception as e:
            return Phase3SchemaResponse(
                status="error",
                message=f"Failed to list processed files: {str(e)}"
            )
