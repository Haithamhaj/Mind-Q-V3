from __future__ import annotations
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, Literal

Agg = Literal["mean", "median", "sum", "count", "min", "max", "std", "p90", "p95"]


class Executor:
    """Advanced query executor with filters and aggregations"""
    
    def __init__(self, df: pd.DataFrame):
        self.df = df
    
    def _apply_filters(self, df: pd.DataFrame, filters: Optional[Dict[str, Any]]) -> pd.DataFrame:
        """
        Apply filters to dataframe
        
        Supported filter types:
        - Simple equality: {"carrier": "DHL"}
        - Range: {"transit_time": {"range": [20, 40]}}
        - In list: {"carrier": {"in": ["DHL", "Aramex"]}}
        - Like (regex): {"description": {"like": "urgent"}}
        """
        if not filters:
            return df
        
        out = df
        for k, v in filters.items():
            if k not in out.columns:
                continue
            
            if isinstance(v, dict):
                # Range filter
                if "range" in v:
                    lo, hi = v["range"]
                    out = out[(out[k] >= lo) & (out[k] <= hi)]
                
                # In list filter
                if "in" in v:
                    out = out[out[k].isin(v["in"])]
                
                # Like filter (regex)
                if "like" in v:
                    out = out[out[k].astype(str).str.contains(v["like"], regex=True, na=False)]
            else:
                # Simple equality
                out = out[out[k] == v]
        
        return out
    
    def _map_column_name(self, requested_name: str) -> str:
        """Map requested column name to actual column name in dataframe"""
        # Direct match  
        if requested_name in self.df.columns:
            return requested_name
        
        for col in self.df.columns:
            if col.lower() == requested_name.lower():
                return col
        
        # Handle underscore/hyphen variations
        variations = [
            requested_name.replace('_', ''),
            requested_name.replace('-', ''),
            requested_name.replace('_', '-'),
            requested_name.replace('-', '_')
        ]
        
        for col in self.df.columns:
            for variation in variations:
                if col.lower() == variation.lower():
                    return col
        
        # Handle camelCase variations
        camel_variations = [
            requested_name.title().replace('_', '').replace('-', ''),
            requested_name.lower().replace('_', '').replace('-', '')
        ]
        
        for col in self.df.columns:
            for variation in camel_variations:
                if col.lower() == variation.lower():
                    return col
        
        # Special handling for Gender column (maps to Gender_F and Gender_M)
        if requested_name.lower() in ['gender', 'gender_f', 'gender_m']:
            # For gender comparisons, we need to handle both Gender_F and Gender_M
            if 'Gender_F' in self.df.columns and 'Gender_M' in self.df.columns:
                return 'Gender_F'  # Default to Gender_F for mapping
        
        return requested_name  # Return original if no match found
    
    def aggregate(
        self,
        metric: str,
        agg: Agg = "mean",
        filters: Optional[Dict] = None,
        dimension: Optional[str] = None
    ) -> Dict[str, Any]:
        """Execute aggregation query"""
        
        df = self._apply_filters(self.df, filters)
        
        # Map metric name to actual column name
        mapped_metric = self._map_column_name(metric)
        
        if mapped_metric not in df.columns:
            return {"error": f"metric {metric} not found (tried: {mapped_metric})"}
        
        if agg == "count" and mapped_metric.lower() in ['patientid', 'appointmentid', 'patient_id', 'appointment_id']:
            # For count operations on ID columns, just count the unique values
            if dimension:
                mapped_dimension = self._map_column_name(dimension) if dimension else None
                if mapped_dimension and mapped_dimension in df.columns:
                    # Count unique IDs per dimension
                    result = df[[mapped_dimension, mapped_metric]].dropna().groupby(mapped_dimension)[mapped_metric].nunique()
                    return result.to_dict()
                else:
                    return {"error": f"dimension {dimension} not found"}
            else:
                # Count total unique IDs
                unique_count = df[mapped_metric].dropna().nunique()
                return {"overall": unique_count}
        
        # Check if the column is numeric first
        if not pd.api.types.is_numeric_dtype(df[mapped_metric]):
            # For non-numeric columns, only allow count operations
            if agg == "count":
                if dimension:
                    mapped_dimension = self._map_column_name(dimension) if dimension else None
                    if mapped_dimension and mapped_dimension in df.columns:
                        result = df.groupby(mapped_dimension)[mapped_metric].count()
                        return result.to_dict()
                    else:
                        return {"error": f"dimension {dimension} not found"}
                else:
                    count = df[mapped_metric].count()
                    return {"overall": count}
            else:
                return {"error": f"metric {metric} is non-numeric and cannot be used for {agg} operation. Use 'count' instead."}
        
        try:
            s = pd.to_numeric(df[mapped_metric], errors='coerce').dropna()
        except:
            # If conversion fails, return error
            return {"error": f"metric {metric} cannot be used for mathematical operations"}
        
        if len(s) == 0:
            return {"error": f"no valid numeric data for metric {metric}"}
        
        # Map dimension name as well
        mapped_dimension = self._map_column_name(dimension) if dimension else None
        
        # Aggregate by dimension
        if mapped_dimension and mapped_dimension in df.columns:
            # For grouped operations, also convert to numeric
            try:
                numeric_col = pd.to_numeric(df[mapped_metric], errors='coerce')
                g = df[[mapped_dimension]].assign(numeric_val=numeric_col).dropna().groupby(mapped_dimension)['numeric_val']
            except:
                return {"error": f"metric {metric} cannot be used for grouped operations"}
            
            fn_map = {
                "mean": lambda g: g.mean(),
                "median": lambda g: g.median(),
                "sum": lambda g: g.sum(),
                "count": lambda g: g.count(),
                "min": lambda g: g.min(),
                "max": lambda g: g.max(),
                "std": lambda g: g.std(),
                "p90": lambda g: g.quantile(0.9),
                "p95": lambda g: g.quantile(0.95)
            }
            
            result = fn_map[agg](g)
            return result.to_dict()
        
        # Overall aggregate
        else:
            val_map = {
                "mean": float(s.mean()),
                "median": float(s.median()),
                "sum": float(s.sum()),
                "count": int(s.count()),
                "min": float(s.min()) if len(s) > 0 else np.nan,
                "max": float(s.max()) if len(s) > 0 else np.nan,
                "std": float(s.std()),
                "p90": float(s.quantile(0.9)),
                "p95": float(s.quantile(0.95))
            }
            
            return {"overall": val_map[agg]}
    
    def compare(
        self,
        metric: str,
        dimension: str,
        filters: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Execute comparison query with multiple stats"""
        
        df = self._apply_filters(self.df, filters)
        
        # Map column names
        mapped_metric = self._map_column_name(metric)
        mapped_dimension = self._map_column_name(dimension)
        
        if (mapped_dimension.lower() in ['gender', 'gender_f'] and 
            mapped_metric.lower() in ['patientid', 'patient_id'] and
            'Gender_F' in df.columns and 'Gender_M' in df.columns):
            return self._compare_gender_patients(df, mapped_metric)
        
        if mapped_metric not in df.columns or mapped_dimension not in df.columns:
            return {"error": f"metric {metric} or dimension {dimension} not found (tried: {mapped_metric}, {mapped_dimension})"}
        
        g = df[[mapped_dimension, mapped_metric]].dropna().groupby(mapped_dimension)[mapped_metric]
        res = g.agg(["mean", "median", "count", "std"]).to_dict("index")
        
        return res
    
    def _compare_gender_patients(self, df: pd.DataFrame, metric: str) -> Dict[str, Any]:
        """Special comparison for gender vs patient count"""
        try:
            # Count patients by gender using the encoded columns
            female_count = df['Gender_F'].sum()  # Sum of True values
            male_count = df['Gender_M'].sum()    # Sum of True values
            
            return {
                "F": int(female_count),
                "M": int(male_count)
            }
        except Exception as e:
            return {"error": f"Failed to compare gender patients: {str(e)}"}
    
    def trend(
        self,
        metric: str,
        freq: str = "D",
        filters: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Execute trend analysis over time"""
        
        df = self._apply_filters(self.df, filters)
        
        # Map metric name
        mapped_metric = self._map_column_name(metric)
        
        if mapped_metric not in df.columns:
            return {"error": f"metric {metric} not found (tried: {mapped_metric})"}
        
        dt_cols = df.select_dtypes(include=["datetime64[ns, UTC]", "datetime64[ns]"]).columns
        if len(dt_cols) == 0:
            return {"error": "no datetime column"}
        
        dt = dt_cols[0]
        tmp = df[[dt, mapped_metric]].dropna().copy()
        
        if tmp.empty:
            return {"error": "no data for trend"}
        
        tmp["date"] = pd.to_datetime(tmp[dt]).dt.to_period(freq).dt.to_timestamp()
        m = tmp.groupby("date")[mapped_metric].mean()
        
        return {str(k): float(v) for k, v in m.items()}
    
    def overview(self) -> Dict[str, Any]:
        """
        Generate a comprehensive overview of the dataset
        
        Returns:
            Dictionary with key statistics and insights
        """
        df = self.df
        
        overview = {
            "total_records": len(df),
            "total_columns": len(df.columns),
            "column_names": list(df.columns),
            "data_types": {col: str(df[col].dtype) for col in df.columns},
            "missing_values": {col: int(df[col].isnull().sum()) for col in df.columns},
            "numeric_summary": {},
            "categorical_summary": {}
        }
        
        # Numeric columns summary
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if col in df.columns:
                overview["numeric_summary"][col] = {
                    "mean": float(df[col].mean()) if not df[col].isnull().all() else None,
                    "median": float(df[col].median()) if not df[col].isnull().all() else None,
                    "std": float(df[col].std()) if not df[col].isnull().all() else None,
                    "min": float(df[col].min()) if not df[col].isnull().all() else None,
                    "max": float(df[col].max()) if not df[col].isnull().all() else None,
                    "unique_count": int(df[col].nunique())
                }
        
        # Categorical columns summary
        categorical_cols = df.select_dtypes(include=['object', 'category', 'bool']).columns
        for col in categorical_cols:
            if col in df.columns:
                value_counts = df[col].value_counts().head(10)
                overview["categorical_summary"][col] = {
                    "unique_count": int(df[col].nunique()),
                    "most_common": value_counts.index.tolist() if len(value_counts) > 0 else [],
                    "most_common_counts": value_counts.values.tolist() if len(value_counts) > 0 else []
                }
        
        return overview
