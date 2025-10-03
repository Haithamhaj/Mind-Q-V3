# Phase 2 Implementation: Goal, Ingestion & Schema

This document describes the implementation of Phase 2 of the Mind-Q V3 project, which includes three foundational phases:

- **Phase 1**: Goal & KPIs Definition with Domain Compatibility
- **Phase 2**: Data Ingestion to Parquet Format  
- **Phase 3**: Schema Validation & Data Type Enforcement

## Overview

Phase 2 implements a complete data pipeline that validates domain compatibility before ingestion, reliably ingests data into Parquet format with compression, and enforces proper data types and schema validation.

## Architecture

### Domain Packs (`domain_packs.py`)

Predefined domain packs with business KPIs and expected columns:

- **Logistics**: SLA_pct, TransitTime_avg, RTO_pct, FAS_pct, NDR_pct
- **Healthcare**: BedOccupancy_pct, AvgLOS_days, Readmission_30d_pct, ProcedureSuccess_pct
- **E-marketing**: CTR_pct, Conversion_pct, CAC, ROAS
- **Retail**: GMV, AOV, CartAbandon_pct, Return_pct
- **Finance**: NPL_pct, ROI_pct, Liquidity_Ratio, Default_pct

### Phase 1: Enhanced Goal & KPIs (`phase1_goal_kpis.py`)

**Decision Rules:**
- `>=70%` expected columns present ⇒ **OK**
- `50%-30%` match ⇒ **WARN** with alternative suggestions
- `<30%` ⇒ **STOP**: Domain Pack not compatible

**Key Features:**
- Domain compatibility checking before ingestion
- Automatic domain suggestion based on column names
- Integration with existing goal and KPI management
- **New GoalKPIsService**: Simplified service for domain compatibility and KPI extraction

### Phase 2: Data Ingestion (`phase2_ingestion.py`)

**Decision Rules:**
- Always write parquet+zstd compression
- Prefer chunking if file > 1GB

**Key Features:**
- Support for CSV, Excel, JSON, and Parquet input formats
- Automatic compression with zstd algorithm
- Chunking for large files (>1GB)
- Comprehensive ingestion metrics
- **New IngestionService**: Simplified service for CSV/Excel ingestion with column sanitization

### Phase 3: Schema Validation (`phase3_schema.py`)

**Decision Rules:**
- IDs → string
- Timestamps → datetime[UTC]
- Numeric → float/int
- Categorical → category
- schema_violations > 0.02 ⇒ WARN

**Key Features:**
- Automatic data type enforcement based on column patterns
- Schema violation detection and reporting
- Domain-specific validation rules
- Comprehensive type conversion tracking
- **New SchemaService**: Simplified service for automatic type inference and schema generation

## API Endpoints

### Phase 1 Enhanced Endpoints

```http
GET /api/v1/phases/domain-packs
POST /api/v1/phases/domain-compatibility
POST /api/v1/phases/goal-kpis
```

### Phase 2 Endpoints

```http
POST /api/v1/phases/ingest
POST /api/v1/phases/ingest-simple
GET /api/v1/phases/ingest/status/{target_file}
GET /api/v1/phases/ingest/files
```

### Phase 3 Endpoints

```http
POST /api/v1/phases/schema/validate
POST /api/v1/phases/schema-simple
GET /api/v1/phases/schema/info/{file_path}
GET /api/v1/phases/schema/files
```

### Workflow Endpoints

```http
POST /api/v1/phases/workflow/domain-check
POST /api/v1/phases/workflow/full-pipeline
```

## Usage Examples

### 1. Domain Compatibility Check

```python
# Check if logistics domain is compatible with your data
response = client.post("/api/v1/phases/domain-compatibility", 
    json={
        "domain": "logistics",
        "columns": ["shipment_id", "order_id", "carrier", "origin", "destination"]
    }
)

# Response: OK status with 100% match
```

### 1b. Goal & KPIs Service (New)

```python
# Execute Phase 1: Goal & KPIs with domain compatibility
response = client.post("/api/v1/phases/goal-kpis",
    json={
        "columns": ["shipment_id", "order_id", "carrier", "origin", "destination"],
        "domain": "logistics"  # Optional - will auto-suggest if not provided
    }
)

# Response: Domain, KPIs, and compatibility result
# {
#   "domain": "logistics",
#   "kpis": ["SLA_pct", "TransitTime_avg", "RTO_pct", "FAS_pct", "NDR_pct"],
#   "compatibility": {
#     "status": "OK",
#     "match_percentage": 1.0,
#     "matched_columns": [...],
#     "missing_columns": [...],
#     "suggestions": {...},
#     "message": "Domain 'logistics' is compatible (100.0% match)"
#   }
# }
```

### 2. Data Ingestion

```python
# Ingest CSV file to Parquet format
response = client.post("/api/v1/phases/ingest",
    json={
        "source_file": "data.csv",
        "config": {
            "compression": "zstd",
            "chunk_size": 10000
        }
    }
)

# Response: Success with ingestion metrics
```

### 2b. Simple Ingestion Service (New)

```python
# Simple ingestion with column sanitization
response = client.post("/api/v1/phases/ingest-simple",
    json={
        "file_path": "data.csv",
        "artifacts_dir": "/path/to/output"  # Optional
    }
)

# Response: DataFrame info and ingestion result
# {
#   "dataframe_info": {
#     "shape": [1000, 5],
#     "columns": ["id", "name", "value", "category", "date"],
#     "dtypes": {"id": "int64", "name": "object", ...}
#   },
#   "ingestion_result": {
#     "rows": 1000,
#     "columns": 5,
#     "column_names": ["id", "name", "value", "category", "date"],
#     "file_size_mb": 0.25,
#     "parquet_path": "/path/to/output/raw_ingested.parquet",
#     "message": "Successfully ingested 1,000 rows × 5 columns"
#   }
# }
```

### 3. Schema Validation

```python
# Validate and enforce schema
response = client.post("/api/v1/phases/schema/validate",
    json={
        "file_path": "data_ingested.parquet",
        "domain_pack": "logistics"
    }
)

# Response: Schema validation results with type conversions
```

### 3b. Simple Schema Service (New)

```python
# Simple schema inference and type casting
with open("data.csv", "rb") as file:
    response = client.post("/api/v1/phases/schema-simple",
        files={"file": ("data.csv", file, "text/csv")}
    )

# Response: Automatic type inference and schema generation
# {
#   "file_info": {
#     "filename": "data.csv",
#     "original_shape": [1000, 5],
#     "typed_shape": [1000, 5],
#     "original_dtypes": {"id": "int64", "name": "object", ...},
#     "typed_dtypes": {"id": "string", "name": "category", ...}
#   },
#   "schema_result": {
#     "dtypes": {"id": "string", "name": "category", "age": "int64", ...},
#     "id_columns": ["id", "user_id"],
#     "datetime_columns": ["created_at", "updated_at"],
#     "numeric_columns": ["age", "salary"],
#     "categorical_columns": ["department", "status"],
#     "violations_pct": 0.0,
#     "warnings": [],
#     "schema_json": {
#       "columns": {...},
#       "index": None,
#       "coerce": True
#     }
#   }
# }
```

### 4. Full Pipeline Workflow

```python
# Complete pipeline: Upload → Domain Check → Ingest → Validate
with open("data.csv", "rb") as file:
    response = client.post("/api/v1/phases/workflow/full-pipeline",
        files={"file": ("data.csv", file, "text/csv")},
        data={
            "domain": "logistics",
            "auto_ingest": "true",
            "auto_validate": "true"
        }
    )

# Response: Complete workflow results
```

## Testing

Comprehensive test suites are provided:

- `test_phase1_enhanced.py`: Domain compatibility and enhanced Phase 1 functionality
- `test_phase2_ingestion.py`: Data ingestion service testing
- `test_phase3_schema.py`: Schema validation and data type enforcement
- `test_api_phases_integration.py`: API endpoint integration tests

Run tests with:

```bash
pytest backend/tests/test_phase1_enhanced.py -v
pytest backend/tests/test_phase2_ingestion.py -v
pytest backend/tests/test_phase3_schema.py -v
pytest backend/tests/test_api_phases_integration.py -v
```

## File Structure

```
backend/
├── app/
│   ├── services/
│   │   ├── domain_packs.py          # Domain pack definitions
│   │   ├── phase1_goal_kpis.py      # Enhanced Phase 1 service
│   │   ├── phase2_ingestion.py      # Phase 2 ingestion service
│   │   └── phase3_schema.py         # Phase 3 schema service
│   ├── models/
│   │   └── schemas.py               # Enhanced schemas
│   └── api/v1/
│       └── phases.py                # Enhanced API endpoints
├── tests/
│   ├── test_phase1_enhanced.py      # Phase 1 tests
│   ├── test_phase2_ingestion.py     # Phase 2 tests
│   ├── test_phase3_schema.py        # Phase 3 tests
│   └── test_api_phases_integration.py # API integration tests
└── artifacts/
    ├── landing/                     # Ingested files
    └── processed/                   # Schema-validated files
```

## Configuration

The services use the existing configuration from `app/config.py`:

- `artifacts_dir`: Base directory for file storage
- `landing/`: Directory for ingested Parquet files
- `processed/`: Directory for schema-validated files

## Error Handling

All services include comprehensive error handling:

- **File not found**: Graceful handling with clear error messages
- **Invalid formats**: Validation with helpful suggestions
- **Conversion errors**: Partial success with warnings
- **Domain incompatibility**: Clear status with alternative suggestions

## Performance Considerations

- **Chunking**: Automatic chunking for files > 1GB
- **Compression**: zstd compression for optimal size/speed ratio
- **Memory efficiency**: Streaming processing for large datasets
- **Caching**: Domain pack definitions cached in memory

## Future Enhancements

- Additional domain packs (manufacturing, education, etc.)
- Custom domain pack creation
- Advanced schema inference
- Real-time data ingestion
- Data quality scoring
- Automated data profiling

## Dependencies

- `pandas`: Data manipulation and analysis
- `fastapi`: Web framework
- `pydantic`: Data validation
- `pyarrow`: Parquet file handling
- `pytest`: Testing framework

## Status

✅ **Phase 1**: Domain compatibility checking implemented
✅ **Phase 2**: Data ingestion to Parquet implemented  
✅ **Phase 3**: Schema validation and type enforcement implemented
✅ **API Endpoints**: All endpoints implemented and tested
✅ **Tests**: Comprehensive test coverage
✅ **Documentation**: Complete implementation documentation

**Total Implementation Time**: ~8 hours
**Phases Implemented**: 4 (Phase 0, 1, 2, 3)
**Total Phases**: 14
