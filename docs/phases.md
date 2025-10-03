# EDA Pipeline Phases

Below is the intended flow. Each phase has corresponding services under `backend/app/services/` and tests under `backend/tests/`.

| Phase | Name | Purpose |
|------:|------|---------|
| 0 | Foundation & Architecture | Project setup and health checks |
| 1 | Goal & KPIs Definition | Capture business goals and KPIs |
| 2 | Data Ingestion | Upload, parse, and validate input files |
| 3 | Schema Discovery | Infer schema, types, constraints |
| 4 | Data Profiling | Descriptive stats, distributions, outliers |
| 5 | Missing Data Analysis | Patterns and extent of missingness |
| 6 | Data Standardization | Clean types, formats, normalization |
| 7 | Feature Engineering | Derived columns and transformations |
| 7.5 | Encoding & Scaling | Categorical encoding, scaling |
| 8 | Data Merging | Join/merge multiple datasets |
| 9 | Correlation Analysis | Associations and correlations |
| 9.5 | Business Validation | Rule-based business checks |
| 10 | Data Packaging | Prepare final dataset for modeling |
| 10.5 | Train/Test Split | Create reproducible splits |
| 11 | Advanced Analytics | Statistical/NLP/others |
| 11.5 | Feature Selection | Select optimal features |
| 12 | Text Analysis | NLP analysis for text data |
| 13 | Monitoring & Reporting | Reports and health monitoring |

## Validators
Each implemented phase should pass its validator under `backend/validation_scripts/` before proceeding.
