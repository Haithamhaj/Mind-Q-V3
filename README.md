# Master EDA Platform V3

## Overview
A comprehensive automated exploratory data analysis platform designed for MVP investor demos. This platform provides end-to-end data analysis capabilities through 14 sequential phases.

## Architecture
- **Backend**: FastAPI + Python 3.11+
- **Frontend**: React 18+ + TypeScript + shadcn/ui
- **Infrastructure**: Docker + Docker Compose
- **Storage**: Local filesystem (no database required for MVP)

## Project Structure
```
eda-platform/
├── backend/              # FastAPI backend service
├── frontend/             # React frontend application
├── docker-compose.yml    # Multi-container setup
├── .env.example         # Environment variables template
├── .gitignore           # Git ignore rules
└── README.md            # This file
```

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.11+ (for local development)
- Node.js 18+ (for frontend development)

### Using Docker (Recommended)
```bash
# Clone repository
git clone <repository-url>
cd eda-platform

# Start all services
docker-compose up --build

# Access applications
# Backend API: http://localhost:8000
# Frontend: http://localhost:3000
# API Docs: http://localhost:8000/api/docs
```

### Local Development
```bash
# Backend
cd backend
pip install -r requirements.txt
uvicorn app.main:app --reload

# Frontend (separate terminal)
cd frontend
npm install
npm run dev
```

## EDA Pipeline Phases

| Phase | Name | Description | Status |
|-------|------|-------------|--------|
| 0 | Foundation & Architecture | Project setup and infrastructure | ✅ Complete |
| 1 | Goal & KPIs Definition | Define business objectives | ⏳ Pending |
| 2 | Data Ingestion | Upload and validate data files | ⏳ Pending |
| 3 | Schema Discovery | Analyze data structure | ⏳ Pending |
| 4 | Data Profiling | Generate comprehensive statistics | ⏳ Pending |
| 5 | Missing Data Analysis | Identify missing data patterns | ⏳ Pending |
| 6 | Data Standardization | Clean and standardize formats | ⏳ Pending |
| 7 | Feature Engineering | Create derived features | ⏳ Pending |
| 7.5 | Encoding & Scaling | Encode categorical variables | ⏳ Pending |
| 8 | Data Merging | Combine multiple datasets | ⏳ Pending |
| 9 | Correlation Analysis | Analyze variable relationships | ⏳ Pending |
| 9.5 | Business Validation | Validate against business rules | ⏳ Pending |
| 10 | Data Packaging | Prepare final dataset | ⏳ Pending |
| 10.5 | Train/Test Split | Split data for modeling | ⏳ Pending |
| 11 | Advanced Analytics | Perform advanced statistical analysis | ⏳ Pending |
| 11.5 | Feature Selection | Select optimal features | ⏳ Pending |
| 12 | Text Analysis | NLP analysis for text data | ⏳ Pending |
| 13 | Monitoring & Reporting | Generate reports and monitoring | ⏳ Pending |

## Supported Domains
- Finance
- Healthcare
- Retail
- Manufacturing
- Technology
- Education
- Government
- General

## Supported File Formats
- CSV
- Excel (XLSX)
- Parquet
- JSON

## API Documentation
- **Swagger UI**: http://localhost:8000/api/docs
- **ReDoc**: http://localhost:8000/api/redoc
- **Health Check**: http://localhost:8000/health

## Documentation
- Full docs: see `docs/README.md` for Getting Started, Architecture, Phases, and more.

## Development Guidelines

### Phase Implementation
1. Each phase must be implemented sequentially
2. Run validation script before proceeding to next phase
3. All phases must pass validation checks
4. Follow the established project structure

### Validation
```bash
# Validate current phase
python backend/validation_scripts/validate_phase0.py
```

## Contributing
1. Follow the phase-by-phase implementation approach
2. Ensure all validation checks pass
3. Maintain backward compatibility
4. Document all changes

## License
[Add your license here]

## Support
For questions or issues, please refer to the project documentation or create an issue in the repository.

## Agent Read‑only Phase Viewer (New)
This optional layer adds a read‑only Agent API and UI to inspect phases, KPIs and policies without executing backend services.

Endpoints
- GET /agent/graph → phases list/graph
- GET /agent/phase?phase=5 → read metrics from artifacts and compute goal score
- POST /agent/qa {question, phase} → answers only from docs/policies/artifacts with sources

Run locally
```bash
# Backend
uvicorn backend.app.main:app --reload --host 0.0.0.0 --port 8000

# Frontend
cd frontend
npm run dev
# Open http://localhost:5173/agent-viewer
```

Config
- AGENT_ARTIFACTS_DIR: override artifacts location (default backend/artifacts)

Notes
- Read‑only: no writes, no job execution, no changes to backend/app/services/*
- Policies are under policies/* (phase rules, scoring, best practices)
