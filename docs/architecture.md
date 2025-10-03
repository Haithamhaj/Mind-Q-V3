# Architecture & Project Layout

## Overview
The platform consists of two primary services:
- Backend (FastAPI) for EDA phases and REST APIs
- Frontend (React) for visualization and user interaction

## Components
- Backend (Python 3.11+): Services under `app/services/` per EDA phase; API under `app/api/v1/`.
- Frontend (React + TS): UI components under `src/components/` and pages under `src/pages/`.
- Docker Compose: Local multi-service orchestration.
- Artifacts: Intermediate outputs under `backend/artifacts/` e.g., `dq_report.json`.

## Structure
```
backend/
  app/
    api/v1/        # REST endpoints per phase
    services/      # Business logic for phases 0 → 13
    models/        # Pydantic schemas
    utils/         # Helpers
  validation_scripts/  # Phase validators
  spec/                # EDA specifications
frontend/
  src/
    components/    # Viewers and UI components
    pages/         # App pages
    api/           # API client
```

## High-Level Flow
1. User uploads/selects data via the frontend.
2. Frontend calls backend endpoints per phase.
3. Backend executes logic under `app/services/` and produces outputs.
4. Outputs are stored under `backend/artifacts/` and visualized in the frontend.

## Design Principles
- Clear separation between API layer and business logic.
- Phases are modular and verifiable via scripts.
- No database dependency for the MVP.
