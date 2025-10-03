# Backend & APIs

## Run Locally
```bash
cd backend
pip install -r requirements.txt
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## Key Endpoints
- Swagger UI: `http://localhost:8000/api/docs`
- ReDoc: `http://localhost:8000/api/redoc`
- Health: `GET /health`

## API Layout
- `app/api/v1/router.py`: Routes assembly
- `app/api/v1/phases.py`: Phase endpoints

## Data Models
- `app/models/schemas.py`: Pydantic input/output schemas

## Services
Phase-specific logic lives in `app/services/`, e.g.:
- `phase0_quality_control.py`
- `phase1_goal_kpis.py`
- `phase2_ingestion.py`
- ...

Each service exposes functions invoked by the API layer.

## Tests
Backend tests live in `backend/tests/` and include integration, unit, and schema tests.
```bash
cd backend
pytest -q
```
