# Master EDA Platform - Backend

## Overview
This is the backend service for the Master EDA Platform, a comprehensive automated exploratory data analysis system.

## Technology Stack
- **Framework**: FastAPI 0.104+
- **Language**: Python 3.11+
- **Data Science**: pandas, scikit-learn, statsmodels, scipy
- **Validation**: pandera
- **Visualization**: plotly, seaborn
- **NLP**: nltk, gensim

## Project Structure
```
backend/
├── app/
│   ├── api/v1/          # API endpoints
│   ├── services/         # Business logic
│   ├── models/          # Pydantic models
│   └── utils/           # Utilities
├── tests/               # Test files
├── validation_scripts/  # Phase validators
├── artifacts/          # Temporary storage
└── spec/               # Platform specification
```

## Quick Start

### Development
```bash
# Install dependencies
pip install -r requirements.txt

# Run development server
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Docker
```bash
# Build image
docker build -t eda-backend .

# Run container
docker run -p 8000:8000 eda-backend
```

## API Documentation
- Swagger UI: http://localhost:8000/api/docs
- ReDoc: http://localhost:8000/api/redoc

## Health Check
```bash
curl http://localhost:8000/health
```

## Phase Validation
```bash
python validation_scripts/validate_phase0.py
```

## Current Status
- Phase 0: ✅ Foundation & Architecture (Completed)
- Phase 1-13: ⏳ Pending implementation
