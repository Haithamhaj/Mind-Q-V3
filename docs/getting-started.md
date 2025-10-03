# Getting Started & Setup

## Prerequisites
- Docker and Docker Compose (recommended for easiest setup)
- Alternatively: Python 3.11+ for backend development, Node.js 18+ for frontend

## Run with Docker (Recommended)
```bash
# Clone repository
git clone <repository-url>
cd "Mind-Q V3"

# Start all services
docker-compose up --build

# Access
# Backend API: http://localhost:8000
# Frontend:    http://localhost:3000
# API Docs:    http://localhost:8000/api/docs
```

## Local Development
```bash
# Backend
cd backend
pip install -r requirements.txt
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# Frontend (separate terminal)
cd frontend
npm install
npm run dev
```

## Folder Overview
```
backend/   # FastAPI service
frontend/  # React + TypeScript app
```

## Environment Files
- You may add a `.env` in the root or per service if needed.
- Adjust ports if already in use locally.

## Quick Verification
- Open `http://localhost:8000/health` to verify backend is healthy.
- Open `http://localhost:8000/api/docs` for Swagger UI.
- Open `http://localhost:3000` for the UI.
