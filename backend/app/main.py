from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from app.config import settings
from app.api.v1.router import api_router

app = FastAPI(
    title=settings.app_name,
    version=settings.version,
    docs_url="/api/docs",
    redoc_url="/api/redoc"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API Routes
app.include_router(api_router, prefix="/api/v1")

# Static files for artifacts download
app.mount("/artifacts", StaticFiles(directory=str(settings.artifacts_dir)), name="artifacts")

@app.get("/health")
async def health_check():
    return {
        "status": "ok",
        "version": settings.version,
        "spec_loaded": settings.spec_path.exists()
    }

@app.get("/")
async def root():
    return {
        "message": f"Welcome to {settings.app_name} v{settings.version}",
        "docs": "/api/docs"
    }
