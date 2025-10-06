from pydantic_settings import BaseSettings
from pathlib import Path
from dotenv import load_dotenv

# Ensure environment variables are loaded regardless of working directory
_backend_dir = Path(__file__).parent.parent
# Load backend/.env first
load_dotenv(_backend_dir / ".env")
# Also try project root .env (one level up from backend)
load_dotenv(_backend_dir.parent / ".env")

class Settings(BaseSettings):
    # App
    app_name: str = "Master EDA Platform"
    version: str = "1.2.2"
    debug: bool = True
    
    # Paths
    base_dir: Path = Path(__file__).parent.parent
    artifacts_dir: Path = base_dir / "artifacts"
    spec_path: Path = base_dir / "spec" / "master_eda_spec_v1.2.2.json"
    
    cors_origins: list[str] = [
        "http://localhost:3000",
        "http://localhost:5173",
        "http://localhost:5174",
        "http://localhost:5175",
        "http://localhost:5176",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:5174",
        "http://127.0.0.1:5175",
        "http://127.0.0.1:5176",
    ]
    
    max_file_size_mb: int = 500
    
    llm_provider: str = "gemini"
    gemini_api_key: str = ""
    anthropic_api_key: str = ""
    openai_api_key: str = ""
    # Phase 14.5 unified key
    llm_api_key: str = ""
    
    class Config:
        # Resolve to backend/.env explicitly so it works when running from repo root
        env_file = str(_backend_dir / ".env")
        extra = "ignore"  # Allow extra environment variables

settings = Settings()

# Ensure artifacts directory exists
settings.artifacts_dir.mkdir(exist_ok=True)