from pydantic_settings import BaseSettings
from pathlib import Path

class Settings(BaseSettings):
    # App
    app_name: str = "Master EDA Platform"
    version: str = "1.2.2"
    debug: bool = True
    
    # Paths
    base_dir: Path = Path(__file__).parent.parent
    artifacts_dir: Path = base_dir / "artifacts"
    spec_path: Path = base_dir / "spec" / "master_eda_spec_v1.2.2.json"
    
    cors_origins: list[str] = ["http://localhost:3000", "http://localhost:5173", "http://localhost:5174", "http://localhost:5175"]
    
    max_file_size_mb: int = 500
    
    llm_provider: str = "gemini"
    gemini_api_key: str = ""
    anthropic_api_key: str = ""
    openai_api_key: str = ""
    
    class Config:
        env_file = ".env"
        extra = "ignore"  # Allow extra environment variables

settings = Settings()

# Ensure artifacts directory exists
settings.artifacts_dir.mkdir(exist_ok=True)