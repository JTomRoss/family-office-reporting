"""
FO Reporting – Configuración central.

Todas las rutas, constantes y settings viven aquí.
Usa pydantic-settings para cargar desde .env o variables de entorno.
"""

from pathlib import Path
from functools import lru_cache

from pydantic_settings import BaseSettings


# ── Rutas base del proyecto ─────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
CACHE_DIR = DATA_DIR / "cache"
SNAPSHOTS_DIR = DATA_DIR / "snapshots"
DB_DIR = DATA_DIR / "db"


class Settings(BaseSettings):
    """Configuración de la aplicación. Se puede sobreescribir con .env"""

    # ── Aplicación ───────────────────────────────────────────────────
    app_name: str = "FO Reporting"
    app_version: str = "0.1.0"
    debug: bool = False

    # ── Base de datos ────────────────────────────────────────────────
    database_url: str = f"sqlite:///{DB_DIR / 'fo_reporting.db'}"

    # ── Backend ──────────────────────────────────────────────────────
    backend_host: str = "0.0.0.0"
    backend_port: int = 8000
    api_prefix: str = "/api/v1"

    # ── Frontend ─────────────────────────────────────────────────────
    frontend_port: int = 8501
    backend_url: str = "http://localhost:8000"

    # ── CORS ─────────────────────────────────────────────────────────
    cors_origins: list[str] = [
        # Streamlit legacy (app antigua)
        "http://localhost:8501",
        "http://127.0.0.1:8501",
        "http://192.168.200.134:8501",
        # Reporting APP (frontend nuevo HTML estático, puerto 8701)
        "http://localhost:8701",
        "http://127.0.0.1:8701",
        "http://192.168.200.134:8701",
    ]
    # Sobreescribible vía FO_CORS_ORIGINS en .env (JSON array).

    # ── Archivos ─────────────────────────────────────────────────────
    max_upload_size_mb: int = 100
    allowed_pdf_extensions: list[str] = [".pdf"]
    allowed_excel_extensions: list[str] = [".xlsx", ".xls", ".csv"]

    # ── Cache ────────────────────────────────────────────────────────
    cache_enabled: bool = True
    cache_format: str = "parquet"  # "parquet" o "json"

    model_config = {"env_prefix": "FO_", "env_file": ".env", "extra": "ignore"}


@lru_cache
def get_settings() -> Settings:
    """Singleton de settings."""
    return Settings()


def ensure_dirs() -> None:
    """Crea directorios de datos si no existen."""
    for d in [RAW_DIR, CACHE_DIR, SNAPSHOTS_DIR, DB_DIR]:
        d.mkdir(parents=True, exist_ok=True)
