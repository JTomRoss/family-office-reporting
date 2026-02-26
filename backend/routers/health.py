"""
FO Reporting – Router de Health y sistema.
"""

import subprocess
from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.config import get_settings
from backend.db.session import get_db
from backend.schemas import HealthResponse, ParserInfo
from parsers.registry import get_registry

router = APIRouter(tags=["system"])


@router.get("/health", response_model=HealthResponse)
def health_check(db: Session = Depends(get_db)):
    """Health check con info del sistema."""
    settings = get_settings()
    registry = get_registry()

    # Git hash
    git_hash = None
    try:
        git_hash = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL
        ).strip()
    except Exception:
        pass

    # Test DB connection
    db_status = "connected"
    try:
        db.execute(text("SELECT 1"))
    except Exception:
        db_status = "error"

    return HealthResponse(
        status="ok",
        version=settings.app_version,
        database=db_status,
        parsers_loaded=len(registry.list_parsers()),
        git_hash=git_hash,
    )


@router.get("/parsers", response_model=list[ParserInfo])
def list_parsers():
    """Lista todos los parsers registrados."""
    registry = get_registry()
    return registry.list_parsers()
