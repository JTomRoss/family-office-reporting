"""
FO Reporting – Router "master" (datos de referencia: cuentas, sociedades, bancos, parsers).

Sirve al frontend nuevo ("Reporting APP"). Todas las rutas son GET y read-only.
"""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from backend.db.session import get_db
from backend.services import reporting_reads

router = APIRouter(prefix="/master", tags=["master"])


@router.get("/accounts")
def list_accounts(db: Session = Depends(get_db)) -> list[dict]:
    """Lista de cuentas activas del maestro (shape para frontend nuevo)."""
    return reporting_reads.get_master_accounts(db)


@router.get("/societies")
def list_societies(db: Session = Depends(get_db)) -> list[dict]:
    """Sociedades (entidades) distintas extraídas del maestro."""
    return reporting_reads.get_master_societies(db)


@router.get("/banks")
def list_banks(db: Session = Depends(get_db)) -> list[dict]:
    """Bancos distintos con metadatos estables (nombre largo, short, país)."""
    return reporting_reads.get_master_banks(db)


@router.get("/parsers")
def list_parsers() -> list[dict]:
    """Inventario canónico de parsers registrados (fallback estático si registry vacío)."""
    return reporting_reads.get_master_parsers()
