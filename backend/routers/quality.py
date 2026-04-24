"""
FO Reporting – Router "quality" (alertas de calidad para el frontend nuevo).

Combina ValidationLog + heurísticas on-the-fly (rentabilidad fuera de rango,
cobertura normalized, etc.).
"""

import re

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from backend.db.session import get_db
from backend.services import reporting_reads

router = APIRouter(prefix="/quality", tags=["quality"])

_PERIOD_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def _validate_period(period: str) -> str:
    if not _PERIOD_RE.match(period or ""):
        raise HTTPException(status_code=400, detail="period inválido. Formato YYYY-MM.")
    return period


@router.get("/alerts")
def get_alerts(
    period: str = Query(..., description="Período YYYY-MM"),
    scope: str = Query("international", description="international | national"),
    limit: int = Query(200, ge=10, le=1000),
    db: Session = Depends(get_db),
) -> dict:
    """Alertas de calidad del período (logs + heurísticas)."""
    _validate_period(period)
    if scope not in ("international", "national"):
        raise HTTPException(status_code=400, detail="scope inválido")
    return reporting_reads.get_alerts(db, period, scope=scope, limit=limit)
