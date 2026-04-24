"""
FO Reporting – Router "reporting" (lectura consolidada para el frontend nuevo).

Todas las rutas son GET, read-only, y consumen la capa normalizada
(monthly_metrics_normalized) como SSOT vía backend.services.reporting_reads.

Este router NO reemplaza /api/v1/data/* (usado por Streamlit); lo complementa
con un shape REST-friendly alineado al README del frontend nuevo.
"""

import re

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from backend.db.session import get_db
from backend.services import reporting_reads

router = APIRouter(prefix="/reporting", tags=["reporting"])

_PERIOD_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def _validate_period(period: str) -> str:
    if not _PERIOD_RE.match(period or ""):
        raise HTTPException(
            status_code=400,
            detail="period inválido. Formato esperado: YYYY-MM (ej. 2026-03)",
        )
    return period


@router.get("/dashboard")
def get_dashboard(
    period: str = Query(..., description="Período YYYY-MM (ej. 2026-03)"),
    scope: str = Query(
        "international",
        description="Ámbito del dashboard: 'international' (no-BICE, USD) o 'national' (BICE CLP o USD). Nunca se mezclan.",
    ),
    bice_currency: str = Query(
        "CLP",
        description="Solo para scope=national: elige qué mundo BICE mostrar ('CLP' | 'USD'). Ignorado en international.",
    ),
    db: Session = Depends(get_db),
) -> dict:
    """
    Snapshot consolidado del período + serie de 13 meses.

    `scope=international`: cuentas no-BICE, datos en USD desde
        monthly_metrics_normalized (+ fallback monthly_closings).
    `scope=national` + `bice_currency=CLP`: cuentas BICE en CLP desde
        bice_monthly_snapshot. Igual para USD.
    Los mundos CLP/USD nunca se mezclan ni se convierten.
    """
    _validate_period(period)
    if scope not in ("international", "national"):
        raise HTTPException(status_code=400, detail="scope inválido")
    if bice_currency not in ("CLP", "USD"):
        raise HTTPException(status_code=400, detail="bice_currency debe ser CLP o USD")
    return reporting_reads.get_dashboard(db, period, scope=scope, bice_currency=bice_currency)


@router.get("/positions")
def get_positions(
    period: str = Query(..., description="Período YYYY-MM (ej. 2026-03)"),
    db: Session = Depends(get_db),
) -> dict:
    """
    Snapshot de posiciones por instrumento del período pedido.
    Foto: último día con datos de `daily_positions` dentro del mes.
    """
    _validate_period(period)
    return reporting_reads.get_positions(db, period)


@router.get("/normalized")
def get_normalized(
    period: str = Query(..., description="Período YYYY-MM (ej. 2026-03)"),
    scope: str = Query("international", description="international | national"),
    bice_currency: str = Query(
        "CLP", description="Solo para scope=national: 'CLP' o 'USD'."
    ),
    db: Session = Depends(get_db),
) -> dict:
    """
    Lectura directa de la capa canónica para auditoría.
    - scope=international → monthly_metrics_normalized + fallback closings
    - scope=national → bice_monthly_snapshot (con bice_currency CLP o USD)
    """
    _validate_period(period)
    if scope not in ("international", "national"):
        raise HTTPException(status_code=400, detail="scope inválido")
    if bice_currency not in ("CLP", "USD"):
        raise HTTPException(status_code=400, detail="bice_currency debe ser CLP o USD")
    return reporting_reads.get_normalized_rows(
        db, period, scope=scope, bice_currency=bice_currency
    )


@router.get("/returns")
def get_returns(
    period: str = Query(..., description="Período YYYY-MM"),
    scope: str = Query("international", description="international | national"),
    db: Session = Depends(get_db),
) -> dict:
    """
    Rentabilidades: ret_monthly[13] consolidado + desglose por sociedad
    (mom%, ytd%, serie de 12m). TWR YTD por chain-linking (§5.2).
    """
    _validate_period(period)
    if scope not in ("international", "national"):
        raise HTTPException(status_code=400, detail="scope inválido")
    return reporting_reads.get_returns(db, period, scope=scope)


@router.get("/alternatives")
def get_alternatives(
    period: str = Query(..., description="Período YYYY-MM"),
    db: Session = Depends(get_db),
) -> dict:
    """
    Alternativos (bank_code='alternativos'): fondos PE/RE con NAV del período,
    strategy, society, vintage. Campos commit/distributed/irr/tvpi retornan
    null hasta que el Excel de alternativos los exponga.
    """
    _validate_period(period)
    return reporting_reads.get_alternatives(db, period)


@router.get("/audit-log")
def get_audit_log(
    limit: int = Query(200, ge=1, le=2000),
    db: Session = Depends(get_db),
) -> list[dict]:
    """
    Log inmutable del sistema (ValidationLog). Shape:
      {ts, user, event, obj, detail, severity, account_id, document_id}
    """
    return reporting_reads.get_audit_log(db, limit=limit)


@router.get("/files")
def get_files(
    limit: int = Query(500, ge=1, le=5000),
    bank_code: str | None = None,
    status: str | None = Query(None, description="SUCCESS | PARTIAL | ERROR"),
    file_type: str | None = Query(None, description="pdf_cartola | pdf_report | excel_alternatives | ..."),
    db: Session = Depends(get_db),
) -> dict:
    """Lista raw_documents con metadata para la página de Archivos."""
    return reporting_reads.get_files(
        db, limit=limit, bank_code=bank_code, status=status, file_type=file_type,
    )


@router.get("/coverage")
def get_coverage(
    months: int = Query(12, ge=1, le=36, description="Últimos N meses"),
    scope: str = Query("international", description="international | national"),
    db: Session = Depends(get_db),
) -> dict:
    """
    Matriz de cobertura cuenta × mes: ¿hay cartola cargada por celda?
    Usa raw_documents con file_type ∈ {pdf_cartola, pdf_report}.
    Excluye bank_code='alternativos' (Excel, no cartola mensual).
    """
    if scope not in ("international", "national"):
        raise HTTPException(status_code=400, detail="scope inválido")
    return reporting_reads.get_coverage(db, months=months, scope=scope)
