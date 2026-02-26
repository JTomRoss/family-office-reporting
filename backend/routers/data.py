"""
FO Reporting – Router de datos financieros (resumen, mandatos, ETF, personal).

Todos los endpoints retornan datos pre-calculados desde cache.
Si el cache no existe, calcula y guarda.
"""

from typing import Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from backend.db.session import get_db
from backend.schemas import FilterParams, SummaryResponse

router = APIRouter(prefix="/data", tags=["data"])


@router.post("/summary")
def get_summary(
    filters: FilterParams,
    db: Session = Depends(get_db),
):
    """
    Retorna datos para la pestaña Resumen.

    TODO: Implementar lectura desde cache Parquet.
    STUB: Retorna estructura vacía con opciones de filtro.
    """
    return {
        "rows": [],
        "totals": {},
        "filter_options": {
            "years": [],
            "months": list(range(1, 13)),
            "bank_codes": [],
            "entity_names": [],
            "account_types": [],
            "currencies": [],
        },
        "active_filters": filters.model_dump(),
        "message": "STUB: Pendiente implementación con datos reales",
    }


@router.post("/mandates")
def get_mandates(
    filters: FilterParams,
    db: Session = Depends(get_db),
):
    """
    Retorna datos para la pestaña Mandatos.
    - % Mandatos 12m
    - Asset allocation 12m
    - AA por banco (0-100%)
    - Tabla bancos x meses
    - Tabla rentabilidad mensual / YTD
    """
    return {
        "mandate_pcts": [],
        "asset_allocation": [],
        "aa_by_bank": {},
        "banks_by_month": [],
        "returns_table": [],
        "message": "STUB: Pendiente implementación",
    }


@router.post("/etf")
def get_etf(
    filters: FilterParams,
    db: Session = Depends(get_db),
):
    """
    Retorna datos para la pestaña ETF.
    - Bancos x sociedades (totales)
    - Composición instrumentos %
    - Composición instrumentos montos
    - Evolución mensual
    - Rentabilidad mensual / YTD
    """
    return {
        "bank_entity_totals": [],
        "composition_pct": [],
        "composition_amounts": [],
        "monthly_evolution": [],
        "returns_table": [],
        "message": "STUB: Pendiente implementación",
    }


@router.post("/personal")
def get_personal(
    person: str = Query(..., description="Nombre de la persona"),
    year: int = Query(..., description="Año"),
    month: Optional[int] = Query(None, description="Mes (opcional)"),
    db: Session = Depends(get_db),
):
    """
    Retorna datos para la pestaña Personal.
    - Saldo consolidado USD/CLP + caja
    - Gráficos torta
    - Tabla sociedades
    - Tabla resumen vertical
    - Tabla rango personalizado
    """
    return {
        "person": person,
        "consolidated_usd": None,
        "consolidated_clp": None,
        "cash": None,
        "pie_charts": {},
        "entities_table": [],
        "summary_table": [],
        "message": "STUB: Pendiente implementación",
    }


@router.post("/reconciliation")
def get_reconciliation(
    filters: FilterParams,
    db: Session = Depends(get_db),
):
    """
    Retorna datos de conciliación (pestaña operacional).
    Diferencias entre datos diarios y cartolas mensuales.
    """
    return {
        "reconciliation_results": [],
        "unresolved_count": 0,
        "total_count": 0,
        "message": "STUB: Pendiente implementación",
    }


@router.get("/validation-logs")
def get_validation_logs(
    severity: Optional[str] = None,
    validation_type: Optional[str] = None,
    limit: int = 100,
    db: Session = Depends(get_db),
):
    """Retorna logs de validación para audit trail."""
    from backend.db.models import ValidationLog

    query = db.query(ValidationLog)
    if severity:
        query = query.filter(ValidationLog.severity == severity)
    if validation_type:
        query = query.filter(ValidationLog.validation_type == validation_type)

    logs = query.order_by(ValidationLog.created_at.desc()).limit(limit).all()
    return [
        {
            "id": log.id,
            "validation_type": log.validation_type,
            "severity": log.severity,
            "message": log.message,
            "created_at": log.created_at.isoformat(),
            "source_module": log.source_module,
        }
        for log in logs
    ]
