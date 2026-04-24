"""
FO Reporting – Router "dictionary" (diccionarios canónicos para UI).

Sirve al frontend nuevo ("Reporting APP"). Todas las rutas son GET y read-only.
Los diccionarios son la fuente de verdad para colores, buckets, ETFs y
categorías de mandato usadas por la UI.
"""

from fastapi import APIRouter

from backend.services import reporting_reads

router = APIRouter(prefix="/dictionary", tags=["dictionary"])


@router.get("/buckets")
def list_buckets() -> list[dict]:
    """Buckets canónicos (§6.1 RULES_INHERITED) con color y orden de display."""
    return reporting_reads.get_buckets()


@router.get("/etf")
def list_etf_instruments() -> list[dict]:
    """Instrumentos ETF canónicos (§6.2)."""
    return reporting_reads.get_etf_dictionary()


@router.get("/mandates")
def list_mandate_categories() -> list[str]:
    """Categorías canónicas de mandato (§6.3)."""
    return reporting_reads.get_mandate_categories()
