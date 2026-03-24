"""Priorización de hallazgos."""

from __future__ import annotations

from decimal import Decimal
from typing import Optional

from backend.services.audit.audit_comparator import to_decimal


def compute_priority(
    *,
    nivel: str,
    diferencia: float,
    diferencia_pct: Optional[float],
) -> float:
    if nivel == "no_auditable":
        return -1.0
    if nivel == "ambiguo":
        return 0.5

    base = abs(float(diferencia))
    if diferencia_pct is not None:
        return base + float(diferencia_pct) * 1000.0
    return base


def classify_level(
    *,
    diferencia: Decimal,
    diferencia_pct: Optional[float],
    ambiguous: bool,
    comparable: bool,
) -> str:
    if ambiguous:
        return "ambiguo"
    if not comparable:
        return "no_auditable"

    ad = abs(float(diferencia))
    if diferencia_pct is not None:
        if diferencia_pct > 1.0 or ad > 10_000:
            return "alta"
        if diferencia_pct >= 0.1:
            return "media"
        return "baja"
    if ad > 10_000:
        return "alta"
    if ad > 100:
        return "media"
    return "baja"
