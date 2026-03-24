"""Tolerancias y comparación numérica para auditoría."""

from __future__ import annotations

from decimal import Decimal
from typing import Optional

ABS_TOL = Decimal("0.01")
REL_TOL = Decimal("0.001")  # 0.1%


def to_decimal(value: object) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except Exception:
        return None


def within_tolerance(a: Optional[Decimal], b: Optional[Decimal]) -> bool:
    """True si ambos None o diferencia dentro de tolerancia absoluta/relativa."""
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    diff = abs(a - b)
    if diff <= ABS_TOL:
        return True
    base = max(abs(a), abs(b), Decimal("1"))
    if diff / base <= REL_TOL:
        return True
    return False


def difference_pct(
    parser_val: Optional[Decimal],
    bd_val: Optional[Decimal],
) -> Optional[float]:
    if parser_val is None or bd_val is None:
        return None
    base = max(abs(bd_val), abs(parser_val), Decimal("1"))
    try:
        return float(abs(parser_val - bd_val) / base * Decimal("100"))
    except Exception:
        return None
