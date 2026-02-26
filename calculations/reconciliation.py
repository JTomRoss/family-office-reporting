"""
FO Reporting – Motor de conciliación.

Compara datos diarios contra cierres mensuales (cartolas).
La cartola manda como verdad de cierre.
Las diferencias se registran en tabla reconciliations.
"""

from dataclasses import dataclass, field
from decimal import Decimal
from datetime import date
from enum import Enum
from typing import Optional


class ReconciliationStatus(str, Enum):
    MATCHED = "matched"
    MINOR_DIFF = "minor_diff"      # < threshold
    MAJOR_DIFF = "major_diff"      # >= threshold
    MISSING_DAILY = "missing_daily"
    MISSING_MONTHLY = "missing_monthly"


@dataclass
class ReconciliationResult:
    """Resultado de una conciliación cuenta × mes."""
    account_id: int
    year: int
    month: int
    status: ReconciliationStatus

    daily_total: Optional[Decimal] = None
    monthly_total: Optional[Decimal] = None
    difference: Optional[Decimal] = None
    difference_pct: Optional[Decimal] = None
    currency: str = "USD"

    # Detalle por instrumento
    instrument_diffs: list[dict] = field(default_factory=list)

    messages: list[str] = field(default_factory=list)


def reconcile_monthly(
    daily_total: Optional[Decimal],
    monthly_total: Optional[Decimal],
    threshold_pct: Decimal = Decimal("0.01"),
    account_id: int = 0,
    year: int = 0,
    month: int = 0,
    currency: str = "USD",
) -> ReconciliationResult:
    """
    Concilia total diario contra cierre mensual.

    Regla: La cartola (monthly_total) manda como verdad.

    Args:
        daily_total: Total calculado desde datos diarios.
        monthly_total: Total de la cartola mensual (verdad).
        threshold_pct: Threshold de diferencia aceptable (%).
        account_id: ID de la cuenta.
        year: Año.
        month: Mes.
        currency: Moneda.

    Returns:
        ReconciliationResult con status y diferencias.
    """
    result = ReconciliationResult(
        account_id=account_id,
        year=year,
        month=month,
        status=ReconciliationStatus.MATCHED,
        currency=currency,
    )

    # Caso: falta dato diario
    if daily_total is None:
        result.status = ReconciliationStatus.MISSING_DAILY
        result.monthly_total = monthly_total
        result.messages.append("No hay datos diarios para este período")
        return result

    # Caso: falta cartola
    if monthly_total is None:
        result.status = ReconciliationStatus.MISSING_MONTHLY
        result.daily_total = daily_total
        result.messages.append("No hay cartola mensual para este período")
        return result

    result.daily_total = daily_total
    result.monthly_total = monthly_total
    result.difference = daily_total - monthly_total

    # Calcular % de diferencia
    if monthly_total != Decimal("0"):
        result.difference_pct = (
            abs(result.difference) / abs(monthly_total) * Decimal("100")
        )
    else:
        result.difference_pct = Decimal("0") if daily_total == Decimal("0") else Decimal("100")

    # Clasificar
    if result.difference == Decimal("0"):
        result.status = ReconciliationStatus.MATCHED
    elif result.difference_pct <= threshold_pct:
        result.status = ReconciliationStatus.MINOR_DIFF
        result.messages.append(
            f"Diferencia menor al threshold: {result.difference} ({result.difference_pct:.4f}%)"
        )
    else:
        result.status = ReconciliationStatus.MAJOR_DIFF
        result.messages.append(
            f"ALERTA: Diferencia mayor: {result.difference} ({result.difference_pct:.4f}%)"
        )

    return result


def reconcile_by_instrument(
    daily_positions: list[dict],
    monthly_positions: list[dict],
    key_field: str = "instrument_code",
    value_field: str = "market_value",
) -> list[dict]:
    """
    Concilia posiciones instrumento por instrumento.

    Returns:
        Lista de diferencias por instrumento.
    """
    daily_map = {p[key_field]: Decimal(str(p.get(value_field, 0))) for p in daily_positions}
    monthly_map = {p[key_field]: Decimal(str(p.get(value_field, 0))) for p in monthly_positions}

    all_instruments = set(daily_map.keys()) | set(monthly_map.keys())
    diffs = []

    for inst in sorted(all_instruments):
        d_val = daily_map.get(inst, Decimal("0"))
        m_val = monthly_map.get(inst, Decimal("0"))
        diff = d_val - m_val

        if diff != Decimal("0"):
            diffs.append({
                "instrument": inst,
                "daily_value": d_val,
                "monthly_value": m_val,
                "difference": diff,
                "in_daily_only": inst not in monthly_map,
                "in_monthly_only": inst not in daily_map,
            })

    return diffs
