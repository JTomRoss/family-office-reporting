"""
FO Reporting – Cálculos de Asset Allocation y Composición.
"""

from decimal import Decimal
from typing import Optional


def weight_pct(
    part_value: Decimal,
    total_value: Decimal,
) -> Optional[Decimal]:
    """
    Peso porcentual de una parte respecto al total.

    Fórmula:
        Weight % = (Part / Total) * 100

    Returns:
        Porcentaje, o None si total es 0.
    """
    if total_value == Decimal("0"):
        return None
    return (part_value / total_value) * Decimal("100")


def validate_allocation_sums_to_100(
    weights: list[Decimal],
    tolerance: Decimal = Decimal("0.01"),
) -> tuple[bool, Decimal]:
    """
    Verifica que los pesos sumen ~100%.

    Returns:
        (is_valid, difference_from_100)
    """
    total = sum(weights, Decimal("0"))
    diff = abs(total - Decimal("100"))
    return diff <= tolerance, diff


def etf_composition_check(
    instrument_values: list[Decimal],
    reported_total: Decimal,
    tolerance: Decimal = Decimal("0.01"),
) -> tuple[bool, Decimal]:
    """
    Verifica: Total composición instrumentos == Total tabla ETF.

    Esta es una regla obligatoria del negocio.

    Returns:
        (is_valid, difference)
    """
    calculated_total = sum(instrument_values, Decimal("0"))
    diff = abs(calculated_total - reported_total)
    return diff <= tolerance, diff


def mandate_allocation_pct(
    mandate_value: Decimal,
    total_portfolio: Decimal,
) -> Optional[Decimal]:
    """
    Porcentaje de cada mandato respecto al portfolio total.

    Fórmula:
        % Mandato = (Valor_Mandato / Total_Portfolio) * 100
    """
    return weight_pct(mandate_value, total_portfolio)
