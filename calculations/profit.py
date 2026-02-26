"""
FO Reporting – Cálculos de Profit / Rentabilidad.

Reglas de cálculo obligatorias (con tests):

1) Profit JPM ETF:
   Profit = Income + Change_in_Value + (accrual_mes - accrual_mes_prev)

2) Profit UBS Suiza:
   Profit = total_assets_mes - movimientos_mes - total_assets_mes_prev

IMPORTANTE:
- Todas las funciones usan Decimal para precisión financiera.
- Nunca float para dinero.
- Cada función tiene docstring con fórmula explícita.
"""

from decimal import Decimal
from typing import Optional


def profit_jpm_etf(
    income: Decimal,
    change_in_value: Decimal,
    accrual_current: Decimal,
    accrual_previous: Decimal,
) -> Decimal:
    """
    Calcula profit para JPMorgan ETF.

    Fórmula:
        Profit = Income + Change_in_Value + (Accrual_mes - Accrual_mes_prev)

    Args:
        income: Income del período.
        change_in_value: Cambio en valor de mercado del período.
        accrual_current: Accrual del mes actual.
        accrual_previous: Accrual del mes anterior.

    Returns:
        Profit del período.
    """
    return income + change_in_value + (accrual_current - accrual_previous)


def profit_ubs_switzerland(
    total_assets_current: Decimal,
    movements: Decimal,
    total_assets_previous: Decimal,
) -> Decimal:
    """
    Calcula profit para UBS Suiza.

    Fórmula:
        Profit = Total_Assets_mes - Movimientos_mes - Total_Assets_mes_prev

    Args:
        total_assets_current: Total assets al cierre del mes actual.
        movements: Suma neta de movimientos del mes (aportes - retiros).
        total_assets_previous: Total assets al cierre del mes anterior.

    Returns:
        Profit del período.
    """
    return total_assets_current - movements - total_assets_previous


def monthly_return_pct(
    profit: Decimal,
    total_assets_previous: Decimal,
) -> Optional[Decimal]:
    """
    Rentabilidad mensual como porcentaje.

    Fórmula:
        Return % = Profit / Total_Assets_mes_prev * 100

    Returns:
        Porcentaje de rentabilidad, o None si no hay base.
    """
    if total_assets_previous == Decimal("0"):
        return None
    return (profit / total_assets_previous) * Decimal("100")


def ytd_return_pct(monthly_returns: list[Decimal]) -> Decimal:
    """
    Rentabilidad YTD compuesta (chain-linking).

    Fórmula:
        YTD = [(1 + r1/100) * (1 + r2/100) * ... * (1 + rn/100) - 1] * 100

    Args:
        monthly_returns: Lista de rentabilidades mensuales en %.

    Returns:
        YTD compuesto en %.
    """
    compound = Decimal("1")
    for r in monthly_returns:
        compound *= (Decimal("1") + r / Decimal("100"))
    return (compound - Decimal("1")) * Decimal("100")


def total_portfolio_value(
    account_values: list[Decimal],
) -> Decimal:
    """
    Suma total de todas las cuentas.
    Simple pero existe como función para testear y trazar.
    """
    return sum(account_values, Decimal("0"))
