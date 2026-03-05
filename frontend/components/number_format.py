"""Helpers de formato numerico para UI (es-CL style)."""

from __future__ import annotations


def _swap_separators(value: str) -> str:
    # Python format usa miles="," y decimal="." -> convertir a miles="." y decimal=",".
    return value.replace(",", "_").replace(".", ",").replace("_", ".")


def fmt_number(value, *, decimals: int = 2, blank_if_zero: bool = False) -> str:
    if value is None:
        return ""
    try:
        num = float(value)
    except (TypeError, ValueError):
        return ""
    if blank_if_zero and num == 0:
        return ""
    return _swap_separators(f"{num:,.{decimals}f}")


def fmt_percent(value, *, decimals: int = 2) -> str:
    if value is None:
        return ""
    try:
        num = float(value)
    except (TypeError, ValueError):
        return ""
    return f"{_swap_separators(f'{num:.{decimals}f}')}%"


def fmt_currency(value, *, decimals: int = 2) -> str:
    return f"${fmt_number(value, decimals=decimals)}"

