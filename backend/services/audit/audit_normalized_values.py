"""Valores de la capa normalizada para comparar contra el LLM (sin leer parser)."""

from __future__ import annotations

from decimal import Decimal
from typing import Optional

from backend.db.models import MonthlyMetricNormalized
from backend.services.audit.audit_comparator import to_decimal
from backend.services.audit.audit_deterministic import _sum_asset_allocation_normalized_json

FOCUS_LABELS = {
    "valor_cierre": "Valor de cierre",
    "movimientos_netos": "Movimientos netos",
    "caja": "Caja",
    "instrumentos": "Instrumentos (diccionario)",
    "aportes": "Aportes",
    "retiros": "Retiros",
}


def elemento_label_for_focus(focus: str) -> str:
    return FOCUS_LABELS.get(focus, focus)


def get_bd_value_for_focus(
    focus: str,
    norm: Optional[MonthlyMetricNormalized],
) -> tuple[Optional[Decimal], str]:
    """
    Devuelve (valor_en_bd, etiqueta_elemento).
    Si no hay fila normalizada, bv es None.
    """
    label = elemento_label_for_focus(focus)
    if norm is None:
        return None, label

    if focus == "valor_cierre":
        return to_decimal(norm.ending_value_with_accrual), label
    if focus == "movimientos_netos":
        return to_decimal(norm.movements_net), label
    if focus == "caja":
        return to_decimal(norm.cash_value), label
    if focus == "instrumentos":
        bv = _sum_asset_allocation_normalized_json(norm.asset_allocation_json)
        return bv, label
    return None, label
