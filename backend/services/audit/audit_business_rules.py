"""Reglas de negocio conocidas para enriquecer notas y niveles."""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any, Optional

from backend.services.audit.audit_comparator import to_decimal, within_tolerance


def _parse_json(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        o = json.loads(raw)
        return o if isinstance(o, dict) else {}
    except (TypeError, ValueError):
        return {}


def _extract_movements_ytd_from_parsed(data: dict[str, Any]) -> Optional[Decimal]:
    v = to_decimal(data.get("movements_ytd"))
    if v is not None:
        return v
    qual = data.get("qualitative_data")
    if isinstance(qual, dict):
        return to_decimal(qual.get("movements_ytd"))
    return None


def _extract_prior_period_adjustments(data: dict[str, Any]) -> Optional[Decimal]:
    v = to_decimal(data.get("prior_period_adjustments"))
    if v is not None:
        return v
    qual = data.get("qualitative_data")
    if isinstance(qual, dict):
        return to_decimal(qual.get("prior_period_adjustments"))
    return None


def enrich_hallazgo(
    *,
    bank_code: str,
    elemento_revisado: str,
    parsed_data_json: str | None,
    nota: str,
    nivel: str,
    norm_movements_ytd: Optional[Decimal] = None,
) -> tuple[str, str]:
    """
    Regla BBH: si hay diferencia en movimientos YTD y cuadra con prior_period_adjustments.
    """
    data = _parse_json(parsed_data_json)
    note = nota
    level = nivel

    if bank_code != "bbh":
        return note, level
    # Regla documentada: aplica a diferencias en movimientos YTD, no al neto mensual.
    if "ytd" not in elemento_revisado.lower():
        return note, level

    p_ytd = _extract_movements_ytd_from_parsed(data)
    if p_ytd is None or norm_movements_ytd is None:
        return note, level

    prior_adj = _extract_prior_period_adjustments(data)
    if prior_adj is None:
        return note, level

    ytd_gap = abs(p_ytd - norm_movements_ytd)
    if not within_tolerance(ytd_gap, abs(prior_adj)):
        return note, level

    note = (
        note + " YTD BBH incluye prior adjustments; la diferencia podría explicarse por ello."
    ).strip()
    level = "baja"
    return note, level


def note_beginning_if_relevant(
    *,
    parsed_data_json: str | None,
    opening_statement: Optional[Decimal],
    prev_ending_normalized: Optional[Decimal],
) -> str:
    """Texto opcional si beginning != prev_ending (para enriquecer otro hallazgo)."""
    if opening_statement is None or prev_ending_normalized is None:
        return ""
    if within_tolerance(opening_statement, prev_ending_normalized):
        return ""
    return (
        "Beginning value de la cartola no coincide con el valor de cierre anterior; "
        "por regla conocida prevalece el ending value auditado."
    )
