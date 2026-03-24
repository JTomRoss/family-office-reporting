"""Comparación determinística: valores persistidos en ParsedStatement vs normalizado."""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Optional

from backend.db.models import MonthlyMetricNormalized, ParsedStatement
from backend.services.audit.audit_comparator import to_decimal


@dataclass
class DeterministicCompareResult:
    parser_value: Optional[Decimal]
    bd_value: Optional[Decimal]
    comparable: bool
    ambiguous: bool
    elemento_label: str


def _parse_json_blob(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except (TypeError, ValueError):
        return {}


def _first_decimal(*candidates: object) -> Optional[Decimal]:
    for c in candidates:
        d = to_decimal(c)
        if d is not None:
            return d
    return None


def _extract_from_balances(data: dict[str, Any]) -> Optional[Decimal]:
    bal = data.get("balances")
    if not isinstance(bal, dict):
        return None
    return _first_decimal(
        bal.get("ending"),
        bal.get("net_assets"),
        bal.get("closing"),
        bal.get("total_net_assets"),
    )


def _extract_parser_closing(stmt: ParsedStatement, data: dict[str, Any]) -> Optional[Decimal]:
    v = to_decimal(stmt.closing_balance)
    if v is not None:
        return v
    v = _first_decimal(
        data.get("closing_balance"),
        data.get("net_value"),
        data.get("ending_value"),
    )
    if v is not None:
        return v
    v = _extract_from_balances(data)
    if v is not None:
        return v
    qual = data.get("qualitative_data")
    if isinstance(qual, dict):
        v = _first_decimal(qual.get("net_value"), qual.get("ending_value"))
        if v is not None:
            return v
    rows = data.get("rows")
    if isinstance(rows, list) and rows:
        for row in rows:
            if not isinstance(row, dict):
                continue
            v = _first_decimal(row.get("ending_balance"), row.get("net_value"))
            if v is not None:
                return v
    return None


def _extract_parser_movements(data: dict[str, Any]) -> Optional[Decimal]:
    v = _first_decimal(
        data.get("movements_net"),
        data.get("change_in_value"),
        data.get("net_movements"),
    )
    if v is not None:
        return v
    qual = data.get("qualitative_data")
    if isinstance(qual, dict):
        v = _first_decimal(
            qual.get("movements_net"),
            qual.get("change_in_value"),
        )
        if v is not None:
            return v
    return None


def _extract_parser_cash(data: dict[str, Any]) -> Optional[Decimal]:
    """Suma heurística de líneas de caja en asset_allocation / rows."""
    aa = data.get("asset_allocation")
    if isinstance(aa, dict):
        total = Decimal("0")
        for _k, payload in aa.items():
            if isinstance(payload, dict):
                raw = (
                    payload.get("value")
                    or payload.get("total")
                    or payload.get("market_value")
                    or payload.get("amount")
                )
            else:
                raw = payload
            d = to_decimal(raw)
            if d is None:
                continue
            label = str(_k).lower()
            if any(
                t in label
                for t in ("cash", "deposit", "money market", "liquidity", "sweep")
            ):
                total += d
        if total != 0:
            return total
    return None


def _sum_positions_market_value(data: dict[str, Any]) -> Optional[Decimal]:
    rows = data.get("rows") or data.get("positions")
    if not isinstance(rows, list):
        return None
    total = Decimal("0")
    n = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        mv = to_decimal(row.get("market_value") or row.get("value"))
        if mv is not None:
            total += mv
            n += 1
    if n == 0:
        return None
    return total


def _sum_asset_allocation_normalized_json(asset_json: str | None) -> Optional[Decimal]:
    if not asset_json:
        return None
    try:
        alloc = json.loads(asset_json)
    except (TypeError, ValueError):
        return None
    total = Decimal("0")
    n = 0

    def add_val(x: object) -> None:
        nonlocal n, total
        if isinstance(x, dict):
            raw = (
                x.get("value")
                or x.get("total")
                or x.get("ending")
                or x.get("market_value")
                or x.get("amount")
            )
        else:
            raw = x
        d = to_decimal(raw)
        if d is not None:
            total += d
            n += 1

    if isinstance(alloc, dict):
        for _k, payload in alloc.items():
            add_val(payload)
    elif isinstance(alloc, list):
        for row in alloc:
            add_val(row)
    if n == 0:
        return None
    return total


def compare_deterministic(
    *,
    focus: str,
    stmt: ParsedStatement,
    norm: Optional[MonthlyMetricNormalized],
) -> DeterministicCompareResult:
    """
    Compara un campo según focus entre JSON parseado / columnas SQL y capa normalizada.
    """
    data = _parse_json_blob(stmt.parsed_data_json)

    if focus == "aportes" or focus == "retiros":
        return DeterministicCompareResult(
            parser_value=None,
            bd_value=None,
            comparable=False,
            ambiguous=True,
            elemento_label="Aportes" if focus == "aportes" else "Retiros",
        )

    if norm is None:
        label = {
            "valor_cierre": "Valor de cierre",
            "movimientos_netos": "Movimientos netos",
            "instrumentos": "Instrumentos (diccionario)",
            "caja": "Caja",
        }.get(focus, focus)
        return DeterministicCompareResult(
            parser_value=None,
            bd_value=None,
            comparable=False,
            ambiguous=False,
            elemento_label=label,
        )

    if focus == "valor_cierre":
        pv = _extract_parser_closing(stmt, data)
        bv = to_decimal(norm.ending_value_with_accrual)
        return DeterministicCompareResult(
            parser_value=pv,
            bd_value=bv,
            comparable=pv is not None and bv is not None,
            ambiguous=pv is None,
            elemento_label="Valor de cierre",
        )

    if focus == "movimientos_netos":
        pv = _extract_parser_movements(data)
        bv = to_decimal(norm.movements_net)
        return DeterministicCompareResult(
            parser_value=pv,
            bd_value=bv,
            comparable=pv is not None and bv is not None,
            ambiguous=pv is None,
            elemento_label="Movimientos netos",
        )

    if focus == "caja":
        pv = _extract_parser_cash(data)
        bv = to_decimal(norm.cash_value)
        if pv is None:
            pv = _first_decimal(data.get("cash_value"))
        return DeterministicCompareResult(
            parser_value=pv,
            bd_value=bv,
            comparable=pv is not None and bv is not None,
            ambiguous=pv is None,
            elemento_label="Caja",
        )

    if focus == "instrumentos":
        pv = _sum_positions_market_value(data)
        bv = _sum_asset_allocation_normalized_json(norm.asset_allocation_json)
        return DeterministicCompareResult(
            parser_value=pv,
            bd_value=bv,
            comparable=pv is not None and bv is not None,
            ambiguous=pv is None or bv is None,
            elemento_label="Instrumentos (diccionario)",
        )

    return DeterministicCompareResult(
        parser_value=None,
        bd_value=None,
        comparable=False,
        ambiguous=True,
        elemento_label=focus,
    )

