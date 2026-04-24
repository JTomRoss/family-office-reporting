"""
FO Reporting – Router "parity" (agente auditor de paridad de datos).

Compara lo que ve la app antigua (Streamlit vía /api/v1/data/*) con lo que ve la
app nueva (/api/v1/reporting/*). Sirve para que el usuario pueda pedir, a demanda,
una confirmación de que el frontend nuevo no está mostrando números distintos.

Fase 1: compara patrimonio USD agregado por período.
Fase 2 (pendiente): comparación por cuenta/campo + drill-down por instrumento.
"""

import re

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from backend.db.session import get_db
from backend.db.models import Account, MonthlyClosing, MonthlyMetricNormalized
from backend.services import reporting_reads
from backend.routers.data import _extract_reporting_value_exclusion_total

router = APIRouter(prefix="/parity", tags=["parity"])

_PERIOD_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def _validate_period(period: str) -> str:
    if not _PERIOD_RE.match(period or ""):
        raise HTTPException(
            status_code=400,
            detail="period inválido. Formato esperado: YYYY-MM (ej. 2026-03)",
        )
    return period


_NATIONAL_BANK_CODES = {"bice_inversiones", "bice_asesorias"}


def _scope_account_filter(query, scope: str):
    if scope == "international":
        return query.filter(~Account.bank_code.in_(_NATIONAL_BANK_CODES))
    if scope == "national":
        return query.filter(Account.bank_code.in_(_NATIONAL_BANK_CODES))
    return query


def _baseline_total_usd(db: Session, year: int, month: int, scope: str) -> dict:
    """
    Calcula el total USD de referencia leyendo directo de la capa normalizada
    (priorizando MonthlyMetricNormalized, fallback a MonthlyClosing) SIN pasar
    por reporting_reads, para que sea una fuente independiente de comparación.
    """
    q_norm = (
        db.query(MonthlyMetricNormalized, Account)
        .join(Account, Account.id == MonthlyMetricNormalized.account_id)
        .filter(
            MonthlyMetricNormalized.year == year,
            MonthlyMetricNormalized.month == month,
            Account.currency.in_(["USD", "USDC"]),
            Account.is_active.is_(True),
        )
    )
    rows_norm = _scope_account_filter(q_norm, scope).all()

    seen_account_ids = {acct.id for (_, acct) in rows_norm}
    total = 0.0
    exclusion_total = 0.0
    n_norm = 0
    for norm, _ in rows_norm:
        if norm.ending_value_with_accrual is not None:
            total += float(norm.ending_value_with_accrual)
            n_norm += 1
        # Aplica la regla estable §5.4: dedupe vs Alternativos.xlsx. Sin esto
        # el baseline sobreestima en ~3.3M y el parity reporta DIFF falso.
        exclusion_total += _extract_reporting_value_exclusion_total(norm.asset_allocation_json)

    # Fallback a MonthlyClosing para cuentas sin fila normalizada
    q_closing = (
        db.query(MonthlyClosing, Account)
        .join(Account, Account.id == MonthlyClosing.account_id)
        .filter(
            MonthlyClosing.year == year,
            MonthlyClosing.month == month,
            Account.currency.in_(["USD", "USDC"]),
            Account.is_active.is_(True),
        )
    )
    rows_closing = _scope_account_filter(q_closing, scope).all()
    n_fallback = 0
    for mc, acct in rows_closing:
        if acct.id in seen_account_ids:
            continue
        if mc.net_value is not None:
            total += float(mc.net_value)
            n_fallback += 1
        exclusion_total += _extract_reporting_value_exclusion_total(mc.asset_allocation_json)

    return {
        "total_usd": round(total - exclusion_total, 2),
        "total_usd_raw": round(total, 2),
        "exclusion_usd": round(exclusion_total, 2),
        "n_accounts_normalized": n_norm,
        "n_accounts_fallback_closing": n_fallback,
    }


@router.get("/dashboard")
def parity_dashboard(
    period: str = Query(..., description="Período YYYY-MM"),
    scope: str = Query("international", description="international | national"),
    tolerance_usd: float = Query(1.0, description="Tolerancia en USD para marcar OK"),
    db: Session = Depends(get_db),
) -> dict:
    """
    Compara el total USD que entrega /api/v1/reporting/dashboard contra la
    misma suma calculada por un lector independiente (directo a la capa
    normalizada + fallback). Si la diferencia supera `tolerance_usd`, devuelve
    status='DIFF'.
    """
    _validate_period(period)
    if scope not in ("international", "national"):
        raise HTTPException(status_code=400, detail="scope inválido")
    year, month = map(int, period.split("-"))

    # Lo que dice el frontend nuevo
    dash = reporting_reads.get_dashboard(db, period, scope=scope)
    new_total = float(dash.get("kpis", {}).get("patrimonio_usd") or 0.0)

    # Lo que dice una lectura directa independiente
    baseline = _baseline_total_usd(db, year, month, scope)
    base_total = baseline["total_usd"]

    diff = round(new_total - base_total, 2)
    status = "OK" if abs(diff) <= tolerance_usd else "DIFF"

    return {
        "period": period,
        "scope": scope,
        "status": status,
        "tolerance_usd": tolerance_usd,
        "new_total_usd": new_total,
        "baseline_total_usd": base_total,
        "diff_usd": diff,
        "baseline_detail": baseline,
    }
