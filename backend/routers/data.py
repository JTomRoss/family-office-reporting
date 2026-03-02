"""
FO Reporting – Router de datos financieros (resumen, mandatos, ETF, personal).

Consulta tablas de reporting pobladas por DataLoadingService.
"""

import json
from typing import Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy import func
from sqlalchemy.orm import Session

from backend.db.models import (
    Account,
    EtfComposition,
    MonthlyClosing,
)
from backend.db.session import get_db
from backend.schemas import FilterParams, SummaryResponse

router = APIRouter(prefix="/data", tags=["data"])


# ═══════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════

def _apply_account_filters(query, filters: FilterParams):
    """Aplica filtros de banco, sociedad, tipo cuenta a un query que
    ya tiene join con Account."""
    if filters.bank_codes:
        query = query.filter(Account.bank_code.in_(filters.bank_codes))
    if filters.entity_names:
        query = query.filter(Account.entity_name.in_(filters.entity_names))
    if filters.account_types:
        query = query.filter(Account.account_type.in_(filters.account_types))
    if filters.currencies:
        query = query.filter(Account.currency.in_(filters.currencies))
    return query


def _get_filter_options(db: Session) -> dict:
    """Obtiene opciones de filtro disponibles basándose en los MonthlyClosings existentes."""
    # Años disponibles con datos
    years = [
        row[0]
        for row in db.query(MonthlyClosing.year).distinct().order_by(MonthlyClosing.year).all()
    ]
    # Bancos con datos
    bank_codes = [
        row[0]
        for row in (
            db.query(Account.bank_code)
            .join(MonthlyClosing, MonthlyClosing.account_id == Account.id)
            .distinct()
            .all()
        )
    ]
    entity_names = [
        row[0]
        for row in (
            db.query(Account.entity_name)
            .join(MonthlyClosing, MonthlyClosing.account_id == Account.id)
            .distinct()
            .all()
        )
    ]
    account_types = [
        row[0]
        for row in (
            db.query(Account.account_type)
            .join(MonthlyClosing, MonthlyClosing.account_id == Account.id)
            .distinct()
            .all()
        )
    ]
    return {
        "years": years,
        "months": list(range(1, 13)),
        "bank_codes": bank_codes,
        "entity_names": entity_names,
        "account_types": account_types,
        "currencies": [],
    }


# ═══════════════════════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════════════════════

@router.post("/summary")
def get_summary(
    filters: FilterParams,
    db: Session = Depends(get_db),
):
    """
    Retorna datos para la pestaña Resumen.
    Consulta monthly_closings con joins a accounts.
    """
    query = (
        db.query(MonthlyClosing, Account)
        .join(Account, MonthlyClosing.account_id == Account.id)
    )

    # Filtros de cuenta
    query = _apply_account_filters(query, filters)

    # Filtro de año
    if filters.years:
        query = query.filter(MonthlyClosing.year.in_(filters.years))

    results = query.order_by(
        Account.entity_name, Account.bank_code, MonthlyClosing.year, MonthlyClosing.month
    ).all()

    # Agrupar por cuenta para generar filas de tabla
    rows_by_account: dict[int, dict] = {}
    for mc, acct in results:
        key = acct.id
        if key not in rows_by_account:
            rows_by_account[key] = {
                "entity_name": acct.entity_name,
                "bank_code": acct.bank_code,
                "account_number": acct.account_number,
                "identification_number": acct.identification_number or "",
                "account_type": acct.account_type,
                "currency": acct.currency,
                "month_values": {},
            }
        month_key = f"{mc.year}-{mc.month:02d}"
        rows_by_account[key]["month_values"][month_key] = (
            str(mc.net_value) if mc.net_value is not None else None
        )

    rows = list(rows_by_account.values())

    # Calcular totales por mes
    totals: dict[str, str] = {}
    if rows:
        all_months = set()
        for r in rows:
            all_months.update(r["month_values"].keys())
        for mk in sorted(all_months):
            total = sum(
                float(r["month_values"].get(mk, 0) or 0)
                for r in rows
            )
            totals[mk] = f"{total:.2f}"

    filter_options = _get_filter_options(db)

    return {
        "rows": rows,
        "totals": totals,
        "filter_options": filter_options,
        "active_filters": filters.model_dump(),
    }


# ═══════════════════════════════════════════════════════════════════
# MANDATES
# ═══════════════════════════════════════════════════════════════════

@router.post("/mandates")
def get_mandates(
    filters: FilterParams,
    db: Session = Depends(get_db),
):
    """
    Retorna datos para la pestaña Mandatos.
    Filtra cuentas con account_type='mandato'.
    """
    query = (
        db.query(MonthlyClosing, Account)
        .join(Account, MonthlyClosing.account_id == Account.id)
        .filter(Account.account_type == "mandato")
    )
    query = _apply_account_filters(query, filters)
    if filters.years:
        query = query.filter(MonthlyClosing.year.in_(filters.years))

    results = query.order_by(
        Account.entity_name, MonthlyClosing.year, MonthlyClosing.month
    ).all()

    if not results:
        return {
            "mandate_pcts": [],
            "asset_allocation": [],
            "aa_by_bank": {},
            "banks_by_month": [],
            "returns_table": [],
            "message": "Sin datos de mandatos",
        }

    # Agrupar por banco x mes
    banks_by_month: list[dict] = []
    for mc, acct in results:
        banks_by_month.append({
            "bank_code": acct.bank_code,
            "entity_name": acct.entity_name,
            "year": mc.year,
            "month": mc.month,
            "net_value": str(mc.net_value) if mc.net_value else None,
            "income": str(mc.income) if mc.income else None,
        })

    return {
        "mandate_pcts": [],
        "asset_allocation": [],
        "aa_by_bank": {},
        "banks_by_month": banks_by_month,
        "returns_table": [],
    }


# ═══════════════════════════════════════════════════════════════════
# ETF
# ═══════════════════════════════════════════════════════════════════

@router.post("/etf")
def get_etf(
    filters: FilterParams,
    db: Session = Depends(get_db),
):
    """
    Retorna datos para la pestaña ETF.
    Usa monthly_closings para evolución y etf_compositions para detalle.
    """
    # --- Evolución mensual (monthly_closings para cuentas ETF) ---
    mc_query = (
        db.query(MonthlyClosing, Account)
        .join(Account, MonthlyClosing.account_id == Account.id)
        .filter(Account.account_type == "etf")
    )
    mc_query = _apply_account_filters(mc_query, filters)
    if filters.years:
        mc_query = mc_query.filter(MonthlyClosing.year.in_(filters.years))

    mc_results = mc_query.order_by(
        MonthlyClosing.year, MonthlyClosing.month
    ).all()

    # Evolución mensual: total por mes
    monthly_evolution: list[dict] = []
    totals_by_month: dict[str, float] = {}
    for mc, acct in mc_results:
        mk = f"{mc.year}-{mc.month:02d}"
        val = float(mc.net_value) if mc.net_value else 0
        totals_by_month[mk] = totals_by_month.get(mk, 0) + val

    for mk in sorted(totals_by_month.keys()):
        parts = mk.split("-")
        monthly_evolution.append({
            "year": int(parts[0]),
            "month": int(parts[1]),
            "total_value": f"{totals_by_month[mk]:.2f}",
        })

    # Bancos x Sociedades (último mes)
    bank_entity_totals: list[dict] = []
    if mc_results:
        latest_year = max(mc.year for mc, _ in mc_results)
        latest_month = max(
            mc.month for mc, _ in mc_results if mc.year == latest_year
        )
        for mc, acct in mc_results:
            if mc.year == latest_year and mc.month == latest_month:
                bank_entity_totals.append({
                    "bank_code": acct.bank_code,
                    "entity_name": acct.entity_name,
                    "account_number": acct.account_number,
                    "net_value": str(mc.net_value) if mc.net_value else None,
                    "currency": mc.currency,
                })

    # --- Composición ETF (etf_compositions) ---
    comp_query = (
        db.query(EtfComposition, Account)
        .join(Account, EtfComposition.account_id == Account.id)
    )
    if filters.bank_codes:
        comp_query = comp_query.filter(Account.bank_code.in_(filters.bank_codes))
    if filters.entity_names:
        comp_query = comp_query.filter(Account.entity_name.in_(filters.entity_names))
    if filters.years:
        comp_query = comp_query.filter(EtfComposition.year.in_(filters.years))

    comp_results = comp_query.order_by(
        EtfComposition.year, EtfComposition.month, EtfComposition.etf_name
    ).all()

    # Composiciones: últimos mes disponible
    composition_pct: list[dict] = []
    composition_amounts: list[dict] = []
    if comp_results:
        latest_yr = max(c.year for c, _ in comp_results)
        latest_mo = max(c.month for c, _ in comp_results if c.year == latest_yr)

        latest_comps = [
            (c, a) for c, a in comp_results
            if c.year == latest_yr and c.month == latest_mo
        ]
        total_val = sum(float(c.market_value or 0) for c, _ in latest_comps)

        for comp, acct in latest_comps:
            mv = float(comp.market_value or 0)
            pct = (mv / total_val * 100) if total_val > 0 else 0
            composition_pct.append({
                "etf_code": comp.etf_code,
                "etf_name": comp.etf_name,
                "weight_pct": f"{pct:.2f}",
            })
            composition_amounts.append({
                "etf_code": comp.etf_code,
                "etf_name": comp.etf_name,
                "market_value": str(comp.market_value) if comp.market_value else "0",
                "currency": comp.currency,
            })

    # Composición por mes (para gráfico stacked)
    comp_by_month: dict[str, dict[str, float]] = {}
    for comp, acct in comp_results:
        mk = f"{comp.year}-{comp.month:02d}"
        if mk not in comp_by_month:
            comp_by_month[mk] = {}
        code = comp.etf_code
        comp_by_month[mk][code] = comp_by_month[mk].get(code, 0) + float(
            comp.market_value or 0
        )

    # Rentabilidad mensual (basada en cambio de net_value)
    returns_table: list[dict] = []
    # Agrupar monthly_closings por cuenta
    by_account: dict[int, list] = {}
    for mc, acct in mc_results:
        by_account.setdefault(acct.id, []).append((mc, acct))

    for acct_id, entries in by_account.items():
        entries.sort(key=lambda x: (x[0].year, x[0].month))
        acct = entries[0][1]
        for i, (mc, _) in enumerate(entries):
            prev_val = float(entries[i - 1][0].net_value or 0) if i > 0 else None
            curr_val = float(mc.net_value or 0)
            ret = None
            if prev_val and prev_val > 0:
                ret = ((curr_val - prev_val) / prev_val) * 100
            returns_table.append({
                "bank_code": acct.bank_code,
                "entity_name": acct.entity_name,
                "year": mc.year,
                "month": mc.month,
                "net_value": f"{curr_val:.2f}",
                "monthly_return_pct": f"{ret:.2f}" if ret is not None else None,
            })

    return {
        "bank_entity_totals": bank_entity_totals,
        "composition_pct": composition_pct,
        "composition_amounts": composition_amounts,
        "composition_by_month": comp_by_month,
        "monthly_evolution": monthly_evolution,
        "returns_table": returns_table,
    }


@router.post("/personal")
def get_personal(
    person: str = Query(..., description="Nombre de la persona"),
    year: int = Query(..., description="Año"),
    month: Optional[int] = Query(None, description="Mes (opcional)"),
    db: Session = Depends(get_db),
):
    """
    Retorna datos para la pestaña Personal.
    - Saldo consolidado USD/CLP + caja
    - Gráficos torta
    - Tabla sociedades
    - Tabla resumen vertical
    - Tabla rango personalizado
    """
    return {
        "person": person,
        "consolidated_usd": None,
        "consolidated_clp": None,
        "cash": None,
        "pie_charts": {},
        "entities_table": [],
        "summary_table": [],
        "message": "STUB: Pendiente implementación",
    }


@router.post("/reconciliation")
def get_reconciliation(
    filters: FilterParams,
    db: Session = Depends(get_db),
):
    """
    Retorna datos de conciliación (pestaña operacional).
    Diferencias entre datos diarios y cartolas mensuales.
    """
    return {
        "reconciliation_results": [],
        "unresolved_count": 0,
        "total_count": 0,
        "message": "STUB: Pendiente implementación",
    }


@router.get("/validation-logs")
def get_validation_logs(
    severity: Optional[str] = None,
    validation_type: Optional[str] = None,
    limit: int = 100,
    db: Session = Depends(get_db),
):
    """Retorna logs de validación para audit trail."""
    from backend.db.models import ValidationLog

    query = db.query(ValidationLog)
    if severity:
        query = query.filter(ValidationLog.severity == severity)
    if validation_type:
        query = query.filter(ValidationLog.validation_type == validation_type)

    logs = query.order_by(ValidationLog.created_at.desc()).limit(limit).all()
    return [
        {
            "id": log.id,
            "validation_type": log.validation_type,
            "severity": log.severity,
            "message": log.message,
            "created_at": log.created_at.isoformat(),
            "source_module": log.source_module,
        }
        for log in logs
    ]
