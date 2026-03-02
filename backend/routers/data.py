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
    Formato: una fila por cuenta por mes (tabla vertical).
    Columnas: fecha, sociedad, banco, id, moneda, ending_value,
              movimientos, profit, rent_mensual_pct, rent_mensual_sin_caja_pct.
    """
    # Query base: todos los monthly_closings con filtros de cuenta
    query = (
        db.query(MonthlyClosing, Account)
        .join(Account, MonthlyClosing.account_id == Account.id)
    )
    query = _apply_account_filters(query, filters)

    # Traer TODOS los meses de las cuentas filtradas (para cálculo de return)
    all_results = query.order_by(
        Account.id, MonthlyClosing.year, MonthlyClosing.month
    ).all()

    # Agrupar por cuenta, ordenadas cronológicamente
    by_account: dict[int, list[tuple]] = {}
    for mc, acct in all_results:
        by_account.setdefault(acct.id, []).append((mc, acct))

    # Determinar qué meses incluir según filtro de años
    year_filter = set(filters.years) if filters.years else None

    rows: list[dict] = []
    for acct_id, entries in by_account.items():
        entries.sort(key=lambda x: (x[0].year, x[0].month))
        for i, (mc, acct) in enumerate(entries):
            # Solo incluir meses que coincidan con el filtro de años
            if year_filter and mc.year not in year_filter:
                continue

            curr_val = float(mc.net_value) if mc.net_value else 0
            prev_val = float(entries[i - 1][0].net_value or 0) if i > 0 else None

            # Rentabilidad mensual %
            monthly_ret = None
            if prev_val and prev_val > 0:
                monthly_ret = round(((curr_val - prev_val) / prev_val) * 100, 4)

            # Movimientos: change_in_value del modelo, o diferencia
            movimientos = None
            if mc.change_in_value is not None:
                movimientos = float(mc.change_in_value)
            elif prev_val is not None:
                movimientos = curr_val - prev_val

            # Profit: income del modelo, o igual a movimientos
            profit = None
            if mc.income is not None:
                profit = float(mc.income)
            elif movimientos is not None:
                profit = movimientos

            # Sin caja: igual para cuentas no-cash, None para cash
            is_cash = acct.account_type in ("current", "savings")

            rows.append({
                "fecha": f"{mc.year}-{mc.month:02d}",
                "sociedad": acct.entity_name,
                "banco": acct.bank_code,
                "id": acct.identification_number or acct.account_number,
                "moneda": mc.currency,
                "ending_value": curr_val,
                "movimientos": movimientos,
                "profit": profit,
                "rent_mensual_pct": monthly_ret,
                "rent_mensual_sin_caja_pct": monthly_ret if not is_cash else None,
                "account_type": acct.account_type,
            })

    rows.sort(key=lambda r: (r["fecha"], r["sociedad"], r["banco"]))

    # Totales por mes (para gráficos)
    totals: dict[str, float] = {}
    for r in rows:
        mk = r["fecha"]
        totals[mk] = totals.get(mk, 0) + (r["ending_value"] or 0)

    filter_options = _get_filter_options(db)

    return {
        "rows": rows,
        "totals": {mk: f"{v:.2f}" for mk, v in totals.items()},
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
# ETF – Helpers
# ═══════════════════════════════════════════════════════════════════

SOCIETY_MAPPING = [
    ("Boatview JPM", lambda en, bc: "boatview" in en.lower() and bc == "jpmorgan"),
    ("Boatview GS", lambda en, bc: "boatview" in en.lower() and bc == "goldman_sachs"),
    ("Telmar", lambda en, bc: "telmar" in en.lower()),
    ("Armel Holdings", lambda en, bc: "armel" in en.lower()),
    ("Ect Internacional", lambda en, bc: "ecoterra" in en.lower()),
]

SOCIETY_COLS = ["Boatview JPM", "Boatview GS", "Telmar",
                "Armel Holdings", "Ect Internacional"]


def _get_society_label(entity_name: str, bank_code: str) -> str:
    """Map (entity_name, bank_code) → etiqueta de sociedad para tabla ETF."""
    for label, matcher in SOCIETY_MAPPING:
        if matcher(entity_name, bank_code):
            return label
    return entity_name


# ═══════════════════════════════════════════════════════════════════
# ETF – Endpoints
# ═══════════════════════════════════════════════════════════════════

@router.get("/etf-dates")
def get_etf_dates(db: Session = Depends(get_db)):
    """Retorna fechas YYYY-MM disponibles con datos ETF."""
    # Desde monthly_closings de cuentas ETF
    mc_dates = (
        db.query(MonthlyClosing.year, MonthlyClosing.month)
        .join(Account, MonthlyClosing.account_id == Account.id)
        .filter(Account.account_type == "etf")
        .distinct()
        .all()
    )
    # Desde etf_compositions
    comp_dates = (
        db.query(EtfComposition.year, EtfComposition.month)
        .distinct()
        .all()
    )
    all_dates = set()
    for y, m in mc_dates + comp_dates:
        all_dates.add(f"{y}-{m:02d}")
    return {"dates": sorted(all_dates, reverse=True)}


@router.post("/etf")
def get_etf(
    filters: FilterParams,
    db: Session = Depends(get_db),
):
    """
    Retorna datos para la pestaña ETF.

    Secciones:
    1) bank_society_table: Bancos×Sociedades (solo filtro fecha)
    2) composition_by_society: torta por sociedades (solo filtro fecha)
    3) composition_by_instrument: torta por instrumentos (solo filtro fecha)
    4) montos_table: instrumentos×meses del año (filtros fecha+banco+sociedad)
    """
    # Parsear fecha seleccionada
    sel_year, sel_month = None, None
    if filters.fecha:
        parts = filters.fecha.split("-")
        sel_year, sel_month = int(parts[0]), int(parts[1])
    elif filters.years:
        sel_year = max(filters.years)

    # ── 1) Bancos × Sociedades (solo filtro fecha) ──────────────
    mc_query = (
        db.query(MonthlyClosing, Account)
        .join(Account, MonthlyClosing.account_id == Account.id)
        .filter(Account.account_type == "etf")
    )
    if sel_year and sel_month:
        mc_query = mc_query.filter(
            MonthlyClosing.year == sel_year,
            MonthlyClosing.month == sel_month,
        )
    elif sel_year:
        mc_query = mc_query.filter(MonthlyClosing.year == sel_year)

    mc_results = mc_query.all()

    # Pivotear: {bank_code: {society_label: total, ...}}
    bank_society_data: dict[str, dict[str, float]] = {}
    society_totals: dict[str, float] = {}

    for mc, acct in mc_results:
        society = _get_society_label(acct.entity_name, acct.bank_code)
        bank = acct.bank_code
        val = float(mc.net_value or 0)

        if bank not in bank_society_data:
            bank_society_data[bank] = {s: 0.0 for s in SOCIETY_COLS}
        if society in bank_society_data[bank]:
            bank_society_data[bank][society] += val
        else:
            bank_society_data[bank][society] = val

        # Total por sociedad
        society_totals[society] = society_totals.get(society, 0) + val

    # Agregar columna Total a cada banco
    for bank, vals in bank_society_data.items():
        vals["Total"] = sum(vals.values())

    # ── 2-3) Composición (solo filtro fecha) ─────────────────────
    comp_query = (
        db.query(EtfComposition, Account)
        .join(Account, EtfComposition.account_id == Account.id)
    )
    if sel_year and sel_month:
        comp_query = comp_query.filter(
            EtfComposition.year == sel_year,
            EtfComposition.month == sel_month,
        )
    elif sel_year:
        comp_query = comp_query.filter(EtfComposition.year == sel_year)

    comp_results = comp_query.all()

    # Por sociedad
    by_society: dict[str, float] = {}
    # Por instrumento
    by_instrument: dict[str, float] = {}

    for comp, acct in comp_results:
        society = _get_society_label(acct.entity_name, comp.bank_code)
        mv = float(comp.market_value or 0)
        by_society[society] = by_society.get(society, 0) + mv
        by_instrument[comp.etf_name] = by_instrument.get(comp.etf_name, 0) + mv

    composition_by_society = [
        {"label": k, "value": round(v, 2)}
        for k, v in sorted(by_society.items(), key=lambda x: -x[1])
    ]
    composition_by_instrument = [
        {"label": k, "value": round(v, 2)}
        for k, v in sorted(by_instrument.items(), key=lambda x: -x[1])
    ]

    # ── 4) Montos: instrumentos × meses del año (todos los filtros) ──
    montos_query = (
        db.query(EtfComposition, Account)
        .join(Account, EtfComposition.account_id == Account.id)
    )
    if sel_year:
        montos_query = montos_query.filter(EtfComposition.year == sel_year)
    if filters.bank_codes:
        montos_query = montos_query.filter(
            Account.bank_code.in_(filters.bank_codes)
        )
    if filters.entity_names:
        montos_query = montos_query.filter(
            Account.entity_name.in_(filters.entity_names)
        )

    montos_results = montos_query.order_by(EtfComposition.month).all()

    # Pivotear: instrumento → {mes: monto}
    montos_data: dict[str, dict[int, float]] = {}
    for comp, acct in montos_results:
        name = comp.etf_name
        if name not in montos_data:
            montos_data[name] = {}
        montos_data[name][comp.month] = (
            montos_data[name].get(comp.month, 0) + float(comp.market_value or 0)
        )

    montos_table = []
    for name in sorted(montos_data.keys()):
        row = {"instrumento": name}
        for m in range(1, 13):
            row[f"{m:02d}"] = round(montos_data[name].get(m, 0), 2)
        montos_table.append(row)

    return {
        "bank_society_table": bank_society_data,
        "society_totals": society_totals,
        "composition_by_society": composition_by_society,
        "composition_by_instrument": composition_by_instrument,
        "montos_table": montos_table,
        "selected_year": sel_year,
        "selected_month": sel_month,
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
