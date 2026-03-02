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

    # Consolidar por mes para gráficos (consistente con tabla)
    consolidated: dict[str, dict] = {}
    for r in rows:
        mk = r["fecha"]
        if mk not in consolidated:
            consolidated[mk] = {
                "ending_value": 0.0,
                "movimientos": 0.0,
                "profit": 0.0,
            }
        consolidated[mk]["ending_value"] += (r["ending_value"] or 0)
        consolidated[mk]["movimientos"] += (r["movimientos"] or 0)
        consolidated[mk]["profit"] += (r["profit"] or 0)

    # Calcular rentabilidad consolidada
    sorted_mks = sorted(consolidated.keys())
    chart_data: list[dict] = []
    for i, mk in enumerate(sorted_mks):
        c = consolidated[mk]
        prev_ev = consolidated[sorted_mks[i - 1]]["ending_value"] if i > 0 else None
        ret_pct = None
        if prev_ev and prev_ev > 0:
            ret_pct = round(((c["ending_value"] - prev_ev) / prev_ev) * 100, 4)
        chart_data.append({
            "fecha": mk,
            "ending_value": round(c["ending_value"], 2),
            "movimientos": round(c["movimientos"], 2),
            "profit": round(c["profit"], 2),
            "rent_pct": ret_pct,
        })

    filter_options = _get_filter_options(db)

    return {
        "rows": rows,
        "chart_data": chart_data,
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

# Diccionario de consolidación de nombres de instrumentos ETF
INSTRUMENT_NAME_MAP: dict[str, str] = {
    "IWDA": "IWDA",
    "ISHARES CORE MSCI WORLD": "IWDA",
    "IEMA": "IEMA",
    "ISHARES MSCI EM-ACC": "IEMA",
    "IHYA": "IHYA",
    "ISHARES USD HY CORP USD ACC": "IHYA",
    "ISHARES USD HIGH YIELD CORP BOND": "IHYA",
    "VDCA": "VDCA",
    "VAND USDCP1-3 USDA": "VDCA",
    "VDPA": "VDPA",
    "VANG USDCPBD USDA": "VDPA",
}

# Orden fijo de instrumentos
INSTRUMENT_ORDER = ["IWDA", "IEMA", "VDCA", "VDPA", "IHYA", "Money Market"]

# Instrumentos considerados caja/money market
CASH_INSTRUMENTS = {"Money Market"}


def _normalize_instrument(name: str) -> str:
    """Normaliza nombre de instrumento según diccionario."""
    upper = name.strip().upper()
    if upper in INSTRUMENT_NAME_MAP:
        return INSTRUMENT_NAME_MAP[upper]
    # Detección heurística de money market / caja
    low = name.lower()
    if any(kw in low for kw in ("sweep", "liquidity", "money market", "cash", "depósito", "deposito")):
        return "Money Market"
    return INSTRUMENT_NAME_MAP.get(upper, name)


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
    mc_dates = (
        db.query(MonthlyClosing.year, MonthlyClosing.month)
        .join(Account, MonthlyClosing.account_id == Account.id)
        .filter(Account.account_type == "etf")
        .distinct()
        .all()
    )
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
    1) instruments_table: Instrumentos×Sociedades (montos, solo Fecha)
    2) instruments_pct_table: Instrumentos×Sociedades (pesos %, solo Fecha)
    3) composition_by_society / composition_by_instrument: tortas
    4) society_montos_table: Sociedades×Meses (montos, todos filtros)
    5) society_returns_table: Sociedades×Meses (rent %, todos filtros)
    """
    # Parsear fecha seleccionada
    sel_year, sel_month = None, None
    if filters.fecha:
        parts = filters.fecha.split("-")
        sel_year, sel_month = int(parts[0]), int(parts[1])
    elif filters.years:
        sel_year = max(filters.years)

    # ── helpers: filtro sin caja ─────────────────────────────────
    sin_caja = getattr(filters, "sin_caja", False)

    # ── 1-2) Instrumentos × Sociedades (solo filtro fecha) ──────
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

    # Pivot: {instrument: {society: monto}}
    instr_society: dict[str, dict[str, float]] = {}
    for comp, acct in comp_results:
        instr = _normalize_instrument(comp.etf_name)
        society = _get_society_label(acct.entity_name, comp.bank_code)
        mv = float(comp.market_value or 0)
        if instr not in instr_society:
            instr_society[instr] = {s: 0.0 for s in SOCIETY_COLS}
        if society in instr_society[instr]:
            instr_society[instr][society] += mv
        else:
            instr_society[instr][society] = mv

    # Montos table (orden fijo)
    instruments_table: dict[str, dict[str, float]] = {}
    for instr in INSTRUMENT_ORDER:
        if instr in instr_society:
            row = instr_society[instr].copy()
            row["Total"] = sum(row.values())
            instruments_table[instr] = row

    # Instrumentos no en el orden fijo
    for instr, vals in instr_society.items():
        if instr not in instruments_table:
            row = vals.copy()
            row["Total"] = sum(row.values())
            instruments_table[instr] = row

    # Pesos % table
    grand_total = sum(v.get("Total", 0) for v in instruments_table.values())
    instruments_pct_table: dict[str, dict[str, float]] = {}
    for instr, vals in instruments_table.items():
        pct_row = {}
        for col in SOCIETY_COLS + ["Total"]:
            v = vals.get(col, 0)
            pct_row[col] = round((v / grand_total * 100), 4) if grand_total > 0 else 0
        instruments_pct_table[instr] = pct_row

    # ── 3) Composición para tortas (afectado por sin_caja) ──────
    by_society: dict[str, float] = {}
    by_instrument: dict[str, float] = {}

    for comp, acct in comp_results:
        instr = _normalize_instrument(comp.etf_name)
        if sin_caja and instr in CASH_INSTRUMENTS:
            continue
        society = _get_society_label(acct.entity_name, comp.bank_code)
        mv = float(comp.market_value or 0)
        by_society[society] = by_society.get(society, 0) + mv
        by_instrument[instr] = by_instrument.get(instr, 0) + mv

    composition_by_society = [
        {"label": k, "value": round(v, 2)}
        for k, v in sorted(by_society.items(), key=lambda x: -x[1])
    ]
    composition_by_instrument = [
        {"label": k, "value": round(v, 2)}
        for k, v in sorted(by_instrument.items(), key=lambda x: -x[1])
    ]

    # ── 4) Society montos × meses del año (todos los filtros) ───
    montos_query = (
        db.query(EtfComposition, Account)
        .join(Account, EtfComposition.account_id == Account.id)
    )
    if sel_year:
        montos_query = montos_query.filter(EtfComposition.year == sel_year)
    if filters.bank_codes:
        montos_query = montos_query.filter(Account.bank_code.in_(filters.bank_codes))
    if filters.entity_names:
        montos_query = montos_query.filter(Account.entity_name.in_(filters.entity_names))

    montos_results = montos_query.order_by(EtfComposition.month).all()

    # Pivotear: society → {mes: monto}
    society_month_montos: dict[str, dict[int, float]] = {}
    for comp, acct in montos_results:
        society = _get_society_label(acct.entity_name, comp.bank_code)
        mv = float(comp.market_value or 0)
        if society not in society_month_montos:
            society_month_montos[society] = {}
        society_month_montos[society][comp.month] = (
            society_month_montos[society].get(comp.month, 0) + mv
        )

    # Construir tabla con orden fijo de sociedades
    society_montos_table = []
    totals_by_month: dict[int, float] = {}
    for soc in SOCIETY_COLS:
        row = {"sociedad": soc}
        for m in range(1, 13):
            val = society_month_montos.get(soc, {}).get(m, 0)
            row[f"{m:02d}"] = round(val, 2)
            totals_by_month[m] = totals_by_month.get(m, 0) + val
        society_montos_table.append(row)

    # Fila total
    total_row = {"sociedad": "Total"}
    for m in range(1, 13):
        total_row[f"{m:02d}"] = round(totals_by_month.get(m, 0), 2)
    society_montos_table.append(total_row)

    # ── 5) Society returns × meses (rent % mensual y YTD) ──────
    # Necesitamos monthly_closings por sociedad para calcular returns
    mc_year_query = (
        db.query(MonthlyClosing, Account)
        .join(Account, MonthlyClosing.account_id == Account.id)
        .filter(Account.account_type == "etf")
    )
    if sel_year:
        # Traer año actual y anterior para cálculo de return enero
        mc_year_query = mc_year_query.filter(
            MonthlyClosing.year.in_([sel_year - 1, sel_year])
        )
    if filters.bank_codes:
        mc_year_query = mc_year_query.filter(Account.bank_code.in_(filters.bank_codes))
    if filters.entity_names:
        mc_year_query = mc_year_query.filter(Account.entity_name.in_(filters.entity_names))

    mc_year_results = mc_year_query.order_by(
        MonthlyClosing.year, MonthlyClosing.month
    ).all()

    # Agrupar por sociedad × (year, month)
    soc_month_val: dict[str, dict[tuple, float]] = {}
    for mc, acct in mc_year_results:
        society = _get_society_label(acct.entity_name, acct.bank_code)
        key = (mc.year, mc.month)
        if society not in soc_month_val:
            soc_month_val[society] = {}
        soc_month_val[society][key] = soc_month_val[society].get(key, 0) + float(mc.net_value or 0)

    # Calcular monthly return % y YTD
    society_returns_monthly: list[dict] = []
    society_returns_ytd: list[dict] = []

    for soc in SOCIETY_COLS:
        monthly_row = {"sociedad": soc}
        ytd_row = {"sociedad": soc}
        vals = soc_month_val.get(soc, {})

        # Base para YTD = dic del año anterior
        base_val = vals.get((sel_year - 1, 12)) if sel_year else None
        cumulative_return = 0.0

        for m in range(1, 13):
            curr = vals.get((sel_year, m)) if sel_year else None
            prev_key = (sel_year, m - 1) if m > 1 else (sel_year - 1, 12) if sel_year else None
            prev = vals.get(prev_key) if prev_key else None

            # Monthly return
            ret = None
            if curr is not None and prev is not None and prev > 0:
                ret = round(((curr - prev) / prev) * 100, 4)

            monthly_row[f"{m:02d}"] = ret

            # YTD return
            ytd_ret = None
            if curr is not None and base_val is not None and base_val > 0:
                ytd_ret = round(((curr - base_val) / base_val) * 100, 4)

            ytd_row[f"{m:02d}"] = ytd_ret

        society_returns_monthly.append(monthly_row)
        society_returns_ytd.append(ytd_row)

    return {
        "instruments_table": instruments_table,
        "instruments_pct_table": instruments_pct_table,
        "composition_by_society": composition_by_society,
        "composition_by_instrument": composition_by_instrument,
        "society_montos_table": society_montos_table,
        "society_returns_monthly": society_returns_monthly,
        "society_returns_ytd": society_returns_ytd,
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
