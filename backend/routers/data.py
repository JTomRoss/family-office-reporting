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
    Retorna datos consolidados para la pestaña Resumen.

    Retorna:
      - consolidated_rows: 13 filas (dic año anterior + 12 meses del año).
        Cada fila: fecha, ending_value, movimientos, utilidad,
        rent_mensual_pct, rent_mensual_sin_caja_pct, is_prev_year.
      - chart_data: solo los 12 meses del año (sin dic anterior).
      - rows: detalle por cuenta (para Detalle Cartolas).
    """
    # Años solicitados y año anterior para dic base
    req_years = set(filters.years) if filters.years else set()
    fetch_years = set(req_years)
    if req_years:
        fetch_years.add(min(req_years) - 1)

    # Query base con filtros de cuenta
    query = (
        db.query(MonthlyClosing, Account)
        .join(Account, MonthlyClosing.account_id == Account.id)
    )
    query = _apply_account_filters(query, filters)
    if fetch_years:
        query = query.filter(MonthlyClosing.year.in_(fetch_years))

    all_results = query.order_by(
        Account.id, MonthlyClosing.year, MonthlyClosing.month
    ).all()

    # Agrupar por cuenta
    by_account: dict[int, list[tuple]] = {}
    for mc, acct in all_results:
        by_account.setdefault(acct.id, []).append((mc, acct))

    # Acumular datos por mes para consolidación y generar detalle
    month_agg: dict[str, dict] = {}
    detail_rows: list[dict] = []
    prev_dec = f"{min(req_years) - 1}-12" if req_years else None

    for acct_id, entries in by_account.items():
        entries.sort(key=lambda x: (x[0].year, x[0].month))
        acct = entries[0][1]
        is_cash = acct.account_type in ("current", "savings")

        for i, (mc, _) in enumerate(entries):
            fecha = f"{mc.year}-{mc.month:02d}"
            curr_val = float(mc.net_value) if mc.net_value else 0
            prev_val = float(entries[i - 1][0].net_value or 0) if i > 0 else None

            movimientos = (
                float(mc.change_in_value)
                if mc.change_in_value is not None
                else (curr_val - prev_val if prev_val is not None else None)
            )
            utilidad = (
                float(mc.income)
                if mc.income is not None
                else movimientos
            )

            # Detalle por cuenta (solo meses del año solicitado)
            if not req_years or mc.year in req_years:
                ret = None
                if prev_val and prev_val > 0:
                    ret = round(((curr_val - prev_val) / prev_val) * 100, 4)
                detail_rows.append({
                    "fecha": fecha,
                    "sociedad": acct.entity_name,
                    "banco": acct.bank_code,
                    "id": acct.identification_number or acct.account_number,
                    "moneda": mc.currency,
                    "ending_value": curr_val,
                    "movimientos": movimientos,
                    "utilidad": utilidad,
                    "rent_mensual_pct": ret,
                    "rent_mensual_sin_caja_pct": ret if not is_cash else None,
                    "account_type": acct.account_type,
                })

            # Acumular para consolidación (todos los meses traídos)
            if fecha not in month_agg:
                month_agg[fecha] = {
                    "ev": 0.0, "mov": 0.0, "util": 0.0,
                    "ev_nc": 0.0, "util_nc": 0.0,
                }
            a = month_agg[fecha]
            a["ev"] += curr_val
            a["mov"] += (movimientos or 0)
            a["util"] += (utilidad or 0)
            if not is_cash:
                a["ev_nc"] += curr_val
                a["util_nc"] += (utilidad or 0)

    detail_rows.sort(key=lambda r: (r["fecha"], r["sociedad"], r["banco"]))

    # ── Construir filas consolidadas (13 meses) ──────────────────
    sorted_fechas = sorted(month_agg.keys())
    consolidated_rows: list[dict] = []
    chart_data: list[dict] = []

    for i, fecha in enumerate(sorted_fechas):
        a = month_agg[fecha]
        yr = int(fecha[:4])
        is_prev = fecha == prev_dec

        # Solo incluir dic anterior y meses del año solicitado
        if req_years and yr not in req_years and not is_prev:
            continue

        # Rentabilidad: utilidad / prev_ending_value
        rent_pct = None
        rent_sin_caja_pct = None
        if not is_prev and i > 0:
            prev_f = sorted_fechas[i - 1]
            prev_a = month_agg[prev_f]
            if prev_a["ev"] > 0:
                rent_pct = round(a["util"] / prev_a["ev"] * 100, 4)
            if prev_a["ev_nc"] > 0:
                rent_sin_caja_pct = round(a["util_nc"] / prev_a["ev_nc"] * 100, 4)

        row = {
            "fecha": fecha,
            "ending_value": round(a["ev"], 2),
            "movimientos": round(a["mov"], 2),
            "utilidad": round(a["util"], 2),
            "rent_mensual_pct": rent_pct,
            "rent_mensual_sin_caja_pct": rent_sin_caja_pct,
            "is_prev_year": is_prev,
        }
        consolidated_rows.append(row)

        if not is_prev:
            chart_data.append({
                "fecha": fecha,
                "ending_value": row["ending_value"],
                "movimientos": row["movimientos"],
                "utilidad": row["utilidad"],
                "rent_pct": rent_pct,
                "rent_sin_caja_pct": rent_sin_caja_pct,
            })

    filter_options = _get_filter_options(db)

    return {
        "rows": detail_rows,
        "consolidated_rows": consolidated_rows,
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
    # ── IWDA ──
    "IWDA": "IWDA",
    "ISHARES CORE MSCI WORLD": "IWDA",
    "P ISHARES CORE MSCI WORLD": "IWDA",
    # ── IEMA ──
    "IEMA": "IEMA",
    "ISHARES MSCI EM-ACC": "IEMA",
    "ISHARES MSCI EM ACC": "IEMA",
    "P ISHARES MSCI EM-ACC": "IEMA",
    # ── IHYA ──
    "IHYA": "IHYA",
    "ISHARES USD HY CORP USD ACC": "IHYA",
    "ISHARES USD HIGH YIELD CORP BOND": "IHYA",
    "P ISHARES USD HY CORP USD ACC": "IHYA",
    # ── VDCA ──
    "VDCA": "VDCA",
    "VAND USDCP1-3 USDA": "VDCA",
    "VANGUARD USD CORPORATE 1-3 YEAR BOND UCITS ETF": "VDCA",
    # ── VDPA ──
    "VDPA": "VDPA",
    "VANG USDCPBD USDA": "VDPA",
    # ── VUCP (Goldman Sachs) ──
    "VUCP": "VUCP",
    "USD CORPORATE BOND UCITS ETF": "VUCP",
    "USD CORPORATE BOND UCITS ETF (VUCP)": "VUCP",
    # ── VDCA (including GS name variant) ──
    "VANGUARD USD CORPORATE 1-3 YEAR BOND UCITS ETF": "VDCA",
    "VANGUARD FUNDS PLC-VANGUARD US CMN CLASS ETF": "VDCA",
    "VANGUARD FUNDS PLC - VANGUARD CMN CLASS ETF STAMP": "VDCA",
    # ── SPDR ──
    "SPDR": "SPDR",
    "SPDR BLOOMBERG 1-10 YEAR U.S.": "SPDR",
    # ── Goldman Sachs ETF name variants ──
    "MSCI WORLD INDEX FUND (ISHARES)": "IWDA",
    "MSCI EMERGING MARKETS INDEX FUND (ISHARES)": "IEMA",
    "ISHARES III PLC-ISHARES MSCI EMERGING MARKETS ETF": "IEMA",
    "MARKIT IBOXX USD LIQUID HY CAPPED INDEX FUND (ISHARES)": "IHYA",
    "ISHARES II PLC-ISHARES $ HIGH YIELD CORP BOND UCITS ETF": "IHYA",
}

# Orden fijo de instrumentos
INSTRUMENT_ORDER = ["IWDA", "IEMA", "VDCA", "VDPA", "VUCP", "SPDR", "IHYA", "Money Market"]

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
    # Track both net_value and utilidad (income) for correct return calculation
    soc_month_val: dict[str, dict[tuple, float]] = {}
    soc_month_util: dict[str, dict[tuple, float]] = {}
    for mc, acct in mc_year_results:
        society = _get_society_label(acct.entity_name, acct.bank_code)
        key = (mc.year, mc.month)
        if society not in soc_month_val:
            soc_month_val[society] = {}
            soc_month_util[society] = {}
        soc_month_val[society][key] = soc_month_val[society].get(key, 0) + float(mc.net_value or 0)
        # utilidad = income field (parsed as utilidad from account_monthly_activity)
        utilidad = float(mc.income or 0) if mc.income is not None else None
        if utilidad is not None:
            soc_month_util[society][key] = soc_month_util[society].get(key, 0) + utilidad

    # Calcular monthly return % y YTD
    society_returns_monthly: list[dict] = []
    society_returns_ytd: list[dict] = []

    for soc in SOCIETY_COLS:
        monthly_row = {"sociedad": soc}
        ytd_row = {"sociedad": soc}
        vals = soc_month_val.get(soc, {})
        utils = soc_month_util.get(soc, {})

        # Base para YTD = dic del año anterior
        base_val = vals.get((sel_year - 1, 12)) if sel_year else None
        cumulative_return = 0.0

        for m in range(1, 13):
            curr = vals.get((sel_year, m)) if sel_year else None
            prev_key = (sel_year, m - 1) if m > 1 else (sel_year - 1, 12) if sel_year else None
            prev = vals.get(prev_key) if prev_key else None

            # Monthly return = utilidad / prev_ending_value (same as Summary)
            ret = None
            util = utils.get((sel_year, m)) if sel_year else None
            if util is not None and prev is not None and prev > 0:
                ret = round((util / prev) * 100, 4)
            elif curr is not None and prev is not None and prev > 0:
                # Fallback: simple price change if no utilidad data
                ret = round(((curr - prev) / prev) * 100, 4)

            monthly_row[f"{m:02d}"] = ret

            # YTD return = compounded monthly returns
            # Accumulate: (1+r1)(1+r2)...(1+rn) - 1
            if ret is not None:
                cumulative_return = (1 + cumulative_return / 100) * (1 + ret / 100) * 100 - 100
                ytd_ret = round(cumulative_return, 4)
            else:
                ytd_ret = round(cumulative_return, 4) if cumulative_return != 0 else None

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
