"""
FO Reporting – Lecturas compartidas para UI (frontend nuevo "Reporting APP").

Este módulo es la fachada de alto nivel que alimenta los endpoints GET REST-friendly
consumidos por el frontend nuevo (HTML/CSS/JS vanilla) bajo /api/v1/{master,
dictionary,reporting}.

Reglas:
- NO duplica lógica financiera. Reutiliza los resolvers ya auditados de
  backend/routers/data.py (helpers con prefijo "_") y de
  backend/services/normalized_reporting_payload.
- Lecturas read-only: nunca muta la BD.
- Salidas en shapes estables alineadas con Reporting APP/assets/mock.js (§3.1 del
  README del frontend nuevo).
- Cuentas CLP (BICE) se exponen aparte de cuentas USD; no se mezclan monedas en
  totales hasta que el sistema tenga FX histórico consistente.
"""

from __future__ import annotations

import logging
from dataclasses import asdict
from datetime import date
from decimal import Decimal
from typing import Any, Iterable, Optional

from sqlalchemy import and_
from sqlalchemy.orm import Session

from asset_taxonomy import asset_bucket_color, asset_bucket_order, classify_etf_asset_bucket
from etf_instrument_dictionary import INSTRUMENT_ORDER
from mandate_taxonomy import (
    MANDATE_CATEGORY_CASH,
    MANDATE_CATEGORY_EQUITIES,
    MANDATE_CATEGORY_FIXED,
    MANDATE_CATEGORY_GLOBAL_EQUITIES,
    MANDATE_CATEGORY_HY_FIXED,
    MANDATE_CATEGORY_IG_FIXED,
    MANDATE_CATEGORY_NON_US_EQUITIES,
    MANDATE_CATEGORY_OTHER_INVESTMENTS,
    MANDATE_CATEGORY_PRIVATE_EQUITY,
    MANDATE_CATEGORY_REAL_ESTATE,
    MANDATE_CATEGORY_US_EQUITIES,
)
from backend.db.models import (
    Account,
    BiceMonthlySnapshot,
    DailyPosition,
    MonthlyClosing,
    MonthlyMetricNormalized,
)
from backend.services.normalized_reporting_payload import (
    extract_canonical_breakdown,
    to_decimal,
)

# Reusamos resolvers auditados del router viejo. Son funciones puras (prefijo _)
# y no tienen efectos secundarios. Esta importación preserva la paridad de datos
# con la app Streamlit.
from backend.routers.data import (  # noqa: E402
    _SyntheticMonthlyClosing,
    _resolve_ending_with_accrual,
    _resolve_cash_value,
    _resolve_raw_movements,
    _resolve_raw_profit,
)

_MANDATE_CATEGORIES = [
    MANDATE_CATEGORY_CASH,
    MANDATE_CATEGORY_IG_FIXED,
    MANDATE_CATEGORY_HY_FIXED,
    MANDATE_CATEGORY_FIXED,
    MANDATE_CATEGORY_US_EQUITIES,
    MANDATE_CATEGORY_NON_US_EQUITIES,
    MANDATE_CATEGORY_GLOBAL_EQUITIES,
    MANDATE_CATEGORY_EQUITIES,
    MANDATE_CATEGORY_PRIVATE_EQUITY,
    MANDATE_CATEGORY_REAL_ESTATE,
    MANDATE_CATEGORY_OTHER_INVESTMENTS,
]

# ── Filtro Internacional vs Nacional ─────────────────────────────────────
# Nacional = universo BICE (CLP y USD). Internacional = todo lo demás.
# Nunca se mezclan: el dashboard opera en UNO de los dos ámbitos a la vez.
_NATIONAL_BANK_CODES = {"bice_inversiones", "bice_asesorias"}


def _scope_matches(bank_code: str, scope: str) -> bool:
    if scope == "national":
        return bank_code in _NATIONAL_BANK_CODES
    if scope == "international":
        return bank_code not in _NATIONAL_BANK_CODES
    # "all" (no documentado; uso diagnóstico)
    return True


# ── Mapeo canónico 7 → buckets §6.1 (9) ──────────────────────────────────
# Aproximación determinística para alimentar el pie chart del dashboard.
# Refinamientos (Short/Long, Non-US RF) quedan para v2.
_CANONICAL_TO_BUCKET = {
    "Cash, Deposits & Money Market": "Caja",
    "Investment Grade Fixed Income": "RF IG Short",
    "High Yield Fixed Income": "HY",
    "US Equities": "RV DM",
    "Non US Equities": "RV EM",
    "Private Equity": "Alternativos",
    "Real Estate": "Real Estate",
    "Other Investments": "Alternativos",
}

logger = logging.getLogger(__name__)


# ───────────────────────────────────────────────────────────────────────────
# HELPERS internos
# ───────────────────────────────────────────────────────────────────────────

def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _period_to_ym(period: str) -> tuple[int, int]:
    """'YYYY-MM' → (year, month). Lanza ValueError si inválido."""
    year_str, month_str = period.split("-")
    return int(year_str), int(month_str)


def _last_n_months(period: str, n: int = 13) -> list[tuple[int, int]]:
    """Retorna lista de (year, month) de los últimos `n` meses terminando en `period`."""
    y, m = _period_to_ym(period)
    out: list[tuple[int, int]] = []
    for _ in range(n):
        out.append((y, m))
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    return list(reversed(out))


def _month_label(year: int, month: int) -> str:
    nombres = ["Ene", "Feb", "Mar", "Abr", "May", "Jun",
              "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
    return f"{nombres[month - 1]} {year % 100:02d}"


# ───────────────────────────────────────────────────────────────────────────
# MASTER — sociedades, bancos, cuentas, parsers
# ───────────────────────────────────────────────────────────────────────────

_BANK_LABELS = {
    "bice_inversiones": ("BICE Inversiones", "BICE", "CL"),
    "bice_asesorias": ("BICE Asesorías · Altos Patrimonios", "Altos", "CL"),
    "jpmorgan": ("J.P. Morgan NY", "JPM NY", "US"),
    "ubs": ("UBS Switzerland", "UBS SW", "CH"),
    "ubs_miami": ("UBS Miami", "UBS MIA", "US"),
    "goldman_sachs": ("Goldman Sachs", "GS", "US"),
    "bbh": ("Brown Brothers Harriman", "BBH", "US"),
    "wellington": ("Wellington", "WELL", "US"),
    "alternativos": ("Alternativos (Excel)", "ALT", "-"),
}


def get_master_accounts(db: Session) -> list[dict]:
    """Lista de cuentas activas del maestro, shape compatible con mock.accounts."""
    rows = (
        db.query(Account)
        .filter(Account.is_active.is_(True))
        .order_by(Account.entity_name, Account.bank_code, Account.account_number)
        .all()
    )
    out: list[dict] = []
    for a in rows:
        out.append({
            "id": f"A{a.id:04d}",
            "account_id": a.id,
            "number": a.account_number,
            "identification_number": a.identification_number,
            "society": a.entity_name,
            "bank": a.bank_code,
            "type": a.account_type,
            "currency": a.currency,
            "holder": a.person_name or a.entity_name,
            "parser": f"{a.bank_code}/{a.account_type}",
            "country": a.country,
            "mandate_type": a.mandate_type,
        })
    return out


def get_master_societies(db: Session) -> list[dict]:
    """Sociedades distintas extraídas del maestro.

    IMPORTANTE: `id == name` para que el lookup `D.getSociety(account.society)`
    del frontend funcione directamente (account.society viene como entity_name).
    """
    accounts = db.query(Account).filter(Account.is_active.is_(True)).all()
    seen: dict[str, dict] = {}
    for a in accounts:
        if not a.entity_name:
            continue
        key = a.entity_name
        if key not in seen:
            seen[key] = {
                "id": a.entity_name,
                "name": a.entity_name,
                "currency": a.currency,
                "jur": (a.country or "").upper()[:3] or "—",
            }
    return sorted(seen.values(), key=lambda s: s["name"])


def get_master_banks(db: Session) -> list[dict]:
    """Bancos distintos (con metadatos estables)."""
    bank_codes = {
        row[0]
        for row in db.query(Account.bank_code).filter(Account.is_active.is_(True)).distinct().all()
        if row[0]
    }
    out: list[dict] = []
    for code in sorted(bank_codes):
        label, short, country = _BANK_LABELS.get(code, (code.replace("_", " ").title(), code[:4].upper(), "-"))
        out.append({
            "code": code,
            "name": label,
            "short": short,
            "country": country,
        })
    return out


def get_master_parsers() -> list[dict]:
    """Inventario canónico de parsers (§3 RULES_INHERITED).

    Intenta leer del registry en vivo (parsers.registry.get_registry().list_parsers()).
    Si el auto-discovery aún no corrió o falla, cae al inventario estático
    documentado en RULES_INHERITED §3.
    """
    try:
        from parsers.registry import get_registry
        registry = get_registry()
        rows = registry.list_parsers()
        if rows:
            return [
                {
                    "name": f"{r.get('bank_code')}/{r.get('account_type')}",
                    "bank": r.get("bank_code"),
                    "account_type": r.get("account_type"),
                    "version": r.get("version"),
                    "description": r.get("description", ""),
                }
                for r in rows
            ]
    except Exception as e:
        logger.warning("No se pudo obtener parsers registrados dinámicamente: %s", e)

    # Fallback estático basado en inventario documentado en RULES_INHERITED §3.
    static = [
        ("bice/brokerage", "bice_inversiones", "brokerage", "3.4.0"),
        ("bice_asesorias/wealth_management", "bice_asesorias", "wealth_management", "1.2.0"),
        ("bbh/custody", "bbh", "custody", "1.4.0"),
        ("bbh/report_mandato", "bbh", "report_mandato", "1.2.0"),
        ("goldman_sachs/custody", "goldman_sachs", "custody", "2.0.3"),
        ("goldman_sachs/etf", "goldman_sachs", "etf", "1.3.0"),
        ("goldman_sachs/report_mandato", "goldman_sachs", "report_mandato", "1.2.0"),
        ("jpmorgan/brokerage", "jpmorgan", "brokerage", "2.1.3"),
        ("jpmorgan/bonds", "jpmorgan", "bonds", "2.0.2"),
        ("jpmorgan/custody", "jpmorgan", "custody", "1.0.0"),
        ("jpmorgan/etf", "jpmorgan", "etf", "1.0.0"),
        ("jpmorgan/report_mandato", "jpmorgan", "report_mandato", "1.3.0"),
        ("ubs/custody", "ubs", "custody", "2.3.3"),
        ("ubs/report_mandato", "ubs", "report_mandato", "1.2.0"),
        ("ubs_miami/custody", "ubs_miami", "custody", "1.4.2"),
        ("ubs_miami/report_mandato", "ubs_miami", "report_mandato", "1.2.0"),
        ("wellington/custody", "wellington", "custody", "1.0.0"),
    ]
    return [
        {"name": name, "bank": bank, "account_type": acct_type, "version": ver, "description": ""}
        for (name, bank, acct_type, ver) in static
    ]


# ───────────────────────────────────────────────────────────────────────────
# DICTIONARY — buckets, ETF instruments, mandate categories
# ───────────────────────────────────────────────────────────────────────────

# Colores canónicos (§6.1). Mantenemos el orden de display definido en taxonomy.
_BUCKET_CSS = {
    "Caja": "bk-caja",
    "RF IG Short": "bk-rf-ig-short",
    "RF IG Long": "bk-rf-ig-long",
    "HY": "bk-hy",
    "Non US RF": "bk-non-us-rf",
    "Alternativos": "bk-alt",
    "Real Estate": "bk-re",
    "RV EM": "bk-rv-em",
    "RV DM": "bk-rv-dm",
}


def get_buckets() -> list[dict]:
    """Buckets canónicos (§6.1) con color y orden de display."""
    order = asset_bucket_order()
    out: list[dict] = []
    for i, name in enumerate(order, start=1):
        try:
            color = asset_bucket_color(name)
        except Exception:
            color = "#999999"
        out.append({
            "id": name,
            "color": color or "#999999",
            "css": _BUCKET_CSS.get(name, "bk-default"),
            "order": i,
        })
    return out


def get_etf_dictionary() -> list[dict]:
    """Instrumentos ETF canónicos (§6.2). Retorna solo el nombre canónico;
    los aliases viven en etf_instrument_dictionary.py y se mantienen allí."""
    return [{"canonical": name} for name in INSTRUMENT_ORDER]


def get_mandate_categories() -> list[str]:
    """Categorías canónicas de mandato (§6.3)."""
    return list(_MANDATE_CATEGORIES)


# ───────────────────────────────────────────────────────────────────────────
# REPORTING — dashboard y positions
# ───────────────────────────────────────────────────────────────────────────

def _load_period_rows(
    db: Session,
    year: int,
    month: int,
    *,
    scope: str = "international",
) -> list[tuple[Optional[MonthlyClosing], Account, Optional[MonthlyMetricNormalized]]]:
    """
    Devuelve, para el período (year, month), una fila por cuenta activa que tiene
    datos en monthly_closings o monthly_metrics_normalized.

    Se prioriza normalized (SSOT). Si falta fila normalizada pero hay closing,
    se retorna con norm=None. Si existe normalized sin closing, se retorna
    mc=None para que el resolver haga fallback correctamente.

    `scope`: filtro Internacional vs Nacional. 'national' = universo BICE;
    'international' = resto. Nunca se mezclan (§dashboard rule del usuario).
    """
    # Join FULL OUTER emulado: traemos ambos lados por separado y unimos por
    # (account_id, year, month).
    closings = (
        db.query(MonthlyClosing, Account)
        .join(Account, MonthlyClosing.account_id == Account.id)
        .filter(MonthlyClosing.year == year, MonthlyClosing.month == month)
        .all()
    )
    normalized = (
        db.query(MonthlyMetricNormalized, Account)
        .join(Account, MonthlyMetricNormalized.account_id == Account.id)
        .filter(MonthlyMetricNormalized.year == year, MonthlyMetricNormalized.month == month)
        .all()
    )
    # Aplicamos scope antes del merge para no gastar ciclos innecesarios.
    closings = [(mc, acct) for (mc, acct) in closings if _scope_matches(acct.bank_code, scope)]
    normalized = [(norm, acct) for (norm, acct) in normalized if _scope_matches(acct.bank_code, scope)]

    by_key_mc: dict[int, tuple[MonthlyClosing, Account]] = {
        acct.id: (mc, acct) for (mc, acct) in closings
    }
    by_key_norm: dict[int, tuple[MonthlyMetricNormalized, Account]] = {
        acct.id: (norm, acct) for (norm, acct) in normalized
    }

    rows: list[tuple[Optional[MonthlyClosing], Account, Optional[MonthlyMetricNormalized]]] = []
    for acct_id in set(by_key_mc.keys()) | set(by_key_norm.keys()):
        mc_pair = by_key_mc.get(acct_id)
        norm_pair = by_key_norm.get(acct_id)
        if mc_pair:
            mc, acct = mc_pair
            norm = norm_pair[0] if norm_pair else None
        elif norm_pair:
            norm, acct = norm_pair
            mc = None
        else:
            continue
        rows.append((mc, acct, norm))
    return rows


def _sum_usd(rows, cash_cache: dict[tuple[int, int], float], db: Session) -> tuple[float, float, float]:
    """
    Para un set de rows (mc, acct, norm) del mismo período, devuelve
    (total_ending_usd, total_cash_usd, total_cuentas_usd_incluidas).
    Solo suma cuentas con currency == USD (o USDC).
    """
    total = 0.0
    cash = 0.0
    n = 0
    for (mc, acct, norm) in rows:
        if acct.currency not in ("USD", "USDC"):
            continue
        # Usamos un MonthlyClosing sintético si no existe para reusar el resolver.
        if mc is None and norm is not None:
            mc_syn = _SyntheticMonthlyClosing(
                account_id=acct.id,
                closing_date=norm.closing_date,
                year=norm.year,
                month=norm.month,
                net_value=norm.ending_value_with_accrual,
                currency=(norm.currency or acct.currency),
                income=norm.profit_period,
                change_in_value=norm.movements_net,
                accrual=norm.accrual_ending,
                asset_allocation_json=norm.asset_allocation_json,
            )
            mc_use = mc_syn
        else:
            mc_use = mc
        ev = _resolve_ending_with_accrual(mc_use, norm)
        if ev is None:
            continue
        total += float(ev)
        n += 1
        c = _resolve_cash_value(
            db=db, acct=acct, mc=mc_use, norm=norm, etf_cash_cache=cash_cache,
        )
        if c:
            cash += float(c)
    return total, cash, n


def _bice_clp_total_for(db: Session, year: int, month: int) -> float:
    """Suma ending_clp de snapshots BICE para cuentas CLP, solo para el período."""
    rows = (
        db.query(BiceMonthlySnapshot, Account)
        .join(Account, Account.id == BiceMonthlySnapshot.account_id)
        .filter(
            BiceMonthlySnapshot.year == year,
            BiceMonthlySnapshot.month == month,
            Account.currency == "CLP",
        )
        .all()
    )
    total = 0.0
    for snap, acct in rows:
        v = _to_float(snap.ending_clp)
        if v is not None:
            total += v
    return total


def _pick_bice_field(snap: BiceMonthlySnapshot, prefix: str, currency: str):
    """Extrae campo por moneda desde un snapshot BICE. currency ∈ {CLP, USD}."""
    attr = f"{prefix}_{currency.lower()}"
    val = getattr(snap, attr, None)
    try:
        return float(val) if val is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def _get_bice_dashboard(db: Session, period: str, bice_currency: str = "CLP") -> dict:
    """
    Dashboard BICE (scope='national') leyendo desde bice_monthly_snapshot.
    Mundos CLP y USD son completamente independientes; NO se mezclan ni
    convierten entre sí. El param `bice_currency` elige qué mundo se muestra.
    """
    if bice_currency not in ("CLP", "USD"):
        raise ValueError(f"bice_currency inválido: {bice_currency}")

    y, m = _period_to_ym(period)
    series_ym = _last_n_months(period, 13)
    months = [f"{yy:04d}-{mm:02d}" for (yy, mm) in series_ym]
    month_labels = [_month_label(yy, mm) for (yy, mm) in series_ym]

    # Cuentas BICE activas
    bice_accts = (
        db.query(Account)
        .filter(
            Account.is_active.is_(True),
            Account.bank_code.in_(["bice_inversiones", "bice_asesorias"]),
        )
        .all()
    )
    acct_ids = [a.id for a in bice_accts]
    accts_by_id = {a.id: a for a in bice_accts}

    if not acct_ids:
        return {
            "period": period,
            "scope": "national",
            "bice_currency": bice_currency,
            "months": months,
            "monthLabels": month_labels,
            "totalUSDSeries": [0.0] * 13,
            "retMonthly": [None] * 13,
            "kpis": {
                "patrimonio_usd": 0.0,
                "patrimonio_clp": 0.0,
                "variacion_usd": None,
                "twr_mom": None,
                "twr_ytd": None,
            },
            "allocation": [],
            "allocation_buckets": [],
            "bySociety": [],
            "byBank": [],
            "byCurrency": [{"currency": bice_currency, "value_native": 0.0}],
            "meta": {
                "scope": "national",
                "bice_currency": bice_currency,
                "coverage_note": "Sin cuentas BICE activas",
            },
        }

    # Snapshot por mes
    snaps_by_period: dict[tuple[int, int], list[tuple]] = {}
    for (yy, mm) in series_ym:
        rows = (
            db.query(BiceMonthlySnapshot)
            .filter(
                BiceMonthlySnapshot.year == yy,
                BiceMonthlySnapshot.month == mm,
                BiceMonthlySnapshot.account_id.in_(acct_ids),
            )
            .all()
        )
        snaps_by_period[(yy, mm)] = rows

    # Serie de patrimonios (nativa, sin convertir)
    total_series: list[float] = []
    for (yy, mm) in series_ym:
        total = sum(_pick_bice_field(s, "ending", bice_currency) for s in snaps_by_period[(yy, mm)])
        total_series.append(round(total, 2))

    patrimonio = total_series[-1] if total_series else 0.0
    prev = total_series[-2] if len(total_series) >= 2 else None
    variacion = (patrimonio - prev) if prev is not None else None

    # Rentabilidad mensual: (ending - (aportes - retiros)) / prev_ending - 1
    # Aportes y retiros se netean; dividendos NO entran porque el profit BICE ya los contempla.
    ret_monthly: list[Optional[float]] = [None]
    for i in range(1, len(total_series)):
        p = total_series[i - 1]
        c = total_series[i]
        rows = snaps_by_period[series_ym[i]]
        mov = sum(
            _pick_bice_field(s, "aportes", bice_currency) - _pick_bice_field(s, "retiros", bice_currency)
            for s in rows
        )
        if p and p > 0:
            r = ((c - mov) / p - 1.0) * 100.0
            ret_monthly.append(round(r, 4))
        else:
            ret_monthly.append(None)

    twr_mom = ret_monthly[-1] if ret_monthly else None
    twr_ytd = _chain_link_ytd(ret_monthly, series_ym, y)

    # Agregados del período: bySociety, byBank, allocation
    current_rows = snaps_by_period[(y, m)]
    by_soc: dict[str, float] = {}
    by_bank: dict[str, float] = {}
    total_cash = 0.0
    total_rf = 0.0
    total_eq = 0.0
    for snap in current_rows:
        acct = accts_by_id.get(snap.account_id)
        if not acct:
            continue
        end = _pick_bice_field(snap, "ending", bice_currency)
        if end:
            by_soc[acct.entity_name] = by_soc.get(acct.entity_name, 0.0) + end
            by_bank[acct.bank_code] = by_bank.get(acct.bank_code, 0.0) + end
        total_cash += _pick_bice_field(snap, "caja", bice_currency)
        total_rf += _pick_bice_field(snap, "renta_fija", bice_currency)
        total_eq += _pick_bice_field(snap, "equities", bice_currency)

    by_society_list = [
        {"key": k, "value_usd": round(v, 2)}
        for k, v in sorted(by_soc.items(), key=lambda kv: -kv[1])
    ]
    by_bank_list = [
        {"key": k, "value_usd": round(v, 2)}
        for k, v in sorted(by_bank.items(), key=lambda kv: -kv[1])
    ]

    # Allocation buckets simplificado para BICE (no hay asset_allocation_json
    # canónico; usamos los 3 ejes disponibles: Caja, RF, Equities).
    allocation_buckets: list[dict] = []
    if total_cash > 0:
        allocation_buckets.append({"bucket": "Caja", "value_usd": round(total_cash, 2)})
    if total_rf > 0:
        allocation_buckets.append({"bucket": "RF IG Short", "value_usd": round(total_rf, 2)})
    if total_eq > 0:
        allocation_buckets.append({"bucket": "RV DM", "value_usd": round(total_eq, 2)})

    # byCurrency: solo la moneda activa en BICE. El otro mundo se ve al togglar.
    by_currency_list = [{"currency": bice_currency, "value_native": round(patrimonio, 2)}]

    return {
        "period": period,
        "scope": "national",
        "bice_currency": bice_currency,
        "months": months,
        "monthLabels": month_labels,
        "totalUSDSeries": total_series,  # nombre preservado para compat con frontend
        "retMonthly": ret_monthly,
        "kpis": {
            "patrimonio_usd": patrimonio if bice_currency == "USD" else 0.0,
            "patrimonio_clp": patrimonio if bice_currency == "CLP" else 0.0,
            "variacion_usd": round(variacion, 2) if (variacion is not None and bice_currency == "USD") else 0.0,
            "variacion_clp": round(variacion, 2) if (variacion is not None and bice_currency == "CLP") else 0.0,
            "twr_mom": twr_mom,
            "twr_ytd": twr_ytd,
        },
        "allocation": [],
        "allocation_buckets": allocation_buckets,
        "bySociety": by_society_list,
        "byBank": by_bank_list,
        "byCurrency": by_currency_list,
        "meta": {
            "scope": "national",
            "bice_currency": bice_currency,
            "coverage_note": (
                f"scope=national, currency={bice_currency}. Datos leídos desde "
                f"bice_monthly_snapshot. Los mundos CLP y USD son independientes: "
                f"no se convierten ni mezclan."
            ),
        },
    }


def get_dashboard(db: Session, period: str, scope: str = "international", bice_currency: str = "CLP") -> dict:
    """
    Snapshot consolidado del período pedido + serie de los últimos 13 meses.

    Salida pensada para consumo directo por el frontend nuevo:
      - kpis: patrimonio_usd, patrimonio_clp, variacion_usd, twr_mom, twr_ytd
      - totalUSDSeries: [13]
      - retMonthly: [13] en %
      - months / monthLabels: [13]
      - allocation: breakdown canónico (USD, 7 categorías) del período pedido
      - allocation_buckets: breakdown en buckets §6.1 (USD) para el pie chart
      - bySociety / byBank / byCurrency: agregados del período pedido

    `scope`: 'international' (default) o 'national'. Nunca mezcla ámbitos.
    """
    if scope not in ("international", "national"):
        raise ValueError(f"scope inválido: {scope}. Usa 'international' o 'national'.")

    # scope=national se sirve desde bice_monthly_snapshot (SSOT paralela CLP/USD)
    if scope == "national":
        return _get_bice_dashboard(db, period, bice_currency=bice_currency)

    y, m = _period_to_ym(period)
    series_ym = _last_n_months(period, 13)
    months = [f"{yy:04d}-{mm:02d}" for (yy, mm) in series_ym]
    month_labels = [_month_label(yy, mm) for (yy, mm) in series_ym]

    # 13 meses: total USD por mes
    total_usd_series: list[float] = []
    cash_cache_by_period: dict[str, dict] = {}
    for (yy, mm) in series_ym:
        rows = _load_period_rows(db, yy, mm, scope=scope)
        cache: dict[tuple[int, int], float] = {}
        total_m, _, _ = _sum_usd(rows, cache, db)
        total_usd_series.append(round(total_m, 2))
        cash_cache_by_period[f"{yy:04d}-{mm:02d}"] = {"rows": rows, "cache": cache}

    current_rows = cash_cache_by_period[period]["rows"]
    current_cache = cash_cache_by_period[period]["cache"]

    # KPIs
    patrimonio_usd = total_usd_series[-1] if total_usd_series else 0.0
    prev_usd = total_usd_series[-2] if len(total_usd_series) >= 2 else None
    variacion_usd = (patrimonio_usd - prev_usd) if prev_usd is not None else None

    # Rentabilidad mensual por período: (ending - prev_ending - movimientos) / prev_ending
    # Aquí la hacemos agregada sobre el universo completo (approximación consolidada).
    ret_monthly: list[Optional[float]] = [None]
    for i in range(1, len(total_usd_series)):
        prev = total_usd_series[i - 1]
        curr = total_usd_series[i]
        # Movimientos netos agregados del mes i
        rows_i = cash_cache_by_period[months[i]]["rows"]
        mov_i = 0.0
        seen_mov = False
        for (mc, acct, norm) in rows_i:
            if acct.currency not in ("USD", "USDC"):
                continue
            raw = _resolve_raw_movements(mc, norm) if mc else _to_float(norm.movements_net if norm else None)
            if raw is not None:
                mov_i += float(raw)
                seen_mov = True
        if prev and prev > 0:
            if seen_mov:
                r = ((curr - mov_i) / prev - 1.0) * 100.0
            else:
                r = (curr / prev - 1.0) * 100.0
            ret_monthly.append(round(r, 4))
        else:
            ret_monthly.append(None)

    # TWR MoM (último mes) y YTD (chain-linking desde enero del año actual)
    twr_mom = ret_monthly[-1] if ret_monthly and ret_monthly[-1] is not None else None
    twr_ytd: Optional[float] = None
    # YTD: compone los meses del año y <= m
    comp = 1.0
    found_any = False
    for idx, (yy, mm) in enumerate(series_ym):
        if yy != y:
            continue
        r = ret_monthly[idx]
        if r is None:
            continue
        comp *= (1.0 + r / 100.0)
        found_any = True
    if found_any:
        twr_ytd = round((comp - 1.0) * 100.0, 4)

    # Allocation canónica agregada del período pedido (sobre cuentas USD, USD nativo)
    allocation_canonical = _aggregate_canonical_allocation(current_rows)
    allocation_buckets = _aggregate_bucket_allocation(allocation_canonical)

    # Agregados del período pedido
    by_society = _aggregate_dimension(current_rows, current_cache, db, dim="society")
    by_bank = _aggregate_dimension(current_rows, current_cache, db, dim="bank")
    by_currency = _aggregate_currency(db, year=y, month=m, rows=current_rows, scope=scope)

    # Patrimonio CLP: solo cuando scope='national'. En internacional siempre 0
    # para garantizar que no se mezclen monedas.
    patrimonio_clp = 0.0
    if scope == "national":
        patrimonio_clp = round(_bice_clp_total_for(db, y, m), 2)

    return {
        "period": period,
        "scope": scope,
        "months": months,
        "monthLabels": month_labels,
        "totalUSDSeries": total_usd_series,
        "retMonthly": ret_monthly,
        "kpis": {
            "patrimonio_usd": patrimonio_usd,
            "patrimonio_clp": patrimonio_clp,
            "variacion_usd": round(variacion_usd, 2) if variacion_usd is not None else None,
            "twr_mom": twr_mom,
            "twr_ytd": twr_ytd,
        },
        "allocation": allocation_canonical,
        "allocation_buckets": allocation_buckets,
        "bySociety": by_society,
        "byBank": by_bank,
        "byCurrency": by_currency,
        "meta": {
            "scope": scope,
            "coverage_note": (
                "scope=international: solo cuentas no-BICE en USD. "
                "scope=national: solo cuentas BICE (CLP/USD). "
                "Nunca se mezclan monedas dentro del mismo dashboard."
            ),
        },
    }


def _aggregate_bucket_allocation(canonical_rows: list[dict]) -> list[dict]:
    """Proyecta el breakdown canónico (7 categorías) a buckets §6.1 (9) usando
    el mapping determinístico _CANONICAL_TO_BUCKET. Si alguna categoría no mapea,
    cae a 'RV DM' (default canónico §6.1)."""
    totals: dict[str, float] = {}
    for row in canonical_rows:
        cat = row.get("category")
        val = row.get("value_usd") or 0.0
        if not val:
            continue
        bucket = _CANONICAL_TO_BUCKET.get(cat, "RV DM")
        totals[bucket] = totals.get(bucket, 0.0) + float(val)
    return [
        {"bucket": b, "value_usd": round(v, 2)}
        for b, v in sorted(totals.items(), key=lambda kv: -kv[1])
        if v and v > 0
    ]


def _aggregate_canonical_allocation(rows) -> list[dict]:
    """Suma los breakdowns canónicos (7 categorías) sobre las cuentas USD del período."""
    totals: dict[str, Decimal] = {}
    for (mc, acct, norm) in rows:
        if acct.currency not in ("USD", "USDC"):
            continue
        # Reuse decode + extract canonical
        raw_json = None
        if norm and norm.asset_allocation_json:
            raw_json = norm.asset_allocation_json
        elif mc and mc.asset_allocation_json:
            raw_json = mc.asset_allocation_json
        if not raw_json:
            continue
        try:
            import json as _json
            payload = _json.loads(raw_json)
        except Exception:
            continue
        breakdown = extract_canonical_breakdown(payload)
        for k, v in (breakdown or {}).items():
            if v is None:
                continue
            totals[k] = totals.get(k, Decimal("0")) + v

    return [
        {"category": k, "value_usd": float(v)}
        for k, v in totals.items()
        if v and float(v) != 0.0
    ]


def _aggregate_dimension(rows, cash_cache: dict, db: Session, *, dim: str) -> list[dict]:
    """Agrupa ending USD por 'society' o 'bank'."""
    agg: dict[str, float] = {}
    for (mc, acct, norm) in rows:
        if acct.currency not in ("USD", "USDC"):
            continue
        key = acct.entity_name if dim == "society" else acct.bank_code
        if not key:
            continue
        if mc is None and norm is not None:
            mc_use = _SyntheticMonthlyClosing(
                account_id=acct.id,
                closing_date=norm.closing_date,
                year=norm.year,
                month=norm.month,
                net_value=norm.ending_value_with_accrual,
                currency=(norm.currency or acct.currency),
                income=norm.profit_period,
                change_in_value=norm.movements_net,
                accrual=norm.accrual_ending,
                asset_allocation_json=norm.asset_allocation_json,
            )
        else:
            mc_use = mc
        ev = _resolve_ending_with_accrual(mc_use, norm)
        if ev is None:
            continue
        agg[key] = agg.get(key, 0.0) + float(ev)
    return [{"key": k, "value_usd": round(v, 2)} for k, v in sorted(agg.items(), key=lambda kv: -kv[1])]


def _aggregate_currency(
    db: Session, *, year: int, month: int, rows, scope: str = "international"
) -> list[dict]:
    """Reporta totales por moneda. Respeta scope para no mezclar ámbitos:
    - international: solo USD (suma de rows).
    - national: CLP + USD (si las cuentas BICE tienen USD), desde BICE snapshot.
    """
    usd_total = 0.0
    for (mc, acct, norm) in rows:
        if acct.currency not in ("USD", "USDC"):
            continue
        if mc is None and norm is not None:
            mc_use = _SyntheticMonthlyClosing(
                account_id=acct.id,
                closing_date=norm.closing_date,
                year=norm.year,
                month=norm.month,
                net_value=norm.ending_value_with_accrual,
                currency=(norm.currency or acct.currency),
                income=norm.profit_period,
                change_in_value=norm.movements_net,
                accrual=norm.accrual_ending,
                asset_allocation_json=norm.asset_allocation_json,
            )
        else:
            mc_use = mc
        ev = _resolve_ending_with_accrual(mc_use, norm)
        if ev is not None:
            usd_total += float(ev)

    if scope == "national":
        clp_total = _bice_clp_total_for(db, year, month)
        return [
            {"currency": "CLP", "value_native": round(clp_total, 2)},
            {"currency": "USD", "value_native": round(usd_total, 2)},
        ]
    # international: no reporta CLP (sería un 0 engañoso).
    return [
        {"currency": "USD", "value_native": round(usd_total, 2)},
    ]


def _get_bice_normalized_rows(db: Session, period: str, bice_currency: str = "CLP") -> dict:
    """
    Tabla canónica BICE para el período. Fila por cuenta BICE, campos en CLP
    o USD según `bice_currency`.
    """
    if bice_currency not in ("CLP", "USD"):
        raise ValueError(f"bice_currency inválido: {bice_currency}")

    y, m = _period_to_ym(period)

    bice_accts = (
        db.query(Account)
        .filter(
            Account.is_active.is_(True),
            Account.bank_code.in_(["bice_inversiones", "bice_asesorias"]),
        )
        .all()
    )
    accts_by_id = {a.id: a for a in bice_accts}

    snaps = (
        db.query(BiceMonthlySnapshot)
        .filter(
            BiceMonthlySnapshot.year == y,
            BiceMonthlySnapshot.month == m,
            BiceMonthlySnapshot.account_id.in_([a.id for a in bice_accts]),
        )
        .all()
    )

    out: list[dict] = []
    for snap in snaps:
        a = accts_by_id.get(snap.account_id)
        if not a:
            continue
        ending = _pick_bice_field(snap, "ending", bice_currency)
        aportes = _pick_bice_field(snap, "aportes", bice_currency)
        retiros = _pick_bice_field(snap, "retiros", bice_currency)
        movs = aportes - retiros
        out.append({
            "account_id": f"A{a.id:04d}",
            "account_number": a.account_number,
            "society": a.entity_name,
            "bank": a.bank_code,
            "account_type": a.account_type,
            "currency": bice_currency,
            "month": period,
            "ending_value_with_accrual": ending,
            "ending_value_without_accrual": ending,  # BICE no distingue accrual
            "accrual_ending": 0.0,
            "cash_value": _pick_bice_field(snap, "caja", bice_currency),
            "movements_net": movs,
            "profit_period": _pick_bice_field(snap, "profit", bice_currency),
            "source": f"bice_snapshot_{bice_currency.lower()}",
            "source_document_id": snap.source_document_id,
        })
    out.sort(key=lambda r: (r["society"] or "", r["bank"] or "", r["account_number"] or ""))

    return {
        "period": period,
        "scope": "national",
        "bice_currency": bice_currency,
        "rows": out,
        "meta": {
            "count": len(out),
            "n_normalized": len(out),
            "n_fallback": 0,
            "source": "bice_monthly_snapshot",
        },
    }


def get_normalized_rows(
    db: Session,
    period: str,
    scope: str = "international",
    bice_currency: str = "CLP",
) -> dict:
    """
    Lee la capa canónica para el período + scope.

    - scope='international': `monthly_metrics_normalized` (+ fallback closings).
    - scope='national': `bice_monthly_snapshot`. El param bice_currency elige
      qué mundo (CLP o USD) se reporta. Los dos mundos son independientes.
    """
    if scope not in ("international", "national"):
        raise ValueError(f"scope inválido: {scope}")

    # scope=national viaja por bice_monthly_snapshot
    if scope == "national":
        return _get_bice_normalized_rows(db, period, bice_currency=bice_currency)

    y, m = _period_to_ym(period)
    rows = _load_period_rows(db, y, m, scope=scope)

    cash_cache: dict[tuple[int, int], float] = {}
    out: list[dict] = []
    for (mc, acct, norm) in rows:
        # Preparar el mc sintético si solo hay normalized
        if mc is None and norm is not None:
            mc_use = _SyntheticMonthlyClosing(
                account_id=acct.id,
                closing_date=norm.closing_date,
                year=norm.year,
                month=norm.month,
                net_value=norm.ending_value_with_accrual,
                currency=(norm.currency or acct.currency),
                income=norm.profit_period,
                change_in_value=norm.movements_net,
                accrual=norm.accrual_ending,
                asset_allocation_json=norm.asset_allocation_json,
            )
        else:
            mc_use = mc

        ending_w = _resolve_ending_with_accrual(mc_use, norm)
        ending_wo = None
        accrual = None
        cash = None
        movs = None
        profit = None
        source_doc_id: Optional[int] = None
        source_label = "normalized" if norm else "closing_fallback"

        if norm:
            ending_wo = _to_float(norm.ending_value_without_accrual)
            accrual = _to_float(norm.accrual_ending)
            cash = _to_float(norm.cash_value)
            movs = _to_float(norm.movements_net)
            profit = _to_float(norm.profit_period)
            source_doc_id = norm.source_document_id
        if ending_wo is None and mc is not None:
            ending_wo = _to_float(mc.net_value)
        if accrual is None and mc is not None:
            accrual = _to_float(mc.accrual)
        if cash is None:
            cash = _resolve_cash_value(
                db=db, acct=acct, mc=mc_use, norm=norm, etf_cash_cache=cash_cache,
            )
        if movs is None and mc is not None:
            movs = _to_float(mc.change_in_value)
        if profit is None and mc is not None:
            profit = _to_float(mc.income)
        if source_doc_id is None and mc is not None:
            source_doc_id = mc.source_document_id

        out.append({
            "account_id": f"A{acct.id:04d}",
            "account_number": acct.account_number,
            "society": acct.entity_name,
            "bank": acct.bank_code,
            "account_type": acct.account_type,
            "currency": (norm.currency if norm else None) or (mc.currency if mc else acct.currency),
            "month": period,
            "ending_value_with_accrual": ending_w,
            "ending_value_without_accrual": ending_wo,
            "accrual_ending": accrual,
            "cash_value": cash,
            "movements_net": movs,
            "profit_period": profit,
            "source": source_label,
            "source_document_id": source_doc_id,
        })

    # Ordenar: por sociedad + banco + cuenta
    out.sort(key=lambda r: (r["society"] or "", r["bank"] or "", r["account_number"] or ""))

    return {
        "period": period,
        "scope": scope,
        "rows": out,
        "meta": {
            "count": len(out),
            "n_normalized": sum(1 for r in out if r["source"] == "normalized"),
            "n_fallback": sum(1 for r in out if r["source"] == "closing_fallback"),
        },
    }


def get_files(
    db: Session,
    *,
    limit: int = 500,
    bank_code: Optional[str] = None,
    status: Optional[str] = None,
    file_type: Optional[str] = None,
) -> dict:
    """
    Lista de raw_documents (cartolas + reportes + Excel) con metadata suficiente
    para auditar qué se cargó, cuándo, con qué parser y en qué estado.

    Shape por fila: {id, name, hash, date, size, parser, version, acct,
                     score, status, warning, file_type, bank, period}
    """
    from backend.db.models import RawDocument, ParserVersion

    query = (
        db.query(RawDocument, ParserVersion)
        .outerjoin(ParserVersion, RawDocument.parser_version_id == ParserVersion.id)
        .order_by(RawDocument.uploaded_at.desc())
    )
    if bank_code:
        query = query.filter(RawDocument.bank_code == bank_code)
    if file_type:
        query = query.filter(RawDocument.file_type == file_type)

    _STATUS_MAP = {
        "parsed": ("SUCCESS", 1.0),
        "validated": ("SUCCESS", 1.0),
        "processing": ("PARTIAL", 0.5),
        "uploaded": ("PARTIAL", 0.5),
        "error": ("ERROR", 0.0),
    }

    rows = query.limit(limit * 3).all()  # margen para filtro post-query
    out: list[dict] = []
    for doc, pv in rows:
        status_raw = (doc.status or "uploaded").lower()
        ui_status, score = _STATUS_MAP.get(status_raw, ("PARTIAL", 0.5))
        if status and status != ui_status:
            continue
        period = None
        if doc.period_year and doc.period_month:
            period = f"{doc.period_year:04d}-{doc.period_month:02d}"
        size_kb = (doc.file_size_bytes or 0) / 1024
        size_label = (
            f"{size_kb:.1f} KB" if size_kb < 1024
            else f"{size_kb / 1024:.1f} MB"
        )
        out.append({
            "id": doc.id,
            "name": doc.filename,
            "hash": (doc.sha256_hash or "")[:12],
            "date": doc.uploaded_at.strftime("%Y-%m-%d %H:%M") if doc.uploaded_at else "",
            "size": size_label,
            "parser": pv.parser_name if pv else (doc.bank_code or "-"),
            "version": pv.version if pv else "-",
            "acct": f"A{doc.account_id:04d}" if doc.account_id else None,
            "score": score,
            "status": ui_status,
            "warning": (doc.error_message or "") if status_raw == "error" else None,
            "file_type": doc.file_type,
            "bank": doc.bank_code,
            "period": period,
        })
        if len(out) >= limit:
            break

    # Totales (sin aplicar filtros de status/bank/file_type para que el header
    # muestre SIEMPRE el total real; los filtros afectan solo las rows visibles)
    total = db.query(RawDocument).count()
    ok = db.query(RawDocument).filter(RawDocument.status.in_(["parsed", "validated"])).count()
    err = db.query(RawDocument).filter(RawDocument.status == "error").count()
    part = db.query(RawDocument).filter(RawDocument.status.in_(["uploaded", "processing"])).count()

    return {
        "files": out,
        "totals": {
            "total": total,
            "SUCCESS": ok,
            "PARTIAL": part,
            "ERROR": err,
        },
    }


def get_coverage(db: Session, months: int = 12, scope: str = "international") -> dict:
    """
    Matriz cuenta × últimos `months` meses: para cada celda, ¿hay cartola
    cargada ese mes?

    Una celda se considera "cubierta" si existe al menos un RawDocument
    `pdf_cartola` con (account_id, period_year, period_month) para el mes.
    También acepta `pdf_report` como cobertura auxiliar.

    Salida:
      months: ["YYYY-MM"]
      rows: [
        {
          account: {id, number, society, bank, type, currency},
          cells: [{month, covered, doc_id, status, file_type}],
          gaps: int (cuántas celdas sin cubrir)
        }
      ]
    """
    if scope not in ("international", "national"):
        raise ValueError(f"scope inválido: {scope}")

    # Universo de meses: últimos `months` terminando en el mes actual.
    from datetime import date as _date
    today = _date.today()
    cur_y, cur_m = today.year, today.month
    months_list: list[tuple[int, int]] = []
    y, m = cur_y, cur_m
    for _ in range(months):
        months_list.append((y, m))
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    months_list = list(reversed(months_list))
    month_labels = [f"{yy:04d}-{mm:02d}" for (yy, mm) in months_list]

    # Cuentas
    accts_q = db.query(Account).filter(Account.is_active.is_(True))
    if scope == "national":
        accts_q = accts_q.filter(Account.bank_code.in_(["bice_inversiones", "bice_asesorias"]))
    else:
        accts_q = accts_q.filter(~Account.bank_code.in_(["bice_inversiones", "bice_asesorias"]))
    # Excluimos 'alternativos' (Excel único, no cartola mensual)
    accts_q = accts_q.filter(Account.bank_code != "alternativos")
    accounts = accts_q.order_by(Account.entity_name, Account.bank_code, Account.account_number).all()

    # Señal de cobertura: una celda "cubierta" si existe fila en
    # monthly_metrics_normalized O monthly_closings para (account, year, month).
    # Esto es más confiable que RawDocument.period_year, que frecuentemente es null.
    if not accounts:
        acct_ids = []
    else:
        acct_ids = [a.id for a in accounts]
    min_y = months_list[0][0]

    docs_map: dict[tuple[int, int, int], dict] = {}

    if acct_ids:
        norm_rows = (
            db.query(MonthlyMetricNormalized)
            .filter(
                MonthlyMetricNormalized.account_id.in_(acct_ids),
                MonthlyMetricNormalized.year >= min_y,
            )
            .all()
        )
        for n in norm_rows:
            key = (n.account_id, n.year, n.month)
            docs_map[key] = {
                "doc_id": n.source_document_id,
                "status": "normalized",
                "file_type": "normalized",
            }

        # Fallback a monthly_closings para cuentas que no tienen fila normalized
        closing_rows = (
            db.query(MonthlyClosing)
            .filter(
                MonthlyClosing.account_id.in_(acct_ids),
                MonthlyClosing.year >= min_y,
            )
            .all()
        )
        for c in closing_rows:
            key = (c.account_id, c.year, c.month)
            if key not in docs_map:
                docs_map[key] = {
                    "doc_id": c.source_document_id,
                    "status": "closing_only",
                    "file_type": "closing_fallback",
                }

        # BICE: las cuentas viven en bice_monthly_snapshot
        if scope == "national":
            from backend.db.models import BiceMonthlySnapshot
            bice_rows = (
                db.query(BiceMonthlySnapshot)
                .filter(
                    BiceMonthlySnapshot.account_id.in_(acct_ids),
                    BiceMonthlySnapshot.year >= min_y,
                )
                .all()
            )
            for b in bice_rows:
                key = (b.account_id, b.year, b.month)
                if key not in docs_map:
                    docs_map[key] = {
                        "doc_id": b.source_document_id,
                        "status": "bice_snapshot",
                        "file_type": "bice_snapshot",
                    }

    rows: list[dict] = []
    for a in accounts:
        cells: list[dict] = []
        gaps = 0
        for (yy, mm) in months_list:
            doc = docs_map.get((a.id, yy, mm))
            if doc is None:
                cells.append({
                    "month": f"{yy:04d}-{mm:02d}",
                    "covered": False,
                })
                gaps += 1
            else:
                cells.append({
                    "month": f"{yy:04d}-{mm:02d}",
                    "covered": True,
                    "doc_id": doc["doc_id"],
                    "status": doc["status"],
                    "file_type": doc["file_type"],
                })
        rows.append({
            "account": {
                "id": f"A{a.id:04d}",
                "number": a.account_number,
                "society": a.entity_name,
                "bank": a.bank_code,
                "type": a.account_type,
                "currency": a.currency,
            },
            "cells": cells,
            "gaps": gaps,
        })

    # Totales
    total_cells = len(rows) * len(months_list)
    total_covered = sum(len(months_list) - r["gaps"] for r in rows)
    return {
        "months": month_labels,
        "rows": rows,
        "totals": {
            "accounts": len(rows),
            "months": len(months_list),
            "cells_total": total_cells,
            "cells_covered": total_covered,
            "cells_gap": total_cells - total_covered,
            "coverage_pct": round(total_covered / total_cells * 100, 1) if total_cells else 0.0,
        },
    }


def get_audit_log(db: Session, limit: int = 200) -> list[dict]:
    """
    Log inmutable del sistema (transforma ValidationLog al shape de la UI).

    Shape por fila: {ts, user, event, obj, detail}
    """
    from backend.db.models import ValidationLog

    logs = (
        db.query(ValidationLog)
        .order_by(ValidationLog.created_at.desc())
        .limit(limit)
        .all()
    )
    out: list[dict] = []
    for log in logs:
        out.append({
            "ts": log.created_at.strftime("%Y-%m-%d %H:%M UTC") if log.created_at else "",
            "user": "system",  # todavía no hay auth; todos los eventos son del pipeline
            "event": (log.validation_type or "event").lower(),
            "obj": (log.source_module or "").replace("services.", "").replace("parsers.", "parsers/"),
            "detail": (log.message or "")[:400],
            "severity": log.severity or "info",
            "account_id": (f"A{log.account_id:04d}" if log.account_id else None),
            "document_id": log.raw_document_id,
        })
    return out


def get_alerts(db: Session, period: str, scope: str = "international", limit: int = 200) -> dict:
    """
    Alertas de calidad: combina
      (a) ValidationLog reciente con severity >= WARNING (filtrado por scope si
          el log tiene account_id asociado; logs sin cuenta se muestran en ambos)
      (b) Heurística on-the-fly: rentabilidad mensual |>15%| por cuenta del scope
      (c) Cobertura normalized vs closings (scope-aware)

    Shape por alerta: {id, sev, kind, title, detail, acct, month}
      - sev ∈ {critical, warning, info}
      - acct = account_id "A0023" (o None)
      - month = "YYYY-MM"
    """
    from backend.db.models import ValidationLog

    if scope not in ("international", "national"):
        raise ValueError(f"scope inválido: {scope}")
    y, m = _period_to_ym(period)
    alerts: list[dict] = []

    # IDs de cuentas del scope (para filtrar logs que tienen account_id).
    scope_acct_ids = {
        a.id for a in db.query(Account).filter(
            Account.is_active.is_(True)
        ).all()
        if _scope_matches(a.bank_code, scope)
    }

    # ── (a) Logs de validación recientes ───────────────────────
    severity_map = {
        "critical": "critical",
        "error": "critical",
        "warning": "warning",
        "info": "info",
    }
    logs = (
        db.query(ValidationLog)
        .order_by(ValidationLog.created_at.desc())
        .limit(limit)
        .all()
    )
    for log in logs:
        sev_raw = (log.severity or "info").lower()
        if sev_raw not in severity_map:
            continue
        if sev_raw == "info":
            continue  # demasiado ruidoso para alerts UI
        # Si el log tiene account_id, solo lo mostramos si la cuenta
        # pertenece al scope. Logs sin account_id se muestran en ambos.
        if log.account_id is not None and log.account_id not in scope_acct_ids:
            continue
        alerts.append({
            "id": f"VL-{log.id}",
            "sev": severity_map[sev_raw],
            "kind": (log.validation_type or "VALIDATION").upper(),
            "title": (log.message or "")[:180],
            "detail": (log.source_module or "")[:200],
            "acct": f"A{log.account_id:04d}" if log.account_id else None,
            "month": period,
            "created_at": log.created_at.isoformat() if log.created_at else None,
        })

    # ── (b) Heurística: rentabilidad mensual |>15%| ────────────
    prev_y, prev_m = (y, m - 1) if m > 1 else (y - 1, 12)
    current_rows = _load_period_rows(db, y, m, scope=scope)
    prev_map: dict[int, float] = {}
    for (mc, acct, norm) in _load_period_rows(db, prev_y, prev_m, scope=scope):
        if acct.currency not in ("USD", "USDC"):
            continue
        if mc is None and norm is not None:
            mc_prev = _SyntheticMonthlyClosing(
                account_id=acct.id, closing_date=norm.closing_date, year=norm.year,
                month=norm.month, net_value=norm.ending_value_with_accrual,
                currency=(norm.currency or acct.currency), income=norm.profit_period,
                change_in_value=norm.movements_net, accrual=norm.accrual_ending,
                asset_allocation_json=norm.asset_allocation_json,
            )
        else:
            mc_prev = mc
        prev_end = _resolve_ending_with_accrual(mc_prev, norm)
        if prev_end:
            prev_map[acct.id] = float(prev_end)

    for (mc, acct, norm) in current_rows:
        if acct.currency not in ("USD", "USDC"):
            continue
        if mc is None and norm is not None:
            mc_use = _SyntheticMonthlyClosing(
                account_id=acct.id, closing_date=norm.closing_date, year=norm.year,
                month=norm.month, net_value=norm.ending_value_with_accrual,
                currency=(norm.currency or acct.currency), income=norm.profit_period,
                change_in_value=norm.movements_net, accrual=norm.accrual_ending,
                asset_allocation_json=norm.asset_allocation_json,
            )
        else:
            mc_use = mc
        curr_end = _resolve_ending_with_accrual(mc_use, norm)
        prev_end = prev_map.get(acct.id)
        if curr_end is None or prev_end is None or prev_end <= 0:
            continue
        movs = None
        if mc is not None:
            movs = _resolve_raw_movements(mc, norm)
        elif norm is not None:
            movs = _to_float(norm.movements_net)
        movs = movs or 0.0
        r = (float(curr_end) - movs) / float(prev_end) - 1.0
        pct = r * 100.0
        if abs(pct) > 15.0:
            alerts.append({
                "id": f"RT-{acct.id}-{period}",
                "sev": "critical" if abs(pct) > 30 else "warning",
                "kind": "RETURN_OUT_OF_RANGE",
                "title": f"Rentabilidad mensual {pct:+.2f}% · {acct.entity_name} · {acct.bank_code} · {acct.account_number}",
                "detail": f"|ret| > 15% umbral. ending={curr_end:,.2f} prev={prev_end:,.2f} movs={movs:,.2f}",
                "acct": f"A{acct.id:04d}",
                "month": period,
            })

    # ── (c) Cobertura normalized (por scope) ───────────────────
    if scope_acct_ids:
        n_norm = db.query(MonthlyMetricNormalized).filter(
            MonthlyMetricNormalized.year == y,
            MonthlyMetricNormalized.month == m,
            MonthlyMetricNormalized.account_id.in_(scope_acct_ids),
        ).count()
        n_closing = db.query(MonthlyClosing).filter(
            MonthlyClosing.year == y,
            MonthlyClosing.month == m,
            MonthlyClosing.account_id.in_(scope_acct_ids),
        ).count()
        if n_closing > 0:
            cov = n_norm / n_closing * 100.0 if n_closing else 100.0
            if cov < 100.0:
                alerts.append({
                    "id": f"COV-{period}-{scope}",
                    "sev": "warning" if cov >= 95 else "critical",
                    "kind": "NORMALIZED_COVERAGE",
                    "title": f"Cobertura normalized ({scope}): {cov:.1f}% ({n_norm}/{n_closing} cuentas)",
                    "detail": f"{n_closing - n_norm} cuenta(s) en monthly_closings sin fila normalizada (fallback activo).",
                    "acct": None,
                    "month": period,
                })

    return {
        "period": period,
        "alerts": alerts,
        "meta": {
            "count_critical": sum(1 for a in alerts if a["sev"] == "critical"),
            "count_warning": sum(1 for a in alerts if a["sev"] == "warning"),
            "count_info": sum(1 for a in alerts if a["sev"] == "info"),
        },
    }


def get_alternatives(db: Session, period: str) -> dict:
    """
    Lectura de Alternativos (bank_code='alternativos') para el período.
    - Por fondo: id, name, class (PE|RE|VC), strategy, society, vintage, nav (USD M).
    - Agregados por asset_class + global.
    - Listas de strategies distintas por class.

    Campos que hoy NO existen en BD (commit, distributed, irr, tvpi): retornamos
    None — el frontend los muestra como "—".
    """
    import json as _json

    y, m = _period_to_ym(period)

    alt_accts = (
        db.query(Account)
        .filter(
            Account.is_active.is_(True),
            Account.bank_code == "alternativos",
        )
        .all()
    )
    acct_ids = [a.id for a in alt_accts]
    if not acct_ids:
        return {
            "period": period,
            "funds": [],
            "aggregate": {
                "PE": {"nav": 0, "commit": None, "distributed": None},
                "RE": {"nav": 0, "commit": None, "distributed": None},
                "VC": {"nav": 0, "commit": None, "distributed": None},
                "global": {"nav": 0},
            },
            "strategies": {"PE": [], "RE": [], "VC": []},
        }

    norms = {
        n.account_id: n
        for n in db.query(MonthlyMetricNormalized).filter(
            MonthlyMetricNormalized.year == y,
            MonthlyMetricNormalized.month == m,
            MonthlyMetricNormalized.account_id.in_(acct_ids),
        ).all()
    }

    funds: list[dict] = []
    for a in alt_accts:
        meta = {}
        if a.metadata_json:
            try:
                meta = _json.loads(a.metadata_json)
            except Exception:
                meta = {}
        asset_class_raw = str(meta.get("asset_class") or "PE").upper()
        # Nuestra taxonomía real reconoce PE y RE. VC se muestra en el frontend
        # pero no existe como categoría hoy; si viniera, la respetamos.
        if asset_class_raw not in ("PE", "RE", "VC"):
            asset_class_raw = "PE"
        norm = norms.get(a.id)
        nav_usd = _to_float(norm.ending_value_with_accrual) if norm else None
        funds.append({
            "id": f"ALT{a.id:04d}",
            "name": meta.get("nemo_reference") or a.account_number,
            "class": asset_class_raw,
            "strategy": meta.get("strategy") or "Sin clasificar",
            "society": a.entity_name,
            "vintage": meta.get("vintage"),  # si no hay, None
            "nav": round((nav_usd or 0.0) / 1e6, 4),  # M USD
            "commit": None,
            "distributed": None,
            "irr": None,
            "tvpi": None,
            "account_id": f"A{a.id:04d}",
            "detail_label": meta.get("detail_label"),
        })

    aggregate = {
        "PE": {"nav": 0.0, "commit": None, "distributed": None},
        "RE": {"nav": 0.0, "commit": None, "distributed": None},
        "VC": {"nav": 0.0, "commit": None, "distributed": None},
        "global": {"nav": 0.0},
    }
    for f in funds:
        cls = f["class"]
        if cls in aggregate:
            aggregate[cls]["nav"] = round(aggregate[cls]["nav"] + float(f["nav"] or 0), 4)
        aggregate["global"]["nav"] = round(aggregate["global"]["nav"] + float(f["nav"] or 0), 4)

    strategies = {"PE": [], "RE": [], "VC": []}
    for f in funds:
        cls = f["class"]
        if cls in strategies:
            s = f["strategy"]
            if s and s not in strategies[cls]:
                strategies[cls].append(s)
    for cls in strategies:
        strategies[cls].sort()

    return {
        "period": period,
        "funds": funds,
        "aggregate": aggregate,
        "strategies": strategies,
    }


def _ret_series(endings: list[float], movements: list[Optional[float]]) -> list[Optional[float]]:
    """ret_i = (ending_i - movements_i) / ending_{i-1} − 1, en % (o None si denom inválido)."""
    out: list[Optional[float]] = [None]
    for i in range(1, len(endings)):
        prev = endings[i - 1]
        curr = endings[i]
        mov = movements[i] if movements[i] is not None else 0.0
        if prev and prev > 0:
            r = ((curr - mov) / prev - 1.0) * 100.0
            out.append(round(r, 4))
        else:
            out.append(None)
    return out


def _chain_link_ytd(rets: list[Optional[float]], series_ym: list[tuple[int, int]], year: int) -> Optional[float]:
    comp = 1.0
    found = False
    for idx, (yy, mm) in enumerate(series_ym):
        if yy != year:
            continue
        r = rets[idx]
        if r is None:
            continue
        comp *= (1.0 + r / 100.0)
        found = True
    return round((comp - 1.0) * 100.0, 4) if found else None


def get_returns(db: Session, period: str, scope: str = "international") -> dict:
    """
    Rentabilidades consolidadas + desglose por sociedad, en USD, para el
    período pedido y los 12 meses previos.

    - Consolidado: ret_monthly[13], twr_ytd.
    - Por sociedad: id, name, mom%, ytd%.

    Solo cuentas en USD (scope=international). Para CLP se necesita otra fuente
    (bice_monthly_snapshot) — pendiente v2.
    """
    if scope not in ("international", "national"):
        raise ValueError(f"scope inválido: {scope}")

    y, m = _period_to_ym(period)
    series_ym = _last_n_months(period, 13)
    months = [f"{yy:04d}-{mm:02d}" for (yy, mm) in series_ym]
    month_labels = [_month_label(yy, mm) for (yy, mm) in series_ym]

    # Precargar rows por mes
    rows_by_month: dict[tuple[int, int], list] = {}
    for (yy, mm) in series_ym:
        rows_by_month[(yy, mm)] = _load_period_rows(db, yy, mm, scope=scope)

    # Consolidado: ending + movements por mes
    consolidated_ending: list[float] = []
    consolidated_movs: list[Optional[float]] = []
    for (yy, mm) in series_ym:
        rows = rows_by_month[(yy, mm)]
        end_t, _, _ = _sum_usd(rows, {}, db)
        mov_t = 0.0
        seen_any = False
        for (mc, acct, norm) in rows:
            if acct.currency not in ("USD", "USDC"):
                continue
            raw = None
            if mc is not None:
                raw = _resolve_raw_movements(mc, norm)
            elif norm is not None:
                raw = _to_float(norm.movements_net)
            if raw is not None:
                mov_t += float(raw)
                seen_any = True
        consolidated_movs.append(mov_t if seen_any else None)
        consolidated_ending.append(end_t)

    ret_monthly = _ret_series(consolidated_ending, consolidated_movs)
    twr_ytd = _chain_link_ytd(ret_monthly, series_ym, y)
    twr_mom = ret_monthly[-1] if ret_monthly else None

    # Por sociedad
    society_names: set[str] = set()
    for rows in rows_by_month.values():
        for (_, acct, _) in rows:
            if acct.entity_name:
                society_names.add(acct.entity_name)

    by_society: list[dict] = []
    for soc_name in sorted(society_names):
        end_by_m: list[float] = []
        mov_by_m: list[Optional[float]] = []
        for (yy, mm) in series_ym:
            rows = rows_by_month[(yy, mm)]
            rows_soc = [(mc, a, n) for (mc, a, n) in rows if a.entity_name == soc_name]
            end_t, _, _ = _sum_usd(rows_soc, {}, db)
            mov_t = 0.0
            seen_any = False
            for (mc, acct, norm) in rows_soc:
                if acct.currency not in ("USD", "USDC"):
                    continue
                raw = None
                if mc is not None:
                    raw = _resolve_raw_movements(mc, norm)
                elif norm is not None:
                    raw = _to_float(norm.movements_net)
                if raw is not None:
                    mov_t += float(raw)
                    seen_any = True
            end_by_m.append(end_t)
            mov_by_m.append(mov_t if seen_any else None)
        rets = _ret_series(end_by_m, mov_by_m)
        by_society.append({
            "id": soc_name,
            "name": soc_name,
            "mom": rets[-1] if rets else None,
            "ytd": _chain_link_ytd(rets, series_ym, y),
            "retMonthly": rets,
        })

    return {
        "period": period,
        "scope": scope,
        "months": months,
        "monthLabels": month_labels,
        "consolidated": {
            "retMonthly": ret_monthly,
            "twr_mom": twr_mom,
            "twr_ytd": twr_ytd,
        },
        "bySociety": by_society,
    }


def get_positions(db: Session, period: str) -> dict:
    """
    Snapshot de posiciones del período pedido.
    Tomamos la foto del último día disponible del mes (por cuenta) en DailyPosition.

    Shape compatible con mock.positions:
      {acct, isin, instr, ccy, qty, price, mv, accrual, bucket}
    """
    y, m = _period_to_ym(period)

    # Último día con DailyPosition por cuenta dentro del mes
    # (usamos subquery con max(position_date) por account_id + año + mes)
    from sqlalchemy import func as sqlfunc
    month_start = date(y, m, 1)
    if m == 12:
        next_month = date(y + 1, 1, 1)
    else:
        next_month = date(y, m + 1, 1)

    subq = (
        db.query(
            DailyPosition.account_id.label("aid"),
            sqlfunc.max(DailyPosition.position_date).label("last_d"),
        )
        .filter(
            DailyPosition.position_date >= month_start,
            DailyPosition.position_date < next_month,
        )
        .group_by(DailyPosition.account_id)
        .subquery()
    )

    rows = (
        db.query(DailyPosition, Account)
        .join(Account, Account.id == DailyPosition.account_id)
        .join(
            subq,
            and_(
                subq.c.aid == DailyPosition.account_id,
                subq.c.last_d == DailyPosition.position_date,
            ),
        )
        .filter(Account.is_active.is_(True))
        .order_by(Account.entity_name, Account.bank_code, DailyPosition.instrument_code)
        .all()
    )

    # Importamos classifier acá para evitar costo al arranque del módulo
    from asset_taxonomy import classify_etf_asset_bucket

    out: list[dict] = []
    for pos, acct in rows:
        try:
            bucket = classify_etf_asset_bucket(pos.instrument_name or pos.instrument_code) or "RV DM"
        except Exception:
            bucket = "RV DM"
        out.append({
            "acct": f"A{acct.id:04d}",
            "account_number": acct.account_number,
            "society": acct.entity_name,
            "bank": acct.bank_code,
            "isin": pos.isin,
            "instr": pos.instrument_name or pos.instrument_code,
            "ccy": pos.currency,
            "qty": _to_float(pos.quantity),
            "price": _to_float(pos.market_price),
            "mv": _to_float(pos.market_value),
            "mv_usd": _to_float(pos.market_value_usd),
            "accrual": _to_float(pos.accrued_interest),
            "bucket": bucket,
            "as_of": pos.position_date.isoformat(),
        })

    return {
        "period": period,
        "positions": out,
        "meta": {
            "source": "daily_positions",
            "note": "Foto del último día con datos del mes. Solo cuentas con carga Excel de posiciones diarias.",
        },
    }
