"""
FO Reporting – Router de datos financieros (resumen, mandatos, ETF, personal).

Consulta tablas de reporting pobladas por DataLoadingService.
"""

import json
import re
from typing import Optional
from decimal import Decimal
from datetime import date
from fastapi import APIRouter, Depends
from sqlalchemy import and_, extract, func
from sqlalchemy.orm import Session

from calculations.profit import monthly_return_pct, ytd_return_pct
from calculations.reconciliation import reconcile_monthly
from backend.db.models import (
    Account,
    DailyPosition,
    EtfComposition,
    MonthlyClosing,
    MonthlyMetricNormalized,
    ParsedStatement,
    Reconciliation,
)
from backend.db.session import get_db
from backend.schemas import FilterParams, HealthAuditParams

router = APIRouter(prefix="/data", tags=["data"])

PERSONAL_ENTITY_NAMES = {"Raíces LP"}


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
    if getattr(filters, "person_names", None):
        query = query.filter(Account.person_name.in_(filters.person_names))
    if filters.account_types:
        query = query.filter(Account.account_type.in_(filters.account_types))
    if filters.currencies:
        query = query.filter(Account.currency.in_(filters.currencies))
    if getattr(filters, "sin_personal", False):
        query = query.filter(~Account.entity_name.in_(PERSONAL_ENTITY_NAMES))
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
    person_names = [
        row[0]
        for row in (
            db.query(Account.person_name)
            .join(MonthlyClosing, MonthlyClosing.account_id == Account.id)
            .filter(Account.person_name.isnot(None))
            .distinct()
            .all()
        )
        if row[0]
    ]
    return {
        "years": years,
        "months": list(range(1, 13)),
        "bank_codes": bank_codes,
        "entity_names": entity_names,
        "person_names": sorted(person_names),
        "account_types": account_types,
        "currencies": [],
    }


def _previous_month_key(fecha: str) -> str:
    """Retorna la clave YYYY-MM del mes calendario anterior."""
    year, month = (int(part) for part in fecha.split("-"))
    if month == 1:
        return f"{year - 1}-12"
    return f"{year}-{month - 1:02d}"


def _extract_cash_from_asset_allocation(asset_alloc_json: str | None) -> float:
    """Extrae monto de caja desde asset_allocation_json persistido."""
    if not asset_alloc_json:
        return 0.0
    try:
        alloc = json.loads(asset_alloc_json)
    except (TypeError, ValueError):
        return 0.0

    def _payload_value(payload) -> Optional[float]:
        if isinstance(payload, dict):
            raw = (
                payload.get("value")
                or payload.get("total")
                or payload.get("ending")
                or payload.get("ending_value")
                or payload.get("market_value")
                or payload.get("amount")
            )
        else:
            raw = payload
        try:
            return float(raw)
        except (TypeError, ValueError):
            return None

    def _label_norm(label: str) -> str:
        return re.sub(r"[^a-z0-9]", "", str(label or "").lower())

    def _is_cash_umbrella(label_norm: str) -> bool:
        return (
            "cash" in label_norm
            and "deposit" in label_norm
            and ("moneymarket" in label_norm or "shortterm" in label_norm)
        )

    def _is_mixed_cash_bucket(label_norm: str) -> bool:
        # Ej: "Cash & Fixed Income" no es caja pura.
        return "cash" in label_norm and any(
            tok in label_norm for tok in ("fixedincome", "bond", "equity", "stock")
        )

    total = 0.0
    if isinstance(alloc, dict):
        items = list(alloc.items())
        # Evitar doble conteo en bancos que reportan total + sublineas (ej. Goldman).
        umbrella_values: list[float] = []
        for key, payload in items:
            key_norm = _label_norm(key)
            if _is_mixed_cash_bucket(key_norm):
                continue
            if not _is_cash_umbrella(key_norm):
                continue
            val = _payload_value(payload)
            if val is not None:
                umbrella_values.append(val)
        if umbrella_values:
            return max(max(umbrella_values), 0.0)

        for key, payload in items:
            key_norm = _label_norm(key)
            if _is_mixed_cash_bucket(key_norm):
                continue
            if not any(
                tok in key_norm for tok in ("cash", "deposit", "moneymarket", "shortterm", "liquidity")
            ):
                continue
            val = _payload_value(payload)
            if val is None:
                continue
            total += val
    elif isinstance(alloc, list):
        for row in alloc:
            if not isinstance(row, dict):
                continue
            name_norm = _label_norm(
                row.get("asset_class")
                or row.get("name")
                or row.get("label")
                or ""
            )
            if not any(
                tok in name_norm for tok in ("cash", "deposit", "moneymarket", "shortterm", "liquidity")
            ):
                continue
            val = _payload_value(row)
            if val is None:
                continue
            total += val

    return max(total, 0.0)


def _is_cash_asset_label(label: str | None) -> bool:
    key = re.sub(r"[^a-z0-9]", "", str(label or "").lower())
    return any(
        token in key
        for token in ("cash", "deposit", "moneymarket", "shortterm", "liquidity")
    )


def _extract_cash_from_etf_compositions(
    db: Session,
    account_id: int,
    year: int,
    month: int,
) -> float:
    """
    Fallback para ETF cuando no hay asset_allocation_json.
    Usa holdings ETF del mismo período para estimar caja.
    """
    rows = (
        db.query(EtfComposition.etf_name, EtfComposition.market_value)
        .filter(
            EtfComposition.account_id == account_id,
            EtfComposition.year == year,
            EtfComposition.month == month,
        )
        .all()
    )
    total = 0.0
    for name, mv in rows:
        nm = (name or "").lower()
        is_cash_like = any(
            kw in nm
            for kw in (
                "sweep",
                "liquidity",
                "money market",
                "cash",
                "deposit",
                "li-liq",
            )
        )
        if not is_cash_like:
            continue
        try:
            total += float(mv or 0)
        except (TypeError, ValueError):
            continue
    return max(total, 0.0)


def _to_float(value) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _query_closing_rows(
    db: Session,
    filters: FilterParams,
    *,
    years: Optional[set[int]] = None,
    months: Optional[list[int]] = None,
    account_type: Optional[str] = None,
):
    """
    Trae cierres + capa normalizada para un mismo período/cuenta.
    Mantiene fallback: si falta fila normalizada, igual retorna MonthlyClosing.
    """
    query = (
        db.query(MonthlyClosing, Account, MonthlyMetricNormalized)
        .join(Account, MonthlyClosing.account_id == Account.id)
        .outerjoin(
            MonthlyMetricNormalized,
            and_(
                MonthlyMetricNormalized.account_id == MonthlyClosing.account_id,
                MonthlyMetricNormalized.year == MonthlyClosing.year,
                MonthlyMetricNormalized.month == MonthlyClosing.month,
            ),
        )
    )
    query = _apply_account_filters(query, filters)
    if account_type:
        query = query.filter(Account.account_type == account_type)
    if years:
        query = query.filter(MonthlyClosing.year.in_(years))
    if months:
        query = query.filter(MonthlyClosing.month.in_(months))
    return query


def _resolve_ending_with_accrual(
    mc: MonthlyClosing,
    norm: Optional[MonthlyMetricNormalized],
) -> Optional[float]:
    if norm:
        v = _to_float(norm.ending_value_with_accrual)
        if v is not None:
            return v
    return _to_float(mc.net_value)


def _resolve_ending_without_accrual(
    mc: MonthlyClosing,
    norm: Optional[MonthlyMetricNormalized],
) -> Optional[float]:
    if norm:
        v = _to_float(norm.ending_value_without_accrual)
        if v is not None:
            return v
        end_w = _to_float(norm.ending_value_with_accrual)
        accr = _to_float(norm.accrual_ending)
        if end_w is not None and accr is not None:
            return end_w - accr
        if end_w is not None:
            return end_w

    end_w = _to_float(mc.net_value)
    if end_w is None:
        return None
    accr = _to_float(mc.accrual)
    if accr is None:
        return end_w
    return end_w - accr


def _resolve_asset_allocation_json(
    mc: MonthlyClosing,
    norm: Optional[MonthlyMetricNormalized],
) -> Optional[str]:
    if norm and norm.asset_allocation_json:
        return norm.asset_allocation_json
    return mc.asset_allocation_json


def _resolve_cash_value(
    db: Session,
    acct: Account,
    mc: MonthlyClosing,
    norm: Optional[MonthlyMetricNormalized],
    *,
    etf_cash_cache: dict[tuple[int, int], float],
) -> float:
    is_cash_account = acct.account_type in {"current", "savings", "checking"}
    end_w = _resolve_ending_with_accrual(mc, norm)
    if is_cash_account:
        return end_w or 0.0

    if norm:
        normalized_cash = _to_float(norm.cash_value)
        if normalized_cash is not None:
            return max(normalized_cash, 0.0)

    cash = _extract_cash_from_asset_allocation(_resolve_asset_allocation_json(mc, norm))
    if cash > 0.0:
        return cash

    if cash == 0.0 and acct.account_type == "etf":
        cache_key = (mc.year, mc.month)
        if cache_key not in etf_cash_cache:
            etf_cash_cache[cache_key] = _extract_cash_from_etf_compositions(
                db=db,
                account_id=acct.id,
                year=mc.year,
                month=mc.month,
            )
        cash = etf_cash_cache[cache_key]

    if cash > 0.0:
        return cash

    return max(cash, 0.0)



def _resolve_raw_movements(mc: MonthlyClosing, norm: Optional[MonthlyMetricNormalized]) -> Optional[float]:
    """Lee movimientos guardados, sin forzar identidad."""
    if norm:
        v = _to_float(norm.movements_net)
        if v is not None:
            return v
    return _to_float(mc.change_in_value)


def _resolve_raw_profit(mc: MonthlyClosing, norm: Optional[MonthlyMetricNormalized]) -> Optional[float]:
    """Lee utilidad guardada, sin forzar identidad."""
    if norm:
        v = _to_float(norm.profit_period)
        if v is not None:
            return v
    return _to_float(mc.income)


def _resolve_audit_movements(
    *,
    ending_value: Optional[float],
    previous_ending: Optional[float],
    movements: Optional[float],
    profit: Optional[float],
) -> Optional[float]:
    """
    Interpretación read-only para Salud BD.

    Si movimientos faltan pero la identidad mensual cuadra con movimiento implícito
    ~0, el reporte de salud los trata como 0 para no marcar un falso faltante.
    No persiste nada ni modifica la BD.
    """
    if movements is not None:
        return movements
    if ending_value is None or previous_ending is None or profit is None:
        return None
    implied_movement = ending_value - previous_ending - profit
    if abs(implied_movement) <= 1:
        return 0.0
    return None


def _extract_statement_beginning_value(
    db: Session,
    mc: MonthlyClosing,
    acct: Account,
) -> Optional[float]:
    statement = None
    if mc.source_document_id:
        statement = (
            db.query(ParsedStatement)
            .filter(
                ParsedStatement.raw_document_id == mc.source_document_id,
                ParsedStatement.account_id == acct.id,
            )
            .order_by(ParsedStatement.id.desc())
            .first()
        )
    if statement is None:
        statement = (
            db.query(ParsedStatement)
            .filter(
                ParsedStatement.account_id == acct.id,
                extract("year", ParsedStatement.statement_date) == mc.year,
                extract("month", ParsedStatement.statement_date) == mc.month,
            )
            .order_by(ParsedStatement.id.desc())
            .first()
        )
    if statement is None:
        return None

    try:
        parsed = json.loads(statement.parsed_data_json or "{}")
    except (TypeError, ValueError):
        return None

    qualitative = parsed.get("qualitative_data") if isinstance(parsed, dict) else {}
    if not isinstance(qualitative, dict):
        qualitative = {}

    for account_row in qualitative.get("accounts", []):
        if not isinstance(account_row, dict):
            continue
        if account_row.get("account_number") == acct.account_number:
            return _to_float(account_row.get("beginning_value"))

    return _to_float(parsed.get("opening_balance")) if isinstance(parsed, dict) else None


def _should_zero_negative_ubs_return(
    bank_code: str | None,
    current_value: Optional[float],
    previous_value: Optional[float],
) -> bool:
    return (
        bank_code == "ubs"
        and (
            (current_value is not None and current_value < 0)
            or (previous_value is not None and previous_value < 0)
        )
    )



def _build_health_report(
    db: Session,
    filters: HealthAuditParams,
) -> dict:
    years = set(filters.years) if filters.years else None
    months = filters.months or None
    limit = max(1, min(int(filters.limit or 200), 1000))

    filtered_results = (
        _query_closing_rows(
            db=db,
            filters=filters,
            years=years,
            months=months,
        )
        .order_by(Account.id, MonthlyClosing.year, MonthlyClosing.month)
        .all()
    )
    if not filtered_results:
        return {
            "summary": {
                "total_rows": 0,
                "rows_with_previous": 0,
                "identity_mismatch_count": 0,
                "missing_components_count": 0,
                "ytd_movement_mismatch_count": 0,
                "ytd_profit_mismatch_count": 0,
                "alert_count": 0,
            },
            "by_bank_type": [],
            "identity_issues": [],
            "missing_component_issues": [],
            "ytd_issues": [],
        }

    account_ids = sorted({acct.id for _, acct, _ in filtered_results})
    history_results = (
        db.query(MonthlyClosing, Account, MonthlyMetricNormalized)
        .join(Account, MonthlyClosing.account_id == Account.id)
        .outerjoin(
            MonthlyMetricNormalized,
            and_(
                MonthlyMetricNormalized.account_id == MonthlyClosing.account_id,
                MonthlyMetricNormalized.year == MonthlyClosing.year,
                MonthlyMetricNormalized.month == MonthlyClosing.month,
            ),
        )
        .filter(MonthlyClosing.account_id.in_(account_ids))
        .order_by(Account.id, MonthlyClosing.year, MonthlyClosing.month)
        .all()
    )

    history_by_account: dict[int, dict[tuple[int, int], dict]] = {}
    for mc, acct, norm in history_results:
        history_by_account.setdefault(acct.id, {})[(mc.year, mc.month)] = {
            "entity_name": acct.entity_name,
            "bank_code": acct.bank_code,
            "account_type": acct.account_type,
            "account_number": acct.account_number,
            "year": mc.year,
            "month": mc.month,
            "ending_value": _resolve_ending_with_accrual(mc, norm),
            "movements": _resolve_raw_movements(mc, norm),
            "profit": _resolve_raw_profit(mc, norm),
        }

    by_bank_type: dict[tuple[str, str], dict] = {}
    identity_issues: list[dict] = []
    missing_component_issues: list[dict] = []
    rows_with_previous = 0

    def _bucket(bank_code: str, account_type: str) -> dict:
        key = (bank_code, account_type)
        if key not in by_bank_type:
            by_bank_type[key] = {
                "bank_code": bank_code,
                "account_type": account_type,
                "identity_mismatch_count": 0,
                "missing_components_count": 0,
                "ytd_movement_mismatch_count": 0,
                "ytd_profit_mismatch_count": 0,
            }
        return by_bank_type[key]

    for mc, acct, norm in filtered_results:
        prev_year = mc.year if mc.month > 1 else mc.year - 1
        prev_month = mc.month - 1 if mc.month > 1 else 12
        current = history_by_account.get(acct.id, {}).get((mc.year, mc.month), {})
        previous = history_by_account.get(acct.id, {}).get((prev_year, prev_month))
        bucket = _bucket(acct.bank_code, acct.account_type)
        if previous is None or previous.get("ending_value") is None:
            continue

        rows_with_previous += 1
        ending_value = current.get("ending_value")
        movements = current.get("movements")
        profit = current.get("profit")
        prev_ending = previous.get("ending_value")
        audit_movements = _resolve_audit_movements(
            ending_value=ending_value,
            previous_ending=prev_ending,
            movements=movements,
            profit=profit,
        )

        if audit_movements is None or profit is None:
            bucket["missing_components_count"] += 1
            if len(missing_component_issues) < limit:
                missing_component_issues.append(
                    {
                        "entity_name": acct.entity_name,
                        "bank_code": acct.bank_code,
                        "account_type": acct.account_type,
                        "account_number": acct.account_number,
                        "year": mc.year,
                        "month": mc.month,
                        "prev_ending_value": prev_ending,
                        "ending_value": ending_value,
                        "movements": audit_movements,
                        "profit": profit,
                        "missing_fields": [
                            field_name
                            for field_name, field_val in (
                                ("movements", audit_movements),
                                ("profit", profit),
                            )
                            if field_val is None
                        ],
                    }
                )
            continue

        if ending_value is None:
            continue

        identity_diff = ending_value - audit_movements - profit - prev_ending
        if abs(identity_diff) > 1:
            statement_beginning_value = _extract_statement_beginning_value(
                db=db,
                mc=mc,
                acct=acct,
            )
            note = None
            if (
                statement_beginning_value is not None
                and prev_ending is not None
                and abs(statement_beginning_value - prev_ending) > 1
            ):
                note = (
                    "Beginning value de la cartola actual no coincide con "
                    "prev_ending_value; prevalece el ending value auditado."
                )
            bucket["identity_mismatch_count"] += 1
            if len(identity_issues) < limit:
                identity_issues.append(
                    {
                        "entity_name": acct.entity_name,
                        "bank_code": acct.bank_code,
                        "account_type": acct.account_type,
                        "account_number": acct.account_number,
                        "year": mc.year,
                        "month": mc.month,
                        "prev_ending_value": prev_ending,
                        "ending_value": ending_value,
                        "movements": audit_movements,
                        "profit": profit,
                        "identity_diff": round(identity_diff, 4),
                        "statement_beginning_value": statement_beginning_value,
                        "note": note,
                    }
                )

    ytd_issues: list[dict] = []
    ytd_rows = (
        db.query(MonthlyMetricNormalized, Account)
        .join(Account, MonthlyMetricNormalized.account_id == Account.id)
        .filter(MonthlyMetricNormalized.account_id.in_(account_ids))
    )
    if years:
        ytd_rows = ytd_rows.filter(MonthlyMetricNormalized.year.in_(years))
    if months:
        ytd_rows = ytd_rows.filter(MonthlyMetricNormalized.month.in_(months))
    ytd_rows = ytd_rows.order_by(
        Account.id, MonthlyMetricNormalized.year, MonthlyMetricNormalized.month,
    ).all()

    for norm_row, acct in ytd_rows:
        mov_ytd = _to_float(norm_row.movements_ytd)
        profit_ytd_val = _to_float(norm_row.profit_ytd)
        if mov_ytd is None and profit_ytd_val is None:
            continue

        account_history = history_by_account.get(acct.id, {})
        rows_up_to = [
            row
            for (yr, mo), row in account_history.items()
            if yr == norm_row.year and mo <= norm_row.month
        ]
        if not rows_up_to:
            continue

        mov_sum = sum((row.get("movements") or 0.0) for row in rows_up_to)
        profit_sum = sum((row.get("profit") or 0.0) for row in rows_up_to)
        bucket = _bucket(acct.bank_code, acct.account_type)

        if mov_ytd is not None:
            diff_mov = mov_ytd - mov_sum
            if abs(diff_mov) > 1:
                bucket["ytd_movement_mismatch_count"] += 1
                if len(ytd_issues) < limit:
                    ytd_issues.append(
                        {
                            "metric": "movements_ytd",
                            "entity_name": acct.entity_name,
                            "bank_code": acct.bank_code,
                            "account_type": acct.account_type,
                            "account_number": acct.account_number,
                            "year": norm_row.year,
                            "month": norm_row.month,
                            "ytd_value": mov_ytd,
                            "monthly_sum": round(mov_sum, 4),
                            "difference": round(diff_mov, 4),
                            "source": "normalized",
                        }
                    )

        if profit_ytd_val is not None:
            diff_profit = profit_ytd_val - profit_sum
            if abs(diff_profit) > 1:
                bucket["ytd_profit_mismatch_count"] += 1
                if len(ytd_issues) < limit:
                    ytd_issues.append(
                        {
                            "metric": "profit_ytd",
                            "entity_name": acct.entity_name,
                            "bank_code": acct.bank_code,
                            "account_type": acct.account_type,
                            "account_number": acct.account_number,
                            "year": norm_row.year,
                            "month": norm_row.month,
                            "ytd_value": profit_ytd_val,
                            "monthly_sum": round(profit_sum, 4),
                            "difference": round(diff_profit, 4),
                            "source": "normalized",
                        }
                    )

    by_bank_type_rows = []
    for row in by_bank_type.values():
        row["total_issues"] = (
            row["identity_mismatch_count"]
            + row["missing_components_count"]
            + row["ytd_movement_mismatch_count"]
            + row["ytd_profit_mismatch_count"]
        )
        by_bank_type_rows.append(row)
    by_bank_type_rows.sort(key=lambda x: x["total_issues"], reverse=True)

    summary = {
        "total_rows": len(filtered_results),
        "rows_with_previous": rows_with_previous,
        "identity_mismatch_count": sum(row["identity_mismatch_count"] for row in by_bank_type_rows),
        "missing_components_count": sum(row["missing_components_count"] for row in by_bank_type_rows),
        "ytd_movement_mismatch_count": sum(row["ytd_movement_mismatch_count"] for row in by_bank_type_rows),
        "ytd_profit_mismatch_count": sum(row["ytd_profit_mismatch_count"] for row in by_bank_type_rows),
    }
    summary["alert_count"] = (
        summary["identity_mismatch_count"]
        + summary["missing_components_count"]
        + summary["ytd_movement_mismatch_count"]
        + summary["ytd_profit_mismatch_count"]
    )

    return {
        "summary": summary,
        "by_bank_type": by_bank_type_rows,
        "identity_issues": identity_issues,
        "missing_component_issues": missing_component_issues,
        "ytd_issues": ytd_issues,
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

    query = _query_closing_rows(
        db=db,
        filters=filters,
        years=fetch_years if fetch_years else None,
    )
    all_results = query.order_by(
        Account.id, MonthlyClosing.year, MonthlyClosing.month
    ).all()

    # Agrupar por cuenta
    by_account: dict[int, list[tuple]] = {}
    for mc, acct, norm in all_results:
        by_account.setdefault(acct.id, []).append((mc, acct, norm))

    # Acumular datos por mes para consolidación y generar detalle
    month_agg: dict[str, dict] = {}
    detail_rows: list[dict] = []
    prev_dec = f"{min(req_years) - 1}-12" if req_years else None

    visible_bank_codes = {entries[0][1].bank_code for entries in by_account.values() if entries}

    for acct_id, entries in by_account.items():
        entries.sort(key=lambda x: (x[0].year, x[0].month))
        acct = entries[0][1]
        is_cash = acct.account_type in {"current", "savings", "checking"}
        etf_cash_cache: dict[tuple[int, int], float] = {}

        account_month_values: dict[str, float] = {}
        account_month_cash: dict[str, float] = {}

        for mc, _, norm in entries:
            fecha = f"{mc.year}-{mc.month:02d}"
            curr_val = _resolve_ending_with_accrual(mc, norm)
            if curr_val is None:
                continue
            account_month_values[fecha] = curr_val
            account_month_cash[fecha] = _resolve_cash_value(
                db=db,
                acct=acct,
                mc=mc,
                norm=norm,
                etf_cash_cache=etf_cash_cache,
            )

        for mc, _, norm in entries:
            fecha = f"{mc.year}-{mc.month:02d}"
            curr_val = account_month_values.get(fecha)
            if curr_val is None:
                continue

            prev_key = _previous_month_key(fecha)
            prev_val = account_month_values.get(prev_key)
            caja = account_month_cash.get(fecha, 0.0)
            ev_sin_caja = max(curr_val - caja, 0.0)
            prev_caja = account_month_cash.get(prev_key)
            prev_ev_sin_caja = None
            if prev_val is not None and prev_caja is not None:
                prev_ev_sin_caja = max(prev_val - prev_caja, 0.0)

            movimientos = _resolve_raw_movements(mc, norm)
            utilidad = _resolve_raw_profit(mc, norm)
            cash_flow = caja - prev_caja if prev_caja is not None else None
            movimientos_sin_caja = None
            if movimientos is not None and cash_flow is not None:
                movimientos_sin_caja = movimientos - cash_flow

            # Detalle por cuenta (solo meses del a?o solicitado)
            if not req_years or mc.year in req_years:
                ret = None
                if _should_zero_negative_ubs_return(acct.bank_code, curr_val, prev_val):
                    ret = 0.0
                elif prev_val and prev_val > 0 and movimientos is not None:
                    ret = round((((curr_val - movimientos) / prev_val) - 1) * 100, 4)
                elif prev_val and prev_val > 0:
                    ret = round(((curr_val / prev_val) - 1) * 100, 4)
                ret_sc = None
                if (
                    not is_cash
                    and prev_ev_sin_caja
                    and prev_ev_sin_caja > 0
                    and movimientos_sin_caja is not None
                ):
                    ret_sc = round(
                        (((ev_sin_caja - movimientos_sin_caja) / prev_ev_sin_caja) - 1) * 100,
                        4,
                    )
                currency = (norm.currency if norm and norm.currency else mc.currency) or acct.currency
                detail_rows.append(
                    {
                        "fecha": fecha,
                        "sociedad": acct.entity_name,
                        "banco": acct.bank_code,
                        "id": acct.identification_number or acct.account_number,
                        "moneda": currency,
                        "ending_value": curr_val,
                        "caja": caja,
                        "movimientos": movimientos,
                        "utilidad": utilidad,
                        "rent_mensual_pct": ret,
                        "rent_mensual_sin_caja_pct": ret_sc,
                        "account_type": acct.account_type,
                    }
                )

            # Acumular para consolidaci?n (todos los meses tra?dos)
            if fecha not in month_agg:
                month_agg[fecha] = {
                    "ev": 0.0,
                    "mov": 0.0,
                    "util": 0.0,
                    "caja": 0.0,
                    "ev_nc": 0.0,
                    "mov_nc": 0.0,
                }
            a = month_agg[fecha]
            a["ev"] += curr_val
            a["caja"] += caja
            a["mov"] += (movimientos or 0)
            a["util"] += (utilidad or 0)
            a["ev_nc"] += ev_sin_caja
            if movimientos_sin_caja is not None:
                a["mov_nc"] += movimientos_sin_caja

    detail_rows.sort(key=lambda r: (r["fecha"], r["sociedad"], r["banco"]))

    # ── Construir filas consolidadas (13 meses) ──────────────────
    if req_years:
        expected_fechas = []
        for target_year in sorted(req_years):
            expected_fechas.append(f"{target_year - 1}-12")
            expected_fechas.extend(f"{target_year}-{m:02d}" for m in range(1, 13))
        # Deduplicar preservando orden.
        expected_fechas = list(dict.fromkeys(expected_fechas))
    else:
        expected_fechas = sorted(month_agg.keys())
    consolidated_rows: list[dict] = []
    chart_data: list[dict] = []

    for fecha in expected_fechas:
        has_data = fecha in month_agg
        a = month_agg.get(
            fecha,
            {
                "ev": 0.0,
                "mov": 0.0,
                "util": 0.0,
                "caja": 0.0,
                "ev_nc": 0.0,
                "mov_nc": 0.0,
            },
        )
        yr = int(fecha[:4])
        is_prev = fecha == prev_dec

        # Solo incluir dic anterior y meses del año solicitado
        if req_years and yr not in req_years and not is_prev:
            continue

        # Rentabilidad: utilidad / prev_ending_value
        rent_pct = None
        rent_sin_caja_pct = None
        if not is_prev and has_data:
            prev_a = month_agg.get(_previous_month_key(fecha))
            if (
                len(visible_bank_codes) == 1
                and "ubs" in visible_bank_codes
                and _should_zero_negative_ubs_return("ubs", a["ev"], prev_a["ev"] if prev_a else None)
            ):
                rent_pct = 0.0
            elif prev_a and prev_a["ev"] > 0:
                rent_pct = round((((a["ev"] - a["mov"]) / prev_a["ev"]) - 1) * 100, 4)
            if prev_a and prev_a["ev_nc"] > 0:
                rent_sin_caja_pct = round((((a["ev_nc"] - a["mov_nc"]) / prev_a["ev_nc"]) - 1) * 100, 4)

        row = {
            "fecha": fecha,
            "ending_value": round(a["ev"], 2) if has_data else None,
            "caja": round(a["caja"], 2) if has_data else None,
            "movimientos": round(a["mov"], 2) if has_data else None,
            "utilidad": round(a["util"], 2) if has_data else None,
            "rent_mensual_pct": rent_pct,
            "rent_mensual_sin_caja_pct": rent_sin_caja_pct,
            "is_prev_year": is_prev,
        }
        consolidated_rows.append(row)

        if not is_prev:
            chart_data.append({
                "fecha": fecha,
                "ending_value": row["ending_value"],
                "caja": row["caja"],
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
    Retorna datos para la pestana Mandatos.
    Filtra cuentas con account_type='mandato'.
    """
    selected_years = set(filters.years) if filters.years else None
    requested_fecha: Optional[str] = None

    if filters.fecha:
        m = re.fullmatch(r"(\d{4})-(\d{2})", filters.fecha.strip())
        if m:
            year = int(m.group(1))
            month = int(m.group(2))
            if 1 <= month <= 12:
                selected_years = {year}
                requested_fecha = f"{year}-{month:02d}"

    query = _query_closing_rows(
        db=db,
        filters=filters,
        years=selected_years,
        account_type="mandato",
    )
    etf_filters = _copy_filters(filters, sin_personal=True)

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
            "available_fechas": [],
            "selected_fecha": None,
            "message": "Sin datos de mandatos",
        }

    banks_by_month: list[dict] = []
    month_totals: dict[str, float] = {}
    month_by_mandate: dict[str, dict[str, float]] = {}
    month_asset_alloc: dict[str, dict[str, float]] = {}
    bank_asset_alloc_by_month: dict[str, dict[str, dict[str, float]]] = {}
    returns_by_bank: dict[str, dict[str, float]] = {}
    values_by_bank: dict[str, dict[str, float]] = {}
    cash_by_bank: dict[str, dict[str, float]] = {}
    income_by_bank: dict[str, dict[str, float]] = {}
    movements_by_bank: dict[str, dict[str, float]] = {}
    months_seen: set[str] = set()
    mandate_cash_cache_by_account: dict[int, dict[tuple[int, int], float]] = {}

    for mc, acct, norm in results:
        key = f"{mc.year}-{mc.month:02d}"
        months_seen.add(key)
        net_value = _resolve_ending_with_accrual(mc, norm) or 0.0
        income = _to_float(norm.profit_period) if norm else None
        if income is None:
            income = _to_float(mc.income) or 0.0
        movements = _to_float(norm.movements_net) if norm else None
        if movements is None:
            movements = _to_float(mc.change_in_value) or 0.0
        account_cache = mandate_cash_cache_by_account.setdefault(acct.id, {})
        cash_value = _resolve_cash_value(
            db=db,
            acct=acct,
            mc=mc,
            norm=norm,
            etf_cash_cache=account_cache,
        )
        mandate = (acct.mandate_type or "unknown").lower()

        banks_by_month.append({
            "bank_code": acct.bank_code,
            "entity_name": acct.entity_name,
            "mandate_type": mandate,
            "year": mc.year,
            "month": mc.month,
            "net_value": net_value,
            "cash_value": cash_value,
            "income": income,
            "movements": movements,
        })

        month_totals[key] = month_totals.get(key, 0.0) + net_value
        month_by_mandate.setdefault(key, {})
        month_by_mandate[key][mandate] = month_by_mandate[key].get(mandate, 0.0) + net_value

        values_by_bank.setdefault(acct.bank_code, {})
        cash_by_bank.setdefault(acct.bank_code, {})
        income_by_bank.setdefault(acct.bank_code, {})
        movements_by_bank.setdefault(acct.bank_code, {})
        values_by_bank[acct.bank_code][key] = values_by_bank[acct.bank_code].get(key, 0.0) + net_value
        cash_by_bank[acct.bank_code][key] = cash_by_bank[acct.bank_code].get(key, 0.0) + cash_value
        income_by_bank[acct.bank_code][key] = income_by_bank[acct.bank_code].get(key, 0.0) + income
        movements_by_bank[acct.bank_code][key] = (
            movements_by_bank[acct.bank_code].get(key, 0.0) + movements
        )

        alloc_json = _resolve_asset_allocation_json(mc, norm)
        if alloc_json:
            try:
                alloc = json.loads(alloc_json)
            except (TypeError, ValueError):
                alloc = {}
            if isinstance(alloc, dict):
                month_asset_alloc.setdefault(key, {})
                bank_asset_alloc_by_month.setdefault(key, {})
                bank_asset_alloc_by_month[key].setdefault(acct.bank_code, {})
                for asset_name, payload in alloc.items():
                    if isinstance(payload, dict):
                        raw = payload.get("value") or payload.get("market_value") or payload.get("amount")
                    else:
                        raw = payload
                    try:
                        val = float(raw)
                    except (TypeError, ValueError):
                        continue
                    label = str(asset_name).strip() or "Other"
                    if getattr(filters, "sin_caja", False) and _is_cash_asset_label(label):
                        continue
                    month_asset_alloc[key][label] = month_asset_alloc[key].get(label, 0.0) + val
                    bank_asset_alloc_by_month[key][acct.bank_code][label] = (
                        bank_asset_alloc_by_month[key][acct.bank_code].get(label, 0.0) + val
                    )

    mandate_pcts: list[dict] = []
    for key in sorted(months_seen):
        total = month_totals.get(key, 0.0)
        by_mandate = month_by_mandate.get(key, {})
        mandate_pcts.append({
            "fecha": key,
            "discretionary": round((by_mandate.get("discretionary", 0.0) / total * 100), 4) if total > 0 else 0.0,
            "advisory": round((by_mandate.get("advisory", 0.0) / total * 100), 4) if total > 0 else 0.0,
            "execution_only": round((by_mandate.get("execution_only", 0.0) / total * 100), 4) if total > 0 else 0.0,
            "other": round((by_mandate.get("unknown", 0.0) / total * 100), 4) if total > 0 else 0.0,
        })

    asset_allocation: list[dict] = []
    for key in sorted(months_seen):
        row = {"fecha": key}
        row.update({k: round(v, 2) for k, v in month_asset_alloc.get(key, {}).items()})
        asset_allocation.append(row)

    selected_fecha = requested_fecha if requested_fecha in months_seen else (max(months_seen) if months_seen else None)
    aa_by_bank: dict[str, dict[str, float]] = {}
    selected_month_alloc = bank_asset_alloc_by_month.get(selected_fecha or "", {})
    for bank, vals in selected_month_alloc.items():
        total = sum(vals.values())
        aa_by_bank[bank] = {
            k: round((v / total * 100), 4) if total > 0 else 0.0
            for k, v in vals.items()
        }
    etf_aa = _build_etf_asset_allocation_pct_for_month(
        db=db,
        filters=etf_filters,
        fecha=selected_fecha,
    )
    if etf_aa:
        aa_by_bank["etf_portfolio"] = etf_aa

    for bank, months in values_by_bank.items():
        sorted_keys = sorted(months.keys())
        prev_val = None
        prev_cash = None
        prev_year = None
        monthly_returns: list[Decimal] = []
        for key in sorted_keys:
            curr_val = float(months[key])
            curr_cash = float(cash_by_bank.get(bank, {}).get(key, 0.0) or 0.0)
            curr_year = int(str(key).split("-")[0])
            if prev_year is None or curr_year != prev_year:
                monthly_returns = []
            prev_year = curr_year
            mov_val = movements_by_bank.get(bank, {}).get(key)
            inc_val = income_by_bank.get(bank, {}).get(key)
            ret: Optional[Decimal] = None
            if getattr(filters, "sin_caja", False):
                prev_effective = None if prev_val is None else (prev_val - float(prev_cash or 0.0))
                curr_effective = curr_val - curr_cash
                mov_effective = None
                if mov_val is not None:
                    mov_effective = float(mov_val) - (curr_cash - float(prev_cash or 0.0))
                if _should_zero_negative_ubs_return(bank, curr_effective, prev_effective):
                    ret = Decimal("0")
                elif prev_effective not in (None, 0.0) and prev_effective > 0 and mov_effective is not None:
                    ret = (
                        (Decimal(str(curr_effective - mov_effective)) / Decimal(str(prev_effective))) - Decimal("1")
                    ) * Decimal("100")
            else:
                if _should_zero_negative_ubs_return(bank, curr_val, prev_val):
                    ret = Decimal("0")
                elif prev_val not in (None, 0.0):
                    if mov_val is not None:
                        ret = (
                            (Decimal(str(curr_val - float(mov_val))) / Decimal(str(prev_val))) - Decimal("1")
                        ) * Decimal("100")
                    elif inc_val is not None:
                        ret = monthly_return_pct(Decimal(str(inc_val)), Decimal(str(prev_val)))
            ret_float = round(float(ret), 4) if ret is not None else None
            ytd_float = None
            if ret is not None:
                monthly_returns.append(ret)
                ytd_float = round(float(ytd_return_pct(monthly_returns)), 4)
            returns_by_bank.setdefault(bank, {})
            returns_by_bank[bank][f"{key}_monthly"] = ret_float
            returns_by_bank[bank][f"{key}_ytd"] = ytd_float
            prev_val = curr_val
            prev_cash = curr_cash

    returns_table = []
    for bank in sorted(returns_by_bank.keys()):
        row = {"bank_code": bank}
        row.update(returns_by_bank[bank])
        returns_table.append(row)

    # ETF total (para comparativo en Mandatos): serie mensual y YTD consolidada.
    etf_query = _query_closing_rows(
        db=db,
        filters=etf_filters,
        years=selected_years,
        account_type="etf",
    )
    etf_results = etf_query.order_by(MonthlyClosing.year, MonthlyClosing.month).all()

    etf_totals_by_month: dict[str, dict[str, float]] = {}
    etf_cash_cache_by_account: dict[int, dict[tuple[int, int], float]] = {}
    for mc, acct, norm in etf_results:
        key = f"{mc.year}-{mc.month:02d}"
        end_val = _resolve_ending_with_accrual(mc, norm) or 0.0
        mov_val = _to_float(norm.movements_net) if norm else None
        if mov_val is None:
            mov_val = _to_float(mc.change_in_value) or 0.0
        account_cache = etf_cash_cache_by_account.setdefault(acct.id, {})
        cash_val = _resolve_cash_value(
            db=db,
            acct=acct,
            mc=mc,
            norm=norm,
            etf_cash_cache=account_cache,
        )
        if key not in etf_totals_by_month:
            etf_totals_by_month[key] = {"net_value": 0.0, "movements": 0.0, "cash_value": 0.0}
        etf_totals_by_month[key]["net_value"] += end_val
        etf_totals_by_month[key]["movements"] += mov_val
        etf_totals_by_month[key]["cash_value"] += cash_val

    etf_total_returns: dict[str, Optional[float]] = {}
    prev_total: Optional[float] = None
    prev_cash_total: Optional[float] = None
    prev_year: Optional[int] = None
    etf_monthly_returns: list[Decimal] = []
    for key in sorted(etf_totals_by_month.keys()):
        curr_year = int(key.split("-")[0])
        if prev_year is None or curr_year != prev_year:
            etf_monthly_returns = []
        prev_year = curr_year

        curr_val = etf_totals_by_month[key]["net_value"]
        mov_val = etf_totals_by_month[key]["movements"]
        curr_cash = etf_totals_by_month[key].get("cash_value", 0.0)
        ret: Optional[Decimal] = None
        if getattr(filters, "sin_caja", False):
            prev_effective = None if prev_total is None else (prev_total - float(prev_cash_total or 0.0))
            curr_effective = curr_val - curr_cash
            mov_effective = mov_val - (curr_cash - float(prev_cash_total or 0.0))
            if prev_effective not in (None, 0.0) and prev_effective > 0:
                ret = (
                    (Decimal(str(curr_effective - mov_effective)) / Decimal(str(prev_effective))) - Decimal("1")
                ) * Decimal("100")
        elif prev_total not in (None, 0.0):
            ret = (
                (Decimal(str(curr_val - mov_val)) / Decimal(str(prev_total))) - Decimal("1")
            ) * Decimal("100")
        ret_float = round(float(ret), 4) if ret is not None else None
        etf_total_returns[f"{key}_monthly"] = ret_float

        ytd_float = None
        if ret is not None:
            etf_monthly_returns.append(ret)
            ytd_float = round(float(ytd_return_pct(etf_monthly_returns)), 4)
        etf_total_returns[f"{key}_ytd"] = ytd_float
        prev_total = curr_val
        prev_cash_total = curr_cash

    return {
        "mandate_pcts": mandate_pcts,
        "asset_allocation": asset_allocation,
        "aa_by_bank": aa_by_bank,
        "banks_by_month": banks_by_month,
        "returns_table": returns_table,
        "available_fechas": sorted(months_seen),
        "selected_fecha": selected_fecha,
        "etf_totals_by_month": etf_totals_by_month,
        "etf_total_returns": etf_total_returns,
    }


# ETF - Helpers
SOCIETY_MAPPING = [
    ("Boatview JPM", lambda en, bc: "boatview" in en.lower() and bc == "jpmorgan"),
    ("Boatview GS", lambda en, bc: "boatview" in en.lower() and bc == "goldman_sachs"),
    ("Telmar", lambda en, bc: "telmar" in en.lower()),
    ("Armel Holdings", lambda en, bc: "armel" in en.lower()),
    ("Ecoterra Internacional", lambda en, bc: "ecoterra" in en.lower()),
    ("Raíces LP", lambda en, bc: "raíces" in en.lower() or "raices" in en.lower()),
]

SOCIETY_COLS = ["Boatview JPM", "Boatview GS", "Telmar",
                "Armel Holdings", "Ecoterra Internacional", "Raíces LP"]

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
    "VANG USDCPBD USDA ACC": "VDPA",
    "VANGUARD USD CORPORATE BOND UCITS ETF": "VDPA",
    # ── VUCP (Goldman Sachs) ──
    "VUCP": "VDPA",
    "USD CORPORATE BOND UCITS ETF": "VDPA",
    "USD CORPORATE BOND UCITS ETF (VUCP)": "VDPA",
    # ── VDCA (including GS name variant) ──
    "VANGUARD USD CORPORATE 1-3 YEAR BOND UCITS ETF": "VDCA",
    "VANGUARD FUNDS PLC-VANGUARD US CMN CLASS ETF": "VDCA",
    "VANGUARD FUNDS PLC - VANGUARD CMN CLASS ETF STAMP": "VDCA",
    # ── SPDR ──
    "SPDR": "SPDR",
    "SPDR BLOOMBERG 1-10 YEAR U.S.": "SPDR",
    # ── JPM money market variants ──
    "JPM LI-LIQ LVNAV FD - USD - W -": "Money Market",
    "P JPM LI-LIQ LVNAV FD - USD - W -": "Money Market",
    "PROCEEDS FROM PENDING SALES": "Money Market",
    # ── Goldman Sachs ETF name variants ──
    "MSCI WORLD INDEX FUND (ISHARES)": "IWDA",
    "MSCI EMERGING MARKETS INDEX FUND (ISHARES)": "IEMA",
    "ISHARES III PLC-ISHARES MSCI EMERGING MARKETS ETF": "IEMA",
    "MARKIT IBOXX USD LIQUID HY CAPPED INDEX FUND (ISHARES)": "IHYA",
    "ISHARES II PLC-ISHARES $ HIGH YIELD CORP BOND UCITS ETF": "IHYA",
}

# Orden fijo de instrumentos
INSTRUMENT_ORDER = ["IWDA", "IEMA", "VDCA", "VDPA", "IHYA", "Money Market"]

# Instrumentos considerados caja/money market
CASH_INSTRUMENTS = {"Money Market"}
ETF_FIXED_INCOME_INSTRUMENTS = {"VDCA", "VDPA", "IHYA", "SPDR"}


def _normalize_instrument(name: str) -> str:
    """Normaliza nombre de instrumento según diccionario."""
    if not name:
        return "Other"
    upper = name.strip().upper()
    upper_compact = re.sub(r"[^A-Z0-9]", "", upper)
    if upper in INSTRUMENT_NAME_MAP:
        return INSTRUMENT_NAME_MAP[upper]
    # JPM: permitir match por ticker o por nombre con variaciones de espacios/símbolos.
    if "VDPA" in upper_compact:
        return "VDPA"
    if "USDCPBD" in upper_compact and "USDA" in upper_compact:
        return "VDPA"
    # Detección heurística de money market / caja
    low = name.lower()
    if any(kw in low for kw in ("sweep", "liquidity", "money market", "cash", "depósito", "deposito", "deposit", "deposits", "li-liq")):
        return "Money Market"
    return INSTRUMENT_NAME_MAP.get(upper, name)


def _copy_filters(filters: FilterParams, **overrides) -> FilterParams:
    payload = filters.model_dump()
    payload.update(overrides)
    return FilterParams(**payload)


def _etf_asset_bucket_from_instrument(name: str) -> str:
    instr = _normalize_instrument(name)
    low = str(name or "").lower()
    if instr in CASH_INSTRUMENTS:
        return "Cash, Deposits & Money Market"
    if instr in ETF_FIXED_INCOME_INSTRUMENTS or any(
        token in low for token in ("bond", "yield", "treasury", "corporate")
    ):
        return "Fixed Income"
    return "Equities"


def _build_etf_asset_allocation_pct_for_month(
    db: Session,
    filters: FilterParams,
    fecha: str | None,
) -> dict[str, float]:
    if not fecha or not re.fullmatch(r"\d{4}-\d{2}", fecha):
        return {}

    year = int(fecha[:4])
    month = int(fecha[5:7])
    query = (
        db.query(EtfComposition.etf_name, EtfComposition.market_value, Account)
        .join(Account, EtfComposition.account_id == Account.id)
        .filter(
            EtfComposition.year == year,
            EtfComposition.month == month,
        )
    )
    if filters.bank_codes:
        query = query.filter(Account.bank_code.in_(filters.bank_codes))
    if filters.entity_names:
        query = query.filter(Account.entity_name.in_(filters.entity_names))
    if filters.person_names:
        query = query.filter(Account.person_name.in_(filters.person_names))
    if getattr(filters, "sin_personal", False):
        query = query.filter(~Account.entity_name.in_(PERSONAL_ENTITY_NAMES))

    totals = {
        "Cash, Deposits & Money Market": 0.0,
        "Fixed Income": 0.0,
        "Equities": 0.0,
    }
    for etf_name, market_value, _ in query.all():
        bucket = _etf_asset_bucket_from_instrument(etf_name or "")
        if getattr(filters, "sin_caja", False) and bucket == "Cash, Deposits & Money Market":
            continue
        totals[bucket] += _to_float(market_value) or 0.0

    grand_total = sum(totals.values())
    if grand_total <= 0:
        return {}

    return {
        key: round((value / grand_total) * 100, 4)
        for key, value in totals.items()
    }


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
    norm_dates = (
        db.query(MonthlyMetricNormalized.year, MonthlyMetricNormalized.month)
        .join(Account, MonthlyMetricNormalized.account_id == Account.id)
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
    for y, m in mc_dates + norm_dates + comp_dates:
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

    # ── helpers: filtros ───────────────────────────────────────────
    sin_caja = getattr(filters, "sin_caja", False)
    sin_personal = getattr(filters, "sin_personal", False)
    active_society_cols = [s for s in SOCIETY_COLS if not (sin_personal and s in PERSONAL_ENTITY_NAMES)]

    # ── 1-2) Instrumentos × Sociedades (solo filtro fecha) ──────
    comp_query = (
        db.query(EtfComposition, Account)
        .join(Account, EtfComposition.account_id == Account.id)
    )
    if sin_personal:
        comp_query = comp_query.filter(~Account.entity_name.in_(PERSONAL_ENTITY_NAMES))
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
        if sin_caja and instr in CASH_INSTRUMENTS:
            continue
        society = _get_society_label(acct.entity_name, comp.bank_code)
        mv = float(comp.market_value or 0)
        if instr not in instr_society:
            instr_society[instr] = {s: 0.0 for s in active_society_cols}
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

    # Control esperado: suma visible de composición ETF según filtros activos.
    control_expected = {s: 0.0 for s in active_society_cols}
    control_expected["Total"] = 0.0
    for row in instruments_table.values():
        for society in active_society_cols:
            control_expected[society] += float(row.get(society, 0.0) or 0.0)
        control_expected["Total"] += float(row.get("Total", 0.0) or 0.0)

    # Pesos % table
    if sin_caja:
        pct_source_items = [
            (instr, vals)
            for instr, vals in instruments_table.items()
            if instr not in CASH_INSTRUMENTS
        ]
    else:
        pct_source_items = list(instruments_table.items())

    grand_total = sum(vals.get("Total", 0.0) for _, vals in pct_source_items)
    col_totals = {
        col: sum(vals.get(col, 0.0) for _, vals in pct_source_items)
        for col in active_society_cols
    }
    instruments_pct_table: dict[str, dict[str, float]] = {}
    for instr, vals in instruments_table.items():
        pct_row = {}
        for col in active_society_cols:
            v = vals.get(col, 0.0)
            denom = col_totals.get(col, 0.0)
            pct_row[col] = round((v / denom * 100), 4) if denom > 0 else 0.0
        total_v = vals.get("Total", 0.0)
        pct_row["Total"] = round((total_v / grand_total * 100), 4) if grand_total > 0 else 0.0
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
    if sin_personal:
        montos_query = montos_query.filter(~Account.entity_name.in_(PERSONAL_ENTITY_NAMES))
    if sel_year:
        montos_query = montos_query.filter(EtfComposition.year == sel_year)
    if filters.bank_codes:
        montos_query = montos_query.filter(Account.bank_code.in_(filters.bank_codes))
    if filters.entity_names:
        montos_query = montos_query.filter(Account.entity_name.in_(filters.entity_names))

    montos_results = montos_query.order_by(EtfComposition.month).all()

    # Pivotear: society → {mes: monto}
    society_month_montos: dict[str, dict[int, float]] = {}
    society_month_cash: dict[str, dict[int, float]] = {}
    for comp, acct in montos_results:
        instr = _normalize_instrument(comp.etf_name)
        society = _get_society_label(acct.entity_name, comp.bank_code)
        mv = float(comp.market_value or 0)
        if instr in CASH_INSTRUMENTS:
            society_month_cash.setdefault(society, {})
            society_month_cash[society][comp.month] = society_month_cash[society].get(comp.month, 0.0) + mv
            if sin_caja:
                continue
        if society not in society_month_montos:
            society_month_montos[society] = {}
        society_month_montos[society][comp.month] = (
            society_month_montos[society].get(comp.month, 0) + mv
        )

    # Construir tabla con orden fijo de sociedades
    society_montos_table = []
    totals_by_month: dict[int, float] = {}
    for soc in active_society_cols:
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

    # ── 5) Society movimientos × meses del año (todos los filtros) ──
    society_movements_table = []
    society_movements_numeric: dict[str, dict[int, float]] = {}
    totals_mov_by_month: dict[int, float] = {}
    if sel_year:
        mov_query = _query_closing_rows(
            db=db,
            filters=filters,
            years={sel_year},
            account_type="etf",
        )
        if sin_personal:
            mov_query = mov_query.filter(~Account.entity_name.in_(PERSONAL_ENTITY_NAMES))
        mov_results = mov_query.order_by(MonthlyClosing.month).all()
        society_month_movs: dict[str, dict[int, float]] = {}

        for mc, acct, norm in mov_results:
            society = _get_society_label(acct.entity_name, acct.bank_code)
            mov = _to_float(norm.movements_net) if norm else None
            if mov is None:
                mov = _to_float(mc.change_in_value) or 0.0
            if sin_caja:
                curr_cash = society_month_cash.get(society, {}).get(mc.month, 0.0)
                prev_cash = society_month_cash.get(society, {}).get(mc.month - 1, 0.0) if mc.month > 1 else 0.0
                mov -= (curr_cash - prev_cash)
            if society not in society_month_movs:
                society_month_movs[society] = {}
            society_month_movs[society][mc.month] = (
                society_month_movs[society].get(mc.month, 0.0) + mov
            )
            totals_mov_by_month[mc.month] = totals_mov_by_month.get(mc.month, 0.0) + mov
        society_movements_numeric = society_month_movs

        for soc in active_society_cols:
            row = {"sociedad": soc}
            for m in range(1, 13):
                row[f"{m:02d}"] = round(society_month_movs.get(soc, {}).get(m, 0.0), 2)
            society_movements_table.append(row)

        total_mov_row = {"sociedad": "Total"}
        for m in range(1, 13):
            total_mov_row[f"{m:02d}"] = round(totals_mov_by_month.get(m, 0.0), 2)
        society_movements_table.append(total_mov_row)

    # ── 6) Society returns × meses (rent % mensual y YTD) ──────
    years_for_returns = {sel_year - 1, sel_year} if sel_year else None
    mc_year_query = _query_closing_rows(
        db=db,
        filters=filters,
        years=years_for_returns,
        account_type="etf",
    )
    if sin_personal:
        mc_year_query = mc_year_query.filter(~Account.entity_name.in_(PERSONAL_ENTITY_NAMES))
    mc_year_results = mc_year_query.order_by(
        MonthlyClosing.year, MonthlyClosing.month
    ).all()

    # Agrupar por sociedad × (year, month)
    # Track both net_value and utilidad (income) for correct return calculation
    soc_month_val: dict[str, dict[tuple, float]] = {}
    soc_month_util: dict[str, dict[tuple, float]] = {}
    for mc, acct, norm in mc_year_results:
        society = _get_society_label(acct.entity_name, acct.bank_code)
        key = (mc.year, mc.month)
        if society not in soc_month_val:
            soc_month_val[society] = {}
            soc_month_util[society] = {}
        if sin_caja and mc.year == sel_year:
            ending_with = society_month_montos.get(society, {}).get(mc.month, 0.0)
        else:
            ending_with = _resolve_ending_with_accrual(mc, norm) or 0.0
        soc_month_val[society][key] = soc_month_val[society].get(key, 0) + ending_with
        utilidad = _to_float(norm.profit_period) if norm else None
        if utilidad is None:
            utilidad = _to_float(mc.income)
        if utilidad is not None:
            soc_month_util[society][key] = soc_month_util[society].get(key, 0) + utilidad

    # Calcular monthly return % y YTD
    society_returns_monthly: list[dict] = []
    society_returns_ytd: list[dict] = []

    for soc in active_society_cols:
        monthly_row = {"sociedad": soc}
        ytd_row = {"sociedad": soc}
        vals = soc_month_val.get(soc, {})
        utils = soc_month_util.get(soc, {})

        # Base para YTD = dic del año anterior
        cumulative_return = 0.0

        for m in range(1, 13):
            curr = vals.get((sel_year, m)) if sel_year else None
            prev_key = (sel_year, m - 1) if m > 1 else (sel_year - 1, 12) if sel_year else None
            prev = vals.get(prev_key) if prev_key else None

            # Monthly return = utilidad / prev_ending_value (same as Summary)
            ret = None
            util = utils.get((sel_year, m)) if sel_year else None
            mov = society_movements_numeric.get(soc, {}).get(m) if sin_caja else None
            if sin_caja and curr is not None and prev is not None and prev > 0 and mov is not None:
                ret = round((((curr - mov) / prev) - 1) * 100, 4)
            elif util is not None and prev is not None and prev > 0:
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

    total_monthly_row: dict = {"sociedad": "Total"}
    total_ytd_row: dict = {"sociedad": "Total"}
    if sel_year:
        summary_payload = get_summary(
            _copy_filters(
                filters,
                years=[sel_year],
                account_types=["etf"],
                sin_personal=sin_personal,
            ),
            db,
        )
        summary_by_fecha = {
            str(row.get("fecha")): row
            for row in summary_payload.get("consolidated_rows", [])
            if row.get("fecha") and not row.get("is_prev_year")
        }
        monthly_key = "rent_mensual_sin_caja_pct" if sin_caja else "rent_mensual_pct"
        cumulative_total = 1.0
        has_total = False
        for m in range(1, 13):
            fecha_key = f"{sel_year}-{m:02d}"
            ret = _to_float(summary_by_fecha.get(fecha_key, {}).get(monthly_key))
            total_monthly_row[f"{m:02d}"] = ret
            if ret is not None:
                cumulative_total *= 1 + (ret / 100)
                has_total = True
                total_ytd_row[f"{m:02d}"] = round((cumulative_total - 1) * 100, 4)
            else:
                total_ytd_row[f"{m:02d}"] = round((cumulative_total - 1) * 100, 4) if has_total else None
    else:
        for m in range(1, 13):
            total_monthly_row[f"{m:02d}"] = None
            total_ytd_row[f"{m:02d}"] = None

    society_returns_monthly.append(total_monthly_row)
    society_returns_ytd.append(total_ytd_row)

    return {
        "instruments_table": instruments_table,
        "control_expected": control_expected,
        "instruments_pct_table": instruments_pct_table,
        "composition_by_society": composition_by_society,
        "composition_by_instrument": composition_by_instrument,
        "society_montos_table": society_montos_table,
        "society_movements_table": society_movements_table,
        "society_returns_monthly": society_returns_monthly,
        "society_returns_ytd": society_returns_ytd,
        "selected_year": sel_year,
        "selected_month": sel_month,
        "society_cols": active_society_cols,
    }


@router.post("/personal")
def get_personal(
    filters: FilterParams,
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
    query = _query_closing_rows(
        db=db,
        filters=filters,
        years=set(filters.years) if filters.years else None,
        months=filters.months if filters.months else None,
    )

    rows = query.order_by(MonthlyClosing.year, MonthlyClosing.month, Account.bank_code).all()
    if not rows:
        return {
            "consolidated_usd": 0.0,
            "consolidated_clp": 0.0,
            "cash": 0.0,
            "pie_charts": {"by_bank": [], "by_type": []},
            "by_bank_detail": [],
            "entities_table": [],
            "summary_table": [],
            "range_table": [],
            "message": "Sin datos para filtros seleccionados",
        }

    by_month: dict[str, float] = {}
    by_bank: dict[str, float] = {}
    by_type: dict[str, float] = {}
    by_bank_detail: dict[str, dict[str, float]] = {}
    entities_table: list[dict] = []
    consolidated_usd = 0.0
    consolidated_clp = 0.0
    cash_total = 0.0
    etf_cash_cache_by_account: dict[int, dict[tuple[int, int], float]] = {}

    last_key = max(f"{mc.year}-{mc.month:02d}" for mc, _, _ in rows)
    for mc, acct, norm in rows:
        key = f"{mc.year}-{mc.month:02d}"
        net = _resolve_ending_with_accrual(mc, norm) or 0.0
        movements = _to_float(norm.movements_net) if norm else None
        if movements is None:
            movements = _to_float(mc.change_in_value) or 0.0
        account_cache = etf_cash_cache_by_account.setdefault(acct.id, {})
        cash_value = _resolve_cash_value(
            db=db,
            acct=acct,
            mc=mc,
            norm=norm,
            etf_cash_cache=account_cache,
        )
        by_month[key] = by_month.get(key, 0.0) + net
        by_bank[acct.bank_code] = by_bank.get(acct.bank_code, 0.0) + net
        by_type[acct.account_type] = by_type.get(acct.account_type, 0.0) + net
        cash_total += cash_value
        currency = (norm.currency if norm and norm.currency else mc.currency) or acct.currency
        if key == last_key:
            if acct.bank_code not in by_bank_detail:
                by_bank_detail[acct.bank_code] = {
                    "monto_usd": 0.0,
                    "movimientos_mes": 0.0,
                    "caja_disponible": 0.0,
                }
            if (currency or "").upper() == "USD":
                by_bank_detail[acct.bank_code]["monto_usd"] += net
            by_bank_detail[acct.bank_code]["movimientos_mes"] += movements
            by_bank_detail[acct.bank_code]["caja_disponible"] += cash_value
            entities_table.append({
                "sociedad": acct.entity_name,
                "banco": acct.bank_code,
                "id": acct.identification_number or acct.account_number,
                "account_number": acct.account_number,
                "nombre": acct.person_name,
                "tipo_cuenta": acct.account_type,
                "moneda": currency,
                "net_value": net,
                "movimientos": movements,
                "caja": cash_value,
            })
            if (currency or "").upper() == "USD":
                consolidated_usd += net
            elif (currency or "").upper() == "CLP":
                consolidated_clp += net

    summary_table = [
        {"fecha": k, "ending_value": round(v, 2)}
        for k, v in sorted(by_month.items())
    ]

    return {
        "consolidated_usd": round(consolidated_usd, 2),
        "consolidated_clp": round(consolidated_clp, 2),
        "cash": round(cash_total, 2),
        "pie_charts": {
            "by_bank": [{"label": k, "value": round(v, 2)} for k, v in sorted(by_bank.items(), key=lambda x: -x[1])],
            "by_type": [{"label": k, "value": round(v, 2)} for k, v in sorted(by_type.items(), key=lambda x: -x[1])],
        },
        "by_bank_detail": [
            {
                "bank_code": bank,
                "monto_usd": round(vals["monto_usd"], 2),
                "movimientos_mes": round(vals["movimientos_mes"], 2),
                "caja_disponible": round(vals["caja_disponible"], 2),
            }
            for bank, vals in sorted(by_bank_detail.items(), key=lambda x: x[0])
        ],
        "entities_table": sorted(
            entities_table, key=lambda x: (x["sociedad"], x["banco"], x["tipo_cuenta"])
        ),
        "summary_table": summary_table,
        "range_table": summary_table,
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
    query = (
        db.query(MonthlyClosing, Account)
        .join(Account, MonthlyClosing.account_id == Account.id)
    )
    query = _apply_account_filters(query, filters)
    if filters.years:
        query = query.filter(MonthlyClosing.year.in_(filters.years))
    if filters.months:
        query = query.filter(MonthlyClosing.month.in_(filters.months))

    closings = query.order_by(MonthlyClosing.year, MonthlyClosing.month).all()
    out = []
    unresolved = 0

    for mc, acct in closings:
        # último snapshot diario del mes
        latest = (
            db.query(DailyPosition.position_date)
            .filter(
                DailyPosition.account_id == acct.id,
                DailyPosition.position_date >= date(mc.year, mc.month, 1),
                DailyPosition.position_date <= mc.closing_date,
            )
            .order_by(DailyPosition.position_date.desc())
            .first()
        )
        daily_total = None
        if latest and latest[0]:
            daily_total = (
                db.query(DailyPosition.market_value)
                .filter(
                    DailyPosition.account_id == acct.id,
                    DailyPosition.position_date == latest[0],
                )
                .all()
            )
            daily_total = sum(float(v[0] or 0) for v in daily_total)

        monthly_total = float(mc.net_value or 0) if mc.net_value is not None else None
        rec = reconcile_monthly(
            daily_total=Decimal(str(daily_total)) if daily_total is not None else None,
            monthly_total=Decimal(str(monthly_total)) if monthly_total is not None else None,
            account_id=acct.id,
            year=mc.year,
            month=mc.month,
            currency=mc.currency,
        )

        existing = (
            db.query(Reconciliation)
            .filter(
                Reconciliation.account_id == acct.id,
                Reconciliation.year == mc.year,
                Reconciliation.month == mc.month,
            )
            .first()
        )
        payload = {
            "monthly_closing_id": mc.id,
            "reconciliation_date": mc.closing_date,
            "daily_total": rec.daily_total,
            "monthly_total": rec.monthly_total,
            "difference": rec.difference,
            "difference_pct": rec.difference_pct,
            "status": rec.status.value,
            "threshold_used": Decimal("0.01"),
            "currency": rec.currency,
            "details_json": json.dumps({"messages": rec.messages}),
        }
        if existing:
            for k, v in payload.items():
                setattr(existing, k, v)
        else:
            db.add(Reconciliation(account_id=acct.id, year=mc.year, month=mc.month, **payload))

        if rec.status.value not in ("matched", "minor_diff"):
            unresolved += 1
        out.append({
            "account_id": acct.id,
            "account_number": acct.account_number,
            "bank_code": acct.bank_code,
            "year": mc.year,
            "month": mc.month,
            "daily_total": float(rec.daily_total) if rec.daily_total is not None else None,
            "monthly_total": float(rec.monthly_total) if rec.monthly_total is not None else None,
            "difference": float(rec.difference) if rec.difference is not None else None,
            "difference_pct": float(rec.difference_pct) if rec.difference_pct is not None else None,
            "status": rec.status.value,
            "messages": rec.messages,
        })

    db.commit()
    return {
        "reconciliation_results": out,
        "unresolved_count": unresolved,
        "total_count": len(out),
    }


@router.post("/asset-allocation-report")
def get_asset_allocation_report(
    filters: FilterParams,
    db: Session = Depends(get_db),
):
    """
    Vista mínima de asignación de activos cargada desde PDF report.
    """
    query = _query_closing_rows(
        db=db,
        filters=filters,
        years=set(filters.years) if filters.years else None,
        months=filters.months if filters.months else None,
    )
    rows = query.order_by(MonthlyClosing.year, MonthlyClosing.month).all()
    timeline: list[dict] = []
    for mc, acct, norm in rows:
        alloc_json = _resolve_asset_allocation_json(mc, norm)
        if not alloc_json:
            continue
        try:
            alloc = json.loads(alloc_json)
        except (TypeError, ValueError):
            continue
        if not isinstance(alloc, dict):
            continue
        flat = {
            "fecha": f"{mc.year}-{mc.month:02d}",
            "bank_code": acct.bank_code,
            "entity_name": acct.entity_name,
            "account_number": acct.account_number,
        }
        for k, payload in alloc.items():
            if isinstance(payload, dict):
                val = payload.get("value") or payload.get("market_value") or payload.get("amount")
            else:
                val = payload
            try:
                flat[k] = float(val)
            except (TypeError, ValueError):
                continue
        timeline.append(flat)

    return {"rows": timeline, "total": len(timeline)}


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


@router.post("/health-report")
def get_health_report(
    filters: HealthAuditParams,
    db: Session = Depends(get_db),
):
    """
    Auditoría read-only de salud de datos.

    Revisa identidad mensual, faltantes y consistencia YTD sin modificar la BD.
    """
    return _build_health_report(db=db, filters=filters)


@router.get("/parser-quality")
def get_parser_quality_report(
    threshold_pct: float = 0.01,
    limit: int = 200,
    db: Session = Depends(get_db),
):
    """
    Reporte de calidad parser-vs-fuente (cartola).
    Compara closing_balance parseado vs net_value en monthly_closings.
    """
    rows = (
        db.query(ParsedStatement, MonthlyClosing, Account)
        .join(Account, ParsedStatement.account_id == Account.id)
        .join(
            MonthlyClosing,
            (MonthlyClosing.account_id == ParsedStatement.account_id)
            & (MonthlyClosing.year == extract("year", ParsedStatement.statement_date))
            & (MonthlyClosing.month == extract("month", ParsedStatement.statement_date)),
        )
        .order_by(ParsedStatement.statement_date.desc())
        .limit(limit)
        .all()
    )

    report = []
    critical_count = 0
    for ps, mc, acct in rows:
        parsed = float(ps.closing_balance or 0)
        loaded = float(mc.net_value or 0)
        diff = parsed - loaded
        diff_pct = abs(diff) / abs(loaded) * 100 if loaded else (0.0 if parsed == 0 else 100.0)
        status = "ok" if diff_pct <= threshold_pct else "critical"
        if status == "critical":
            critical_count += 1
        report.append(
            {
                "statement_date": ps.statement_date.isoformat(),
                "account_number": acct.account_number,
                "bank_code": acct.bank_code,
                "currency": ps.currency,
                "parsed_closing_balance": parsed,
                "loaded_monthly_net_value": loaded,
                "difference": diff,
                "difference_pct": round(diff_pct, 6),
                "status": status,
            }
        )

    return {
        "threshold_pct": threshold_pct,
        "total": len(report),
        "critical_count": critical_count,
        "rows": report,
    }


@router.get("/normalization-quality")
def get_normalization_quality(
    limit: int = 100,
    db: Session = Depends(get_db),
):
    """
    Diagnóstico de cobertura y consistencia de monthly_metrics_normalized.
    """
    total_closings = db.query(func.count(MonthlyClosing.id)).scalar() or 0
    total_normalized = db.query(func.count(MonthlyMetricNormalized.id)).scalar() or 0
    coverage_pct = round((total_normalized / total_closings) * 100, 4) if total_closings else 100.0

    missing_rows = (
        db.query(MonthlyClosing, Account)
        .join(Account, MonthlyClosing.account_id == Account.id)
        .outerjoin(
            MonthlyMetricNormalized,
            and_(
                MonthlyMetricNormalized.account_id == MonthlyClosing.account_id,
                MonthlyMetricNormalized.year == MonthlyClosing.year,
                MonthlyMetricNormalized.month == MonthlyClosing.month,
            ),
        )
        .filter(MonthlyMetricNormalized.id.is_(None))
        .order_by(MonthlyClosing.year.desc(), MonthlyClosing.month.desc(), Account.id)
        .limit(limit)
        .all()
    )

    mismatches = []
    compared_rows = (
        db.query(MonthlyClosing, MonthlyMetricNormalized, Account)
        .join(Account, MonthlyClosing.account_id == Account.id)
        .join(
            MonthlyMetricNormalized,
            and_(
                MonthlyMetricNormalized.account_id == MonthlyClosing.account_id,
                MonthlyMetricNormalized.year == MonthlyClosing.year,
                MonthlyMetricNormalized.month == MonthlyClosing.month,
            ),
        )
        .all()
    )
    for mc, norm, acct in compared_rows:
        diffs = {}
        net = _to_float(mc.net_value)
        end_w = _to_float(norm.ending_value_with_accrual)
        if net is not None and end_w is not None and abs(net - end_w) > 1:
            diffs["ending_with_accrual_diff"] = round(end_w - net, 4)

        mov = _to_float(mc.change_in_value)
        mov_norm = _to_float(norm.movements_net)
        if mov is not None and mov_norm is not None and abs(mov - mov_norm) > 1:
            diffs["movements_diff"] = round(mov_norm - mov, 4)

        util = _to_float(mc.income)
        util_norm = _to_float(norm.profit_period)
        if util is not None and util_norm is not None and abs(util - util_norm) > 1:
            diffs["profit_diff"] = round(util_norm - util, 4)

        if diffs:
            mismatches.append(
                {
                    "account_number": acct.account_number,
                    "bank_code": acct.bank_code,
                    "year": mc.year,
                    "month": mc.month,
                    "diffs": diffs,
                }
            )
            if len(mismatches) >= limit:
                break

    return {
        "totals": {
            "monthly_closings": int(total_closings),
            "normalized_rows": int(total_normalized),
            "coverage_pct": coverage_pct,
        },
        "missing_count": len(missing_rows),
        "mismatch_count": len(mismatches),
        "missing_examples": [
            {
                "account_number": acct.account_number,
                "bank_code": acct.bank_code,
                "entity_name": acct.entity_name,
                "year": mc.year,
                "month": mc.month,
            }
            for mc, acct in missing_rows
        ],
        "mismatch_examples": mismatches,
    }

