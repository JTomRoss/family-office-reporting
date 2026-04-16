"""
FO Reporting – Servicio de carga de datos (ParseResult → tablas de reporting).

Toma el resultado de un parser (ParseResult) y lo persiste en:
- parsed_statements   (registro intermedio del parsing)
- monthly_closings    (cierre mensual oficial – fuente de verdad)
- etf_compositions    (detalle de holdings para cuentas ETF)

Este servicio es la pieza que conecta el parsing con el reporting.
"""

import json
import logging
import calendar
import hashlib
import re
from datetime import date, timezone, datetime
from decimal import Decimal, InvalidOperation
from typing import Optional, Any

from sqlalchemy.orm import Session

from asset_taxonomy import (
    asset_bucket_order,
    classify_etf_asset_bucket,
    classify_etf_asset_bucket_with_match,
)
from etf_instrument_dictionary import normalize_etf_instrument
from backend.services.normalized_reporting_payload import (
    canonical_breakdown_from_payload,
    cash_from_asset_allocation_json,
    compose_asset_allocation_payload,
    decode_asset_allocation_json,
    extract_fi_metrics,
    to_decimal,
)
from mandate_taxonomy import (
    MANDATE_CATEGORY_CASH,
    MANDATE_CATEGORY_EQUITIES,
    MANDATE_CATEGORY_FIXED,
    MANDATE_CATEGORY_GLOBAL_EQUITIES,
    MANDATE_CATEGORY_HY_FIXED,
    MANDATE_CATEGORY_IG_FIXED,
    MANDATE_CATEGORY_NON_US_EQUITIES,
    MANDATE_CATEGORY_PRIVATE_EQUITY,
    MANDATE_CATEGORY_REAL_ESTATE,
    MANDATE_CATEGORY_US_EQUITIES,
    classify_mandate_asset_label,
)
from backend.db.models import (
    Account,
    BiceMonthlySnapshot,
    DailyMovement,
    DailyPosition,
    DailyPrice,
    EtfComposition,
    MonthlyMetricNormalized,
    MovementType,
    MonthlyClosing,
    ParsedStatement,
    RawDocument,
    ValidationLog,
)
from parsers.base import ParseResult

logger = logging.getLogger(__name__)
_ASSET_BUCKET_ORDER = tuple(asset_bucket_order())
_BROKERAGE_ETF_SEARCH_BUCKETS = {"RV DM", "RV EM", "HY", "RF IG Long", "RF IG Short"}
_BROKERAGE_OTHERS_LABEL_TO_BUCKET = {
    "shortterm": "RF IG Short",
    "rfigshort": "RF IG Short",
    "nonusfixedincome": "Non US RF",
    "nonusrf": "Non US RF",
}
_BROKERAGE_CASH_LABEL_TO_BUCKET = {
    "cash": "Caja",
    "caja": "Caja",
}
_BROKERAGE_CASH_MARKERS = (
    "depositsweep",
    "liquiditysweep",
    "liliq",
    "primemmfd",
    "moneymarket",
    "proceedsfrompendingsales",
    "creditbalance",
    "availablebalance",
    "cashequivalent",
    "liqheritag",
)

_ALTERNATIVES_BANK_CODE = "alternativos"
_ALTERNATIVES_BANK_NAME = "Alternativos"
_ALTERNATIVES_ACCOUNT_TYPE = "investment"
_ALTERNATIVES_SOURCE_TAG = "alternatives_excel"

_UBS_MANUAL_MONTHLY_OVERRIDES: dict[tuple[str, int, int], dict[str, Any]] = {
    (
        "206-560552-02",
        2025,
        2,
    ): {
        "bank_code": "ubs",
        "closing_date": date(2025, 2, 28),
        "currency": "USD",
        "ending_value_with_accrual": Decimal("82100670"),
        "ending_value_without_accrual": Decimal("82100670"),
        "accrual_ending": Decimal("0"),
        "movements_net": Decimal("82089481"),
        "profit_period": Decimal("11189"),
        "source_filename": "202502 Boatview UBS SW (206-560552-02) 511UBS SW_P2.pdf",
        "related_accounts": ["206-560402-01"],
        "trigger_source_filenames": [
            "202502 Boatview UBS SW (206-560552-02) 511UBS SW_P2.pdf",
            "202502 Telmar UBS SW Mandato (0402 60P y 61K).pdf",
        ],
        "reason": (
            "Override manual UBS Suiza: creación extraordinaria de Boatview 206-560552-02 "
            "por traspaso interno desde Telmar en 2025-02. Se fuerza el movimiento de inicio "
            "para que la cuenta parta desde la cartola de Boatview."
        ),
    },
    (
        "206-560402-01",
        2025,
        2,
    ): {
        "bank_code": "ubs",
        "closing_date": date(2025, 2, 28),
        "currency": "USD",
        "ending_value_with_accrual": Decimal("0"),
        "ending_value_without_accrual": Decimal("0"),
        "accrual_ending": Decimal("0"),
        "movements_net": Decimal("-82089481"),
        "profit_period": Decimal("231316"),
        "cash_value": Decimal("0"),
        "asset_allocation_json": None,
        "source_filename": "202502 Telmar UBS SW Mandato (0402 60P y 61K).pdf",
        "related_accounts": ["206-560552-02"],
        "trigger_source_filenames": [
            "202502 Boatview UBS SW (206-560552-02) 511UBS SW_P2.pdf",
            "202502 Telmar UBS SW Mandato (0402 60P y 61K).pdf",
        ],
        "reason": (
            "Override manual UBS Suiza: salida extraordinaria de Telmar 206-560402-01 en 2025-02 "
            "contra la apertura de Boatview 206-560552-02. El monto manda por Boatview; la diferencia "
            "necesaria para dejar Telmar en cero se reconoce como utilidad."
        ),
    },
}


_REPORT_MONTH_NAME_MAP = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}

_REPORTING_VALUE_EXCLUSION_KEY = "__reporting_value_exclusion"
_REPORTING_VALUE_EXCLUSION_RULES: dict[tuple[str, str, str], dict[str, Any]] = {
    (
        "goldman_sachs",
        "mandato",
        "097-4",
    ): {
        "rule_id": "telmar_gs_mandato_private_equity_duplicated_in_alternatives",
        "labels_norm": {"privateequity"},
        "description": (
            "Exclude Private Equity from GS Telmar mandato because it is already "
            "reported in Alternativos.xlsx."
        ),
    },
    (
        "jpmorgan",
        "brokerage",
        "B43459001",
    ): {
        "rule_id": "telmar_jpm_brokerage_alternative_assets_duplicated_in_alternatives",
        "labels_norm": {"alternativeassets"},
        "description": (
            "Exclude Alternative Assets from JPM Telmar brokerage because it is "
            "already reported in Alternativos.xlsx."
        ),
    },
}


def _safe_decimal(value) -> Optional[Decimal]:
    """Convierte un valor a Decimal de forma segura."""
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _extract_allocation_amount(
    payload: Any,
    *,
    ending_value_with_accrual: Decimal | None,
) -> Optional[Decimal]:
    if isinstance(payload, dict):
        raw_value = (
            payload.get("value")
            or payload.get("total")
            or payload.get("ending")
            or payload.get("ending_value")
            or payload.get("market_value")
            or payload.get("amount")
        )
        unit = str(payload.get("unit") or "").strip()
    else:
        raw_value = payload
        unit = ""

    amount = _safe_decimal(raw_value)
    if amount is None:
        return None
    if unit == "%":
        if ending_value_with_accrual is None or ending_value_with_accrual <= 0:
            return None
        return (ending_value_with_accrual * amount) / Decimal("100")
    return amount


def _cash_from_jpmorgan_holdings_rows(rows: Any) -> Optional[Decimal]:
    """
    Extrae caja JPM desde holdings parseados cuando no existe
    asset_allocation estructurado.
    """
    if not isinstance(rows, list):
        return None

    cash_markers = (
        "depositsweep",
        "liquiditysweep",
        "liliq",
        "primemmfd",
        "moneymarket",
        "proceedsfrompendingsales",
        "creditbalance",
        "availablebalance",
        "cashequivalent",
        "liqheritag",
    )

    total = Decimal("0")
    found = False
    for row in rows:
        if not isinstance(row, dict):
            continue
        name_norm = re.sub(r"[^a-z0-9]", "", str(row.get("instrument") or "").lower())
        if not name_norm or not any(marker in name_norm for marker in cash_markers):
            continue
        value = _safe_decimal(
            row.get("market_value") or row.get("value") or row.get("amount")
        )
        if value is None:
            continue
        total += value
        found = True
    return total if found else None


def _cash_from_jpmorgan_parsed_payload(parsed_data_json: str | None) -> Optional[Decimal]:
    if not parsed_data_json:
        return None
    try:
        payload = json.loads(parsed_data_json)
    except (TypeError, ValueError):
        return None
    rows = payload.get("rows") if isinstance(payload, dict) else None
    return _cash_from_jpmorgan_holdings_rows(rows)


def _rows_from_parsed_payload(
    parsed_data_json: str | None,
    *,
    account_number: str | None = None,
) -> list[dict[str, Any]]:
    if not parsed_data_json:
        return []
    try:
        payload = json.loads(parsed_data_json)
    except (TypeError, ValueError):
        return []
    rows = payload.get("rows") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return []

    filtered_rows: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_account = str(row.get("account_number") or "").strip()
        if account_number and row_account and row_account != account_number:
            continue
        filtered_rows.append(row)
    return filtered_rows


def _normalize_asset_label(label: Any) -> str:
    text = str(label or "").strip().lower()
    return re.sub(r"[^a-z0-9]", "", text)


def _extract_asset_allocation_entries(asset_alloc: Any) -> list[tuple[str, Decimal]]:
    """
    Devuelve lista de (label_normalized, value) desde dict/list heterogéneo.
    Prioriza campos de cierre/total para Mandatos.
    """
    entries: list[tuple[str, Decimal]] = []
    if isinstance(asset_alloc, dict):
        iterable = list(asset_alloc.items())
    elif isinstance(asset_alloc, list):
        iterable = []
        for row in asset_alloc:
            if not isinstance(row, dict):
                continue
            label = (
                row.get("asset_class")
                or row.get("name")
                or row.get("label")
                or row.get("class")
                or row.get("category")
            )
            iterable.append((label, row))
    else:
        return entries

    for raw_label, payload in iterable:
        label_norm = _normalize_asset_label(raw_label)
        if not label_norm:
            continue
        if isinstance(payload, dict):
            raw_value = (
                payload.get("total")
                or payload.get("ending")
                or payload.get("value")
                or payload.get("market_value")
                or payload.get("amount")
                or payload.get("ending_value")
            )
        else:
            raw_value = payload
        val = _safe_decimal(raw_value)
        if val is None:
            continue
        entries.append((label_norm, val))
    return entries


def _asset_allocation_total_value(asset_alloc: Any) -> Optional[Decimal]:
    entries = _extract_asset_allocation_entries(asset_alloc)
    if not entries:
        return None
    total = Decimal("0")
    for _, amount in entries:
        total += amount
    return total


_MANDATE_ASSET_ORDER = (
    "Cash, Deposits & Money Market",
    "Investment Grade Fixed Income",
    "High Yield Fixed Income",
    "Fixed Income",
    "US Equities",
    "Non US Equities",
    "Equities",
    "Private Equity",
    "Real Estate",
    "Other Investments",
)


def _classify_mandate_asset_label(*, label_norm: str, bank_code: str = "") -> str | None:
    if not label_norm:
        return None
    key = label_norm

    if "totalportfolio" in key or "totalnetmarketvalue" in key or "netassets" in key:
        return None

    if "cash" in key and ("fixedincome" in key or "bond" in key):
        return None

    if "privateequity" in key:
        return "Private Equity"
    if "realestate" in key:
        return "Real Estate"
    if any(token in key for token in ("otherinvestment", "assetallocationinvestment", "miscellaneous", "hedgefund")):
        return "Other Investments"
    if "alternativeinvestment" in key:
        return None

    if any(token in key for token in ("cash", "deposit", "moneymarket", "liquidity")):
        return "Cash, Deposits & Money Market"

    fixed_income_hint = (
        "fixedincome" in key
        or "bond" in key
        or key in {
            "investmentgrade",
            "highgrade",
            "uscorporates",
            "corporateigcredit",
            "corporatebonds",
            "shortduration",
            "globalbonds",
            "investmentgrademultisector",
        }
    )
    if fixed_income_hint:
        if any(token in key for token in ("highyield", "noninvestmentgrade", "otherfixedincome", "ushighyield")):
            return "High Yield Fixed Income"
        if bank_code == "ubs_miami" and "emerging" in key:
            return "High Yield Fixed Income"
        if any(token in key for token in ("investmentgrade", "highgrade", "corporate", "government", "treasury", "tips", "shortduration")):
            return "Investment Grade Fixed Income"
        return "Fixed Income"

    equity_hint = "equit" in key or "stock" in key
    if equity_hint:
        if "nonus" in key:
            return "Non US Equities"
        if any(
            token in key
            for token in (
                "international",
                "global",
                "emerging",
                "eafe",
                "europe",
                "japan",
                "switzerland",
                "uk",
                "emu",
            )
        ) and not key.startswith("us") and "uslarge" not in key and "usmidsmall" not in key:
            return "Non US Equities"
        if key.startswith("us") or "usequity" in key or "uslargecap" in key or "usmidsmall" in key:
            return "US Equities"
        return "Equities"

    return None


def _normalize_mandate_asset_allocation(
    asset_alloc: Any,
    *,
    bank_code: str = "",
    macro_only: bool = False,
) -> dict[str, dict[str, str]] | None:
    """
    Normaliza asset allocation de Mandatos preservando split:
    - Cash
    - IG / HY / Fixed Income
    - US / Non-US / Equities
    """
    entries = _extract_asset_allocation_entries(asset_alloc)
    metadata: dict[str, Any] = {}
    if isinstance(asset_alloc, dict):
        for key, payload in asset_alloc.items():
            if str(key).startswith("__") and isinstance(payload, dict):
                metadata[str(key)] = payload

    totals: dict[str, Decimal] = {}
    cash_values: list[Decimal] = []
    cash_umbrella_values: list[Decimal] = []
    other_investments_umbrella_values: list[Decimal] = []
    other_investments_component_values: list[Decimal] = []
    hedge_fund_values: list[Decimal] = []
    for label_norm, value in entries:
        canonical = _classify_mandate_asset_label(label_norm=label_norm, bank_code=bank_code)
        if not canonical:
            continue
        if canonical == "Cash, Deposits & Money Market":
            if "cash" in label_norm and "deposit" in label_norm and ("moneymarket" in label_norm or "shortterm" in label_norm):
                cash_umbrella_values.append(value)
            else:
                cash_values.append(value)
            continue
        if canonical == "Other Investments":
            if "hedgefund" in label_norm:
                hedge_fund_values.append(value)
            elif "otherinvestment" in label_norm:
                other_investments_umbrella_values.append(value)
            else:
                other_investments_component_values.append(value)
            continue
        totals[canonical] = totals.get(canonical, Decimal("0")) + value

    if cash_umbrella_values:
        totals["Cash, Deposits & Money Market"] = max(cash_umbrella_values)
    elif cash_values:
        totals["Cash, Deposits & Money Market"] = sum(cash_values, Decimal("0"))

    if hedge_fund_values or other_investments_umbrella_values or other_investments_component_values:
        other_investments_total = sum(hedge_fund_values, Decimal("0"))
        if other_investments_umbrella_values:
            other_investments_total += max(other_investments_umbrella_values)
        else:
            other_investments_total += sum(other_investments_component_values, Decimal("0"))
        totals["Other Investments"] = other_investments_total

    ig = totals.get("Investment Grade Fixed Income")
    hy = totals.get("High Yield Fixed Income")
    fi = totals.get("Fixed Income")
    if (ig is not None or hy is not None) and fi is None:
        totals["Fixed Income"] = (ig or Decimal("0")) + (hy or Decimal("0"))

    us_eq = totals.get("US Equities")
    non_us_eq = totals.get("Non US Equities")
    eq = totals.get("Equities")
    if (us_eq is not None or non_us_eq is not None) and eq is None:
        totals["Equities"] = (us_eq or Decimal("0")) + (non_us_eq or Decimal("0"))

    macro_labels = ("Cash, Deposits & Money Market", "Fixed Income", "Equities")
    for base_label in macro_labels:
        totals.setdefault(base_label, Decimal("0"))

    if not totals and not metadata:
        return None

    normalized: dict[str, dict[str, str]] = {}
    labels_to_persist = (
        macro_labels + ("Private Equity", "Real Estate", "Other Investments")
        if macro_only
        else _MANDATE_ASSET_ORDER
    )
    for label in labels_to_persist:
        value = totals.get(label)
        if value is None:
            continue
        normalized[label] = {"value": str(value)}

    for key, payload in metadata.items():
        normalized[key] = payload

    return normalized


def _asset_bucket_sort_key(bucket: str) -> tuple[int, str]:
    try:
        return (_ASSET_BUCKET_ORDER.index(bucket), bucket)
    except ValueError:
        return (len(_ASSET_BUCKET_ORDER), bucket)


def _normalize_alloc_label(label: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(label or "").lower())


def _last_day_of_month(year: int, month: int) -> date:
    return date(year, month, calendar.monthrange(year, month)[1])


def _infer_pdf_report_period_end_from_filename(filename: str) -> date | None:
    name = str(filename or "")
    if not name:
        return None

    # 1) Nombres con mes explícito (e.g. "Feb 2026", "2026 February")
    month_re = r"(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
    month_year = re.search(rf"\b{month_re}\s+([12]\d{{3}})\b", name, re.IGNORECASE)
    if month_year:
        month_token = month_year.group(1).lower()
        year = int(month_year.group(2))
        month = _REPORT_MONTH_NAME_MAP.get(month_token)
        if month:
            return _last_day_of_month(year, month)

    year_month = re.search(rf"\b([12]\d{{3}})\s+{month_re}\b", name, re.IGNORECASE)
    if year_month:
        year = int(year_month.group(1))
        month_token = year_month.group(2).lower()
        month = _REPORT_MONTH_NAME_MAP.get(month_token)
        if month:
            return _last_day_of_month(year, month)

    # 2) Fechas completas con separadores (dd-mm-yyyy / mm-dd-yyyy / dd.mm.yyyy / mm.dd.yyyy)
    for m in re.finditer(r"\b(\d{1,2})[./-](\d{1,2})[./-]([12]\d{3})\b", name):
        first = int(m.group(1))
        second = int(m.group(2))
        year = int(m.group(3))
        # Preferir mm-dd-yyyy (común en filenames UBS Miami); fallback a dd-mm-yyyy.
        month = first
        day = second
        try:
            parsed = date(year, month, day)
        except ValueError:
            month = second
            day = first
            try:
                parsed = date(year, month, day)
            except ValueError:
                continue
        return parsed

    # 3) Prefijo YYYY MM (e.g. "2026 02 ...")
    prefix = re.search(r"^\D*([12]\d{3})[\s._-]+(0?[1-9]|1[0-2])\b", name)
    if prefix:
        year = int(prefix.group(1))
        month = int(prefix.group(2))
        return _last_day_of_month(year, month)

    return None


def _resolve_pdf_report_period_end(result: ParseResult, raw_document: RawDocument) -> date | None:
    if raw_document.period_year and raw_document.period_month:
        return _last_day_of_month(raw_document.period_year, raw_document.period_month)

    inferred = _infer_pdf_report_period_end_from_filename(raw_document.filename or "")
    if inferred is not None:
        return inferred

    if result.period_end:
        return result.period_end
    if result.statement_date:
        return result.statement_date
    return None


class DataLoadingService:
    """Carga datos parseados a las tablas de reporting."""

    def __init__(self, db: Session):
        self.db = db

    def sync_normalized_for_account_year(self, account: Account, year: int) -> None:
        """Punto pÃºblico para alinear capa normalizada con monthly_closings."""
        refresh_years = {year}
        if account.bank_code == "ubs":
            refresh_years |= self._recompute_ubs_income_series(account=account)
        for refresh_year in sorted(refresh_years):
            self._refresh_normalized_activity_from_monthly_closings(
                account=account,
                year=refresh_year,
            )

    def load_parse_result(
        self,
        result: ParseResult,
        raw_document: RawDocument,
        parser_version_id: int,
    ) -> dict:
        """
        Punto de entrada principal: toma un ParseResult y lo persiste.

        Returns:
            {"parsed_statements": n, "monthly_closings": n, "etf_compositions": n, "errors": [...]}
        """
        stats = {
            "parsed_statements": 0,
            "monthly_closings": 0,
            "etf_compositions": 0,
            "errors": [],
        }

        if not result.is_success:
            stats["errors"].append("ParseResult no exitoso, se omite carga")
            return stats

        # --- Resolver cuentas ---
        accounts = self._resolve_accounts(result, raw_document)
        if not accounts:
            stats["errors"].append(
                f"No se encontraron cuentas en el maestro para "
                f"account_number={result.account_number}, "
                f"account_numbers={result.account_numbers}, "
                f"bank_code={raw_document.bank_code}"
            )
            self._log("load", "warning", stats["errors"][-1], raw_document.id)
            return stats

        # --- Para cada cuenta, crear registros ---
        for account in accounts:
            try:
                # Rama BICE: flujo de inversiones nacionales → bice_monthly_snapshot
                if (raw_document.bank_code or result.bank_code or "").lower() in {"bice", "bice_inversiones"}:
                    # 1) ParsedStatement (trazabilidad)
                    ps_ok = self._upsert_parsed_statement(
                        result, raw_document, account, parser_version_id
                    )
                    if ps_ok:
                        stats["parsed_statements"] += 1

                    # 2) BiceMonthlySnapshot (SSOT inversiones nacionales)
                    bice_ok = self._load_bice_snapshot(result, raw_document, account)
                    if bice_ok:
                        stats["monthly_closings"] += 1
                    continue

                # 1) ParsedStatement
                ps_ok = self._upsert_parsed_statement(
                    result, raw_document, account, parser_version_id
                )
                if ps_ok:
                    stats["parsed_statements"] += 1

                # 2) MonthlyClosing
                mc_ok = self._upsert_monthly_closing(
                    result, raw_document, account
                )
                if mc_ok:
                    stats["monthly_closings"] += 1

                # 3) EtfComposition (solo para cuentas ETF)
                if account.account_type == "etf":
                    n = self._upsert_etf_compositions(
                        result, raw_document, account
                    )
                    stats["etf_compositions"] += n

            except Exception as exc:
                msg = f"Error cargando datos para cuenta {account.account_number}: {exc}"
                stats["errors"].append(msg)
                self.db.rollback()
                self._log("load", "error", msg, raw_document.id, account.id)
                logger.exception(msg)

        self.db.flush()

        if result.statement_date:
            self.apply_manual_monthly_overrides(
                bank_code=raw_document.bank_code or result.bank_code or "",
                year=result.statement_date.year,
                trigger_filename=raw_document.filename,
                trigger_account_numbers=set(result.account_numbers or []) | {result.account_number},
                trigger_month=result.statement_date.month,
            )

        self.db.commit()

        self._log(
            "load", "info",
            f"Carga completada para doc {raw_document.id}: "
            f"{stats['parsed_statements']} statements, "
            f"{stats['monthly_closings']} closings, "
            f"{stats['etf_compositions']} compositions",
            raw_document.id,
        )

        return stats

    # ─────────────────────────────────────────────────────────────────────
    # BICE — Inversiones nacionales
    # ─────────────────────────────────────────────────────────────────────

    def _load_bice_snapshot(
        self,
        result: ParseResult,
        raw_document: "RawDocument",
        account: "Account",
    ) -> bool:
        """
        Persiste una cartola BICE en bice_monthly_snapshot (SSOT inversiones nacionales).

        Profit CLP/USD = ending_mes - ending_mes_anterior - (aportes - retiros).
        Los dividendos no afectan el profit: quedan en Caja dentro del mismo saldo.
        Si no hay mes anterior en BD, profit queda NULL.
        """
        if not result.statement_date:
            self._log("load", "warning", "BICE: sin statement_date, se omite carga", raw_document.id)
            return False

        year = result.statement_date.year
        month = result.statement_date.month
        closing_date = result.statement_date

        balances = result.balances or {}
        positions = balances.get("positions", {})
        movements = balances.get("movements", {})

        clp_pos = positions.get("CLP", {})
        usd_pos = positions.get("USD", {})
        clp_mov = movements.get("CLP", {})
        usd_mov = movements.get("USD", {})

        def _d(val) -> Optional[Decimal]:
            if val is None:
                return None
            try:
                return Decimal(str(val))
            except Exception:
                return None

        ending_clp = _d(clp_pos.get("Total"))
        ending_usd = _d(usd_pos.get("Total"))
        aportes_clp = _d(clp_mov.get("aportes"))
        retiros_clp = _d(clp_mov.get("retiros"))
        aportes_usd = _d(usd_mov.get("aportes"))
        retiros_usd = _d(usd_mov.get("retiros"))

        # Calcular profit buscando el mes anterior en BD
        def _calc_profit(ending, aportes, retiros, prev_ending) -> Optional[Decimal]:
            if ending is None or prev_ending is None:
                return None
            net_mov = (aportes or Decimal("0")) - (retiros or Decimal("0"))
            return ending - prev_ending - net_mov

        prev = (
            self.db.query(BiceMonthlySnapshot)
            .filter(
                BiceMonthlySnapshot.account_id == account.id,
                BiceMonthlySnapshot.year == (year if month > 1 else year - 1),
                BiceMonthlySnapshot.month == (month - 1 if month > 1 else 12),
            )
            .first()
        )
        prev_ending_clp = prev.ending_clp if prev else None
        prev_ending_usd = prev.ending_usd if prev else None

        profit_clp = _calc_profit(ending_clp, aportes_clp, retiros_clp, prev_ending_clp)
        profit_usd = _calc_profit(ending_usd, aportes_usd, retiros_usd, prev_ending_usd)

        existing = (
            self.db.query(BiceMonthlySnapshot)
            .filter(
                BiceMonthlySnapshot.account_id == account.id,
                BiceMonthlySnapshot.year == year,
                BiceMonthlySnapshot.month == month,
            )
            .first()
        )

        if existing:
            existing.closing_date = closing_date
            existing.ending_clp = ending_clp
            existing.caja_clp = _d(clp_pos.get("Caja"))
            existing.renta_fija_clp = _d(clp_pos.get("Renta Fija"))
            existing.equities_clp = _d(clp_pos.get("Equities"))
            existing.aportes_clp = aportes_clp
            existing.retiros_clp = retiros_clp
            existing.dividendos_clp = _d(clp_mov.get("dividendos_otros"))
            existing.profit_clp = profit_clp
            existing.ending_usd = ending_usd
            existing.caja_usd = _d(usd_pos.get("Caja"))
            existing.renta_fija_usd = _d(usd_pos.get("Renta Fija"))
            existing.equities_usd = _d(usd_pos.get("Equities"))
            existing.aportes_usd = aportes_usd
            existing.retiros_usd = retiros_usd
            existing.dividendos_usd = _d(usd_mov.get("dividendos_otros"))
            existing.profit_usd = profit_usd
            existing.source_document_id = raw_document.id
            existing.loaded_at = datetime.now(timezone.utc)
            self._log("load", "info", f"BICE snapshot actualizado: acct={account.account_number} {year}-{month:02d}", raw_document.id, account.id)
        else:
            snap = BiceMonthlySnapshot(
                account_id=account.id,
                year=year,
                month=month,
                closing_date=closing_date,
                ending_clp=ending_clp,
                caja_clp=_d(clp_pos.get("Caja")),
                renta_fija_clp=_d(clp_pos.get("Renta Fija")),
                equities_clp=_d(clp_pos.get("Equities")),
                aportes_clp=aportes_clp,
                retiros_clp=retiros_clp,
                dividendos_clp=_d(clp_mov.get("dividendos_otros")),
                profit_clp=profit_clp,
                ending_usd=ending_usd,
                caja_usd=_d(usd_pos.get("Caja")),
                renta_fija_usd=_d(usd_pos.get("Renta Fija")),
                equities_usd=_d(usd_pos.get("Equities")),
                aportes_usd=aportes_usd,
                retiros_usd=retiros_usd,
                dividendos_usd=_d(usd_mov.get("dividendos_otros")),
                profit_usd=profit_usd,
                source_document_id=raw_document.id,
                loaded_at=datetime.now(timezone.utc),
            )
            self.db.add(snap)
            self._log("load", "info", f"BICE snapshot creado: acct={account.account_number} {year}-{month:02d}", raw_document.id, account.id)

        self.db.flush()

        # Recalcular profit del mes siguiente si ya existe en BD
        next_snap = (
            self.db.query(BiceMonthlySnapshot)
            .filter(
                BiceMonthlySnapshot.account_id == account.id,
                BiceMonthlySnapshot.year == (year if month < 12 else year + 1),
                BiceMonthlySnapshot.month == (month + 1 if month < 12 else 1),
            )
            .first()
        )
        if next_snap:
            next_snap.profit_clp = _calc_profit(
                next_snap.ending_clp, next_snap.aportes_clp, next_snap.retiros_clp, ending_clp
            )
            next_snap.profit_usd = _calc_profit(
                next_snap.ending_usd, next_snap.aportes_usd, next_snap.retiros_usd, ending_usd
            )
            self.db.flush()

        return True

    def apply_manual_monthly_overrides(
        self,
        *,
        bank_code: str,
        year: int,
        trigger_filename: str | None = None,
        trigger_account_numbers: set[str | None] | None = None,
        trigger_month: int | None = None,
    ) -> int:
        if bank_code != "ubs":
            return 0

        trigger_accounts = {acct for acct in (trigger_account_numbers or set()) if acct}
        applied = 0
        for (account_number, ov_year, ov_month), override in _UBS_MANUAL_MONTHLY_OVERRIDES.items():
            if override.get("bank_code") != bank_code or ov_year != year:
                continue
            if trigger_month is not None and ov_month != trigger_month:
                continue

            related_accounts = {
                account_number,
                *(override.get("related_accounts") or []),
            }
            source_filenames = {
                override.get("source_filename"),
                *(override.get("trigger_source_filenames") or []),
            }
            source_filenames.discard(None)

            is_triggered = False
            if trigger_filename and trigger_filename in source_filenames:
                is_triggered = True
            if trigger_accounts and trigger_accounts.intersection(related_accounts):
                is_triggered = True
            if not is_triggered:
                continue

            account = (
                self.db.query(Account)
                .filter(Account.account_number == account_number)
                .first()
            )
            if account is None:
                continue

            applied += self._upsert_manual_monthly_override(
                account=account,
                year=ov_year,
                month=ov_month,
                override=override,
            )

        return applied

    def load_operational_result(
        self,
        result: ParseResult,
        raw_document: RawDocument,
        file_type: str,
    ) -> dict:
        """
        Carga ParseResult de archivos operativos Excel/CSV a tablas daily_*.

        Args:
            result: salida del parser Excel diario
            raw_document: documento raw asociado
            file_type: excel_positions | excel_movements | excel_prices
        """
        stats = {
            "daily_positions": 0,
            "daily_movements": 0,
            "daily_prices": 0,
            "errors": [],
        }

        if not result.is_success:
            stats["errors"].append("ParseResult no exitoso, se omite carga operativa")
            return stats

        source_hash = result.source_file_hash or raw_document.sha256_hash
        account_cache: dict[str, Account] = {}

        if file_type == "excel_positions":
            for row in result.rows:
                data = row.data or {}
                account_number = self._clean_str(data.get("account_number"))
                instrument_code = self._clean_str(data.get("instrument_code"))
                position_date = self._safe_date(data.get("position_date"))

                if not account_number or not instrument_code or not position_date:
                    stats["errors"].append(
                        f"Fila {row.row_number}: faltan account_number/instrument_code/position_date"
                    )
                    continue

                account = self._resolve_account(account_number, account_cache)
                if not account:
                    stats["errors"].append(
                        f"Fila {row.row_number}: cuenta no encontrada en maestro: {account_number}"
                    )
                    continue

                existing = (
                    self.db.query(DailyPosition)
                    .filter(
                        DailyPosition.account_id == account.id,
                        DailyPosition.position_date == position_date,
                        DailyPosition.instrument_code == instrument_code,
                    )
                    .first()
                )

                payload = {
                    "instrument_name": self._clean_str(data.get("instrument_name")),
                    "instrument_type": self._clean_str(data.get("instrument_type")),
                    "isin": self._clean_str(data.get("isin")),
                    "quantity": _safe_decimal(data.get("quantity")),
                    "market_price": _safe_decimal(data.get("market_price")),
                    "market_value": _safe_decimal(data.get("market_value")),
                    "cost_basis": _safe_decimal(data.get("cost_basis")),
                    "unrealized_pnl": _safe_decimal(data.get("unrealized_pnl")),
                    "currency": self._clean_str(data.get("currency")) or account.currency,
                    "market_value_usd": _safe_decimal(data.get("market_value_usd")),
                    "accrued_interest": _safe_decimal(data.get("accrued_interest")),
                    "source_file_hash": source_hash,
                }

                if existing:
                    for k, v in payload.items():
                        setattr(existing, k, v)
                else:
                    self.db.add(
                        DailyPosition(
                            account_id=account.id,
                            position_date=position_date,
                            instrument_code=instrument_code,
                            **payload,
                        )
                    )
                stats["daily_positions"] += 1

        elif file_type == "excel_movements":
            # No hay UNIQUE en DailyMovement: borrar previos del mismo archivo para idempotencia.
            self.db.query(DailyMovement).filter(
                DailyMovement.source_file_hash == source_hash
            ).delete(synchronize_session=False)

            valid_movement_types = {m.value for m in MovementType}
            for row in result.rows:
                data = row.data or {}
                account_number = self._clean_str(data.get("account_number"))
                movement_date = self._safe_date(data.get("movement_date"))
                if not account_number or not movement_date:
                    stats["errors"].append(
                        f"Fila {row.row_number}: faltan account_number/movement_date"
                    )
                    continue

                account = self._resolve_account(account_number, account_cache)
                if not account:
                    stats["errors"].append(
                        f"Fila {row.row_number}: cuenta no encontrada en maestro: {account_number}"
                    )
                    continue

                movement_type = self._clean_str(data.get("movement_type")) or "other"
                movement_type = movement_type.lower()
                if movement_type not in valid_movement_types:
                    movement_type = "other"

                self.db.add(
                    DailyMovement(
                        account_id=account.id,
                        movement_date=movement_date,
                        settlement_date=self._safe_date(data.get("settlement_date")),
                        movement_type=movement_type,
                        instrument_code=self._clean_str(data.get("instrument_code")),
                        instrument_name=self._clean_str(data.get("instrument_name")),
                        description=self._clean_str(data.get("description")),
                        quantity=_safe_decimal(data.get("quantity")),
                        price=_safe_decimal(data.get("price")),
                        gross_amount=_safe_decimal(data.get("gross_amount")),
                        net_amount=_safe_decimal(data.get("net_amount")),
                        fees=_safe_decimal(data.get("fees")),
                        tax=_safe_decimal(data.get("tax")),
                        currency=self._clean_str(data.get("currency")) or account.currency,
                        amount_usd=_safe_decimal(data.get("amount_usd")),
                        source_file_hash=source_hash,
                    )
                )
                stats["daily_movements"] += 1

        elif file_type == "excel_prices":
            for row in result.rows:
                data = row.data or {}
                instrument_code = self._clean_str(data.get("instrument_code"))
                price_date = self._safe_date(data.get("price_date"))
                price = _safe_decimal(data.get("price"))
                if not instrument_code or not price_date or price is None:
                    stats["errors"].append(
                        f"Fila {row.row_number}: faltan instrument_code/price_date/price"
                    )
                    continue

                existing = (
                    self.db.query(DailyPrice)
                    .filter(
                        DailyPrice.price_date == price_date,
                        DailyPrice.instrument_code == instrument_code,
                    )
                    .first()
                )

                payload = {
                    "instrument_type": self._clean_str(data.get("instrument_type")) or "other",
                    "price": price,
                    "currency": self._clean_str(data.get("currency")) or "USD",
                    "source": self._clean_str(data.get("source")),
                    "source_file_hash": source_hash,
                }

                if existing:
                    for k, v in payload.items():
                        setattr(existing, k, v)
                else:
                    self.db.add(
                        DailyPrice(
                            price_date=price_date,
                            instrument_code=instrument_code,
                            **payload,
                        )
                    )
                stats["daily_prices"] += 1
        else:
            stats["errors"].append(f"Tipo de archivo no soportado para carga operativa: {file_type}")

        self.db.commit()
        self._log(
            "load",
            "info",
            (
                f"Carga operativa completada para doc {raw_document.id}: "
                f"{stats['daily_positions']} posiciones, "
                f"{stats['daily_movements']} movimientos, "
                f"{stats['daily_prices']} precios"
            ),
            raw_document.id,
        )
        return stats

    def load_alternatives_result(
        self,
        result: ParseResult,
        raw_document: RawDocument,
    ) -> dict:
        stats = {
            "normalized_rows": 0,
            "accounts_created": 0,
            "accounts_updated": 0,
            "accounts_deleted": 0,
            "errors": [],
        }
        if not result.is_success:
            stats["errors"].append("ParseResult no exitoso para alternativos")
            return stats

        managed_accounts = (
            self.db.query(Account)
            .filter(Account.bank_code == _ALTERNATIVES_BANK_CODE)
            .all()
        )
        managed_by_number = {acct.account_number: acct for acct in managed_accounts}
        managed_ids = [acct.id for acct in managed_accounts]
        if managed_ids:
            self.db.query(MonthlyMetricNormalized).filter(
                MonthlyMetricNormalized.account_id.in_(managed_ids)
            ).delete(synchronize_session=False)

        # Lookup person_name por entity_name desde "Excel Cuentas Contables.xlsx"
        # (Sociedad → Nombre persona). Match exacto primero; si falla, match fuzzy
        # con difflib (umbral 0.85) para tolerar pequeñas diferencias tipográficas.
        import difflib as _difflib
        from pathlib import Path as _Path
        import pandas as _pd

        entity_to_person: dict[str, str] = {}
        try:
            _cc_path = _Path(__file__).parents[2] / "Documentos" / "Excel" / "Excel Cuentas Contables.xlsx"
            if _cc_path.exists():
                _cc_df = _pd.read_excel(_cc_path, sheet_name="Hoja1", header=0)
                for _, _row in _cc_df.iterrows():
                    _sociedad = str(_row.get("Sociedad") or "").strip()
                    _persona = str(_row.get("Nombre persona") or "").strip()
                    if _sociedad and _persona and _persona.lower() not in ("nan", "none", ""):
                        entity_to_person[_sociedad] = _persona
        except Exception as _exc:
            self._log("load", "warning", f"No se pudo leer Excel Cuentas Contables para mapeo persona: {_exc}")

        _known_entities = list(entity_to_person.keys())

        def _resolve_person(entity_name: str) -> str | None:
            if entity_name in entity_to_person:
                return entity_to_person[entity_name]
            matches = _difflib.get_close_matches(
                entity_name, _known_entities, n=1, cutoff=0.85
            )
            return entity_to_person[matches[0]] if matches else None

        seen_account_numbers: set[str] = set()
        created_account_numbers: set[str] = set()
        updated_account_numbers: set[str] = set()
        for row in result.rows:
            data = row.data or {}
            entity_name = self._clean_str(data.get("entity_name"))
            asset_class = self._clean_str(data.get("asset_class"))
            strategy = self._clean_str(data.get("strategy"))
            currency = self._clean_str(data.get("currency")) or "USD"
            nemo_reference = self._clean_str(data.get("nemo_reference"))
            closing_date = self._safe_date(data.get("closing_date"))
            year = data.get("year")
            month = data.get("month")
            if not entity_name or not asset_class or not strategy or not closing_date or year is None or month is None:
                stats["errors"].append(f"Fila {row.row_number}: metadata mensual incompleta")
                continue

            account_number = self._alternatives_account_number(
                entity_name=entity_name,
                asset_class=asset_class,
                strategy=strategy,
                currency=currency,
            )
            account = managed_by_number.get(account_number)
            metadata_json = json.dumps(
                {
                    "source": _ALTERNATIVES_SOURCE_TAG,
                    "asset_class": asset_class,
                    "strategy": strategy,
                    "currency": currency,
                    "nemo_reference": nemo_reference,
                    "account_group_label": f"{entity_name}-ALT-{asset_class}",
                    "detail_label": f"{entity_name} | {asset_class} | {strategy} | {currency}",
                },
                ensure_ascii=True,
            )
            if account is None:
                account = Account(
                    account_number=account_number,
                    identification_number=self._alternatives_identification_number(
                        nemo_reference=nemo_reference,
                        account_number=account_number,
                    ),
                    bank_code=_ALTERNATIVES_BANK_CODE,
                    bank_name=_ALTERNATIVES_BANK_NAME,
                    account_type=_ALTERNATIVES_ACCOUNT_TYPE,
                    entity_name=entity_name,
                    entity_type="sociedad",
                    currency=currency,
                    country="",
                    person_name=_resolve_person(entity_name),
                    metadata_json=metadata_json,
                    source_file_hash=result.source_file_hash or raw_document.sha256_hash,
                )
                self.db.add(account)
                self.db.flush()
                managed_by_number[account_number] = account
                created_account_numbers.add(account_number)
            else:
                account.identification_number = self._alternatives_identification_number(
                    nemo_reference=nemo_reference,
                    account_number=account_number,
                )
                account.bank_name = _ALTERNATIVES_BANK_NAME
                account.account_type = _ALTERNATIVES_ACCOUNT_TYPE
                account.entity_name = entity_name
                account.entity_type = "sociedad"
                account.currency = currency
                resolved = _resolve_person(entity_name)
                if resolved is not None:
                    account.person_name = resolved
                account.metadata_json = metadata_json
                account.source_file_hash = result.source_file_hash or raw_document.sha256_hash
                if account_number not in created_account_numbers:
                    updated_account_numbers.add(account_number)

            seen_account_numbers.add(account_number)
            self.db.add(
                MonthlyMetricNormalized(
                    account_id=account.id,
                    closing_date=closing_date,
                    year=int(year),
                    month=int(month),
                    ending_value_with_accrual=_safe_decimal(data.get("ending_value")),
                    ending_value_without_accrual=_safe_decimal(data.get("ending_value")),
                    accrual_ending=Decimal("0"),
                    cash_value=Decimal("0"),
                    movements_net=_safe_decimal(data.get("movements_net")),
                    profit_period=_safe_decimal(data.get("profit_period")),
                    movements_ytd=_safe_decimal(data.get("movements_ytd")),
                    profit_ytd=_safe_decimal(data.get("profit_ytd")),
                    asset_allocation_json=None,
                    currency=currency,
                    source_document_id=raw_document.id,
                )
            )
            stats["normalized_rows"] += 1

        stale_accounts = [
            acct for acct in managed_by_number.values()
            if acct.account_number not in seen_account_numbers
        ]
        if stale_accounts:
            stale_ids = [acct.id for acct in stale_accounts if acct.id is not None]
            if stale_ids:
                self.db.query(MonthlyMetricNormalized).filter(
                    MonthlyMetricNormalized.account_id.in_(stale_ids)
                ).delete(synchronize_session=False)
                self.db.query(Account).filter(Account.id.in_(stale_ids)).delete(synchronize_session=False)
                stats["accounts_deleted"] = len(stale_ids)

        stats["accounts_created"] = len(created_account_numbers)
        stats["accounts_updated"] = len(updated_account_numbers)

        self.db.commit()

        # Auto-limpiar documentos Alternativos anteriores: solo se conserva el más
        # reciente (raw_document.id). Los demás son versiones supersedidas del mismo
        # Excel operativo y no aportan valor de trazabilidad.
        try:
            old_ids = [
                row[0]
                for row in self.db.query(RawDocument.id)
                .filter(
                    RawDocument.bank_code == _ALTERNATIVES_BANK_CODE,
                    RawDocument.id != raw_document.id,
                )
                .all()
            ]
            if old_ids:
                self.db.query(MonthlyMetricNormalized).filter(
                    MonthlyMetricNormalized.source_document_id.in_(old_ids)
                ).update(
                    {"source_document_id": None},
                    synchronize_session=False,
                )
                self.db.query(RawDocument).filter(
                    RawDocument.id.in_(old_ids)
                ).delete(synchronize_session=False)
                self.db.commit()
                self._log(
                    "load",
                    "info",
                    f"Auto-limpieza alternativos: {len(old_ids)} documento(s) anterior(es) eliminado(s)",
                    raw_document.id,
                )
        except Exception as _exc:
            self._log("load", "warning", f"Auto-limpieza alternativos falló (no crítico): {_exc}")

        self._log(
            "load",
            "info",
            f"Carga alternativos completada para doc {raw_document.id}: {stats['normalized_rows']} filas normalizadas",
            raw_document.id,
        )
        return stats

    def load_asset_allocation_report(
        self,
        result: ParseResult,
        raw_document: RawDocument,
    ) -> dict:
        """
        Carga un PDF de reporte (asset allocation) y actualiza allocation mensual
        sin alterar la base financiera/trazabilidad de cartolas.
        """
        stats = {"monthly_closings_updated": 0, "skipped": 0, "errors": []}
        if not result.is_success:
            stats["errors"].append("ParseResult no exitoso para pdf_report")
            return stats

        asset_alloc = result.qualitative_data.get("asset_allocation")
        has_asset_alloc = isinstance(asset_alloc, dict) and bool(asset_alloc)
        metrics_series = result.qualitative_data.get("fixed_income_metrics_by_month")
        has_metrics_series = isinstance(metrics_series, list) and bool(metrics_series)

        if not has_asset_alloc and not has_metrics_series:
            stats["errors"].append("El reporte no contiene asset_allocation ni serie de metricas")
            return stats

        closing_date = _resolve_pdf_report_period_end(result=result, raw_document=raw_document) if has_asset_alloc else None
        target_accounts = self._resolve_pdf_report_target_accounts(
            result=result,
            raw_document=raw_document,
            closing_date=closing_date,
        )
        if not target_accounts:
            stats["errors"].append("No se pudo resolver cuenta para pdf_report")
            return stats

        touched_years: set[int] = set()

        if has_metrics_series:
            for account in target_accounts:
                touched_years |= self._apply_pdf_report_metrics_series(
                    account=account,
                    metrics_series=metrics_series,
                    raw_document_id=raw_document.id,
                    stats=stats,
                )

        if has_asset_alloc:
            if closing_date is None:
                stats["errors"].append("No se pudo determinar periodo para pdf_report")
            else:
                for account in target_accounts:
                    existing = (
                        self.db.query(MonthlyClosing)
                        .filter(
                            MonthlyClosing.account_id == account.id,
                            MonthlyClosing.year == closing_date.year,
                            MonthlyClosing.month == closing_date.month,
                        )
                        .first()
                    )
                    if existing is None:
                        # Guardrail: reportes de asset allocation no crean cierres financieros nuevos.
                        stats["skipped"] += 1
                        self._log(
                            "load",
                            "warning",
                            (
                                f"pdf_report omitido para {account.bank_code}/{account.account_number} "
                                f"{closing_date.year}-{closing_date.month:02d}: no existe cierre base"
                            ),
                            raw_document.id,
                            account_id=account.id,
                        )
                        continue

                    merged = self._merge_asset_allocation_payload(
                        existing_json=existing.asset_allocation_json,
                        incoming_allocation=asset_alloc,
                        bank_code=account.bank_code,
                        account_type=account.account_type,
                        ending_value=_safe_decimal(existing.net_value),
                    )
                    if merged is not None:
                        existing.asset_allocation_json = json.dumps(merged)
                        stats["monthly_closings_updated"] += 1
                        touched_years.add(closing_date.year)

        self.db.flush()
        for account in target_accounts:
            for year in sorted(touched_years):
                self._refresh_normalized_activity_from_monthly_closings(
                    account=account,
                    year=year,
                )
        self.db.commit()
        return stats

    def _resolve_pdf_report_account(
        self,
        *,
        result: ParseResult,
        raw_document: RawDocument,
    ) -> Optional[Account]:
        bank_code = (raw_document.bank_code or result.bank_code or "").strip()
        result_account_number = str(result.account_number or "").strip()

        if result_account_number and result_account_number.lower() != "varios":
            q = self.db.query(Account).filter(Account.account_number == result_account_number)
            if bank_code:
                q = q.filter(Account.bank_code == bank_code)
            account = q.first()
            if account:
                return account

            digits = "".join(ch for ch in result_account_number if ch.isdigit())
            if digits and bank_code:
                candidates = (
                    self.db.query(Account)
                    .filter(
                        Account.bank_code == bank_code,
                        Account.account_type == "mandato",
                        Account.account_number.like(f"%{digits}%"),
                    )
                    .all()
                )
                if len(candidates) == 1:
                    return candidates[0]

        doc_account: Optional[Account] = None
        if raw_document.account_id:
            doc_account = (
                self.db.query(Account)
                .filter(Account.id == raw_document.account_id)
                .first()
            )
            if doc_account and doc_account.account_type == "mandato":
                return doc_account

        if doc_account and bank_code:
            # If report was assigned to non-mandato account, remap to mandato sibling.
            mandate_q = (
                self.db.query(Account)
                .filter(
                    Account.bank_code == bank_code,
                    Account.account_type == "mandato",
                )
            )
            if doc_account.entity_name:
                mandate_q = mandate_q.filter(Account.entity_name == doc_account.entity_name)
            mandate_candidates = mandate_q.all()
            if len(mandate_candidates) == 1:
                return mandate_candidates[0]

            digits = "".join(ch for ch in result_account_number if ch.isdigit())
            if digits:
                by_digits = [acct for acct in mandate_candidates if digits in (acct.account_number or "")]
                if len(by_digits) == 1:
                    return by_digits[0]

            return doc_account

        if bank_code:
            mandate_candidates = (
                self.db.query(Account)
                .filter(
                    Account.bank_code == bank_code,
                    Account.account_type == "mandato",
                )
                .all()
            )
            if len(mandate_candidates) == 1:
                return mandate_candidates[0]

        return doc_account

    def _resolve_pdf_report_target_accounts(
        self,
        *,
        result: ParseResult,
        raw_document: RawDocument,
        closing_date: date | None,
    ) -> list[Account]:
        primary = self._resolve_pdf_report_account(result=result, raw_document=raw_document)
        if primary is None:
            return []
        targets = [primary]

        result_account_number = str(result.account_number or "").strip().lower()
        if (
            primary.bank_code == "jpmorgan"
            and primary.account_type == "mandato"
            and primary.entity_name
            and closing_date is not None
            and result_account_number in {"", "varios"}
        ):
            primary_closing = (
                self.db.query(MonthlyClosing)
                .filter(
                    MonthlyClosing.account_id == primary.id,
                    MonthlyClosing.year == closing_date.year,
                    MonthlyClosing.month == closing_date.month,
                )
                .first()
            )
            if primary_closing is not None:
                sibling_query = (
                    self.db.query(Account)
                    .join(MonthlyClosing, MonthlyClosing.account_id == Account.id)
                    .filter(
                        Account.bank_code == primary.bank_code,
                        Account.account_type == "mandato",
                        Account.entity_name == primary.entity_name,
                        MonthlyClosing.year == closing_date.year,
                        MonthlyClosing.month == closing_date.month,
                    )
                )
                if primary_closing.source_document_id is not None:
                    sibling_query = sibling_query.filter(
                        MonthlyClosing.source_document_id == primary_closing.source_document_id,
                    )
                siblings = sibling_query.all()
                if siblings:
                    targets = siblings

        deduped: dict[int, Account] = {acct.id: acct for acct in targets}
        return list(deduped.values())

    @staticmethod
    def _merge_asset_allocation_payload(
        *,
        existing_json: str | None,
        incoming_allocation: dict[str, Any] | None,
        incoming_metrics: dict[str, Any] | None = None,
        bank_code: str | None = None,
        account_type: str | None = None,
        ending_value: Decimal | None = None,
    ) -> dict[str, Any] | None:
        base: dict[str, Any] = {}
        if existing_json:
            try:
                parsed = json.loads(existing_json)
                if isinstance(parsed, dict):
                    base = parsed
            except (TypeError, ValueError):
                base = {}

        if isinstance(incoming_allocation, dict):
            if str(account_type or "").strip().lower() == "mandato":
                base = DataLoadingService._merge_mandate_report_allocation(
                    base=base,
                    incoming_allocation=incoming_allocation,
                    bank_code=bank_code,
                    ending_value=ending_value,
                )
            else:
                for key, value in incoming_allocation.items():
                    if key == "__mandate_metrics" and isinstance(value, dict):
                        prev = base.get("__mandate_metrics")
                        merged_metrics = dict(prev) if isinstance(prev, dict) else {}
                        merged_metrics.update(value)
                        base["__mandate_metrics"] = merged_metrics
                        continue
                    base[key] = value

        if incoming_metrics:
            prev = base.get("__mandate_metrics")
            merged_metrics = dict(prev) if isinstance(prev, dict) else {}
            merged_metrics.update(incoming_metrics)
            base["__mandate_metrics"] = merged_metrics

        return base or None

    @staticmethod
    def _merge_cartola_mandate_allocation(
        *,
        cartola_json: str,
        existing_json: str | None,
        bank_code: str | None,
    ) -> dict[str, Any] | None:
        try:
            cartola_payload = json.loads(cartola_json)
        except (TypeError, ValueError):
            return None
        if not isinstance(cartola_payload, dict):
            return None

        existing_payload: dict[str, Any] = {}
        if existing_json:
            try:
                parsed_existing = json.loads(existing_json)
                if isinstance(parsed_existing, dict):
                    existing_payload = parsed_existing
            except (TypeError, ValueError):
                existing_payload = {}

        if not existing_payload:
            return cartola_payload

        merged = DataLoadingService._merge_mandate_report_allocation(
            base=cartola_payload,
            incoming_allocation=existing_payload,
            bank_code=bank_code,
        )
        return merged or cartola_payload

    @staticmethod
    def _payload_amount_and_unit(payload: Any) -> tuple[Decimal | None, str | None]:
        if isinstance(payload, dict):
            raw = (
                payload.get("value")
                or payload.get("total")
                or payload.get("ending")
                or payload.get("ending_value")
                or payload.get("market_value")
                or payload.get("amount")
            )
            raw_unit = payload.get("unit")
            unit = str(raw_unit).strip() if raw_unit is not None else None
        else:
            raw = payload
            unit = None
        return _safe_decimal(raw), unit

    @staticmethod
    def _payload_absolute_amount(payload: Any) -> Decimal | None:
        amount, unit = DataLoadingService._payload_amount_and_unit(payload)
        if amount is None:
            return None
        if str(unit or "").strip() == "%":
            return None
        return amount

    @staticmethod
    def _units_compatible(unit_a: str | None, unit_b: str | None) -> bool:
        a = str(unit_a or "").strip()
        b = str(unit_b or "").strip()
        if not a or not b:
            return True
        return a == b

    @staticmethod
    def _coalesced_unit(*units: str | None) -> str | None:
        non_empty = [str(unit).strip() for unit in units if str(unit or "").strip()]
        if not non_empty:
            return None
        first = non_empty[0]
        if all(unit == first for unit in non_empty):
            return first
        return None

    @staticmethod
    def _adjust_equity_split_entries(
        *,
        us_entry: tuple[Decimal, str | None] | None,
        non_us_entry: tuple[Decimal, str | None] | None,
        global_entry: tuple[Decimal, str | None] | None,
    ) -> tuple[tuple[Decimal, str | None] | None, tuple[Decimal, str | None] | None]:
        if global_entry is None:
            return us_entry, non_us_entry

        unit = DataLoadingService._coalesced_unit(
            us_entry[1] if us_entry else None,
            non_us_entry[1] if non_us_entry else None,
            global_entry[1],
        )
        if unit is None and any(
            str(raw_unit or "").strip()
            for raw_unit in (
                us_entry[1] if us_entry else None,
                non_us_entry[1] if non_us_entry else None,
                global_entry[1],
            )
        ):
            return us_entry, non_us_entry

        global_value = global_entry[0]
        global_non_us = global_value / Decimal("3")
        global_us = global_value - global_non_us
        us_value = (us_entry[0] if us_entry else Decimal("0")) + global_us
        non_us_value = (non_us_entry[0] if non_us_entry else Decimal("0")) + global_non_us
        return (us_value, unit), (non_us_value, unit)

    @staticmethod
    def _normalized_split(
        a_value: Decimal | None,
        b_value: Decimal | None,
    ) -> tuple[Decimal, Decimal] | None:
        a = max(a_value or Decimal("0"), Decimal("0"))
        b = max(b_value or Decimal("0"), Decimal("0"))
        total = a + b
        if total <= 0:
            return None
        a_ratio = a / total
        return a_ratio, Decimal("1") - a_ratio

    @staticmethod
    def _split_from_parent(
        *,
        child_value: Decimal | None,
        parent_value: Decimal | None,
    ) -> tuple[Decimal, Decimal] | None:
        if child_value is None or parent_value is None or parent_value <= 0:
            return None
        ratio = child_value / parent_value
        if ratio < 0:
            ratio = Decimal("0")
        if ratio > 1:
            ratio = Decimal("1")
        return ratio, Decimal("1") - ratio

    @staticmethod
    def _canonical_amount_payload(value: Decimal) -> dict[str, str]:
        clean = max(value, Decimal("0"))
        return {"value": str(clean)}

    @staticmethod
    def _absolute_macro_amount(base: dict[str, Any], *labels: str) -> Decimal | None:
        for label in labels:
            if label not in base:
                continue
            amount = DataLoadingService._payload_absolute_amount(base.get(label))
            if amount is not None:
                return amount
        return None

    @staticmethod
    def _drop_category_keys(
        *,
        payload: dict[str, Any],
        bank_code: str | None,
        categories: set[str],
    ) -> None:
        for key in list(payload.keys()):
            if str(key).startswith("__"):
                continue
            cat = classify_mandate_asset_label(label=str(key), bank_code=bank_code)
            if cat in categories:
                payload.pop(key, None)

    @staticmethod
    def _merge_mandate_report_allocation(
        *,
        base: dict[str, Any],
        incoming_allocation: dict[str, Any],
        bank_code: str | None,
        ending_value: Decimal | None = None,
    ) -> dict[str, Any]:
        merged = dict(base)

        incoming_by_category: dict[str, tuple[Decimal, str | None]] = {}
        incoming_payload_by_category: dict[str, Any] = {}
        incoming_metrics_payload: dict[str, Any] | None = None

        for key, value in incoming_allocation.items():
            if key == "__mandate_metrics" and isinstance(value, dict):
                incoming_metrics_payload = value
                continue
            if str(key).startswith("__"):
                merged[key] = value
                continue

            amount, unit = DataLoadingService._payload_amount_and_unit(value)
            category = classify_mandate_asset_label(label=str(key), bank_code=bank_code)
            if category:
                incoming_payload_by_category[category] = value
            if category and amount is not None:
                incoming_by_category[category] = (amount, unit)

        if incoming_metrics_payload:
            prev = merged.get("__mandate_metrics")
            metrics = dict(prev) if isinstance(prev, dict) else {}
            metrics.update(incoming_metrics_payload)
            merged["__mandate_metrics"] = metrics

        fixed_total = DataLoadingService._absolute_macro_amount(merged, "Fixed Income")
        equities_total = DataLoadingService._absolute_macro_amount(merged, "Equities")

        ig_entry = incoming_by_category.get(MANDATE_CATEGORY_IG_FIXED)
        hy_entry = incoming_by_category.get(MANDATE_CATEGORY_HY_FIXED)
        fixed_entry = incoming_by_category.get(MANDATE_CATEGORY_FIXED)
        us_entry = incoming_by_category.get(MANDATE_CATEGORY_US_EQUITIES)
        non_us_entry = incoming_by_category.get(MANDATE_CATEGORY_NON_US_EQUITIES)
        global_entry = incoming_by_category.get(MANDATE_CATEGORY_GLOBAL_EQUITIES)
        equities_entry = incoming_by_category.get(MANDATE_CATEGORY_EQUITIES)

        if (
            str(bank_code or "").strip().lower() == "jpmorgan"
            and ig_entry is not None
            and hy_entry is None
            and fixed_entry is None
            and DataLoadingService._payload_amount_and_unit(merged.get("High Yield Fixed Income"))[0] is not None
        ):
            # JPM Investment Review may carry a one-sided IG view; preserve prior HG/HY split from complementario.
            ig_entry = None

        fixed_split: tuple[Decimal, Decimal] | None = None
        if ig_entry and hy_entry and DataLoadingService._units_compatible(ig_entry[1], hy_entry[1]):
            fixed_split = DataLoadingService._normalized_split(ig_entry[0], hy_entry[0])
        elif ig_entry and fixed_entry and DataLoadingService._units_compatible(ig_entry[1], fixed_entry[1]):
            fixed_split = DataLoadingService._split_from_parent(
                child_value=ig_entry[0],
                parent_value=fixed_entry[0],
            )
        elif hy_entry and fixed_entry and DataLoadingService._units_compatible(hy_entry[1], fixed_entry[1]):
            hy_first = DataLoadingService._split_from_parent(
                child_value=hy_entry[0],
                parent_value=fixed_entry[0],
            )
            if hy_first is not None:
                fixed_split = (hy_first[1], hy_first[0])

        adj_us_entry, adj_non_us_entry = DataLoadingService._adjust_equity_split_entries(
            us_entry=us_entry,
            non_us_entry=non_us_entry,
            global_entry=global_entry,
        )

        eq_split: tuple[Decimal, Decimal] | None = None
        if (
            adj_us_entry
            and adj_non_us_entry
            and DataLoadingService._units_compatible(adj_us_entry[1], adj_non_us_entry[1])
        ):
            eq_split = DataLoadingService._normalized_split(adj_us_entry[0], adj_non_us_entry[0])
        elif (
            adj_us_entry
            and equities_entry
            and DataLoadingService._units_compatible(adj_us_entry[1], equities_entry[1])
        ):
            eq_split = DataLoadingService._split_from_parent(
                child_value=adj_us_entry[0],
                parent_value=equities_entry[0],
            )
        elif (
            adj_non_us_entry
            and equities_entry
            and DataLoadingService._units_compatible(adj_non_us_entry[1], equities_entry[1])
        ):
            non_us_first = DataLoadingService._split_from_parent(
                child_value=adj_non_us_entry[0],
                parent_value=equities_entry[0],
            )
            if non_us_first is not None:
                eq_split = (non_us_first[1], non_us_first[0])

        if fixed_total is not None and fixed_split is not None:
            DataLoadingService._drop_category_keys(
                payload=merged,
                bank_code=bank_code,
                categories={
                    MANDATE_CATEGORY_IG_FIXED,
                    MANDATE_CATEGORY_HY_FIXED,
                },
            )
            ig_amount = fixed_total * fixed_split[0]
            hy_amount = fixed_total - ig_amount
            merged["Investment Grade Fixed Income"] = DataLoadingService._canonical_amount_payload(ig_amount)
            merged["High Yield Fixed Income"] = DataLoadingService._canonical_amount_payload(hy_amount)
        elif fixed_total is not None and ending_value is not None and ending_value > 0:
            if ig_entry and str(ig_entry[1] or "").strip() == "%" and hy_entry is None:
                ig_amount = min((ending_value * ig_entry[0]) / Decimal("100"), fixed_total)
                merged["Investment Grade Fixed Income"] = DataLoadingService._canonical_amount_payload(ig_amount)
                merged["High Yield Fixed Income"] = DataLoadingService._canonical_amount_payload(fixed_total - ig_amount)
            elif hy_entry and str(hy_entry[1] or "").strip() == "%" and ig_entry is None:
                hy_amount = min((ending_value * hy_entry[0]) / Decimal("100"), fixed_total)
                merged["Investment Grade Fixed Income"] = DataLoadingService._canonical_amount_payload(fixed_total - hy_amount)
                merged["High Yield Fixed Income"] = DataLoadingService._canonical_amount_payload(hy_amount)

        if equities_total is not None and eq_split is not None:
            DataLoadingService._drop_category_keys(
                payload=merged,
                bank_code=bank_code,
                categories={
                    MANDATE_CATEGORY_US_EQUITIES,
                    MANDATE_CATEGORY_NON_US_EQUITIES,
                    MANDATE_CATEGORY_GLOBAL_EQUITIES,
                },
            )
            us_amount = equities_total * eq_split[0]
            non_us_amount = equities_total - us_amount
            merged["US Equities"] = DataLoadingService._canonical_amount_payload(us_amount)
            merged["Non US Equities"] = DataLoadingService._canonical_amount_payload(non_us_amount)
        elif equities_total is not None and ending_value is not None and ending_value > 0:
            if adj_us_entry and str(adj_us_entry[1] or "").strip() == "%" and adj_non_us_entry is None:
                us_amount = min((ending_value * adj_us_entry[0]) / Decimal("100"), equities_total)
                merged["US Equities"] = DataLoadingService._canonical_amount_payload(us_amount)
                merged["Non US Equities"] = DataLoadingService._canonical_amount_payload(equities_total - us_amount)
            elif adj_non_us_entry and str(adj_non_us_entry[1] or "").strip() == "%" and adj_us_entry is None:
                non_us_amount = min((ending_value * adj_non_us_entry[0]) / Decimal("100"), equities_total)
                merged["US Equities"] = DataLoadingService._canonical_amount_payload(equities_total - non_us_amount)
                merged["Non US Equities"] = DataLoadingService._canonical_amount_payload(non_us_amount)

        return merged

    def _apply_pdf_report_metrics_series(
        self,
        *,
        account: Account,
        metrics_series: list[Any],
        raw_document_id: int,
        stats: dict[str, Any],
    ) -> set[int]:
        touched_years: set[int] = set()
        for item in metrics_series:
            if not isinstance(item, dict):
                continue
            year = int(item.get("year") or 0)
            month = int(item.get("month") or 0)
            if year <= 0 or month < 1 or month > 12:
                continue

            duration_val = _safe_decimal(item.get("fixed_income_duration"))
            yield_val = _safe_decimal(item.get("fixed_income_yield"))
            if duration_val is None and yield_val is None:
                continue

            existing = (
                self.db.query(MonthlyClosing)
                .filter(
                    MonthlyClosing.account_id == account.id,
                    MonthlyClosing.year == year,
                    MonthlyClosing.month == month,
                )
                .first()
            )
            if existing is None:
                stats["skipped"] += 1
                continue

            metrics_payload: dict[str, Any] = {}
            source = str(item.get("source") or "pdf_report_series")
            if duration_val is not None:
                metrics_payload["fixed_income_duration"] = {
                    "value": float(duration_val),
                    "unit": "years",
                    "source": source,
                }
            if yield_val is not None:
                metrics_payload["fixed_income_yield"] = {
                    "value": float(yield_val),
                    "unit": str(item.get("yield_unit") or "%"),
                    "source": source,
                }

            merged = self._merge_asset_allocation_payload(
                existing_json=existing.asset_allocation_json,
                incoming_allocation=None,
                incoming_metrics=metrics_payload,
                bank_code=account.bank_code,
                account_type=account.account_type,
                ending_value=_safe_decimal(existing.net_value),
            )
            if merged is None:
                continue
            existing.asset_allocation_json = json.dumps(merged)
            stats["monthly_closings_updated"] += 1
            touched_years.add(year)

        if touched_years:
            self._log(
                "load",
                "info",
                (
                    f"Serie duration/yield aplicada para {account.bank_code}/{account.account_number}: "
                    f"{len(touched_years)} ano(s) afectados"
                ),
                raw_document_id=raw_document_id,
                account_id=account.id,
            )
        return touched_years
    # ═══════════════════════════════════════════════════════════════
    # RESOLUCIÓN DE CUENTAS
    # ═══════════════════════════════════════════════════════════════

    def _resolve_accounts(
        self, result: ParseResult, doc: RawDocument
    ) -> list[Account]:
        """
        Busca las cuentas en el maestro que corresponden al ParseResult.

        Estrategia:
        1. Si hay account_numbers (multi-cuenta), buscar cada uno
        2. Si hay account_number único, buscar ese
        3. Si no, intentar encontrar por bank_code + file_type del documento
        """
        accounts: list[Account] = []
        parser_scoped_types = self._scoped_account_types_for_parser(result.parser_name)

        def _query_account_by_number(acct_num: str) -> Optional[Account]:
            q = self.db.query(Account).filter(Account.account_number == acct_num)
            if parser_scoped_types:
                q = q.filter(Account.account_type.in_(parser_scoped_types))
            return q.first()

        # Multi-cuenta
        if result.account_numbers:
            for acct_num in result.account_numbers:
                acct = _query_account_by_number(acct_num)
                if acct:
                    accounts.append(acct)
                else:
                    logger.warning(
                        "Cuenta %s no encontrada en maestro%s",
                        acct_num,
                        (
                            f" (scope parser={result.parser_name}, tipos={parser_scoped_types})"
                            if parser_scoped_types
                            else ""
                        ),
                    )
        # Cuenta única
        elif result.account_number and result.account_number != "Varios":
            acct = _query_account_by_number(result.account_number)
            if acct:
                accounts.append(acct)

        # Fallback: si el documento tiene account_id asignado
        if not accounts and doc.account_id:
            acct = (
                self.db.query(Account)
                .filter(Account.id == doc.account_id)
                .first()
            )
            if acct and (
                not parser_scoped_types or acct.account_type in parser_scoped_types
            ):
                accounts.append(acct)
            elif acct:
                logger.warning(
                    "Documento %s apunta a cuenta %s con tipo %s fuera del scope parser=%s (%s)",
                    doc.id,
                    acct.account_number,
                    acct.account_type,
                    result.parser_name,
                    parser_scoped_types,
                )

        return accounts

    @staticmethod
    def _scoped_account_types_for_parser(parser_name: str | None) -> list[str] | None:
        """
        Aislamiento explícito para parsers JPM con paquetes que contienen subcuentas mixtas.
        Evita que un PDF ETF actualice cuentas Brokerage y viceversa.
        """
        key = (parser_name or "").strip().lower()
        mapping = {
            "parsers.jpmorgan.etf": ["etf"],
            "parsers.jpmorgan.brokerage": ["brokerage"],
        }
        return mapping.get(key)

    # ═══════════════════════════════════════════════════════════════
    # PARSED STATEMENTS
    # ═══════════════════════════════════════════════════════════════

    def _upsert_parsed_statement(
        self,
        result: ParseResult,
        doc: RawDocument,
        account: Account,
        parser_version_id: int,
    ) -> bool:
        """Crea o actualiza un ParsedStatement."""
        if not result.statement_date:
            return False

        period_start = result.period_start or result.statement_date
        period_end = result.period_end or result.statement_date

        # Buscar existente (UNIQUE: raw_document_id, account_id, statement_date)
        existing = (
            self.db.query(ParsedStatement)
            .filter(
                ParsedStatement.raw_document_id == doc.id,
                ParsedStatement.account_id == account.id,
                ParsedStatement.statement_date == result.statement_date,
            )
            .first()
        )

        # Serializar datos completos
        parsed_json = self._serialize_parse_result(result, account)

        if existing:
            existing.period_start = period_start
            existing.period_end = period_end
            existing.opening_balance = _safe_decimal(result.opening_balance)
            existing.closing_balance = _safe_decimal(result.closing_balance)
            existing.total_credits = _safe_decimal(result.total_credits)
            existing.total_debits = _safe_decimal(result.total_debits)
            existing.currency = result.currency or account.currency
            existing.parsed_data_json = parsed_json
            existing.parser_version_id = parser_version_id
        else:
            ps = ParsedStatement(
                raw_document_id=doc.id,
                account_id=account.id,
                statement_date=result.statement_date,
                period_start=period_start,
                period_end=period_end,
                opening_balance=_safe_decimal(result.opening_balance),
                closing_balance=_safe_decimal(result.closing_balance),
                total_credits=_safe_decimal(result.total_credits),
                total_debits=_safe_decimal(result.total_debits),
                currency=result.currency or account.currency,
                parsed_data_json=parsed_json,
                parser_version_id=parser_version_id,
            )
            self.db.add(ps)

        return True

    # ═══════════════════════════════════════════════════════════════
    # MONTHLY CLOSINGS
    # ═══════════════════════════════════════════════════════════════

    def _upsert_monthly_closing(
        self,
        result: ParseResult,
        doc: RawDocument,
        account: Account,
    ) -> bool:
        """Crea o actualiza un MonthlyClosing."""
        if not result.statement_date:
            return False

        # UBS Suiza: quarter-end statements expose prior months in Performance table.
        # Those rows may refine prior-month movements, but the auditable month-end
        # balance still comes from the monthly closing already persisted for that month.
        self._upsert_ubs_historical_monthly_activity(result, doc, account)
        # Important with Session(autoflush=False): ensure historical rows are visible
        # to the UNIQUE(account_id, year, month) lookup below.
        self.db.flush()

        closing_date = result.period_end or result.statement_date
        year = closing_date.year
        month = closing_date.month

        # Para cuentas multi-cuenta, buscar valores específicos en qualitative_data
        account_values = self._get_account_specific_values(result, account)

        # Determinar closing_balance para esta cuenta
        closing_bal = account_values.get("ending_value")
        if closing_bal is None:
            # Si es cuenta única, usar el total
            if len(result.account_numbers or []) <= 1:
                closing_bal = _safe_decimal(result.closing_balance)
        if closing_bal is None and (result.bank_code or doc.bank_code) == "ubs":
            # UBS Suiza: preferir siempre el portafolio seleccionado antes del
            # total agregado de la relación bancaria.
            balances = result.balances or {}
            selected_portfolio = balances.get("selected_portfolio")
            if isinstance(selected_portfolio, dict):
                closing_bal = _safe_decimal(selected_portfolio.get("net_assets"))
            if closing_bal is None:
                suffix_match = re.search(r"-(\d{2})$", account.account_number or "")
                if suffix_match:
                    portfolios = balances.get("portfolios")
                    if isinstance(portfolios, dict):
                        pdata = portfolios.get(f"Portfolio{suffix_match.group(1)}")
                        if isinstance(pdata, dict):
                            closing_bal = _safe_decimal(pdata.get("net_assets"))
            if closing_bal is None:
                # Fallback legacy: total de la página, solo si no hubo forma de
                # identificar el portafolio puntual.
                closing_bal = _safe_decimal(balances.get("total_net_assets"))

        opening_bal = account_values.get("beginning_value")
        if opening_bal is None:
            if len(result.account_numbers or []) <= 1:
                opening_bal = _safe_decimal(result.opening_balance)

        # Income y cambios de valor
        income = account_values.get("income")
        change_in_value = account_values.get("change_investment")
        accrual = account_values.get("accrual")
        for note in account_values.get("interpretation_notes", []):
            self._log(
                "load",
                "info",
                (
                    f"Heurística mensual aplicada {account.bank_code}/{account.account_number} "
                    f"{year}-{month:02d}: {note}"
                ),
                raw_document_id=doc.id,
                account_id=account.id,
            )

        parsed_rows = self._rows_for_account(result, account)

        # Asset allocation (normalizado para Mandatos, por cuenta cuando aplica).
        raw_asset_alloc = self._resolve_asset_allocation_for_account(
            result=result,
            account=account,
            account_values=account_values,
        )
        if account.account_type == "mandato":
            # Wellington custody cartolas entregan el split IG/HY/RV real
            # por sub-fondo → no usar macro_only para preservar el detalle.
            _macro = account.bank_code != "wellington"
            asset_alloc = _normalize_mandate_asset_allocation(
                raw_asset_alloc,
                bank_code=account.bank_code or "",
                macro_only=_macro,
            )
        elif account.bank_code == "jpmorgan" and account.account_type == "brokerage":
            asset_alloc = self._derive_jpm_brokerage_asset_allocation(
                account=account,
                raw_asset_alloc=raw_asset_alloc,
                parsed_rows=parsed_rows,
                year=year,
                month=month,
                source_document_id=doc.id,
            )
        else:
            asset_alloc = raw_asset_alloc

        if (
            closing_bal is None
            and account.bank_code == "jpmorgan"
            and account.account_type == "brokerage"
        ):
            closing_from_alloc = _asset_allocation_total_value(asset_alloc)
            if closing_from_alloc is not None:
                closing_bal = closing_from_alloc
        asset_alloc_json = json.dumps(asset_alloc) if asset_alloc else None

        parsed_bank_code = result.bank_code or doc.bank_code

        # UNIQUE: account_id, year, month
        existing = (
            self.db.query(MonthlyClosing)
            .filter(
                MonthlyClosing.account_id == account.id,
                MonthlyClosing.year == year,
                MonthlyClosing.month == month,
            )
            .first()
        )

        # SALVAGUARDA: un pdf_report NUNCA puede sobreescribir net_value,
        # total_assets, income, change_in_value ni source_document_id de un
        # registro que ya tiene net_value confirmado (proveniente de una
        # cartola oficial).  El reporte de gestión solo puede enriquecer
        # asset_allocation_json con los sub-splits del portafolio.
        is_report_doc = (doc.file_type or "").lower() == "pdf_report"
        _preserve_financials = is_report_doc and existing is not None and existing.net_value is not None
        if _preserve_financials:
            closing_bal = existing.net_value
            income = existing.income
            change_in_value = existing.change_in_value
            accrual = existing.accrual

        if existing:
            if account.account_type == "mandato" and asset_alloc_json and existing.asset_allocation_json:
                preserved = self._merge_cartola_mandate_allocation(
                    cartola_json=asset_alloc_json,
                    existing_json=existing.asset_allocation_json,
                    bank_code=account.bank_code,
                )
                if preserved is not None:
                    asset_alloc_json = json.dumps(preserved)
            existing.closing_date = closing_date
            existing.total_assets = closing_bal
            existing.net_value = closing_bal
            existing.currency = result.currency or account.currency
            if parsed_bank_code == "ubs":
                # En UBS Suiza, ending value es auditable por cartola mensual.
                # Movimientos pueden ser refinados luego por tablas trimestrales UBS,
                # y la utilidad final siempre se recalcula por identidad.
                existing.income = income
                existing.change_in_value = change_in_value
            else:
                if income is not None:
                    existing.income = income
                if change_in_value is not None:
                    existing.change_in_value = change_in_value
            if accrual is not None:
                existing.accrual = accrual
            existing.asset_allocation_json = asset_alloc_json
            if not _preserve_financials:
                existing.source_document_id = doc.id
            if opening_bal is not None:
                existing.total_liabilities = None  # No aplica a ETF
        else:
            mc = MonthlyClosing(
                account_id=account.id,
                closing_date=closing_date,
                year=year,
                month=month,
                total_assets=closing_bal,
                total_liabilities=None,
                net_value=closing_bal,
                currency=result.currency or account.currency,
                income=income,
                change_in_value=change_in_value,
                accrual=accrual,
                asset_allocation_json=asset_alloc_json,
                source_document_id=doc.id,
            )
            self.db.add(mc)

        # Cuando la salvaguarda preserva datos financieros existentes, el source
        # de la capa normalizada debe apuntar al documento original (la cartola),
        # no al pdf_report que solo aportó sub-splits de asset allocation.
        effective_source_doc_id = (
            (existing.source_document_id if existing is not None else None) or doc.id
            if _preserve_financials
            else doc.id
        )

        # Persistir capa canónica mensual (Fase 1 normalización).
        self._upsert_monthly_metric_normalized(
            account=account,
            year=year,
            month=month,
            closing_date=closing_date,
            currency=result.currency or account.currency,
            source_document_id=effective_source_doc_id,
            account_values=account_values,
            closing_bal=closing_bal,
            accrual=accrual,
            movements=change_in_value,
            profit=income,
            asset_alloc_json=asset_alloc_json,
            parsed_rows=parsed_rows,
        )

        self._recompute_ubs_income_from_identity(
            account=account,
            year=year,
            month=month,
        )
        refresh_years = {year}
        if account.bank_code == "ubs":
            refresh_years |= self._recompute_ubs_income_series(account=account)
        self._validate_ytd_consistency(
            account=account,
            year=year,
            month=month,
            account_values=account_values,
            raw_document_id=doc.id,
        )
        self._reconcile_account_ytd_series(
            account=account,
            year=year,
            raw_document_id=doc.id,
        )
        for refresh_year in sorted(refresh_years):
            self._refresh_normalized_activity_from_monthly_closings(
                account=account,
                year=refresh_year,
            )

        return True

    @staticmethod
    def _account_asset_class(account: Account) -> str | None:
        try:
            metadata = json.loads(account.metadata_json or "{}")
        except (TypeError, ValueError):
            metadata = {}
        raw = metadata.get("asset_class")
        if raw is None:
            return None
        return str(raw).strip() or None

    def _load_etf_instrument_amounts(
        self,
        *,
        account: Account,
        year: int,
        month: int,
    ) -> dict[str, Decimal]:
        rows = (
            self.db.query(EtfComposition.etf_name, EtfComposition.market_value_usd, EtfComposition.market_value)
            .filter(
                EtfComposition.account_id == account.id,
                EtfComposition.year == year,
                EtfComposition.month == month,
            )
            .all()
        )
        grouped: dict[str, Decimal] = {}
        for etf_name, market_value_usd, market_value in rows:
            instrument = normalize_etf_instrument(str(etf_name or "").strip())
            amount = to_decimal(market_value_usd)
            if amount is None:
                amount = to_decimal(market_value)
            if amount is None or amount <= 0:
                continue
            grouped[instrument] = grouped.get(instrument, Decimal("0")) + amount
        return grouped

    @staticmethod
    def _canonical_from_etf_instruments(instrument_amounts: dict[str, Decimal]) -> dict[str, Decimal]:
        canonical = {
            "Cash, Deposits & Money Market": Decimal("0"),
            "Investment Grade Fixed Income": Decimal("0"),
            "High Yield Fixed Income": Decimal("0"),
            "US Equities": Decimal("0"),
            "Non US Equities": Decimal("0"),
            "Private Equity": Decimal("0"),
            "Real Estate": Decimal("0"),
        }
        for instrument, amount in instrument_amounts.items():
            if amount <= 0:
                continue
            bucket = classify_etf_asset_bucket(instrument, normalized_name=instrument)
            if bucket == "Caja":
                canonical["Cash, Deposits & Money Market"] += amount
            elif bucket == "HY":
                canonical["High Yield Fixed Income"] += amount
            elif bucket in {"RF IG Short", "RF IG Long", "RF IG", "Non US RF", "RF"}:
                canonical["Investment Grade Fixed Income"] += amount
            elif bucket == "RV EM":
                canonical["Non US Equities"] += amount
            elif bucket in {"RV DM", "Global Equity"}:
                # Global Equity (ej. IWDA / MSCI World): regla 2/3 US + 1/3 Non-US.
                canonical["US Equities"] += amount * Decimal("2") / Decimal("3")
                canonical["Non US Equities"] += amount * Decimal("1") / Decimal("3")
            elif bucket in {"Real Estate", "RE"}:
                canonical["Real Estate"] += amount
            elif bucket in {"Alternativos", "PE"}:
                canonical["Private Equity"] += amount
            else:
                canonical["Non US Equities"] += amount

        return canonical

    @staticmethod
    def _reporting_value_exclusion_rule(account: Account) -> dict[str, Any] | None:
        key = (
            str(account.bank_code or "").strip().lower(),
            str(account.account_type or "").strip().lower(),
            str(account.account_number or "").strip(),
        )
        return _REPORTING_VALUE_EXCLUSION_RULES.get(key)

    @staticmethod
    def _apply_reporting_value_exclusion_payload(
        *,
        account: Account,
        year: int,
        month: int,
        payload: dict | list | None,
        ending_value_with_accrual: Decimal | None,
        ending_value_without_accrual: Decimal | None,
        db: Session,
    ) -> dict | list | None:
        rule = DataLoadingService._reporting_value_exclusion_rule(account)
        if rule is None:
            return payload
        if not isinstance(payload, dict):
            return payload

        labels_norm = {
            str(token).strip().lower()
            for token in (rule.get("labels_norm") or set())
            if str(token).strip()
        }
        if not labels_norm:
            return payload

        components: list[dict[str, str]] = []
        total_excluded = Decimal("0")
        for raw_label, raw_value in payload.items():
            if str(raw_label).startswith("__"):
                continue
            label_norm = _normalize_alloc_label(raw_label)
            if label_norm not in labels_norm:
                continue
            amount = _extract_allocation_amount(
                raw_value,
                ending_value_with_accrual=ending_value_with_accrual,
            )
            if amount is None or amount <= 0:
                continue
            total_excluded += amount
            components.append(
                {
                    "rule_id": str(rule.get("rule_id") or ""),
                    "label": str(raw_label),
                    "amount_usd": str(amount),
                }
            )

        rule_id = str(rule.get("rule_id") or "")
        if not components and rule_id == "telmar_gs_mandato_private_equity_duplicated_in_alternatives":
            month_start = date(year, month, 1)
            next_month = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
            ps_row = (
                db.query(ParsedStatement.parsed_data_json)
                .filter(
                    ParsedStatement.account_id == account.id,
                    ParsedStatement.statement_date >= month_start,
                    ParsedStatement.statement_date < next_month,
                )
                .order_by(ParsedStatement.id.desc())
                .first()
            )
            if ps_row and ps_row[0]:
                try:
                    ps_payload = json.loads(ps_row[0] or "{}")
                except (TypeError, ValueError):
                    ps_payload = {}
                rows = ps_payload.get("rows") if isinstance(ps_payload, dict) else None
                token = "weststreetcapitalpartnersviioffshore"
                if isinstance(rows, list):
                    for row in rows:
                        if not isinstance(row, dict):
                            continue
                        instrument = str(row.get("instrument") or "").strip()
                        instrument_norm = _normalize_alloc_label(instrument)
                        if token not in instrument_norm:
                            continue
                        amount = _safe_decimal(
                            row.get("market_value")
                            or row.get("value")
                            or row.get("amount")
                        )
                        if amount is None or amount <= 0:
                            continue
                        total_excluded += amount
                        components.append(
                            {
                                "rule_id": rule_id,
                                "label": instrument or "Private Equity",
                                "amount_usd": str(amount),
                            }
                        )
                        break

        if not components and rule_id == "telmar_jpm_brokerage_alternative_assets_duplicated_in_alternatives":
            alloc_total = _asset_allocation_total_value(payload)
            base_ending = ending_value_without_accrual
            if base_ending is None:
                base_ending = ending_value_with_accrual
            if alloc_total is not None and base_ending is not None:
                residual = base_ending - alloc_total
                if residual > Decimal("1"):
                    total_excluded += residual
                    components.append(
                        {
                            "rule_id": rule_id,
                            "label": "Alternative Assets (residual vs ending without accrual)",
                            "amount_usd": str(residual),
                        }
                    )

        next_payload = dict(payload)
        if total_excluded <= 0 or not components:
            next_payload.pop(_REPORTING_VALUE_EXCLUSION_KEY, None)
            return next_payload

        next_payload[_REPORTING_VALUE_EXCLUSION_KEY] = {
            "version": "1.0.0",
            "applied_total_usd": str(total_excluded),
            "period": f"{year:04d}-{month:02d}",
            "description": str(rule.get("description") or ""),
            "components": components,
        }
        return next_payload

    def _compose_normalized_asset_allocation_json(
        self,
        *,
        account: Account,
        year: int,
        month: int,
        ending_value_with_accrual: Decimal | None,
        ending_value_without_accrual: Decimal | None = None,
        raw_asset_allocation_json: str | None,
    ) -> str | None:
        raw_payload = decode_asset_allocation_json(raw_asset_allocation_json)
        raw_payload = self._apply_reporting_value_exclusion_payload(
            account=account,
            year=year,
            month=month,
            payload=raw_payload,
            ending_value_with_accrual=ending_value_with_accrual,
            ending_value_without_accrual=ending_value_without_accrual,
            db=self.db,
        )

        account_type = str(account.account_type or "").strip().lower()
        if account_type not in {"mandato", "etf"}:
            if raw_payload is None:
                return raw_asset_allocation_json
            return json.dumps(raw_payload)

        canonical_amounts = canonical_breakdown_from_payload(
            payload=raw_payload,
            ending_value=ending_value_with_accrual,
            bank_code=account.bank_code,
            account_type=account.account_type,
            fallback_asset_class=self._account_asset_class(account),
        )

        instrument_amounts: dict[str, Decimal] | None = None
        if str(account.account_type or "").strip().lower() == "etf":
            loaded_instruments = self._load_etf_instrument_amounts(account=account, year=year, month=month)
            if loaded_instruments:
                instrument_amounts = loaded_instruments
                canonical_from_instruments = self._canonical_from_etf_instruments(loaded_instruments)
                if any(value > 0 for value in canonical_from_instruments.values()):
                    canonical_amounts = canonical_from_instruments

        fi_metrics = extract_fi_metrics(raw_payload)
        composed = compose_asset_allocation_payload(
            raw_payload=raw_payload,
            canonical_amounts=canonical_amounts,
            ending_value=ending_value_with_accrual,
            instrument_amounts=instrument_amounts,
            fi_metrics=fi_metrics or None,
        )
        if not composed:
            return None
        return json.dumps(composed)

    def _upsert_monthly_metric_normalized(
        self,
        account: Account,
        year: int,
        month: int,
        closing_date: date,
        currency: str,
        source_document_id: int,
        account_values: dict,
        closing_bal: Optional[Decimal],
        accrual: Optional[Decimal],
        movements: Optional[Decimal],
        profit: Optional[Decimal],
        asset_alloc_json: Optional[str],
        parsed_rows: list[dict[str, Any]] | None = None,
    ) -> None:
        """
        Upsert de capa canónica mensual con campos explícitos de accrual.
        """
        end_w = _safe_decimal(account_values.get("ending_value_with_accrual"))
        end_wo = _safe_decimal(account_values.get("ending_value_without_accrual"))
        accr = _safe_decimal(accrual)
        closing = _safe_decimal(closing_bal)

        if end_w is None and end_wo is None:
            end_w = closing
            if closing is not None and accr is not None:
                end_wo = closing - accr
            else:
                end_wo = closing
        elif end_w is None and end_wo is not None:
            if accr is not None:
                end_w = end_wo + accr
            else:
                end_w = end_wo
        elif end_wo is None and end_w is not None:
            if accr is not None:
                end_wo = end_w - accr
            else:
                end_wo = end_w

        cash_value = self._resolve_normalized_cash_value(
            account=account,
            year=year,
            month=month,
            asset_alloc_json=asset_alloc_json,
            source_document_id=source_document_id,
            parsed_rows=parsed_rows,
        )

        movements_ytd = _safe_decimal(account_values.get("change_investment_ytd"))
        profit_ytd = _safe_decimal(account_values.get("income_ytd"))
        normalized_alloc_json = self._compose_normalized_asset_allocation_json(
            account=account,
            year=year,
            month=month,
            ending_value_with_accrual=end_w,
            ending_value_without_accrual=end_wo,
            raw_asset_allocation_json=asset_alloc_json,
        )

        existing = (
            self.db.query(MonthlyMetricNormalized)
            .filter(
                MonthlyMetricNormalized.account_id == account.id,
                MonthlyMetricNormalized.year == year,
                MonthlyMetricNormalized.month == month,
            )
            .first()
        )

        payload = {
            "closing_date": closing_date,
            "ending_value_with_accrual": end_w,
            "ending_value_without_accrual": end_wo,
            "accrual_ending": accr,
            "cash_value": cash_value,
            "movements_net": _safe_decimal(movements),
            "profit_period": _safe_decimal(profit),
            "movements_ytd": movements_ytd,
            "profit_ytd": profit_ytd,
            "asset_allocation_json": normalized_alloc_json,
            "currency": currency,
            "source_document_id": source_document_id,
        }
        if existing:
            for k, v in payload.items():
                setattr(existing, k, v)
        else:
            self.db.add(
                MonthlyMetricNormalized(
                    account_id=account.id,
                    year=year,
                    month=month,
                    **payload,
                )
            )

    def _refresh_normalized_activity_from_monthly_closings(
        self,
        account: Account,
        year: int,
    ) -> None:
        """
        Mantiene capa normalizada sincronizada con MonthlyClosing tras ajustes YTD/prior period.
        No sobreescribe ending with/without accrual si ya existen.
        """
        closings = (
            self.db.query(MonthlyClosing)
            .filter(
                MonthlyClosing.account_id == account.id,
                MonthlyClosing.year == year,
            )
            .all()
        )
        if not closings:
            return

        existing_rows = (
            self.db.query(MonthlyMetricNormalized)
            .filter(
                MonthlyMetricNormalized.account_id == account.id,
                MonthlyMetricNormalized.year == year,
            )
            .all()
        )
        normalized_by_month = {row.month: row for row in existing_rows}

        for closing in closings:
            normalized = normalized_by_month.get(closing.month)

            existing_accrual = normalized.accrual_ending if normalized else None
            accrual_value = closing.accrual if closing.accrual is not None else existing_accrual

            existing_end_w = normalized.ending_value_with_accrual if normalized else None
            ending_with = existing_end_w if existing_end_w is not None else closing.net_value

            existing_end_wo = normalized.ending_value_without_accrual if normalized else None
            if existing_end_wo is not None:
                ending_without = existing_end_wo
            elif ending_with is not None and accrual_value is not None:
                ending_without = ending_with - accrual_value
            else:
                ending_without = ending_with

            existing_alloc = normalized.asset_allocation_json if normalized else None
            # Keep normalized layer synced with MonthlyClosing allocation updates
            # (e.g. mandate report enrichments IG/HY, US/Non-US, duration/yield).
            alloc_json = (
                closing.asset_allocation_json
                if closing.asset_allocation_json is not None
                else existing_alloc
            )
            alloc_payload = None
            if alloc_json:
                try:
                    decoded = json.loads(alloc_json)
                    if isinstance(decoded, (dict, list)):
                        alloc_payload = decoded
                except (TypeError, ValueError):
                    alloc_payload = None
            if account.bank_code == "jpmorgan" and account.account_type == "brokerage":
                derived_alloc = self._derive_jpm_brokerage_asset_allocation(
                    account=account,
                    raw_asset_alloc=alloc_payload,
                    year=closing.year,
                    month=closing.month,
                    source_document_id=closing.source_document_id,
                )
                if derived_alloc:
                    alloc_json = json.dumps(derived_alloc)
                    closing.asset_allocation_json = alloc_json

            normalized_alloc_json = self._compose_normalized_asset_allocation_json(
                account=account,
                year=closing.year,
                month=closing.month,
                ending_value_with_accrual=to_decimal(ending_with),
                ending_value_without_accrual=to_decimal(ending_without),
                raw_asset_allocation_json=alloc_json,
            )

            existing_cash = normalized.cash_value if normalized else None
            cash_value = (
                existing_cash
                if existing_cash is not None
                else self._resolve_normalized_cash_value(
                    account=account,
                    year=closing.year,
                    month=closing.month,
                    asset_alloc_json=alloc_json,
                    source_document_id=closing.source_document_id,
                )
            )

            payload = {
                "closing_date": closing.closing_date,
                "ending_value_with_accrual": ending_with,
                "ending_value_without_accrual": ending_without,
                "accrual_ending": accrual_value,
                "cash_value": cash_value,
                "movements_net": closing.change_in_value,
                "profit_period": closing.income,
                "asset_allocation_json": normalized_alloc_json,
                "currency": closing.currency or account.currency,
                "source_document_id": closing.source_document_id,
            }
            if normalized:
                for key, value in payload.items():
                    setattr(normalized, key, value)
            else:
                self.db.add(
                    MonthlyMetricNormalized(
                        account_id=account.id,
                        year=year,
                        month=closing.month,
                        **payload,
                    )
                )

    def _resolve_asset_allocation_for_account(
        self,
        result: ParseResult,
        account: Account,
        account_values: dict,
    ) -> dict | list | None:
        """
        Obtiene asset allocation para la cuenta puntual.
        Prioridad:
        1) account_monthly_activity.asset_allocation (subcuentas multi-account)
        2) qualitative_data.asset_allocation
        3) fallback UBS desde balances.selected_portfolio / balances.portfolios
        """
        account_level = account_values.get("asset_allocation")
        if isinstance(account_level, (dict, list)) and account_level:
            return account_level

        top_level = result.qualitative_data.get("asset_allocation")
        if isinstance(top_level, (dict, list)) and top_level:
            return top_level

        if account.bank_code == "ubs":
            balances = result.balances or {}
            selected = balances.get("selected_portfolio")
            if isinstance(selected, dict):
                alloc = self._ubs_asset_allocation_from_portfolio_block(selected)
                if alloc:
                    return alloc

            suffix_match = re.search(r"-(\d{2})$", account.account_number or "")
            if suffix_match:
                key = f"Portfolio{suffix_match.group(1)}"
                portfolios = balances.get("portfolios")
                if isinstance(portfolios, dict):
                    pdata = portfolios.get(key)
                    if isinstance(pdata, dict):
                        alloc = self._ubs_asset_allocation_from_portfolio_block(pdata)
                        if alloc:
                            return alloc

        return None

    def _derive_jpm_brokerage_asset_allocation(
        self,
        *,
        account: Account,
        raw_asset_alloc: dict | list | None,
        parsed_rows: list[dict[str, Any]] | None = None,
        year: int | None = None,
        month: int | None = None,
        source_document_id: int | None = None,
    ) -> dict | list | None:
        if account.bank_code != "jpmorgan" or account.account_type != "brokerage":
            return raw_asset_alloc

        other_bucket_totals = self._brokerage_others_bucket_totals_from_raw_asset_allocation(raw_asset_alloc)
        summary_cash_total = self._brokerage_cash_total_from_raw_asset_allocation(raw_asset_alloc)
        rows = list(parsed_rows or [])
        if not rows and year is not None and month is not None:
            rows = self._persisted_rows_for_account_month(
                account=account,
                year=year,
                month=month,
                source_document_id=source_document_id,
            )

        # Búsqueda ETF: solo instrumentos ETF/cash (misma clasificación base de ETF).
        bucket_totals = self._brokerage_etf_bucket_totals_from_rows(rows)

        # Búsqueda Otros: Short Term + Non-US Fixed Income salen del summary.
        for bucket, amount in other_bucket_totals.items():
            bucket_totals[bucket] = amount

        # Fallback defensivo: si no se detectó cash en holdings ETF, usar Cash del summary.
        if "Caja" not in bucket_totals and summary_cash_total is not None:
            bucket_totals["Caja"] = summary_cash_total

        if not bucket_totals:
            return raw_asset_alloc

        return {
            bucket: {"value": str(bucket_totals[bucket])}
            for bucket in sorted(bucket_totals, key=_asset_bucket_sort_key)
            if bucket_totals[bucket] != Decimal("0")
        }

    @staticmethod
    def _brokerage_bucket_from_etf_search_instrument(instrument: str) -> str | None:
        instrument_norm = re.sub(r"[^a-z0-9]", "", str(instrument or "").lower())
        if any(marker in instrument_norm for marker in _BROKERAGE_CASH_MARKERS):
            return "Caja"

        bucket, matched = classify_etf_asset_bucket_with_match(instrument)
        if matched and bucket == "Caja":
            return "Caja"
        if matched and bucket in _BROKERAGE_ETF_SEARCH_BUCKETS:
            return bucket
        return None

    @classmethod
    def _brokerage_etf_bucket_totals_from_rows(
        cls,
        rows: list[dict[str, Any]],
    ) -> dict[str, Decimal]:
        bucket_totals: dict[str, Decimal] = {}
        for row in rows:
            if not isinstance(row, dict) or row.get("is_total"):
                continue
            instrument = str(row.get("instrument") or "").strip()
            if not instrument:
                continue
            amount = _safe_decimal(
                row.get("market_value") or row.get("value") or row.get("amount")
            )
            if amount is None or amount == Decimal("0"):
                continue

            bucket = cls._brokerage_bucket_from_etf_search_instrument(instrument)
            if not bucket:
                continue
            bucket_totals[bucket] = bucket_totals.get(bucket, Decimal("0")) + amount
        return bucket_totals

    @staticmethod
    def _brokerage_others_bucket_totals_from_raw_asset_allocation(
        raw_asset_alloc: dict | list | None,
    ) -> dict[str, Decimal]:
        if not isinstance(raw_asset_alloc, dict):
            return {}

        bucket_totals: dict[str, Decimal] = {}
        for label, payload in raw_asset_alloc.items():
            amount = _safe_decimal(
                payload.get("value")
                if isinstance(payload, dict)
                else payload
            )
            if amount is None:
                continue
            key = _normalize_alloc_label(label)
            bucket = _BROKERAGE_OTHERS_LABEL_TO_BUCKET.get(key)
            if not bucket:
                continue
            bucket_totals[bucket] = amount
        return bucket_totals

    @staticmethod
    def _brokerage_cash_total_from_raw_asset_allocation(
        raw_asset_alloc: dict | list | None,
    ) -> Decimal | None:
        if not isinstance(raw_asset_alloc, dict):
            return None

        for label, payload in raw_asset_alloc.items():
            key = _normalize_alloc_label(label)
            if key not in _BROKERAGE_CASH_LABEL_TO_BUCKET:
                continue
            amount = _safe_decimal(
                payload.get("value")
                if isinstance(payload, dict)
                else payload
            )
            if amount is not None:
                return amount
        return None

    def _persisted_rows_for_account_month(
        self,
        *,
        account: Account,
        year: int,
        month: int,
        source_document_id: int | None,
    ) -> list[dict[str, Any]]:
        month_start = date(year, month, 1)
        next_month = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)

        base_query = self.db.query(ParsedStatement.parsed_data_json).filter(
            ParsedStatement.account_id == account.id,
            ParsedStatement.statement_date >= month_start,
            ParsedStatement.statement_date < next_month,
        )

        if source_document_id is not None:
            row = (
                base_query
                .filter(ParsedStatement.raw_document_id == source_document_id)
                .order_by(ParsedStatement.id.desc())
                .first()
            )
            rows = _rows_from_parsed_payload(row[0], account_number=account.account_number) if row else []
            if rows:
                return rows

        row = base_query.order_by(ParsedStatement.id.desc()).first()
        if not row:
            return []
        return _rows_from_parsed_payload(row[0], account_number=account.account_number)

    @staticmethod
    def _ubs_asset_allocation_from_portfolio_block(
        portfolio_block: dict[str, Any] | None,
    ) -> dict[str, dict[str, str]] | None:
        if not isinstance(portfolio_block, dict):
            return None
        classes = portfolio_block.get("asset_classes")
        net_assets = _safe_decimal(portfolio_block.get("net_assets"))
        if not isinstance(classes, dict):
            if net_assets == Decimal("0"):
                return {
                    "Liquidity": {"total": "0"},
                    "Bonds": {"total": "0"},
                    "Equities": {"total": "0"},
                }
            return None

        mapping = {
            "liquidity": "Liquidity",
            "bonds": "Bonds",
            "equities": "Equities",
        }
        alloc: dict[str, dict[str, str]] = {}
        for raw_key, label in mapping.items():
            val = _safe_decimal(classes.get(raw_key))
            if val is None:
                continue
            alloc[label] = {"total": str(val)}
        return alloc or None

    def _upsert_ubs_historical_monthly_activity(
        self,
        result: ParseResult,
        doc: RawDocument,
        account: Account,
    ) -> None:
        bank_code = result.bank_code or doc.bank_code
        if bank_code != "ubs":
            return

        history = result.qualitative_data.get("account_monthly_activity_history", [])
        if not history:
            return

        stmt_year = result.statement_date.year if result.statement_date else None
        stmt_month = result.statement_date.month if result.statement_date else None

        for row in history:
            if row.get("account_number") != account.account_number:
                continue

            try:
                year = int(row.get("period_year"))
                month = int(row.get("period_month"))
            except (TypeError, ValueError):
                continue
            if month < 1 or month > 12:
                continue
            if stmt_year == year and stmt_month == month:
                # Statement month is handled by the main upsert path.
                continue

            ending_value = _safe_decimal(row.get("ending_value_with_accrual"))
            if ending_value is None:
                ending_value = _safe_decimal(row.get("ending_value_without_accrual"))
            change_in_value = _safe_decimal(row.get("net_contributions"))
            income = _safe_decimal(row.get("utilidad"))

            if ending_value is None and change_in_value is None and income is None:
                continue

            closing_date = self._safe_date(row.get("period_end"))
            if closing_date is None:
                last_day = calendar.monthrange(year, month)[1]
                closing_date = date(year, month, last_day)

            existing = (
                self.db.query(MonthlyClosing)
                .filter(
                    MonthlyClosing.account_id == account.id,
                    MonthlyClosing.year == year,
                    MonthlyClosing.month == month,
                )
                .first()
            )

            if self._has_ubs_manual_monthly_override(
                account_number=account.account_number,
                year=year,
                month=month,
            ):
                # Manual override months are intentionally authoritative and must
                # not be altered later by quarterly historical backfills.
                continue

            if not existing:
                existing = MonthlyClosing(
                    account_id=account.id,
                    closing_date=closing_date,
                    year=year,
                    month=month,
                    total_assets=ending_value,
                    total_liabilities=None,
                    net_value=ending_value,
                    currency=result.currency or account.currency,
                    income=income,
                    change_in_value=change_in_value,
                    source_document_id=doc.id,
                )
                self.db.add(existing)
                self._recompute_ubs_income_from_identity(
                    account=account,
                    year=year,
                    month=month,
                )
                continue

            has_direct_statement = self._is_direct_statement_month(
                account=account,
                year=year,
                month=month,
                source_document_id=existing.source_document_id,
            )
            if has_direct_statement and month in {3, 6, 9, 12}:
                # Quarter-end statement month keeps its own auditable current-period row.
                continue

            if not has_direct_statement:
                existing.closing_date = closing_date
            if ending_value is not None:
                # Regla UBS: el backfill historico NO debe sobreescribir net_value
                # ya auditado por cartola mensual; ending value manda por cartola.
                if existing.net_value is None:
                    existing.net_value = ending_value
                if existing.total_assets is None:
                    existing.total_assets = ending_value
            if change_in_value is not None:
                existing.change_in_value = change_in_value
            if not has_direct_statement and income is not None:
                existing.income = income
            if existing.source_document_id is None:
                existing.source_document_id = doc.id
            self._recompute_ubs_income_from_identity(
                account=account,
                year=year,
                month=month,
            )

    def _is_direct_statement_month(
        self,
        *,
        account: Account,
        year: int,
        month: int,
        source_document_id: int | None,
    ) -> bool:
        if source_document_id is None:
            return False

        statement = (
            self.db.query(ParsedStatement.statement_date)
            .filter(
                ParsedStatement.raw_document_id == source_document_id,
                ParsedStatement.account_id == account.id,
            )
            .order_by(ParsedStatement.id.desc())
            .first()
        )
        if statement and statement[0]:
            stmt_date = statement[0]
            return stmt_date.year == year and stmt_date.month == month

        raw_doc = (
            self.db.query(RawDocument.period_year, RawDocument.period_month)
            .filter(RawDocument.id == source_document_id)
            .first()
        )
        if raw_doc is None:
            return False
        return raw_doc[0] == year and raw_doc[1] == month

    @staticmethod
    def _has_ubs_manual_monthly_override(
        *,
        account_number: str | None,
        year: int,
        month: int,
    ) -> bool:
        if not account_number:
            return False
        return (account_number, year, month) in _UBS_MANUAL_MONTHLY_OVERRIDES

    def _resolve_override_source_document_id(
        self,
        *,
        source_filename: str | None,
    ) -> int | None:
        if not source_filename:
            return None
        row = (
            self.db.query(RawDocument.id)
            .filter(RawDocument.filename == source_filename)
            .order_by(RawDocument.id.desc())
            .first()
        )
        return row[0] if row else None

    def _upsert_manual_monthly_override(
        self,
        *,
        account: Account,
        year: int,
        month: int,
        override: dict[str, Any],
    ) -> int:
        closing_date = override["closing_date"]
        end_w = _safe_decimal(override.get("ending_value_with_accrual"))
        end_wo = _safe_decimal(override.get("ending_value_without_accrual"))
        accrual = _safe_decimal(override.get("accrual_ending"))
        movements = _safe_decimal(override.get("movements_net"))
        profit = _safe_decimal(override.get("profit_period"))
        source_document_id = self._resolve_override_source_document_id(
            source_filename=override.get("source_filename"),
        )
        currency = override.get("currency") or account.currency

        existing = (
            self.db.query(MonthlyClosing)
            .filter(
                MonthlyClosing.account_id == account.id,
                MonthlyClosing.year == year,
                MonthlyClosing.month == month,
            )
            .first()
        )

        asset_alloc_json = (
            override["asset_allocation_json"]
            if "asset_allocation_json" in override
            else (existing.asset_allocation_json if existing else None)
        )

        closing_payload = {
            "closing_date": closing_date,
            "total_assets": end_w,
            "net_value": end_w,
            "currency": currency,
            "income": profit,
            "change_in_value": movements,
            "accrual": accrual,
            "asset_allocation_json": asset_alloc_json,
            "source_document_id": source_document_id or (existing.source_document_id if existing else None),
        }
        if existing:
            for key, value in closing_payload.items():
                setattr(existing, key, value)
        else:
            self.db.add(
                MonthlyClosing(
                    account_id=account.id,
                    year=year,
                    month=month,
                    total_liabilities=None,
                    **closing_payload,
                )
            )

        normalized = (
            self.db.query(MonthlyMetricNormalized)
            .filter(
                MonthlyMetricNormalized.account_id == account.id,
                MonthlyMetricNormalized.year == year,
                MonthlyMetricNormalized.month == month,
            )
            .first()
        )

        cash_value = (
            _safe_decimal(override.get("cash_value"))
            if "cash_value" in override
            else (
                normalized.cash_value
                if normalized and normalized.cash_value is not None
                else self._resolve_normalized_cash_value(
                    account=account,
                    year=year,
                    month=month,
                    asset_alloc_json=asset_alloc_json,
                    source_document_id=source_document_id or (existing.source_document_id if existing else None),
                )
            )
        )
        normalized_alloc_json = self._compose_normalized_asset_allocation_json(
            account=account,
            year=year,
            month=month,
            ending_value_with_accrual=to_decimal(end_w),
            ending_value_without_accrual=to_decimal(end_wo),
            raw_asset_allocation_json=asset_alloc_json,
        )
        normalized_payload = {
            "closing_date": closing_date,
            "ending_value_with_accrual": end_w,
            "ending_value_without_accrual": end_wo,
            "accrual_ending": accrual,
            "cash_value": cash_value,
            "movements_net": movements,
            "profit_period": profit,
            "asset_allocation_json": normalized_alloc_json,
            "currency": currency,
            "source_document_id": source_document_id or (normalized.source_document_id if normalized else None),
        }
        if normalized:
            for key, value in normalized_payload.items():
                setattr(normalized, key, value)
        else:
            self.db.add(
                MonthlyMetricNormalized(
                    account_id=account.id,
                    year=year,
                    month=month,
                    **normalized_payload,
                )
            )

        self._log(
            "load",
            "info",
            (
                f"Override mensual aplicado {account.bank_code}/{account.account_number} "
                f"{year}-{month:02d}: {override.get('reason')}"
            ),
            raw_document_id=source_document_id,
            account_id=account.id,
        )
        refresh_years = {year}
        if account.bank_code == "ubs":
            refresh_years |= self._recompute_ubs_income_series(account=account)
        for refresh_year in sorted(refresh_years):
            self._refresh_normalized_activity_from_monthly_closings(
                account=account,
                year=refresh_year,
            )
        return 1

    def _get_account_specific_values(
        self, result: ParseResult, account: Account
    ) -> dict:
        """
        Extrae valores específicos de una cuenta desde qualitative_data.
        Para reportes multi-cuenta (JPMorgan), busca en account_monthly_activity,
        account_ytd e income_summary.

        Prioridad para income/change_investment:
        1. account_monthly_activity (período actual — JPMorgan ETF v2.1+)
        2. account_ytd (YTD fallback)
        3. income_summary
        """
        values: dict = {}
        parser_key = (result.parser_name or "").strip().lower()
        allow_ytd_monthly_fill = parser_key not in {"parsers.jpmorgan.brokerage"}

        # -- accounts (summary) --
        for acct_info in result.qualitative_data.get("accounts", []):
            if acct_info.get("account_number") == account.account_number:
                values["beginning_value"] = _safe_decimal(acct_info.get("beginning_value"))
                values["ending_value"] = _safe_decimal(acct_info.get("ending_value"))
                break

        # -- account_monthly_activity (current period — highest priority) --
        for monthly in result.qualitative_data.get("account_monthly_activity", []):
            if monthly.get("account_number") == account.account_number:
                end_wo = _safe_decimal(monthly.get("ending_value_without_accrual"))
                end_w = _safe_decimal(monthly.get("ending_value_with_accrual"))
                # Reporting contract: net_value in resumen = ending value WITH accruals.
                if end_w is not None:
                    values["ending_value"] = end_w
                elif end_wo is not None:
                    values["ending_value"] = end_wo
                if end_wo is not None:
                    values["ending_value_without_accrual"] = end_wo
                if end_w is not None:
                    values["ending_value_with_accrual"] = end_w
                # utilidad = Income & Distrib + Change Invest + accrual_end - accrual_beg
                utilidad = _safe_decimal(monthly.get("utilidad"))
                if utilidad is not None:
                    values["income"] = utilidad
                # net_contributions = movimientos
                net_contrib = _safe_decimal(monthly.get("net_contributions"))
                if net_contrib is not None:
                    values["change_investment"] = net_contrib
                net_contrib_ytd = _safe_decimal(monthly.get("net_contributions_ytd"))
                if net_contrib_ytd is not None:
                    values["change_investment_ytd"] = net_contrib_ytd
                # accrual
                accrual_ending = _safe_decimal(monthly.get("accrual_ending"))
                if accrual_ending is not None:
                    values["accrual"] = accrual_ending
                utilidad_ytd = _safe_decimal(monthly.get("utilidad_ytd"))
                if utilidad_ytd is not None:
                    values["income_ytd"] = utilidad_ytd
                prior_adj = _safe_decimal(monthly.get("prior_period_adjustments"))
                if prior_adj is not None:
                    values["prior_period_adjustments"] = prior_adj
                prior_adj_ytd = _safe_decimal(monthly.get("prior_period_adjustments_ytd"))
                if prior_adj_ytd is not None:
                    values["prior_period_adjustments_ytd"] = prior_adj_ytd
                monthly_alloc = monthly.get("asset_allocation")
                if isinstance(monthly_alloc, (dict, list)) and monthly_alloc:
                    values["asset_allocation"] = monthly_alloc
                interpretation_notes = monthly.get("interpretation_notes")
                if isinstance(interpretation_notes, list) and interpretation_notes:
                    values["interpretation_notes"] = [str(note) for note in interpretation_notes]
                break

        # -- portfolio_activity (fallback para JPMorgan single-account bonds/custody) --
        if (
            "income" not in values
            or "change_investment" not in values
            or "ending_value" not in values
        ):
            single_account_jpm = parser_key in {
                "parsers.jpmorgan.bonds",
                "parsers.jpmorgan.custody",
            }
            portfolio_activity = result.qualitative_data.get("portfolio_activity") or {}
            if (
                single_account_jpm
                and isinstance(portfolio_activity, dict)
                and str(result.account_number or "").strip() == account.account_number
            ):
                beginning = _safe_decimal(
                    (portfolio_activity.get("beginning_market_value") or {}).get("current_period")
                )
                ending = _safe_decimal(
                    (portfolio_activity.get("ending_market_value") or {}).get("current_period")
                )
                net_cash = _safe_decimal(
                    (portfolio_activity.get("net_cash_contributions") or {}).get("current_period")
                )
                net_cash_ytd = _safe_decimal(
                    (portfolio_activity.get("net_cash_contributions") or {}).get("ytd")
                )
                # Formato pre-2021: "Net Security Contributions / Withdrawals" es una línea separada;
                # se suma a net_cash para obtener el total de movimientos.
                net_security = _safe_decimal(
                    (portfolio_activity.get("net_security_contributions") or {}).get("current_period")
                )
                net_security_ytd = _safe_decimal(
                    (portfolio_activity.get("net_security_contributions") or {}).get("ytd")
                )
                # Total movimientos = cash + security (si alguno existe)
                if net_cash is not None or net_security is not None:
                    net_contrib_total: Optional[Decimal] = (
                        (net_cash or Decimal("0")) + (net_security or Decimal("0"))
                    )
                else:
                    net_contrib_total = None
                if net_cash_ytd is not None or net_security_ytd is not None:
                    net_contrib_ytd_total: Optional[Decimal] = (
                        (net_cash_ytd or Decimal("0")) + (net_security_ytd or Decimal("0"))
                    )
                else:
                    net_contrib_ytd_total = None

                income_dist = _safe_decimal(
                    (portfolio_activity.get("income_distributions") or {}).get("current_period")
                )
                income_dist_ytd = _safe_decimal(
                    (portfolio_activity.get("income_distributions") or {}).get("ytd")
                )
                change_inv = _safe_decimal(
                    (portfolio_activity.get("change_investment") or {}).get("current_period")
                )
                change_inv_ytd = _safe_decimal(
                    (portfolio_activity.get("change_investment") or {}).get("ytd")
                )

                if "beginning_value" not in values and beginning is not None:
                    values["beginning_value"] = beginning
                if "ending_value" not in values and ending is not None:
                    values["ending_value"] = ending
                if "change_investment" not in values and net_contrib_total is not None:
                    values["change_investment"] = net_contrib_total
                if "change_investment_ytd" not in values and net_contrib_ytd_total is not None:
                    values["change_investment_ytd"] = net_contrib_ytd_total
                if "income" not in values and (income_dist is not None or change_inv is not None):
                    values["income"] = (income_dist or Decimal("0")) + (change_inv or Decimal("0"))
                if "income_ytd" not in values and (
                    income_dist_ytd is not None or change_inv_ytd is not None
                ):
                    values["income_ytd"] = (income_dist_ytd or Decimal("0")) + (change_inv_ytd or Decimal("0"))
                if "asset_allocation" not in values:
                    top_alloc = result.qualitative_data.get("asset_allocation")
                    if isinstance(top_alloc, (dict, list)) and top_alloc:
                        values["asset_allocation"] = top_alloc

        # -- account_ytd (fallback if monthly not available) --
        if "income" not in values or "change_investment" not in values:
            for ytd in result.qualitative_data.get("account_ytd", []):
                if ytd.get("account_number") == account.account_number:
                    if "beginning_value" not in values:
                        values["beginning_value"] = _safe_decimal(ytd.get("beginning_value"))
                    if "ending_value" not in values:
                        values["ending_value"] = _safe_decimal(ytd.get("ending_value"))
                    if allow_ytd_monthly_fill and "income" not in values:
                        values["income"] = _safe_decimal(ytd.get("income"))
                    if allow_ytd_monthly_fill and "change_investment" not in values:
                        values["change_investment"] = _safe_decimal(ytd.get("change_investment"))
                    break

        # -- income_summary --
        for inc in result.qualitative_data.get("income_summary", []):
            if inc.get("account_number") == account.account_number:
                if "income" not in values:
                    values["income"] = _safe_decimal(inc.get("income"))
                break

        return values

    def _validate_ytd_consistency(
        self,
        account: Account,
        year: int,
        month: int,
        account_values: dict,
        raw_document_id: int,
    ) -> None:
        ytd_mov = account_values.get("change_investment_ytd")
        ytd_util = account_values.get("income_ytd")
        prior_adj = account_values.get("prior_period_adjustments")
        if ytd_mov is None and ytd_util is None:
            return

        # Exclude the current month from the DB query: with autoflush=False the
        # upsert for this month may or may not be flushed yet, making the result
        # non-deterministic. Sum prior months from DB, then add current values
        # from the in-memory account_values dict.
        rows = (
            self.db.query(MonthlyClosing)
            .filter(
                MonthlyClosing.account_id == account.id,
                MonthlyClosing.year == year,
                MonthlyClosing.month < month,
            )
            .all()
        )
        sum_mov = sum((row.change_in_value or Decimal("0")) for row in rows)
        sum_util = sum((row.income or Decimal("0")) for row in rows)
        sum_mov += account_values.get("change_investment") or Decimal("0")
        sum_util += account_values.get("income") or Decimal("0")

        if ytd_mov is not None:
            diff_mov = ytd_mov - sum_mov
            if account.bank_code == "bbh" and prior_adj is not None and prior_adj != 0:
                # BBH prior_period_adjustments are control-only: they are included in
                # BBH's YTD figure and explain why our monthly sum diverges, but we do
                # not mutate prior months' closings. Log so the gap is traceable.
                self._log(
                    "load",
                    "info",
                    (
                        f"BBH prior_period_adjustments={prior_adj} detectado en "
                        f"{account.account_number} {year}-{month:02d}: "
                        f"explica gap YTD (ytd={ytd_mov}, suma={sum_mov}, diff={diff_mov})"
                    ),
                    raw_document_id=raw_document_id,
                    account_id=account.id,
                )
            if abs(diff_mov) > Decimal("1"):
                self._log(
                    "load",
                    "warning",
                    (
                        f"YTD caja inconsistente {account.bank_code}/{account.account_number} "
                        f"{year}-{month:02d}: ytd={ytd_mov} vs suma={sum_mov} "
                        f"(diff={diff_mov})"
                    ),
                    raw_document_id=raw_document_id,
                    account_id=account.id,
                )

        if ytd_util is not None:
            diff_util = ytd_util - sum_util
            if abs(diff_util) > Decimal("1"):
                self._log(
                    "load",
                    "warning",
                    (
                        f"YTD utilidad inconsistente {account.bank_code}/{account.account_number} "
                        f"{year}-{month:02d}: ytd={ytd_util} vs suma={sum_util} "
                        f"(diff={diff_util})"
                    ),
                    raw_document_id=raw_document_id,
                    account_id=account.id,
                )

    def _recompute_ubs_income_from_identity(
        self,
        account: Account,
        year: int,
        month: int,
    ) -> None:
        if account.bank_code != "ubs":
            return
        current = (
            self.db.query(MonthlyClosing)
            .filter(
                MonthlyClosing.account_id == account.id,
                MonthlyClosing.year == year,
                MonthlyClosing.month == month,
            )
            .first()
        )
        if current is None or current.net_value is None or current.change_in_value is None:
            return
        prev_year = year if month > 1 else year - 1
        prev_month = month - 1 if month > 1 else 12
        prev = (
            self.db.query(MonthlyClosing)
            .filter(
                MonthlyClosing.account_id == account.id,
                MonthlyClosing.year == prev_year,
                MonthlyClosing.month == prev_month,
            )
            .first()
        )
        if prev is None or prev.net_value is None:
            return
        # UBS Suiza policy:
        # - ending value is the auditable month-end balance (monthly statement wins)
        # - quarterly tables may refine prior-month movements
        # - profit absorbs any continuity mismatch against the previous audited ending
        recomputed = current.net_value - current.change_in_value - prev.net_value
        if current.net_value != 0 and abs(recomputed) > abs(current.net_value) * Decimal("0.5"):
            self._log(
                "load",
                "warning",
                (
                    f"UBS income recomputado sospechoso: "
                    f"{account.bank_code}/{account.account_number} "
                    f"{year}-{month:02d}: income={recomputed}, "
                    f"ending={current.net_value}, movements={current.change_in_value}, "
                    f"prev_ending={prev.net_value}"
                ),
                account_id=account.id,
            )
        current.income = recomputed

    def _recompute_ubs_income_series(self, account: Account) -> set[int]:
        """
        Recalcula utilidad UBS por identidad sobre la serie persistida.

        Cubre reprocesos fuera de orden: si un mes previo cambia despuÃ©s,
        los meses siguientes que dependen de ese ending auditado se corrigen.
        """
        if account.bank_code != "ubs":
            return set()

        closings = (
            self.db.query(MonthlyClosing)
            .filter(MonthlyClosing.account_id == account.id)
            .order_by(MonthlyClosing.year, MonthlyClosing.month)
            .all()
        )
        if not closings:
            return set()

        by_period = {
            (closing.year, closing.month): closing
            for closing in closings
        }
        touched_years: set[int] = set()

        for current in closings:
            if current.net_value is None or current.change_in_value is None:
                continue

            prev_year = current.year if current.month > 1 else current.year - 1
            prev_month = current.month - 1 if current.month > 1 else 12
            prev = by_period.get((prev_year, prev_month))
            if prev is None or prev.net_value is None:
                continue

            recomputed_income = current.net_value - current.change_in_value - prev.net_value
            if current.income != recomputed_income:
                current.income = recomputed_income
                touched_years.add(current.year)

        return touched_years

    def _reconcile_account_ytd_series(
        self,
        account: Account,
        year: int,
        raw_document_id: int,
    ) -> None:
        """
        Recorre statements del año y alinea monthly_closings a los YTD reportados.

        Blindaje contra cargas/reprocesos fuera de orden:
        si un mes previo se reprocesa después, la serie vuelve a cuadrar.
        """
        year_start = date(year, 1, 1)
        year_end = date(year + 1, 1, 1)
        statements = (
            self.db.query(ParsedStatement)
            .filter(
                ParsedStatement.account_id == account.id,
                ParsedStatement.statement_date >= year_start,
                ParsedStatement.statement_date < year_end,
            )
            .order_by(ParsedStatement.statement_date.asc())
            .all()
        )
        if not statements:
            return

        for ps in statements:
            try:
                payload = json.loads(ps.parsed_data_json or "{}")
            except (TypeError, ValueError):
                continue
            qualitative = payload.get("qualitative_data") or {}
            monthly_rows = qualitative.get("account_monthly_activity") or []
            if not monthly_rows:
                continue

            monthly = next(
                (m for m in monthly_rows if m.get("account_number") == account.account_number),
                None,
            )
            if monthly is None and len(monthly_rows) == 1:
                monthly = monthly_rows[0]
            if monthly is None:
                continue

            ytd_mov = _safe_decimal(monthly.get("net_contributions_ytd"))
            ytd_util = _safe_decimal(monthly.get("utilidad_ytd"))
            prior_adj = _safe_decimal(monthly.get("prior_period_adjustments"))
            if ytd_mov is None and ytd_util is None:
                continue

            month = ps.statement_date.month
            rows = (
                self.db.query(MonthlyClosing)
                .filter(
                    MonthlyClosing.account_id == account.id,
                    MonthlyClosing.year == year,
                    MonthlyClosing.month <= month,
                )
                .all()
            )
            if not rows:
                continue

            current = next((r for r in rows if r.month == month), None)
            if current is None:
                continue

            sum_mov = sum((row.change_in_value or Decimal("0")) for row in rows)
            sum_util = sum((row.income or Decimal("0")) for row in rows)

            if ytd_mov is not None:
                diff_mov = ytd_mov - sum_mov
                if abs(diff_mov) > Decimal("1"):
                    self._log(
                        "load",
                        "warning",
                        (
                            f"YTD serie caja inconsistente {account.bank_code}/{account.account_number} "
                            f"{year}-{month:02d}: ytd={ytd_mov} vs suma={sum_mov} "
                            f"(diff={diff_mov})"
                        ),
                        raw_document_id=raw_document_id,
                        account_id=account.id,
                    )

            if ytd_util is not None:
                diff_util = ytd_util - sum_util
                if abs(diff_util) > Decimal("1"):
                    self._log(
                        "load",
                        "warning",
                        (
                            f"YTD serie utilidad inconsistente {account.bank_code}/{account.account_number} "
                            f"{year}-{month:02d}: ytd={ytd_util} vs suma={sum_util} "
                            f"(diff={diff_util})"
                        ),
                        raw_document_id=raw_document_id,
                        account_id=account.id,
                    )

    # ═══════════════════════════════════════════════════════════════
    # ETF COMPOSITIONS
    # ═══════════════════════════════════════════════════════════════

    def _upsert_etf_compositions(
        self,
        result: ParseResult,
        doc: RawDocument,
        account: Account,
    ) -> int:
        """Crea/actualiza registros de composición ETF a partir de holdings."""
        if not result.statement_date:
            return 0

        report_date = result.period_end or result.statement_date
        year = report_date.year
        month = report_date.month
        bank_code = result.bank_code or doc.bank_code or "unknown"
        grouped: dict[str, dict] = {}
        for row in result.rows:
            data = row.data
            # Solo holdings, no totales
            if data.get("is_total"):
                continue

            # Filtrar por cuenta si hay info
            row_account = data.get("account_number")
            if row_account and row_account != account.account_number:
                continue

            instrument = data.get("instrument", "").strip()
            if not instrument:
                continue

            market_value = _safe_decimal(data.get("market_value"))

            # Generar código corto del instrumento
            etf_code = self._instrument_to_code(instrument)
            if etf_code not in grouped:
                grouped[etf_code] = {
                    "etf_name": instrument,
                    "market_value": market_value,
                }
            else:
                prev = grouped[etf_code]["market_value"]
                if prev is None:
                    grouped[etf_code]["market_value"] = market_value
                elif market_value is not None:
                    grouped[etf_code]["market_value"] = prev + market_value

        if not grouped:
            return 0

        # Replace full snapshot for account/bank/month to avoid stale rows when
        # parser normalization changes etf_code between reprocesos.
        self.db.query(EtfComposition).filter(
            EtfComposition.account_id == account.id,
            EtfComposition.bank_code == bank_code,
            EtfComposition.year == year,
            EtfComposition.month == month,
        ).delete(synchronize_session=False)

        count = 0
        for etf_code, payload in grouped.items():
            instrument = payload["etf_name"]
            market_value = payload["market_value"]
            comp = EtfComposition(
                account_id=account.id,
                bank_code=bank_code,
                report_date=report_date,
                year=year,
                month=month,
                etf_code=etf_code,
                etf_name=instrument,
                market_value=market_value,
                currency=result.currency or account.currency,
                source_document_id=doc.id,
            )
            self.db.add(comp)

            count += 1

        if count > 0:
            # Asegura que el refresh lea los ETF recién insertados
            # (Session está con autoflush=False).
            self.db.flush()
            self._refresh_normalized_activity_from_monthly_closings(
                account=account,
                year=year,
            )

        return count

    @staticmethod
    def _instrument_to_code(name: str) -> str:
        """Genera un código corto a partir del nombre del instrumento."""
        # Limpiar y tomar primeras palabras significativas
        clean = name.upper().strip()
        # Si ya es corto, usarlo directamente
        if len(clean) <= 20:
            return clean.replace(" ", "_")
        # Tomar iniciales significativas
        words = clean.split()
        if len(words) >= 3:
            return "_".join(words[:3])
        return clean[:20].replace(" ", "_")

    # ═══════════════════════════════════════════════════════════════
    # UTILIDADES
    # ═══════════════════════════════════════════════════════════════

    def _rows_for_account(self, result: ParseResult, account: Account) -> list[dict[str, Any]]:
        return [
            row.data
            for row in result.rows
            if not row.data.get("is_total")
            and (
                not row.data.get("account_number")
                or row.data.get("account_number") == account.account_number
            )
        ]

    def _resolve_normalized_cash_value(
        self,
        *,
        account: Account,
        year: int,
        month: int,
        asset_alloc_json: str | None,
        source_document_id: int | None,
        parsed_rows: list[dict[str, Any]] | None = None,
    ) -> Optional[Decimal]:
        cash_value = cash_from_asset_allocation_json(asset_alloc_json)
        if cash_value is not None:
            return cash_value

        if account.bank_code != "jpmorgan" or account.account_type not in {"brokerage", "etf"}:
            return None

        parsed_cash = _cash_from_jpmorgan_holdings_rows(parsed_rows)
        if parsed_cash is not None:
            return parsed_cash

        return self._cash_from_persisted_jpmorgan_holdings(
            account=account,
            year=year,
            month=month,
            source_document_id=source_document_id,
        )

    def _cash_from_persisted_jpmorgan_holdings(
        self,
        *,
        account: Account,
        year: int,
        month: int,
        source_document_id: int | None,
    ) -> Optional[Decimal]:
        month_start = date(year, month, 1)
        next_month = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)

        base_query = self.db.query(ParsedStatement.parsed_data_json).filter(
            ParsedStatement.account_id == account.id,
            ParsedStatement.statement_date >= month_start,
            ParsedStatement.statement_date < next_month,
        )
        if source_document_id is not None:
            row = (
                base_query
                .filter(ParsedStatement.raw_document_id == source_document_id)
                .order_by(ParsedStatement.id.desc())
                .first()
            )
            if row:
                cash_value = _cash_from_jpmorgan_parsed_payload(row[0])
                if cash_value is not None:
                    return cash_value

        row = base_query.order_by(ParsedStatement.id.desc()).first()
        if not row:
            return None
        return _cash_from_jpmorgan_parsed_payload(row[0])

    def _serialize_parse_result(self, result: ParseResult, account: Account) -> str:
        """Serializa ParseResult relevante a JSON para almacenar en parsed_data_json."""
        data = {
            "rows": self._rows_for_account(result, account),
            "qualitative_data": result.qualitative_data,
            "balances": result.balances,
            "opening_balance": str(result.opening_balance) if result.opening_balance else None,
            "closing_balance": str(result.closing_balance) if result.closing_balance else None,
        }
        return json.dumps(data, default=str)

    def _resolve_account(self, account_number: str, cache: dict[str, Account]) -> Optional[Account]:
        account = cache.get(account_number)
        if account:
            return account
        account = (
            self.db.query(Account)
            .filter(Account.account_number == account_number)
            .first()
        )
        if account:
            cache[account_number] = account
        return account

    @staticmethod
    def _alternatives_account_number(
        *,
        entity_name: str,
        asset_class: str,
        strategy: str,
        currency: str,
    ) -> str:
        base = "||".join(
            [
                str(entity_name).strip(),
                str(asset_class).strip(),
                str(strategy).strip(),
                str(currency).strip().upper(),
            ]
        )
        digest = hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]
        return f"ALT-{digest}"

    @staticmethod
    def _alternatives_identification_number(
        *,
        nemo_reference: str | None,
        account_number: str,
    ) -> str:
        if nemo_reference:
            return str(nemo_reference).strip().upper()[:5]
        suffix = str(account_number).split("-")[-1][-6:].upper()
        return f"ALT-{suffix}"

    @staticmethod
    def _safe_date(value) -> Optional[date]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        try:
            # pandas.Timestamp expone to_pydatetime
            if hasattr(value, "to_pydatetime"):
                return value.to_pydatetime().date()
        except Exception:
            pass
        text = str(value).strip()
        if not text:
            return None
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None

    @staticmethod
    def _clean_str(value) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    def _log(
        self,
        validation_type: str,
        severity: str,
        message: str,
        raw_document_id: Optional[int] = None,
        account_id: Optional[int] = None,
    ) -> None:
        """Registra log de validación."""
        log = ValidationLog(
            raw_document_id=raw_document_id,
            account_id=account_id,
            validation_type=validation_type,
            severity=severity,
            message=message,
            source_module="data_loading_service",
        )
        self.db.add(log)
