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

from backend.db.models import (
    Account,
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


def _safe_decimal(value) -> Optional[Decimal]:
    """Convierte un valor a Decimal de forma segura."""
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _cash_from_asset_allocation_json(asset_alloc_json: str | None) -> Optional[Decimal]:
    """Extrae caja desde asset_allocation_json si está disponible."""
    if not asset_alloc_json:
        return None
    try:
        alloc = json.loads(asset_alloc_json)
    except (TypeError, ValueError):
        return None

    def _label_norm(label: Any) -> str:
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

    def _value_from_payload(payload: Any) -> Optional[Decimal]:
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
        return _safe_decimal(raw)

    total = Decimal("0")
    found = False
    if isinstance(alloc, dict):
        umbrella_values: list[Decimal] = []
        for key, payload in alloc.items():
            key_norm = _label_norm(key)
            if _is_mixed_cash_bucket(key_norm):
                continue
            if not _is_cash_umbrella(key_norm):
                continue
            val = _value_from_payload(payload)
            if val is not None:
                umbrella_values.append(val)
        if umbrella_values:
            return max(umbrella_values)

        for key, payload in alloc.items():
            key_norm = _label_norm(key)
            if _is_mixed_cash_bucket(key_norm):
                continue
            if not any(
                tok in key_norm for tok in ("cash", "deposit", "moneymarket", "shortterm", "liquidity")
            ):
                continue
            val = _value_from_payload(payload)
            if val is None:
                continue
            total += val
            found = True
    elif isinstance(alloc, list):
        for row in alloc:
            if not isinstance(row, dict):
                continue
            name_norm = _label_norm(row.get("asset_class") or row.get("name") or row.get("label") or "")
            if _is_mixed_cash_bucket(name_norm):
                continue
            if not any(
                tok in name_norm for tok in ("cash", "deposit", "moneymarket", "shortterm", "liquidity")
            ):
                continue
            val = _value_from_payload(row)
            if val is None:
                continue
            total += val
            found = True
    return total if found else None


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


def _pick_asset_class_value(
    entries: list[tuple[str, Decimal]],
    *,
    preferred_exact: list[str],
    include_tokens: list[str],
    exclude_tokens: list[str] | None = None,
) -> Decimal | None:
    if not entries:
        return None
    by_label: dict[str, Decimal] = {}
    for label, val in entries:
        by_label[label] = val

    for label in preferred_exact:
        if label in by_label:
            return by_label[label]

    total = Decimal("0")
    found = False
    excludes = exclude_tokens or []
    for label, val in entries:
        if not any(tok in label for tok in include_tokens):
            continue
        if any(tok in label for tok in excludes):
            continue
        total += val
        found = True
    return total if found else None


def _normalize_mandate_asset_allocation(asset_alloc: Any) -> dict[str, dict[str, str]] | None:
    """
    Normaliza asset allocation de Mandatos a 3 clases:
    - Cash, Deposits & Money Market
    - Fixed Income
    - Equities
    """
    entries = _extract_asset_allocation_entries(asset_alloc)
    if not entries:
        return None

    cash = _pick_asset_class_value(
        entries,
        preferred_exact=[
            "cashdepositsmoneymarketfunds",
            "cashdepositsshortterm",
            "liquidity",
            "cash",
        ],
        include_tokens=["cash", "deposit", "moneymarket", "liquidity"],
        exclude_tokens=["totalportfolio", "netassets", "totalmarketvalue", "totalnetmarketvalue"],
    )
    fixed_income = _pick_asset_class_value(
        entries,
        preferred_exact=["fixedincome", "bonds"],
        include_tokens=["fixedincome", "bond"],
        exclude_tokens=["totalportfolio", "netassets"],
    )
    equities = _pick_asset_class_value(
        entries,
        preferred_exact=["equities", "publicequity", "equity"],
        include_tokens=["equity", "equities", "stock"],
        exclude_tokens=["totalportfolio", "netassets"],
    )

    if cash is None and fixed_income is None and equities is None:
        return None

    cash = cash if cash is not None else Decimal("0")
    fixed_income = fixed_income if fixed_income is not None else Decimal("0")
    equities = equities if equities is not None else Decimal("0")

    return {
        "Cash, Deposits & Money Market": {"value": str(cash)},
        "Fixed Income": {"value": str(fixed_income)},
        "Equities": {"value": str(equities)},
    }


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
        Carga un PDF de reporte (asset allocation) y actualiza monthly_closings.
        """
        stats = {"monthly_closings_updated": 0, "errors": []}
        if not result.is_success:
            stats["errors"].append("ParseResult no exitoso para pdf_report")
            return stats

        asset_alloc = result.qualitative_data.get("asset_allocation")
        if not isinstance(asset_alloc, dict) or not asset_alloc:
            stats["errors"].append("El reporte no contiene asset_allocation estructurado")
            return stats

        account: Optional[Account] = None
        if result.account_number:
            account = (
                self.db.query(Account)
                .filter(Account.account_number == result.account_number)
                .first()
            )
        if account is None and raw_document.account_id:
            account = (
                self.db.query(Account)
                .filter(Account.id == raw_document.account_id)
                .first()
            )
        if account is None:
            stats["errors"].append("No se pudo resolver cuenta para pdf_report")
            return stats

        closing_date = result.period_end or result.statement_date
        if closing_date is None:
            if raw_document.period_year and raw_document.period_month:
                last_day = calendar.monthrange(raw_document.period_year, raw_document.period_month)[1]
                closing_date = date(raw_document.period_year, raw_document.period_month, last_day)
            else:
                stats["errors"].append("No se pudo determinar período para pdf_report")
                return stats

        existing = (
            self.db.query(MonthlyClosing)
            .filter(
                MonthlyClosing.account_id == account.id,
                MonthlyClosing.year == closing_date.year,
                MonthlyClosing.month == closing_date.month,
            )
            .first()
        )

        payload_json = json.dumps(asset_alloc)
        if existing:
            existing.asset_allocation_json = payload_json
            existing.source_document_id = raw_document.id
            if not existing.closing_date:
                existing.closing_date = closing_date
            if not existing.currency:
                existing.currency = result.currency or account.currency
        else:
            self.db.add(
                MonthlyClosing(
                    account_id=account.id,
                    closing_date=closing_date,
                    year=closing_date.year,
                    month=closing_date.month,
                    currency=result.currency or account.currency,
                    asset_allocation_json=payload_json,
                    source_document_id=raw_document.id,
                )
            )
        self.db.flush()
        self._refresh_normalized_activity_from_monthly_closings(
            account=account,
            year=closing_date.year,
        )
        stats["monthly_closings_updated"] += 1
        self.db.commit()
        return stats

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

        # Asset allocation (normalizado para Mandatos, por cuenta cuando aplica).
        raw_asset_alloc = self._resolve_asset_allocation_for_account(
            result=result,
            account=account,
            account_values=account_values,
        )
        if account.account_type == "mandato":
            asset_alloc = _normalize_mandate_asset_allocation(raw_asset_alloc) or raw_asset_alloc
        else:
            asset_alloc = raw_asset_alloc
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

        if existing:
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

        # Persistir capa canónica mensual (Fase 1 normalización).
        self._upsert_monthly_metric_normalized(
            account=account,
            year=year,
            month=month,
            closing_date=closing_date,
            currency=result.currency or account.currency,
            source_document_id=doc.id,
            account_values=account_values,
            closing_bal=closing_bal,
            accrual=accrual,
            movements=change_in_value,
            profit=income,
            asset_alloc_json=asset_alloc_json,
            parsed_rows=self._rows_for_account(result, account),
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
            "asset_allocation_json": asset_alloc_json,
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
            alloc_json = (
                existing_alloc
                if existing_alloc is not None
                else closing.asset_allocation_json
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
                "asset_allocation_json": alloc_json,
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
        normalized_payload = {
            "closing_date": closing_date,
            "ending_value_with_accrual": end_w,
            "ending_value_without_accrual": end_wo,
            "accrual_ending": accrual,
            "cash_value": cash_value,
            "movements_net": movements,
            "profit_period": profit,
            "asset_allocation_json": asset_alloc_json,
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
                if "change_investment" not in values and net_cash is not None:
                    values["change_investment"] = net_cash
                if "change_investment_ytd" not in values and net_cash_ytd is not None:
                    values["change_investment_ytd"] = net_cash_ytd
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

    def _apply_bbh_prior_adjustments(
        self,
        account: Account,
        year: int,
        month: int,
        account_values: dict,
    ) -> None:
        if account.bank_code != "bbh":
            return
        prior_adj = account_values.get("prior_period_adjustments")
        if prior_adj is None:
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
        if prev is None:
            return
        base = prev.change_in_value or Decimal("0")
        prev.change_in_value = base + prior_adj

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

        rows = (
            self.db.query(MonthlyClosing)
            .filter(
                MonthlyClosing.account_id == account.id,
                MonthlyClosing.year == year,
                MonthlyClosing.month <= month,
            )
            .all()
        )
        sum_mov = sum((row.change_in_value or Decimal("0")) for row in rows)
        sum_util = sum((row.income or Decimal("0")) for row in rows)
        current = (
            self.db.query(MonthlyClosing)
            .filter(
                MonthlyClosing.account_id == account.id,
                MonthlyClosing.year == year,
                MonthlyClosing.month == month,
            )
            .first()
        )

        if ytd_mov is not None:
            diff_mov = ytd_mov - sum_mov
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
        current.income = current.net_value - current.change_in_value - prev.net_value

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

        count = 0
        for etf_code, payload in grouped.items():
            instrument = payload["etf_name"]
            market_value = payload["market_value"]

            # UNIQUE: account_id, bank_code, year, month, etf_code
            existing = (
                self.db.query(EtfComposition)
                .filter(
                    EtfComposition.account_id == account.id,
                    EtfComposition.bank_code == bank_code,
                    EtfComposition.year == year,
                    EtfComposition.month == month,
                    EtfComposition.etf_code == etf_code,
                )
                .first()
            )

            if existing:
                existing.etf_name = instrument
                existing.market_value = market_value
                existing.report_date = report_date
                existing.source_document_id = doc.id
            else:
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
        cash_value = _cash_from_asset_allocation_json(asset_alloc_json)
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
