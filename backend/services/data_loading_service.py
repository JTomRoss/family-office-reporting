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
        self._refresh_normalized_activity_from_monthly_closings(account=account, year=year)

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
        # Persist those months as official closings before handling the statement month.
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
            # UBS Suiza monthly statements may only expose "Total net assets"
            # without account_monthly_activity block.
            closing_bal = _safe_decimal((result.balances or {}).get("total_net_assets"))

        opening_bal = account_values.get("beginning_value")
        if opening_bal is None:
            if len(result.account_numbers or []) <= 1:
                opening_bal = _safe_decimal(result.opening_balance)

        # Income y cambios de valor
        income = account_values.get("income")
        change_in_value = account_values.get("change_investment")
        accrual = account_values.get("accrual")

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
        )

        self._recompute_ubs_income_from_identity(
            account=account,
            year=year,
            month=month,
        )
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
        self._refresh_normalized_activity_from_monthly_closings(
            account=account,
            year=year,
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

        cash_value = _cash_from_asset_allocation_json(asset_alloc_json)
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

            existing_cash = normalized.cash_value if normalized else None
            cash_value = (
                existing_cash
                if existing_cash is not None
                else _cash_from_asset_allocation_json(closing.asset_allocation_json)
            )

            payload = {
                "closing_date": closing.closing_date,
                "ending_value_with_accrual": ending_with,
                "ending_value_without_accrual": ending_without,
                "accrual_ending": accrual_value,
                "cash_value": cash_value,
                "movements_net": closing.change_in_value,
                "profit_period": closing.income,
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
                if income is None:
                    self._recompute_ubs_income_from_identity(
                        account=account,
                        year=year,
                        month=month,
                    )
                continue

            existing.closing_date = closing_date
            if ending_value is not None:
                # Regla UBS: el backfill historico NO debe sobreescribir net_value
                # de meses previos cuando ya existe cierre mensual.
                if existing.net_value is None:
                    existing.net_value = ending_value
                if existing.total_assets is None:
                    existing.total_assets = ending_value
            if change_in_value is not None:
                existing.change_in_value = change_in_value
            if income is not None:
                existing.income = income
            if existing.source_document_id is None:
                existing.source_document_id = doc.id
            if income is None:
                self._recompute_ubs_income_from_identity(
                    account=account,
                    year=year,
                    month=month,
                )

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
                break

        # -- account_ytd (fallback if monthly not available) --
        if "income" not in values or "change_investment" not in values:
            for ytd in result.qualitative_data.get("account_ytd", []):
                if ytd.get("account_number") == account.account_number:
                    if "beginning_value" not in values:
                        values["beginning_value"] = _safe_decimal(ytd.get("beginning_value"))
                    if "ending_value" not in values:
                        values["ending_value"] = _safe_decimal(ytd.get("ending_value"))
                    if "income" not in values:
                        values["income"] = _safe_decimal(ytd.get("income"))
                    if "change_investment" not in values:
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
                target = current
                # BBH rule: prior-period adjustment belongs to previous month.
                if (
                    account.bank_code == "bbh"
                    and month > 1
                    and prior_adj is not None
                    and abs(prior_adj) > Decimal("0.0001")
                ):
                    target = (
                        self.db.query(MonthlyClosing)
                        .filter(
                            MonthlyClosing.account_id == account.id,
                            MonthlyClosing.year == year,
                            MonthlyClosing.month == month - 1,
                        )
                        .first()
                    )
                if target is not None:
                    target.change_in_value = (target.change_in_value or Decimal("0")) + diff_mov
                    self._log(
                        "load",
                        "info",
                        (
                            f"YTD caja alineada {account.bank_code}/{account.account_number} "
                            f"{year}-{month:02d}: ajuste={diff_mov}"
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
                if current is not None:
                    current.income = (current.income or Decimal("0")) + diff_util
                    self._log(
                        "load",
                        "info",
                        (
                            f"YTD utilidad alineada {account.bank_code}/{account.account_number} "
                            f"{year}-{month:02d}: ajuste={diff_util}"
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
        if month in {3, 6, 9, 12}:
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
        current.income = current.net_value - current.change_in_value - prev.net_value

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
                    target = current
                    if (
                        account.bank_code == "bbh"
                        and month > 1
                        and prior_adj is not None
                        and abs(prior_adj) > Decimal("0.0001")
                    ):
                        target = next((r for r in rows if r.month == month - 1), target)
                    if target is not None:
                        target.change_in_value = (target.change_in_value or Decimal("0")) + diff_mov
                        self._log(
                            "load",
                            "info",
                            (
                                f"YTD serie caja alineada {account.bank_code}/{account.account_number} "
                                f"{year}-{month:02d}: ajuste={diff_mov}"
                            ),
                            raw_document_id=raw_document_id,
                            account_id=account.id,
                        )

            if ytd_util is not None:
                diff_util = ytd_util - sum_util
                if abs(diff_util) > Decimal("1"):
                    current.income = (current.income or Decimal("0")) + diff_util
                    self._log(
                        "load",
                        "info",
                        (
                            f"YTD serie utilidad alineada {account.bank_code}/{account.account_number} "
                            f"{year}-{month:02d}: ajuste={diff_util}"
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

    def _serialize_parse_result(self, result: ParseResult, account: Account) -> str:
        """Serializa ParseResult relevante a JSON para almacenar en parsed_data_json."""
        data = {
            "rows": [
                row.data
                for row in result.rows
                if not row.data.get("is_total")
                and (
                    not row.data.get("account_number")
                    or row.data.get("account_number") == account.account_number
                )
            ],
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
