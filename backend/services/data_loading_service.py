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
from datetime import date, timezone, datetime
from decimal import Decimal, InvalidOperation
from typing import Optional

from sqlalchemy.orm import Session

from backend.db.models import (
    Account,
    EtfComposition,
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


class DataLoadingService:
    """Carga datos parseados a las tablas de reporting."""

    def __init__(self, db: Session):
        self.db = db

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

        # Multi-cuenta
        if result.account_numbers:
            for acct_num in result.account_numbers:
                acct = (
                    self.db.query(Account)
                    .filter(Account.account_number == acct_num)
                    .first()
                )
                if acct:
                    accounts.append(acct)
                else:
                    logger.warning(
                        "Cuenta %s no encontrada en maestro", acct_num
                    )
        # Cuenta única
        elif result.account_number and result.account_number != "Varios":
            acct = (
                self.db.query(Account)
                .filter(Account.account_number == result.account_number)
                .first()
            )
            if acct:
                accounts.append(acct)

        # Fallback: si el documento tiene account_id asignado
        if not accounts and doc.account_id:
            acct = (
                self.db.query(Account)
                .filter(Account.id == doc.account_id)
                .first()
            )
            if acct:
                accounts.append(acct)

        return accounts

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

        opening_bal = account_values.get("beginning_value")
        if opening_bal is None:
            if len(result.account_numbers or []) <= 1:
                opening_bal = _safe_decimal(result.opening_balance)

        # Income y cambios de valor
        income = account_values.get("income")
        change_in_value = account_values.get("change_investment")

        # Asset allocation (nivel consolidado)
        asset_alloc = result.qualitative_data.get("asset_allocation")
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
            existing.income = income
            existing.change_in_value = change_in_value
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
                asset_allocation_json=asset_alloc_json,
                source_document_id=doc.id,
            )
            self.db.add(mc)

        return True

    def _get_account_specific_values(
        self, result: ParseResult, account: Account
    ) -> dict:
        """
        Extrae valores específicos de una cuenta desde qualitative_data.
        Para reportes multi-cuenta (JPMorgan), busca en account_ytd e income_summary.
        """
        values: dict = {}

        # -- accounts (summary) --
        for acct_info in result.qualitative_data.get("accounts", []):
            if acct_info.get("account_number") == account.account_number:
                values["beginning_value"] = _safe_decimal(acct_info.get("beginning_value"))
                values["ending_value"] = _safe_decimal(acct_info.get("ending_value"))
                break

        # -- account_ytd --
        for ytd in result.qualitative_data.get("account_ytd", []):
            if ytd.get("account_number") == account.account_number:
                if "beginning_value" not in values:
                    values["beginning_value"] = _safe_decimal(ytd.get("beginning_value"))
                if "ending_value" not in values:
                    values["ending_value"] = _safe_decimal(ytd.get("ending_value"))
                values["income"] = _safe_decimal(ytd.get("income"))
                values["change_investment"] = _safe_decimal(ytd.get("change_investment"))
                break

        # -- income_summary --
        for inc in result.qualitative_data.get("income_summary", []):
            if inc.get("account_number") == account.account_number:
                if "income" not in values:
                    values["income"] = _safe_decimal(inc.get("income"))
                break

        return values

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
        count = 0

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
