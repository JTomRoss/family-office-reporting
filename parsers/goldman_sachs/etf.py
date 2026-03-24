"""
Parser: Goldman Sachs – ETF Brokerage Statement (PDF).
v2.1.0 – Real extraction using PyMuPDF (fitz).
         Fix: strip XXX-XX mask, set period dates, populate activity data.

CRITICAL: pdfplumber CANNOT extract text from GS PDFs.
Must use ``fitz`` (PyMuPDF).

TESTED AGAINST
==============
- 202512 Boatview - ETF - GoldmanSachs.pdf  (21 pages)
  Portfolio XXX-XX452-2 (group), XXX-XX062-3 (individual)
  Total: $45,553,310.46
  5 ETF holdings: VUCP, VDCA, IHYA, IWDA, IEMA
"""

from __future__ import annotations

import re
import logging
from datetime import date as date_type, datetime
from pathlib import Path
from decimal import Decimal
from typing import Any

from parsers.base import BaseParser, ParseResult, ParsedRow, ParserStatus
from parsers.goldman_sachs._gs_common import (
    parse_usd,
    extract_all_text_fitz,
    extract_page_texts_fitz,
    extract_detection_text,
    extract_period,
    extract_portfolio_number,
    extract_overview,
    extract_tax_summary,
    extract_asset_strategy,
    extract_holdings,
)

logger = logging.getLogger(__name__)


class GoldmanSachsEtfParser(BaseParser):
    BANK_CODE = "goldman_sachs"
    ACCOUNT_TYPE = "etf"
    VERSION = "2.1.1"
    DESCRIPTION = "Parser para cartolas ETF Goldman Sachs – Brokerage Statement (PDF)"
    SUPPORTED_EXTENSIONS = [".pdf"]

    # Known ETFs in this portfolio
    VALID_ETFS = {
        "IWDA": "ISHARES CORE MSCI WORLD",
        "IEMA": "ISHARES MSCI EM-ACC",
        "IHYA": "ISHARES USD HY CORP USD ACC",
        "VUCP": "USD CORPORATE BOND UCITS ETF",
        "VDCA": "VANGUARD USD CORPORATE 1-3 YEAR BOND UCITS ETF",
    }

    # ── detection ──────────────────────────────────────────────────────
    def detect(self, filepath: Path) -> float:
        if filepath.suffix.lower() != ".pdf":
            return 0.0
        try:
            text, n_pages = extract_detection_text(str(filepath))
            if n_pages == 0:
                return 0.0

            text_lower = text.lower()
            score = 0.0

            # Goldman Sachs markers
            if "goldman sachs" in text_lower:
                score += 0.30
            # ETF marker
            if "brokerage etf" in text_lower or "etf statement" in text_lower:
                score += 0.25
            # Portfolio number pattern
            if re.search(r"xxx-\w+-\d+", text_lower):
                score += 0.15
            # File name bonus
            fname = filepath.stem.lower()
            if "goldmansachs" in fname or "goldman" in fname:
                score += 0.15
            if "etf" in fname:
                score += 0.15
            # EXCLUDE mandato/wrap statements (those go to custody parser)
            if "ex brokerage" in text_lower or "statement wrap" in text_lower:
                score -= 0.30

            return max(0.0, min(score, 1.0))
        except Exception:
            return 0.0

    # ── parse ─────────────────────────────────────────────────────────
    def parse(self, filepath: Path) -> ParseResult:
        file_hash = self.compute_file_hash(filepath)
        warnings: list[str] = []
        rows: list[ParsedRow] = []
        balances: dict[str, Any] = {}
        qualitative: dict[str, Any] = {}

        try:
            page_texts = extract_page_texts_fitz(filepath)
            all_text = "\n".join(page_texts)

            # 1) Period & portfolio number
            period = extract_period(all_text)
            acct = extract_portfolio_number(all_text)
            if period:
                balances["period"] = period
            if acct:
                balances["account_number"] = acct
            balances["currency"] = "USD"

            # 2) Overview (page 3 — asset alloc, portfolio activity)
            if len(page_texts) >= 3:
                overview = extract_overview(page_texts[2])
                if overview:
                    balances.update(overview)

            # 3) Tax summary (page 4)
            if len(page_texts) >= 4:
                tax = extract_tax_summary(page_texts[3])
                if tax:
                    qualitative["tax_summary"] = tax

            # 4) Asset strategy analysis (page 5)
            strategy_text = ""
            for i in range(4, min(7, len(page_texts))):
                strategy_text += page_texts[i] + "\n"
            strategy = extract_asset_strategy(strategy_text)
            if strategy:
                qualitative["asset_strategy"] = strategy

            # 5) Asset Strategy → generate instrument rows (correct per-instrument data)
            # asset_strategy has the accurate per-instrument market values;
            # extract_holdings is too fragile for GS format.
            if strategy:
                for s in strategy:
                    name = s.get("name", "")
                    mv = s.get("market_value")
                    if not name or mv is None:
                        continue
                    resolved_name = self._resolve_strategy_instrument_name(
                        name=name,
                        full_text=all_text,
                    )
                    rows.append(ParsedRow(
                        data={
                            "instrument": resolved_name,
                            "market_value": str(mv),
                            "percentage": s.get("percentage", ""),
                            "asset_class": s.get("category", ""),
                        },
                        confidence=0.90,
                    ))

            # 6) Cash activity — look for closing balance
            for i, pt in enumerate(page_texts):
                if "Cash Activity" in pt:
                    m = re.search(
                        r"CLOSING BALANCE AS OF DEC 31 25\s*\n?([\d,.]+)",
                        pt.replace("\n", " "),
                    )
                    if not m:
                        m = re.search(r"CLOSING BALANCE.*?(\d[\d,.]+)", pt)
                    if m:
                        balances["cash_closing_balance"] = parse_usd(m.group(1))
                    break

        except Exception as exc:
            logger.exception("Goldman Sachs ETF parse error: %s", exc)
            return ParseResult(
                status=ParserStatus.ERROR,
                parser_name=self.get_parser_name(),
                parser_version=self.VERSION,
                source_file_hash=file_hash,
                bank_code=self.BANK_CODE,
                warnings=[f"Parse error: {exc}"],
            )

        status = ParserStatus.SUCCESS if balances.get("account_number") else ParserStatus.PARTIAL
        if not balances.get("account_number"):
            warnings.append("Could not extract portfolio number")

        # ── Derive period_start/period_end from period dict ──────
        period_start_date = None
        period_end_date = None
        if period:
            period_end_date = self._parse_gs_date(period.get("end"))
            period_start_date = self._parse_gs_date(period.get("start"))

        # ── Set closing/opening balance from overview data ───────
        closing_bal = None
        opening_bal = None
        pa = balances.get("portfolio_activity", {})
        ir = balances.get("investment_results", {})
        if pa.get("closing_value"):
            closing_bal = pa["closing_value"]
        elif balances.get("total_portfolio"):
            closing_bal = balances["total_portfolio"]
        if pa.get("opening_value"):
            opening_bal = pa["opening_value"]
        elif ir.get("beginning_market_value"):
            opening_bal = ir["beginning_market_value"]

        # ── Populate account_monthly_activity for DataLoadingService ──
        acct_num = balances.get("account_number")
        if acct_num and ir:
            # utilidad = investment_results (profit)
            utilidad = ir.get("investment_results", Decimal("0"))
            # movimientos = net_deposits_withdrawals
            net_contrib = ir.get("net_deposits_withdrawals", Decimal("0"))
            qualitative["account_monthly_activity"] = [{
                "account_number": acct_num,
                "net_contributions": str(net_contrib) if net_contrib is not None else "0",
                "utilidad": str(utilidad) if utilidad is not None else "0",
                "income_distributions": str(pa.get("interest_received", Decimal("0"))),
                "change_investment": str(pa.get("change_in_value", Decimal("0"))),
                "accrual_beginning": None,
                "accrual_ending": None,
            }]
        elif acct_num and pa:
            # Fallback: utilidad = closing - opening - net_deposits
            net_dep = Decimal("0")
            if ir:
                net_dep = ir.get("net_deposits_withdrawals", Decimal("0")) or Decimal("0")
            op = opening_bal or Decimal("0")
            cl = closing_bal or Decimal("0")
            utilidad = cl - op - net_dep
            qualitative["account_monthly_activity"] = [{
                "account_number": acct_num,
                "net_contributions": str(net_dep),
                "utilidad": str(utilidad),
                "income_distributions": "0",
                "change_investment": str(pa.get("change_in_value", Decimal("0"))),
                "accrual_beginning": None,
                "accrual_ending": None,
            }]

        # ── Populate accounts for ending/beginning value ─────────
        if acct_num:
            qualitative["accounts"] = [{
                "account_number": acct_num,
                "beginning_value": str(opening_bal) if opening_bal else None,
                "ending_value": str(closing_bal) if closing_bal else None,
            }]

        result = ParseResult(
            status=status,
            parser_name=self.get_parser_name(),
            parser_version=self.VERSION,
            source_file_hash=file_hash,
            bank_code=self.BANK_CODE,
            rows=rows,
            balances=balances,
            qualitative_data=qualitative,
            account_number=acct_num,
            currency="USD",
            period_start=period_start_date,
            period_end=period_end_date,
            statement_date=period_end_date,
            opening_balance=opening_bal,
            closing_balance=closing_bal,
            warnings=warnings,
        )
        return result

    @staticmethod
    def _resolve_strategy_instrument_name(*, name: str, full_text: str) -> str:
        """
        Ajuste aislado GS ETF:
        algunos estados antiguos emiten "OTHER INVESTMENT GRADE SECURITIES"
        como label genérico y dejan el nombre ETF real en la línea siguiente.
        """
        normalized = str(name or "").strip()
        if not normalized:
            return normalized

        if normalized.upper() != "OTHER INVESTMENT GRADE SECURITIES":
            return normalized

        compact_text = re.sub(r"[^A-Z0-9]", "", str(full_text or "").upper())
        spdr_aliases = (
            "SSGA SPDR ETFS EU I PB L C-SPD ETF ON BLOOMBERG",
            "SPDR BLOOMBERG 1-10 YEAR U.S.",
            "SPDR BLOOMBERG 1-10 YEAR U.S",
        )
        for alias in spdr_aliases:
            compact_alias = re.sub(r"[^A-Z0-9]", "", alias.upper())
            if compact_alias and compact_alias in compact_text:
                return alias

        return normalized

    @staticmethod
    def _parse_gs_date(date_str: str | None) -> date_type | None:
        """Parse 'December 31, 2025' → date(2025, 12, 31)."""
        if not date_str:
            return None
        for fmt in ("%B %d, %Y", "%B %d %Y"):
            try:
                return datetime.strptime(date_str.replace(",", "").strip(), "%B %d %Y").date()
            except ValueError:
                continue
        return None

    # ── validate ──────────────────────────────────────────────────────
    def validate(self, result: ParseResult) -> list[str]:
        errors: list[str] = []
        bal = result.balances or {}

        # Cross-check: total portfolio vs sum of allocation
        total = bal.get("total_portfolio")
        alloc = bal.get("asset_allocation", {})
        if total and "TOTAL PORTFOLIO" in alloc:
            alloc_total = alloc["TOTAL PORTFOLIO"].get("market_value")
            if alloc_total and abs(Decimal(str(total)) - Decimal(str(alloc_total))) > Decimal("1"):
                errors.append(
                    f"GS ETF: total {total} != allocation total {alloc_total}"
                )

        # Cross-check: investment results ending vs total portfolio
        results = bal.get("investment_results", {})
        ending = results.get("ending_market_value")
        if total and ending and abs(Decimal(str(total)) - Decimal(str(ending))) > Decimal("1"):
            errors.append(
                f"GS ETF: total {total} != investment results ending {ending}"
            )

        return errors
