"""
Parser: Goldman Sachs – ETF Brokerage Statement (PDF).
v2.0.0 – Real extraction using PyMuPDF (fitz).

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
from pathlib import Path
from decimal import Decimal
from typing import Any

from parsers.base import BaseParser, ParseResult, ParsedRow, ParserStatus
from parsers.goldman_sachs._gs_common import (
    parse_usd,
    extract_all_text_fitz,
    extract_page_texts_fitz,
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
    VERSION = "2.0.0"
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
            import fitz
            doc = fitz.open(str(filepath))
            if len(doc) == 0:
                doc.close()
                return 0.0

            text = ""
            for i in range(min(3, len(doc))):
                text += doc[i].get_text() + "\n"
            doc.close()

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

            # 5) Holdings (pages 11+)
            holdings = extract_holdings(page_texts)
            for h in holdings:
                confidence = 0.85
                w: list[str] = []
                if not h.get("market_value"):
                    w.append("Missing market_value")
                    confidence = 0.60
                rows.append(ParsedRow(
                    data={k: str(v) if isinstance(v, Decimal) else v for k, v in h.items()},
                    confidence=confidence,
                    warnings=w,
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

        return ParseResult(
            status=status,
            parser_name=self.get_parser_name(),
            parser_version=self.VERSION,
            source_file_hash=file_hash,
            bank_code=self.BANK_CODE,
            rows=rows,
            balances=balances,
            qualitative_data=qualitative,
            account_number=balances.get("account_number"),
            currency="USD",
            warnings=warnings,
        )

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
