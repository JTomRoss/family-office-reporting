"""
Parser: Goldman Sachs – Mandato / Wrap Statement (PDF).
v2.1.0 – Real extraction using PyMuPDF (fitz).

CRITICAL: pdfplumber CANNOT extract text from GS PDFs.
Must use ``fitz`` (PyMuPDF).

TESTED AGAINST
==============
- 202512 Boatview - Mandato - GoldmanSachs.pdf  (222 pages)
  Portfolio XXX-XX451-9 (group)
  9 individual sub-portfolios:
    Advisory (XXX-XX063-1) — $117.4M
    Aristotle LCV (XXX-XX064-9)
    Eastern Shore (XXX-XX065-6)
    Brokerage (XXX-XX066-4)
    Corporate Fixed Income (XXX-XX067-2)
    Harding Loevner (XXX-XX069-8)
    Wellington Non-US EQ (XXX-XX072-2)
    Brokerage 2 (XXX-XX147-6)
    Brokerage (XXX-XX195-0)
  Total: $273,860,843.16
"""

from __future__ import annotations

import re
import logging
from pathlib import Path
from datetime import datetime, date
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
    extract_sub_portfolios,
)

logger = logging.getLogger(__name__)


class GoldmanSachsCustodyParser(BaseParser):
    BANK_CODE = "goldman_sachs"
    ACCOUNT_TYPE = "custody"
    VERSION = "2.1.0"
    DESCRIPTION = "Parser para cartolas Goldman Sachs Mandato/Wrap (PDF)"
    SUPPORTED_EXTENSIONS = [".pdf"]

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
            # Wrap/Mandato marker (this distinguishes from ETF)
            if "ex brokerage" in text_lower or "statement wrap" in text_lower:
                score += 0.30
            # Multiple sub-portfolios
            if text_lower.count("xxx-") >= 3:
                score += 0.15
            # Portfolio number pattern
            if re.search(r"xxx-\w+-\d+", text_lower):
                score += 0.10
            # File name bonus
            fname = filepath.stem.lower()
            if "goldmansachs" in fname or "goldman" in fname:
                score += 0.10
            if "mandato" in fname:
                score += 0.15

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
        statement_date = None
        period_start = None
        period_end = None
        opening_balance = None
        closing_balance = None

        try:
            page_texts = extract_page_texts_fitz(filepath)
            all_text = "\n".join(page_texts[:10])  # First 10 pages for summary data

            # 1) Period & master portfolio number
            period = extract_period(all_text)
            acct = extract_portfolio_number(all_text)
            if period:
                balances["period"] = period
                period_start = self._parse_period_date(period.get("start"))
                period_end = self._parse_period_date(period.get("end"))
                statement_date = period_end or period_start
            if acct:
                balances["account_number"] = acct
            balances["currency"] = "USD"

            # 2) Sub-portfolios (page 2)
            if len(page_texts) >= 2:
                sub_ports = extract_sub_portfolios(page_texts[1])
                if sub_ports:
                    balances["sub_portfolios"] = sub_ports

            # 3) Overview (page 3 — total portfolio, asset allocation, activity)
            if len(page_texts) >= 3:
                overview = extract_overview(page_texts[2])
                if overview:
                    balances.update(overview)
                    if overview.get("asset_allocation"):
                        qualitative["asset_allocation"] = self._json_safe(overview["asset_allocation"])
                    activity = overview.get("portfolio_activity", {})
                    inv_results = overview.get("investment_results", {})
                    opening_balance = (
                        activity.get("opening_value")
                        or inv_results.get("beginning_market_value")
                    )
                    closing_balance = (
                        activity.get("closing_value")
                        or inv_results.get("ending_market_value")
                        or overview.get("total_portfolio")
                    )

            # 4) Tax summary (pages 4-5)
            tax_text = ""
            for i in range(3, min(6, len(page_texts))):
                tax_text += page_texts[i] + "\n"
            tax = extract_tax_summary(tax_text)
            if tax:
                qualitative["tax_summary"] = tax

            # 5) Asset strategy analysis (pages 6-8)
            strategy_text = ""
            for i in range(5, min(9, len(page_texts))):
                strategy_text += page_texts[i] + "\n"
            strategy = extract_asset_strategy(strategy_text)
            if strategy:
                qualitative["asset_strategy"] = strategy

            # 6) Holdings from all sub-portfolio sections
            holdings = extract_holdings(page_texts)
            for h in holdings:
                confidence = 0.80
                w: list[str] = []
                if not h.get("market_value"):
                    w.append("Missing market_value")
                    confidence = 0.55
                rows.append(ParsedRow(
                    data={k: str(v) if isinstance(v, Decimal) else v for k, v in h.items()},
                    confidence=confidence,
                    warnings=w,
                ))

            # 7) Extract per-sub-portfolio overviews
            sub_overviews = self._extract_sub_portfolio_overviews(page_texts, warnings)
            if sub_overviews:
                qualitative["sub_portfolio_overviews"] = sub_overviews

            monthly = self._build_account_monthly_activity(
                account_number=balances.get("account_number"),
                opening_balance=opening_balance,
                closing_balance=closing_balance,
                investment_results=balances.get("investment_results", {}),
            )
            if monthly:
                qualitative["account_monthly_activity"] = monthly

        except Exception as exc:
            logger.exception("Goldman Sachs Custody parse error: %s", exc)
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
            statement_date=statement_date,
            period_start=period_start,
            period_end=period_end,
            currency="USD",
            opening_balance=opening_balance,
            closing_balance=closing_balance,
            warnings=warnings,
        )

    @staticmethod
    def _parse_period_date(raw: str | None) -> date | None:
        if not raw:
            return None
        cleaned = raw.strip()
        for fmt in ("%B %d, %Y", "%b %d, %Y", "%B %d,%Y", "%b %d,%Y"):
            try:
                return datetime.strptime(cleaned, fmt).date()
            except ValueError:
                continue
        return None

    @staticmethod
    def _to_decimal(value: Any) -> Decimal | None:
        if value is None:
            return None
        if isinstance(value, Decimal):
            return value
        return parse_usd(str(value))

    @staticmethod
    def _json_safe(value: Any) -> Any:
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, dict):
            return {k: GoldmanSachsCustodyParser._json_safe(v) for k, v in value.items()}
        if isinstance(value, list):
            return [GoldmanSachsCustodyParser._json_safe(v) for v in value]
        return value

    def _build_account_monthly_activity(
        self,
        account_number: str | None,
        opening_balance: Decimal | None,
        closing_balance: Decimal | None,
        investment_results: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Estandariza movimientos/utilidad para carga a monthly_closings."""
        if not account_number:
            return []

        net_contributions = self._to_decimal(
            investment_results.get("net_deposits_withdrawals")
        )
        utilidad = self._to_decimal(investment_results.get("investment_results"))

        if (
            utilidad is None
            and opening_balance is not None
            and closing_balance is not None
            and net_contributions is not None
        ):
            utilidad = closing_balance - opening_balance - net_contributions

        if (
            net_contributions is None
            and opening_balance is not None
            and closing_balance is not None
            and utilidad is not None
        ):
            net_contributions = closing_balance - opening_balance - utilidad

        if utilidad is None and net_contributions is None:
            return []

        return [
            {
                "account_number": account_number,
                "beginning_value": (
                    str(opening_balance) if opening_balance is not None else None
                ),
                "ending_value_with_accrual": (
                    str(closing_balance) if closing_balance is not None else None
                ),
                "ending_value_without_accrual": (
                    str(closing_balance) if closing_balance is not None else None
                ),
                "net_contributions": (
                    str(net_contributions) if net_contributions is not None else None
                ),
                "utilidad": str(utilidad) if utilidad is not None else None,
                "source": "gs_investment_results",
            }
        ]

    # ── validate ──────────────────────────────────────────────────────
    def validate(self, result: ParseResult) -> list[str]:
        errors: list[str] = []
        bal = result.balances or {}

        # Cross-check: total portfolio vs investment results ending
        total = bal.get("total_portfolio")
        results = bal.get("investment_results", {})
        ending = results.get("ending_market_value")
        if total and ending and abs(Decimal(str(total)) - Decimal(str(ending))) > Decimal("1"):
            errors.append(
                f"GS Custody: total {total} != investment results ending {ending}"
            )

        return errors

    # ── sub-portfolio overviews ───────────────────────────────────────
    def _extract_sub_portfolio_overviews(
        self, page_texts: list[str], warnings: list[str],
    ) -> list[dict[str, Any]]:
        """Scan for sub-portfolio Overview pages and extract their summaries.

        Each sub-portfolio has an Overview page with TOTAL PORTFOLIO and
        PORTFOLIO ASSET ALLOCATION.  We look for pages where the footer
        says 'Overview' and 'Statement Detail'.
        """
        overviews: list[dict[str, Any]] = []

        for i, pt in enumerate(page_texts):
            lines = [l.strip() for l in pt.splitlines() if l.strip()]
            # Check if this is an Overview page for a sub-portfolio
            has_overview = any("Overview" in l for l in lines[-8:]) if len(lines) > 8 else False
            has_detail = any("Statement Detail" in l for l in lines[-8:]) if len(lines) > 8 else False
            has_total = any("TOTAL PORTFOLIO" in l for l in lines[:10])

            if has_overview and has_detail and has_total:
                # Extract portfolio name and number from the page
                port_num = None
                port_name = None
                for l in lines[-10:]:
                    m = re.search(r"Portfolio No:\s*([\w-]+)", l)
                    if m:
                        port_num = m.group(1)
                # Name from the page header area (first few lines typically)
                for l in lines[:5]:
                    if "BOATVIEW" in l and l != "BOATVIEW LIMITED":
                        port_name = l

                overview_data = extract_overview(pt)
                if overview_data:
                    overview_data["portfolio_number"] = port_num
                    overview_data["portfolio_name"] = port_name
                    overview_data["page"] = i + 1
                    overviews.append(overview_data)

        return overviews
