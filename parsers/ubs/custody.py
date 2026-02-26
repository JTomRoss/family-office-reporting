"""
Parser: UBS Suiza – Statement of Assets (Cartola PDF).
v2.0.0 – Real extraction from concatenated-text UBS Switzerland PDFs.

FORMAT NOTES
============
- UBS Suiza PDFs have **concatenated text** (no spaces between words).
  e.g. "UBSSwitzerlandAG", "BoatviewLimitedRoadTown,Tortola"
- Numbers ARE separated from surrounding text in most cases.
- Portfolio number pattern: "Portfolio206-XXXXXX-NN"
- Page 3 ("Total assets") contains BOTH Portfolio 01 and Portfolio 02 summaries
  plus currency allocation – this is the richest data page.
- Later pages are specific to the current portfolio only.

TESTED AGAINST
==============
- 202512 Boatview UBS Suiza (Portfolio 1) - Mandato.pdf  (15 pages, Portfolio 01)
"""

from __future__ import annotations

import re
import logging
from pathlib import Path
from decimal import Decimal, InvalidOperation
from typing import Any

import pdfplumber

from parsers.base import BaseParser, ParseResult, ParsedRow, ParserStatus

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_usd(raw: str | None) -> Decimal | None:
    """Parse a USD amount from UBS Suiza text.

    Numbers may appear as '92054308', '52974', '420163', '37633194'.
    Some may have decimals like '2974.10'.
    """
    if not raw:
        return None
    cleaned = raw.strip().replace(",", "").replace(" ", "")
    # Remove parentheses (negative)
    neg = False
    if cleaned.startswith("(") and cleaned.endswith(")"):
        neg = True
        cleaned = cleaned[1:-1]
    if cleaned.startswith("-"):
        neg = True
        cleaned = cleaned[1:]
    try:
        val = Decimal(cleaned)
        return -val if neg else val
    except (InvalidOperation, ValueError):
        return None


def _extract_all_text(pdf: pdfplumber.PDF) -> str:
    """Concatenate text from all pages."""
    parts = []
    for page in pdf.pages:
        txt = page.extract_text() or ""
        parts.append(txt)
    return "\n".join(parts)


def _extract_page_text(pdf: pdfplumber.PDF, page_idx: int) -> str:
    """Extract text from a specific page (0-indexed)."""
    if page_idx < len(pdf.pages):
        return pdf.pages[page_idx].extract_text() or ""
    return ""


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

class UBSSwitzerlandCustodyParser(BaseParser):
    BANK_CODE = "ubs"
    ACCOUNT_TYPE = "custody"
    VERSION = "2.0.0"
    DESCRIPTION = "Parser para Statement of Assets UBS Suiza (PDF)"
    SUPPORTED_EXTENSIONS = [".pdf"]

    # ── detection ──────────────────────────────────────────────────────
    def detect(self, filepath: Path) -> float:
        if filepath.suffix.lower() != ".pdf":
            return 0.0
        try:
            with pdfplumber.open(filepath) as pdf:
                if not pdf.pages:
                    return 0.0
                # Check first 3 pages
                text = ""
                for i in range(min(3, len(pdf.pages))):
                    text += (pdf.pages[i].extract_text() or "") + "\n"
                text_lower = text.lower()

                score = 0.0
                # Must have UBS Switzerland markers
                if "ubsswitzerlandag" in text_lower.replace(" ", ""):
                    score += 0.35
                elif "ubs" in text_lower and "switzerland" in text_lower:
                    score += 0.30
                # Statement of assets
                if "statementofassets" in text_lower.replace(" ", ""):
                    score += 0.25
                elif "statement of assets" in text_lower:
                    score += 0.25
                # Portfolio number pattern
                if re.search(r"portfolio\s*206-\d{6}-\d{2}", text_lower.replace(" ", "")):
                    score += 0.20
                # File name bonus
                fname = filepath.stem.lower()
                if "suiza" in fname or "switzerland" in fname:
                    score += 0.15
                if "portfolio" in fname:
                    score += 0.05

                return min(score, 1.0)
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
            with pdfplumber.open(filepath) as pdf:
                all_text = _extract_all_text(pdf)

                # 1) Portfolio number & period
                acct = self._extract_portfolio_number(all_text)
                period = self._extract_period(all_text)
                if acct:
                    balances["account_number"] = acct
                if period:
                    balances["period"] = period
                balances["currency"] = "USD"

                # 2) Total assets overview (page 3 – both portfolios)
                p3_text = _extract_page_text(pdf, 2)  # 0-indexed
                total_overview = self._extract_total_assets(p3_text)
                if total_overview:
                    balances["total_net_assets"] = total_overview.get("total_net_assets")
                    balances["portfolios"] = total_overview.get("portfolios", {})
                    balances["currency_allocation"] = total_overview.get("currency_allocation", {})

                # 3) Portfolio overview (page 4)
                p4_text = _extract_page_text(pdf, 3)
                overview = self._extract_portfolio_overview(p4_text)
                if overview:
                    balances["portfolio_overview"] = overview

                # 4) Asset allocation (pages 5-7)
                alloc_text = ""
                for i in range(4, min(7, len(pdf.pages))):
                    alloc_text += _extract_page_text(pdf, i) + "\n"
                allocation = self._extract_asset_allocation(alloc_text)
                if allocation:
                    balances["asset_allocation"] = allocation

                # 5) Performance (pages 8-9)
                perf_text = ""
                for i in range(7, min(9, len(pdf.pages))):
                    perf_text += _extract_page_text(pdf, i) + "\n"
                performance = self._extract_performance(perf_text)
                if performance:
                    qualitative["performance"] = performance

                # 6) Holdings / Detailed positions (pages 10-11)
                holdings = self._extract_holdings(pdf, warnings)
                rows.extend(holdings)

        except Exception as exc:
            logger.exception("UBS Suiza parse error: %s", exc)
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
        # Cross-check: if total_net_assets available, verify portfolio sums
        bal = result.balances or {}
        total = bal.get("total_net_assets")
        portfolios = bal.get("portfolios", {})
        if total and portfolios:
            computed_sum = Decimal("0")
            for pkey, pdata in portfolios.items():
                net = pdata.get("net_assets")
                if net is not None:
                    computed_sum += Decimal(str(net))
            if computed_sum > 0:
                diff = abs(Decimal(str(total)) - computed_sum)
                if diff > Decimal("1"):
                    errors.append(
                        f"UBS portfolio sum {computed_sum} != total {total} (diff={diff})"
                    )
        return errors

    # ── internal extraction methods ───────────────────────────────────

    def _extract_portfolio_number(self, text: str) -> str | None:
        """Extract portfolio number like 206-560552-01."""
        # Text may be "Portfolio206-560552-01" or "Portfolio 206-560552-01"
        m = re.search(r"Portfolio\s*(\d{3}-\d{6}-\d{2})", text)
        return m.group(1) if m else None

    def _extract_period(self, text: str) -> dict[str, str] | None:
        """Extract statement date from 'Statementofassetsasof31December2025'."""
        # Concatenated form
        m = re.search(
            r"[Ss]tatement\s*of\s*assets\s*as\s*of\s*(\d{1,2})\s*([A-Z][a-z]+)\s*(\d{4})",
            text
        )
        if not m:
            # Try fully concatenated
            m = re.search(
                r"Statementofassetsasof(\d{1,2})([A-Z][a-z]+)(\d{4})",
                text
            )
        if m:
            return {
                "as_of_date": f"{m.group(1)} {m.group(2)} {m.group(3)}",
                "day": m.group(1),
                "month": m.group(2),
                "year": m.group(3),
            }
        return None

    def _extract_total_assets(self, page3_text: str) -> dict[str, Any] | None:
        """Extract total net assets and both portfolio summaries from page 3.

        Page 3 has:
        - Totalnetassetsasof31.12.2025 USD92054308
        - Portfolio01: asset class breakdown
        - Portfolio02-UBSManagePremium...: asset class breakdown
        - Currency allocation table
        """
        result: dict[str, Any] = {}

        # Total net assets
        m = re.search(
            r"Totalnetassets(?:asof[\d.]+)?\s+USD\s*([\d]+)",
            page3_text.replace(" ", "")
        )
        if not m:
            # Try with spaces
            m = re.search(r"Total\s*net\s*assets.*?USD\s*([\d,]+)", page3_text)
        if m:
            result["total_net_assets"] = _parse_usd(m.group(1))

        # Portfolio sections
        portfolios: dict[str, dict[str, Any]] = {}

        # Portfolio 01
        p01_block = self._extract_portfolio_block(page3_text, "Portfolio01")
        if p01_block:
            portfolios["Portfolio01"] = p01_block

        # Portfolio 02
        p02_block = self._extract_portfolio_block(page3_text, "Portfolio02")
        if p02_block:
            portfolios["Portfolio02"] = p02_block

        if portfolios:
            result["portfolios"] = portfolios

        # Currency allocation
        currencies = self._extract_currency_allocation(page3_text)
        if currencies:
            result["currency_allocation"] = currencies

        return result if result else None

    def _extract_portfolio_block(self, text: str, portfolio_label: str) -> dict[str, Any] | None:
        """Extract asset class breakdown for a portfolio from the Total assets page."""
        result: dict[str, Any] = {}
        # Remove all spaces for pattern matching in concatenated text
        no_space = text.replace(" ", "")

        # Find the portfolio section
        start_idx = no_space.find(portfolio_label)
        if start_idx == -1:
            return None

        # Find the end (next Portfolio or currency allocation section)
        end_idx = len(no_space)
        for boundary in ["Portfolio0", "Totalnetassetsbyexposurecurrency",
                         "Totalnetassetsbyexposure"]:
            pos = no_space.find(boundary, start_idx + len(portfolio_label))
            if pos != -1 and pos < end_idx:
                end_idx = pos

        block = no_space[start_idx:end_idx]

        # Extract asset classes: Liquidity, Bonds, Equities
        asset_patterns = [
            (r"Liquidity(\d+)", "liquidity"),
            (r"Bonds(\d+)", "bonds"),
            (r"Equities(\d+)", "equities"),
        ]
        asset_classes: dict[str, Decimal | None] = {}
        for pattern, key in asset_patterns:
            m = re.search(pattern, block)
            if m:
                asset_classes[key] = _parse_usd(m.group(1))

        if asset_classes:
            result["asset_classes"] = asset_classes

        # Net assets for this portfolio
        m = re.search(r"Netassets(\d+)", block)
        if m:
            result["net_assets"] = _parse_usd(m.group(1))

        # Performance TWR if present
        m = re.search(r"TWR\)inUSD\s*([\d.%-]+)", block)
        if not m:
            m = re.search(r"TWR\)inUSD([\d.]+%)", block)
        if m:
            result["performance_twr_ytd"] = m.group(1)

        # Portfolio description (for Portfolio 02)
        if "UBSManagePremium" in block:
            m = re.search(r"Portfolio02-(.*?)(?:Assetclass|Marketvalue)", block)
            if m:
                result["description"] = m.group(1).strip()

        return result if result else None

    def _extract_currency_allocation(self, text: str) -> dict[str, dict[str, Any]] | None:
        """Extract currency allocation from page 3.

        Pattern: USD USDollar 71324799 77.49%
                 EUR Euro 5671525 6.16%  etc.
        """
        currencies: dict[str, dict[str, Any]] = {}

        # These appear as concatenated lines but with currency code separated
        patterns = [
            (r"USD\s+USDollar\s+(\d+)\s+([\d.]+)%", "USD", "US Dollar"),
            (r"EUR\s+Euro\s+(\d+)\s+([\d.]+)%", "EUR", "Euro"),
            (r"GBP\s+PoundSterling\s+(\d+)\s+([\d.]+)%", "GBP", "Pound Sterling"),
            (r"GLB\s+Global\s+(\d+)\s+([\d.]+)%", "GLB", "Global"),
            (r"JPY\s+JapaneseYen\s+(\d+)\s+([\d.]+)%", "JPY", "Japanese Yen"),
            (r"CHF\s+SwissFranc\s+(\d+)\s+([\d.]+)%", "CHF", "Swiss Franc"),
            (r"Various\s+(\d+)\s+([\d.]+)%", "Various", "Various"),
        ]

        for pattern, code, name in patterns:
            m = re.search(pattern, text)
            if m:
                if code == "Various":
                    val = _parse_usd(m.group(1))
                    pct = m.group(2)
                else:
                    val = _parse_usd(m.group(1))
                    pct = m.group(2)
                currencies[code] = {
                    "name": name,
                    "market_value": val,
                    "percentage": pct,
                }

        return currencies if currencies else None

    def _extract_portfolio_overview(self, text: str) -> dict[str, Any] | None:
        """Extract portfolio overview from page 4 (net assets, performance, dates)."""
        result: dict[str, Any] = {}

        # Net assets and performance by period
        # Pattern: 31.12.2024-31.12.2025 52974 2.63% 1397
        periods = re.findall(
            r"(\d{2}\.\d{2}\.\d{4})-(\d{2}\.\d{2}\.\d{4})\s+(\d+)\s+([\d.]+%)\s+(\d+)",
            text,
        )
        if periods:
            annual_perf = []
            for start, end, net_assets, twr, value in periods:
                annual_perf.append({
                    "period_start": start,
                    "period_end": end,
                    "net_assets": _parse_usd(net_assets),
                    "twr": twr,
                    "performance_value": _parse_usd(value),
                })
            result["periods"] = annual_perf

        # Reference currency
        m = re.search(r"Referencecurrency\s+(\w+)", text)
        if not m:
            m = re.search(r"Referencecurrency(\w+)", text.replace(" ", ""))
        if m:
            result["reference_currency"] = m.group(1)

        return result if result else None

    def _extract_asset_allocation(self, text: str) -> dict[str, Any] | None:
        """Extract asset allocation from pages 5-7."""
        result: dict[str, Any] = {}

        # By asset class: "Liquidity 52974 100.00 ..."
        # By currency: "USD USDollar 52974 100.00 ..."
        # By instrument category: "Directinvestments 50000 94.39"

        # Instrument category
        instruments: dict[str, dict[str, Any]] = {}
        instr_patterns = [
            (r"Directinvestments\s+(\d+)\s+([\d.]+)", "Direct investments"),
            (r"Account\s+(\d+)\s+([\d.]+)", "Account"),
        ]
        for pattern, name in instr_patterns:
            m = re.search(pattern, text)
            if m:
                instruments[name] = {
                    "amount": _parse_usd(m.group(1)),
                    "percentage": m.group(2),
                }
        if instruments:
            result["by_instrument"] = instruments

        return result if result else None

    def _extract_performance(self, text: str) -> dict[str, Any] | None:
        """Extract performance data from pages 8-9.

        Targets:
        - Monthly TWR values
        - Performance summary (current month, quarter, YTD, since inception)
        - Monthly detail table
        """
        result: dict[str, Any] = {}

        # Performance summary line:
        # "PerformanceTWR(NetbeforeTax) 0.18% 0.59% 2.63% 2.63% -18.66%"
        m = re.search(
            r"PerformanceTWR\(NetbeforeTax\)\s+([-\d.]+%)\s+([-\d.]+%)\s+([-\d.]+%)\s+([-\d.]+%)\s+([-\d.]+%)",
            text.replace(" ", ""),
        )
        if not m:
            # Try with spaces
            m = re.search(
                r"PerformanceTWR\s*\(Net\s*before\s*Tax\)\s+([-\d.]+%)\s+([-\d.]+%)\s+([-\d.]+%)\s+([-\d.]+%)\s+([-\d.]+%)",
                text,
            )
        if m:
            result["summary"] = {
                "current_month": m.group(1),
                "current_quarter": m.group(2),
                "year_to_date": m.group(3),
                "last_12_months": m.group(4),
                "since_inception": m.group(5),
            }

        # Average annual performance
        m = re.search(r"AverageannualperformanceTWR\(NetbeforeTax\)\s+([-\d.]+%)", text.replace(" ", ""))
        if m:
            result["average_annual_twr"] = m.group(1)

        # Monthly performance rows:
        # "31December2025 52974 0 -640 96 0.18% 1397 2.63%"
        months_data = []
        month_pattern = re.compile(
            r"(\d{1,2}\s*[A-Z][a-z]+\s*\d{4})\s+(\d+)\s+(\d+)\s+([-\d]+)\s+([-\d]+)\s+([-\d.]+%)\s+(\d+)\s+([-\d.]+%)"
        )
        for match in month_pattern.finditer(text):
            months_data.append({
                "period": match.group(1).strip(),
                "final_value": _parse_usd(match.group(2)),
                "inflows": _parse_usd(match.group(3)),
                "outflows": _parse_usd(match.group(4)),
                "performance_value": _parse_usd(match.group(5)),
                "twr_monthly": match.group(6),
                "cumulative_value": _parse_usd(match.group(7)),
                "twr_cumulative": match.group(8),
            })
        if months_data:
            result["monthly"] = months_data

        return result if result else None

    def _extract_holdings(
        self, pdf: pdfplumber.PDF, warnings: list[str]
    ) -> list[ParsedRow]:
        """Extract detailed positions from pages 10+.

        Portfolio 01 has minimal holdings:
        - Liquidity-Accounts: USD current account
        - Liquidity-Call deposits: Fiduciary call investment

        For larger portfolios (Portfolio 02), this would include bonds and equities.
        """
        rows: list[ParsedRow] = []

        # Scan pages for "Detailed positions" or "Positions overview"
        for page_idx in range(len(pdf.pages)):
            page_text = _extract_page_text(pdf, page_idx)
            no_space = page_text.replace(" ", "")

            # ── Liquidity – Accounts ──
            if "Liquidity-Accounts" in no_space or "Liquidity-Accounts" in page_text:
                acct_rows = self._parse_account_holdings(page_text, no_space)
                rows.extend(acct_rows)

            # ── Liquidity – Call Deposits ──
            if "Liquidity-Calldeposits" in no_space or "CallDeposits" in no_space:
                deposit_rows = self._parse_call_deposit_holdings(page_text, no_space)
                rows.extend(deposit_rows)

            # ── Bonds ──
            if "Bonds-" in no_space or "Fixedincome" in no_space:
                bond_rows = self._parse_bond_holdings(page_text, no_space)
                rows.extend(bond_rows)

            # ── Equities ──
            if "Equities-" in no_space:
                equity_rows = self._parse_equity_holdings(page_text, no_space)
                rows.extend(equity_rows)

        if not rows:
            warnings.append("No detailed position rows extracted")

        return rows

    def _parse_account_holdings(self, text: str, no_space: str) -> list[ParsedRow]:
        """Parse liquidity account holdings.

        Pattern: USD 2974.10 UBSCurrentAccountforPrivateClients USD 4509.04 2974 5.61
                 CH850020620656055260F 31.12.2024 0.00
        """
        rows: list[ParsedRow] = []

        # Match account lines - currency amount description
        pattern = re.compile(
            r"(USD|EUR|CHF|GBP)\s+([\d.]+)\s+(.*?)\s+(USD|EUR|CHF|GBP)\s+([\d.]+)\s+(\d+)\s+([\d.]+)"
        )
        for m in pattern.finditer(text):
            data = {
                "asset_class": "Liquidity",
                "sub_class": "Accounts",
                "currency": m.group(1),
                "amount": str(_parse_usd(m.group(2))),
                "description": m.group(3).strip(),
                "opening_currency": m.group(4),
                "opening_balance": str(_parse_usd(m.group(5))),
                "market_value": str(_parse_usd(m.group(6))),
                "percentage": m.group(7),
            }
            rows.append(ParsedRow(data=data, confidence=0.80,
                                  warnings=["Concatenated text – verify description"]))

        # Also try to find IBAN
        iban_match = re.search(r"(CH\d{2}\d{16,20}\w*)", no_space)
        if iban_match and rows:
            rows[0].data["iban"] = iban_match.group(1)

        return rows

    def _parse_call_deposit_holdings(self, text: str, no_space: str) -> list[ParsedRow]:
        """Parse call deposit holdings.

        Pattern: USD 50000 FiduciaryCallInvestment 100% 100%I 50000 94.39
        """
        rows: list[ParsedRow] = []

        # Fiduciary call investment
        m = re.search(
            r"(USD|EUR|CHF|GBP)\s+(\d+)\s+(Fiduciary\s*Call\s*Investment|FiduciaryCallInvestment)",
            text,
        )
        if not m:
            m = re.search(r"(USD|EUR|CHF)\s+(\d+)\s+Fiduciary", text)

        if m:
            # Find the market value and percentage
            market_val = _parse_usd(m.group(2))
            # Look for interest rate
            rate_match = re.search(r"Interestrate:\s*([\d.]+%)", text)

            data = {
                "asset_class": "Liquidity",
                "sub_class": "Call deposits",
                "currency": m.group(1),
                "description": "Fiduciary Call Investment",
                "face_value": str(market_val) if market_val else None,
                "market_value": str(market_val) if market_val else None,
            }
            if rate_match:
                data["interest_rate"] = rate_match.group(1)

            # Serial number
            serial = re.search(r"Serialno\.\s*(\d+)", text.replace(" ", ""))
            if serial:
                data["serial_number"] = serial.group(1)

            rows.append(ParsedRow(data=data, confidence=0.85, warnings=[]))

        # Total call deposits as verification
        total_match = re.search(r"TotalCalldeposits\s+(\d+)", no_space)
        if total_match and rows:
            rows[-1].data["total_call_deposits"] = str(_parse_usd(total_match.group(1)))

        return rows

    def _parse_bond_holdings(self, text: str, no_space: str) -> list[ParsedRow]:
        """Parse bond holdings (for Portfolio 02 PDFs with fixed income).

        Expected pattern per holding (multi-line):
        CCY FACE_VALUE DESCRIPTION RATING COST_PRICE MKT_PRICE GAIN MARKET_VALUE %
                       SECTOR                     EXCH_RATE  EXCH_GAIN ACCR_INT
                       DURATION                   COST_VALUE DATE      UNREAL_PL
                       YIELD                      LAST_PURCHASE
        """
        rows: list[ParsedRow] = []

        # Generic bond line: Currency, face value, and market value
        bond_pattern = re.compile(
            r"(USD|EUR|GBP|CHF|JPY)\s+([\d,]+)\s+(.+?)\s+"
            r"([\d.]+%?)\s+([\d.]+%?)\s+([-\d,]+)\s+([\d,]+)\s+([\d.]+)"
        )
        for m in bond_pattern.finditer(text):
            data = {
                "asset_class": "Bonds",
                "currency": m.group(1),
                "face_value": str(_parse_usd(m.group(2))),
                "description": m.group(3).strip(),
                "cost_price": m.group(4),
                "market_price": m.group(5),
                "market_gain": str(_parse_usd(m.group(6))),
                "market_value": str(_parse_usd(m.group(7))),
                "percentage": m.group(8),
            }
            rows.append(ParsedRow(data=data, confidence=0.70,
                                  warnings=["Bond extract from concatenated text"]))

        return rows

    def _parse_equity_holdings(self, text: str, no_space: str) -> list[ParsedRow]:
        """Parse equity holdings (for Portfolio 02 PDFs with equities)."""
        rows: list[ParsedRow] = []

        # Generic equity line pattern
        equity_pattern = re.compile(
            r"(USD|EUR|GBP|CHF|JPY)\s+([\d,]+)\s+(.+?)\s+"
            r"([\d.]+)\s+([\d.]+)\s+([-\d,]+)\s+([\d,]+)\s+([\d.]+)"
        )
        for m in equity_pattern.finditer(text):
            data = {
                "asset_class": "Equities",
                "currency": m.group(1),
                "quantity": str(_parse_usd(m.group(2))),
                "description": m.group(3).strip(),
                "avg_cost": str(_parse_usd(m.group(4))),
                "market_price": str(_parse_usd(m.group(5))),
                "market_gain": str(_parse_usd(m.group(6))),
                "market_value": str(_parse_usd(m.group(7))),
                "percentage": m.group(8),
            }
            rows.append(ParsedRow(data=data, confidence=0.70,
                                  warnings=["Equity extract from concatenated text"]))

        return rows
