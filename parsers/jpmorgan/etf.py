"""
Parser: JPMorgan – Cuenta ETF (Cartola PDF – Consolidated Statement).

Formato:  "For the Period M/D/YY to M/D/YY"
Página 1: Account Summary (cuentas, beginning/ending market value)
Página 3: Consolidated Summary (asset allocation, portfolio activity)
Página 4: Per-account YTD (contributions, income, gains)
Páginas 5+: Holdings detail por cuenta

AISLADO: No comparte lógica con otros bancos.
"""

from __future__ import annotations

import re
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Optional

import pdfplumber

from parsers.base import BaseParser, ParseResult, ParsedRow, ParserStatus


# ── Helpers locales (aislados en este módulo) ────────────────────

def _parse_usd(text: str) -> Optional[Decimal]:
    """Parse US dollar string: '$1,234.56', '(1,234.56)', '-1,234.56'."""
    if not text or text.strip() in ("", "N/A", "--", "n/a"):
        return None
    s = text.strip().replace("$", "").replace(",", "").strip()
    negative = False
    if s.startswith("(") and s.endswith(")"):
        s = s[1:-1]
        negative = True
    if s.startswith("-"):
        s = s[1:]
        negative = True
    try:
        val = Decimal(s)
        return -val if negative else val
    except (InvalidOperation, ValueError):
        return None


def _parse_date_mdy_short(text: str) -> Optional[date]:
    """Parse 'M/D/YY' → date (assumes 2000s)."""
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{2,4})", text)
    if not m:
        return None
    month, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if year < 100:
        year += 2000
    try:
        return date(year, month, day)
    except ValueError:
        return None


def _extract_all_text(filepath: Path) -> list[str]:
    """Return list of text strings, one per page."""
    pages: list[str] = []
    with pdfplumber.open(filepath) as pdf:
        for page in pdf.pages:
            pages.append(page.extract_text() or "")
    return pages


# ── Parser ───────────────────────────────────────────────────────

class JPMorganEtfParser(BaseParser):
    BANK_CODE = "jpmorgan"
    ACCOUNT_TYPE = "etf"
    VERSION = "2.0.0"
    DESCRIPTION = "Parser para cartolas ETF JPMorgan (Consolidated Statement PDF)"
    SUPPORTED_EXTENSIONS = [".pdf"]

    _DETECTION_MARKERS = [
        "J.P. Morgan",
        "JPMorgan",
        "Consolidated Statement",
    ]

    # ── parse ────────────────────────────────────────────────────

    def parse(self, filepath: Path) -> ParseResult:
        file_hash = self.compute_file_hash(filepath)
        pages = _extract_all_text(filepath)

        result = ParseResult(
            status=ParserStatus.SUCCESS,
            parser_name=self.get_parser_name(),
            parser_version=self.VERSION,
            source_file_hash=file_hash,
            bank_code=self.BANK_CODE,
            currency="USD",
        )

        if not pages:
            result.status = ParserStatus.ERROR
            result.errors.append("PDF vacío o ilegible")
            return result

        result.raw_text_preview = pages[0][:500]

        # 1) Period dates
        self._extract_period(pages, result)

        # 2) Account summary (page 1)
        self._extract_account_summary(pages, result)

        # 3) Consolidated summary (usually page 3)
        self._extract_consolidated_summary(pages, result)

        # 4) Per-account YTD (usually page 4)
        self._extract_account_ytd(pages, result)

        # 5) Holdings from detail pages
        self._extract_holdings(pages, result)

        if not result.account_number and not result.rows:
            result.status = ParserStatus.PARTIAL
            result.warnings.append("No se encontró número de cuenta ni holdings")

        return result

    # ── Period dates ─────────────────────────────────────────────

    def _extract_period(self, pages: list[str], result: ParseResult) -> None:
        for text in pages[:5]:
            m = re.search(
                r"For the Period\s+(\d{1,2}/\d{1,2}/\d{2,4})\s+to\s+(\d{1,2}/\d{1,2}/\d{2,4})",
                text,
            )
            if m:
                result.period_start = _parse_date_mdy_short(m.group(1))
                result.period_end = _parse_date_mdy_short(m.group(2))
                result.statement_date = result.period_end
                return

    # ── Account Summary (page 1) ────────────────────────────────

    def _extract_account_summary(self, pages: list[str], result: ParseResult) -> None:
        text = pages[0] if pages else ""
        accounts: list[dict] = []

        # Regex: account number followed by beginning/ending values
        # Example: B99719001¹ 21,542,506.53 18,885,468.69 (2,657,037.84) 4
        for m in re.finditer(
            r"([A-Z0-9]{5,15})[¹²³\u00b9\u00b2\u00b3\u2071]*"
            r"\s+([\d,]+\.\d{2})\s+([\d,]+\.\d{2})\s+"
            r"(\([\d,]+\.\d{2}\)|[\d,]+\.\d{2})\s+(\d+)",
            text,
        ):
            acct = m.group(1)
            beginning = _parse_usd(m.group(2))
            ending = _parse_usd(m.group(3))
            change = _parse_usd(m.group(4))
            accounts.append({
                "account_number": acct,
                "beginning_value": str(beginning) if beginning else None,
                "ending_value": str(ending) if ending else None,
                "change": str(change) if change else None,
            })

        # Total Value line
        total_m = re.search(
            r"Total Value\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})",
            text,
        )
        if total_m:
            result.opening_balance = _parse_usd(total_m.group(1))
            result.closing_balance = _parse_usd(total_m.group(2))

        if accounts:
            if len(accounts) > 1:
                # Multi-cuenta (ej: Mandatos agrupa 2600, 3400, 9200)
                result.account_number = "Varios"
                result.account_numbers = [a["account_number"] for a in accounts]
            else:
                result.account_number = accounts[0]["account_number"]
            result.qualitative_data["accounts"] = accounts

    # ── Consolidated Summary ─────────────────────────────────────

    def _extract_consolidated_summary(self, pages: list[str], result: ParseResult) -> None:
        # Search in first 10 pages for "Consolidated Summary"
        for text in pages[:10]:
            if "Consolidated Summary" not in text:
                continue

            asset_alloc: dict[str, dict] = {}
            # Equity  4,357,711.89  5,272,882.80  915,170.91  16%
            for m in re.finditer(
                r"(Equity|Cash & Fixed Income)\s+"
                r"([\d,]+\.\d{2})\s+([\d,]+\.\d{2})\s+"
                r"(\([\d,]+\.\d{2}\)|[\d,]+\.\d{2})"
                r"(?:\s+([\d,]+\.\d{2}))?"
                r"\s+(\d+)%",
                text,
            ):
                asset_alloc[m.group(1)] = {
                    "beginning": str(_parse_usd(m.group(2))),
                    "ending": str(_parse_usd(m.group(3))),
                    "change": str(_parse_usd(m.group(4))),
                    "annual_income": str(_parse_usd(m.group(5))) if m.group(5) else None,
                    "allocation_pct": int(m.group(6)),
                }

            if asset_alloc:
                result.qualitative_data["asset_allocation"] = asset_alloc

            # Portfolio Activity
            activity = {}
            patterns = [
                ("beginning_market_value", r"Beginning Market Value\s+([\d,]+\.\d{2})\s+([\d,]+\.\d{2})"),
                ("net_contributions", r"Net Contributions/Withdrawals\s+(\([\d,]+\.\d{2}\)|[\d,]+\.\d{2})\s+(\([\d,]+\.\d{2}\)|[\d,]+\.\d{2})"),
                ("income_distributions", r"Income & Distributions\s+([\d,]+\.\d{2})\s+([\d,]+\.\d{2})"),
                ("change_investment", r"Change in Investment Value\s+(\([\d,]+\.\d{2}\)|[\d,]+\.\d{2})\s+([\d,]+\.\d{2})"),
                ("ending_market_value", r"Ending Market Value\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})"),
            ]
            for key, pat in patterns:
                m = re.search(pat, text)
                if m:
                    activity[key] = {
                        "current_period": str(_parse_usd(m.group(1))),
                        "ytd": str(_parse_usd(m.group(2))),
                    }

            if activity:
                result.qualitative_data["portfolio_activity"] = activity
            break  # only process first consolidated summary page

    # ── Per-account YTD ──────────────────────────────────────────

    def _extract_account_ytd(self, pages: list[str], result: ParseResult) -> None:
        for text in pages[:10]:
            if "INVESTMENT ACCOUNT(S) YEAR-TO-DATE" not in text:
                continue

            ytd_accounts: list[dict] = []
            for m in re.finditer(
                r"([A-Z0-9]{5,15})\s+"
                r"([\d,]+\.\d{2})\s+"
                r"(\([\d,]+\.\d{2}\)|[\d,]+\.\d{2})\s+"
                r"([\d,]+\.\d{2})\s+"
                r"(\([\d,]+\.\d{2}\)|[\d,]+\.\d{2})\s+"
                r"([\d,]+\.\d{2})",
                text,
            ):
                ytd_accounts.append({
                    "account_number": m.group(1),
                    "beginning_value": str(_parse_usd(m.group(2))),
                    "net_contributions": str(_parse_usd(m.group(3))),
                    "income": str(_parse_usd(m.group(4))),
                    "change_investment": str(_parse_usd(m.group(5))),
                    "ending_value": str(_parse_usd(m.group(6))),
                })

            if ytd_accounts:
                result.qualitative_data["account_ytd"] = ytd_accounts

            # Income Summary
            income: list[dict] = []
            for m in re.finditer(
                r"([A-Z0-9]{5,15})\s+"
                r"([\d,]+\.\d{2})\s+"
                r"([\d,]+\.\d{2})\s+"
                r"(\([\d,]+\.\d{2}\)|[\d,]+\.\d{2})\s+"
                r"(\([\d,]+\.\d{2}\)|[\d,]+\.\d{2})",
                text,
            ):
                # Only after "Income Summary" section
                income.append({
                    "account_number": m.group(1),
                    "income": str(_parse_usd(m.group(2))),
                    "other_income": str(_parse_usd(m.group(3))),
                    "realized_gl": str(_parse_usd(m.group(4))),
                    "unrealized_gl": str(_parse_usd(m.group(5))),
                })

            if income:
                result.qualitative_data["income_summary"] = income
            break

    # ── Holdings detail ──────────────────────────────────────────

    def _extract_holdings(self, pages: list[str], result: ParseResult) -> None:
        """Extract individual positions from holdings detail pages."""
        current_account: Optional[str] = None
        current_section: str = "unknown"

        for page_num, text in enumerate(pages):
            # Track current account
            acct_m = re.search(r"ACCT\.\s+([A-Z0-9]+)", text)
            if acct_m:
                current_account = acct_m.group(1)

            # Track section
            if "Cash & Fixed Income" in text:
                current_section = "cash_fixed_income"
            elif "Equity" in text and "Detail" in text:
                current_section = "equity"

            # Skip non-holdings pages
            if "Detail" not in text and "Holdings" not in text:
                continue

            # Parse holdings lines:
            # INSTRUMENT_NAME  PRICE  QUANTITY  VALUE  COST  GAIN/LOSS  INCOME  YIELD
            # Simplified: look for lines with dollar values that look like holdings
            for line in text.split("\n"):
                # Holdings pattern: name followed by numbers
                # JPM USD LIQUIDITY SWEEP C SHARE 1.00 445,951.86 445,951.86 445,951.86 16,972.92 3.81%
                h_m = re.match(
                    r"^(.{15,60}?)\s+"
                    r"([\d,.]+)\s+"        # price or quantity
                    r"([\d,.]+)\s+"        # quantity or value
                    r"([\d,]+\.\d{2})\s+"  # market value
                    r"([\d,]+\.\d{2}|N/A)",  # cost
                    line.strip(),
                )
                if h_m:
                    name = h_m.group(1).strip()
                    # Skip header lines and totals
                    if any(skip in name.lower() for skip in [
                        "price", "quantity", "total", "account", "period",
                        "beginning", "ending", "summary", "asset",
                    ]):
                        continue

                    market_value = _parse_usd(h_m.group(4))
                    cost = _parse_usd(h_m.group(5))
                    unrealized = None
                    if market_value and cost:
                        unrealized = market_value - cost

                    result.rows.append(ParsedRow(
                        data={
                            "instrument": name,
                            "market_value": str(market_value) if market_value else None,
                            "cost": str(cost) if cost else None,
                            "unrealized_gain_loss": str(unrealized) if unrealized else None,
                            "account_number": current_account,
                            "section": current_section,
                        },
                        row_number=page_num + 1,
                        confidence=0.8,
                    ))

            # Total lines: "Total Cash $16,258,536.38..."
            for m in re.finditer(
                r"Total\s+([\w\s&-]+?)\s+\$?([\d,]+\.\d{2})",
                text,
            ):
                section_name = m.group(1).strip()
                total_value = _parse_usd(m.group(2))
                if total_value and total_value > Decimal("0"):
                    result.rows.append(ParsedRow(
                        data={
                            "instrument": f"TOTAL: {section_name}",
                            "market_value": str(total_value),
                            "is_total": True,
                            "account_number": current_account,
                        },
                        row_number=page_num + 1,
                        confidence=0.9,
                    ))

    # ── validate ─────────────────────────────────────────────────

    def validate(self, result: ParseResult) -> list[str]:
        errors = []
        if result.opening_balance is not None and result.closing_balance is not None:
            if result.closing_balance <= Decimal("0"):
                errors.append(
                    f"Closing balance sospechoso: {result.closing_balance}"
                )
        return errors

    # ── detect ───────────────────────────────────────────────────

    def detect(self, filepath: Path) -> float:
        if filepath.suffix.lower() != ".pdf":
            return 0.0
        try:
            with pdfplumber.open(filepath) as pdf:
                if not pdf.pages:
                    return 0.0
                text = pdf.pages[0].extract_text() or ""
                score = 0.0
                for marker in self._DETECTION_MARKERS:
                    if marker.lower() in text.lower():
                        score += 0.25
                # Bonus for ETF-specific markers in filename
                if "etf" in filepath.name.lower():
                    score += 0.25
                return min(score, 1.0)
        except Exception:
            return 0.0
