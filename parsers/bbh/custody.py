"""
Parser: BBH – Brown Brothers Harriman Custody / Mandato (PDF).

Formato:  "December 1 toDecember 31, 2025", Account XXXXXX7085
Página 1: Overview (opening/closing values)
Página 2: Analysis – summary, income, gains/losses, asset allocation
Página 5: Fixed income maturity schedule
Páginas 6-15: Holdings detail (text-based, multi-line per holding)
Páginas 16+: Transactions

AISLADO: No comparte lógica con otros parsers.
"""

from __future__ import annotations

import re
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Optional

import pdfplumber

from parsers.base import BaseParser, ParseResult, ParsedRow, ParserStatus


# ── Helpers locales ──────────────────────────────────────────────

_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "may": 5, "jun": 6, "jul": 7, "aug": 8,
    "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    "january": 1, "february": 2, "march": 3, "april": 4,
    "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def _parse_usd(text: str) -> Optional[Decimal]:
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


def _parse_bbh_date(text: str) -> Optional[date]:
    """Parse 'Dec 1, 2025' or 'December 31, 2025'."""
    m = re.search(r"(\w+)\s+(\d{1,2}),?\s+(\d{4})", text)
    if not m:
        return None
    month_str = m.group(1).lower()
    month = _MONTHS.get(month_str)
    if not month:
        return None
    try:
        return date(int(m.group(3)), month, int(m.group(2)))
    except ValueError:
        return None


# ── Parser ───────────────────────────────────────────────────────

class BBHCustodyParser(BaseParser):
    BANK_CODE = "bbh"
    ACCOUNT_TYPE = "custody"
    VERSION = "2.1.0"
    DESCRIPTION = "Parser para cartolas Mandato BBH (Brown Brothers Harriman PDF)"
    SUPPORTED_EXTENSIONS = [".pdf"]

    _DETECTION_MARKERS = [
        "Brown Brothers Harriman",
        "BBH",
        "Your Investment Statement",
    ]

    def parse(self, filepath: Path) -> ParseResult:
        file_hash = self.compute_file_hash(filepath)

        pages: list[str] = []
        with pdfplumber.open(filepath) as pdf:
            for page in pdf.pages:
                pages.append(page.extract_text() or "")

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

        self._extract_overview(pages, result)
        self._extract_analysis(pages, result)
        self._extract_maturity_schedule(pages, result)
        self._extract_holdings(pages, result)
        self._emit_account_monthly_activity(result)

        if not result.account_number:
            result.status = ParserStatus.PARTIAL
            result.warnings.append("No se encontró número de cuenta")

        return result

    def _extract_overview(self, pages: list[str], result: ParseResult) -> None:
        """Page 1: Account name, number, opening/closing values."""
        text = pages[0] if pages else ""

        # Account number appears as "Account number: XXXXXX7085" or "Account number: 7085".
        # Keep only the real suffix (strip BBH masking prefix "XXXXXX").
        # Prefer masked token anywhere in page (most reliable in BBH PDFs):
        # e.g. "... $89,688,007.83 XXXXXX7085".
        m = re.search(r"\bX{4,}([A-Za-z0-9-]{3,})\b", text)
        if m:
            result.account_number = m.group(1).strip()
        else:
            # Fallback to explicit "Account number" line when present inline.
            m = re.search(
                r"Account\s*number\s*:?\s*(?:X{2,})?([A-Za-z0-9-]{3,})",
                text,
                flags=re.IGNORECASE,
            )
            if m:
                candidate = m.group(1).strip()
                if candidate.lower() != "market":
                    result.account_number = candidate

        # Period: "December 1 toDecember 31, 2025" (BBH has no space before "to")
        m = re.search(
            r"(\w+\s+\d{1,2})\s*to\s*(\w+\s+\d{1,2},?\s+\d{4})",
            text,
        )
        if m:
            # End date has year
            result.period_end = _parse_bbh_date(m.group(2))
            result.statement_date = result.period_end
            # Start date needs the year from end date
            if result.period_end:
                start_text = m.group(1) + f", {result.period_end.year}"
                result.period_start = _parse_bbh_date(start_text)

        # Opening value: "Market value on Dec 1, 2025 $89,688,007.83"
        m = re.search(r"Market value on .+?\$?([\d,]+\.\d{2})", text)
        if m:
            result.opening_balance = _parse_usd(m.group(1))

        # Closing value: next "Market value on..."
        vals = re.findall(r"Market value on .+?\$?([\d,]+\.\d{2})", text)
        if len(vals) >= 2:
            result.closing_balance = _parse_usd(vals[1])

    def _emit_account_monthly_activity(self, result: ParseResult) -> None:
        """Emite bloque estándar account_monthly_activity para DataLoadingService."""
        if not result.account_number:
            return

        summary = result.qualitative_data.get("investment_summary", {})
        if not summary and result.opening_balance is None and result.closing_balance is None:
            return

        def _summary_val(key: str, col: str = "this_period") -> Optional[Decimal]:
            raw = (summary.get(key) or {}).get(col)
            if raw is None:
                return None
            if isinstance(raw, Decimal):
                return raw
            return _parse_usd(str(raw))

        opening = result.opening_balance
        closing = result.closing_balance
        net_contributions = _summary_val("contributions_withdrawals")
        net_contributions_ytd = _summary_val("contributions_withdrawals", "ytd")
        prior_adjustments = _summary_val("prior_period_adjustments")
        prior_adjustments_ytd = _summary_val("prior_period_adjustments", "ytd")
        utilidad = None
        utilidad_ytd = None

        # Prioridad: identidad contable exacta del período.
        if (
            opening is not None
            and closing is not None
            and net_contributions is not None
        ):
            utilidad = closing - opening - net_contributions
        else:
            # Fallback a componentes reportados por BBH.
            components = [
                _summary_val("dividend_interest"),
                _summary_val("other_income"),
                _summary_val("accrued_income_change"),
                _summary_val("market_value_change"),
            ]
            components_ytd = [
                _summary_val("dividend_interest", "ytd"),
                _summary_val("other_income", "ytd"),
                _summary_val("accrued_income_change", "ytd"),
                _summary_val("market_value_change", "ytd"),
            ]
            if any(v is not None for v in components):
                utilidad = sum((v or Decimal("0")) for v in components)
                if any(v is not None for v in components_ytd):
                    utilidad_ytd = sum((v or Decimal("0")) for v in components_ytd)
            elif opening is not None and closing is not None:
                utilidad = closing - opening

        # Si falta movimiento pero sí tenemos utilidad y delta de ending, despejamos.
        if (
            net_contributions is None
            and opening is not None
            and closing is not None
            and utilidad is not None
        ):
            net_contributions = closing - opening - utilidad

        if utilidad is None and net_contributions is None:
            return

        result.qualitative_data["account_monthly_activity"] = [
            {
                "account_number": result.account_number,
                "beginning_value": str(opening) if opening is not None else None,
                "ending_value_with_accrual": str(closing) if closing is not None else None,
                "ending_value_without_accrual": str(closing) if closing is not None else None,
                "net_contributions": (
                    str(net_contributions) if net_contributions is not None else None
                ),
                "net_contributions_ytd": (
                    str(net_contributions_ytd) if net_contributions_ytd is not None else None
                ),
                "prior_period_adjustments": (
                    str(prior_adjustments) if prior_adjustments is not None else None
                ),
                "prior_period_adjustments_ytd": (
                    str(prior_adjustments_ytd) if prior_adjustments_ytd is not None else None
                ),
                "utilidad": str(utilidad) if utilidad is not None else None,
                "utilidad_ytd": str(utilidad_ytd) if utilidad_ytd is not None else None,
                "source": "bbh_investment_summary",
            }
        ]

    def _extract_analysis(self, pages: list[str], result: ParseResult) -> None:
        """Page 2: Analysis of investments - summary, income, gains, allocation."""
        for text in pages[:5]:
            if "Analysis of your investments" not in text:
                continue

            # Summary of investments
            summary = {}
            patterns = [
                ("opening_value", r"Opening value\s+([\d,]+\.\d{2})\s+([\d,]+\.\d{2})"),
                ("dividend_interest", r"Dividend and interest income\s+([\d,.-]+)\s+([\d,.-]+)"),
                ("other_income", r"Other income\s+([\d,.-]+)\s+([\d,.-]+)"),
                ("accrued_income_change", r"Change in value of accrued income\s+(-?[\d,]+\.\d{2})\s+(-?[\d,]+\.\d{2})"),
                ("contributions_withdrawals", r"Contributions less withdrawals\s+(-?[\d,]+\.\d{2})\s+(-?[\d,]+\.\d{2})"),
                ("prior_period_adjustments", r"Prior period adjustments\s+(-?[\d,]+\.\d{2})\s+(-?[\d,]+\.\d{2})"),
                ("fees", r"Fees\s+(-?[\d,]+\.\d{2})\s+(-?[\d,]+\.\d{2})"),
                ("market_value_change", r"Change in market value\s+(-?[\d,]+\.\d{2})\s+(-?[\d,]+\.\d{2})"),
                ("closing_value", r"Closing value\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})"),
            ]
            for key, pat in patterns:
                m = re.search(pat, text)
                if m:
                    summary[key] = {
                        "this_period": str(_parse_usd(m.group(1))),
                        "ytd": str(_parse_usd(m.group(2))),
                    }
            if summary:
                result.qualitative_data["investment_summary"] = summary

            # Income breakdown
            income = {}
            for key, pat in [
                ("taxable_interest", r"Taxable interest\s+([\d,.-]+)\s+([\d,.-]+)"),
                ("taxable_dividends", r"Taxable dividends\s+([\d,.-]+)\s+([\d,.-]+)"),
                ("total_income", r"Total income\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})"),
            ]:
                m = re.search(pat, text)
                if m:
                    income[key] = {
                        "this_period": str(_parse_usd(m.group(1))),
                        "ytd": str(_parse_usd(m.group(2))),
                    }
            if income:
                result.qualitative_data["income_breakdown"] = income

            # Gains and losses
            gains = {}
            for m in re.finditer(
                r"(Short-term|Long-term)\s+(-?[\d,]+\.\d{2})\s+(-?[\d,]+\.\d{2})\s+(-?[\d,]+\.\d{2})",
                text,
            ):
                gains[m.group(1).lower()] = {
                    "realized_this_period": str(_parse_usd(m.group(2))),
                    "realized_ytd": str(_parse_usd(m.group(3))),
                    "unrealized": str(_parse_usd(m.group(4))),
                }
            if gains:
                result.qualitative_data["gains_losses"] = gains

            # Asset allocation
            alloc: dict[str, dict] = {}
            for m in re.finditer(
                r"(Cash|Fixed income|Equity|Real assets)\s+(-?[\d,]+\.\d{2})\s+([\d.]+)%",
                text,
            ):
                alloc[m.group(1)] = {
                    "value": str(_parse_usd(m.group(2))),
                    "pct": m.group(3) + "%",
                }
            if alloc:
                result.qualitative_data["asset_allocation"] = alloc
            break

    def _extract_maturity_schedule(self, pages: list[str], result: ParseResult) -> None:
        """Page 5: Fixed income maturity schedule."""
        for text in pages[:8]:
            if "Fixed income maturity schedule" not in text:
                continue

            maturity: dict[str, dict] = {}
            for m in re.finditer(
                r"(\d{4}|Ten to twenty years|Over twenty years)\s+"
                r"([\d,]+\.\d{2})\s+"    # tax cost
                r"([\d,]+\.\d{2})\s+"     # face value
                r"([\d,]+\.\d{2})\s+"     # market value
                r"([\d.]+)%",             # % of FI
                text,
            ):
                maturity[m.group(1)] = {
                    "tax_cost": str(_parse_usd(m.group(2))),
                    "face_value": str(_parse_usd(m.group(3))),
                    "market_value": str(_parse_usd(m.group(4))),
                    "pct_fixed_income": m.group(5) + "%",
                }
            if maturity:
                result.qualitative_data["maturity_schedule"] = maturity
            break

    def _extract_holdings(self, pages: list[str], result: ParseResult) -> None:
        """Pages 6-15: Holdings detail.

        Each holding has:
        - Description (1-3 lines)
        - Data line: DUR/CY | units/shares | unit_price | unit_cost | market_value | total_cost | gain_loss | % | income | yield
        """
        current_section = "unknown"

        for page_num, text in enumerate(pages):
            if "Details of your investments" not in text and page_num > 2:
                # After page 2, only process detail pages
                if "Transactions" in text:
                    break  # Stop at transactions section
                if page_num > 20:
                    break

            # Track asset sections
            if "TOTAL CASH" in text or "CASH" in text.split("\n")[0:5]:
                current_section = "cash"
            if "FIXED INCOME" in text:
                current_section = "fixed_income"
            if "EQUITY" in text:
                current_section = "equity"

            # Sub-section tracking
            sub_section = ""
            if "Corporate bonds" in text:
                sub_section = "corporate_bonds"
            elif "Mortgage backed" in text:
                sub_section = "mbs"
            elif "Alternative assets" in text:
                sub_section = "alternative"
            elif "short duration" in text.lower():
                sub_section = "short_duration"

            # Parse holdings lines
            # Pattern: SHARES UNIT_PRICE UNIT_COST MARKET_VALUE TOTAL_COST GAIN_LOSS PCT INCOME YIELD
            for m in re.finditer(
                r"(?:DUR:\s*[\d.]+\s*YRS\s*(?:CY:\s*[\d.]+%)?\s+)?"
                r"([\d,]+\.\d{2})\s+"      # units/shares
                r"([\d,]+\.\d{2})\s+"       # unit price
                r"([\d,]+\.\d{2})\s+"       # unit cost
                r"(-?[\d,]+\.\d{2})\s+"     # market value
                r"(-?[\d,]+\.\d{2})\s+"     # total cost
                r"(-?[\d,]+\.\d{2})\s+"     # gains/losses
                r"([\d.]+)%\s+"             # % of total
                r"(-?[\d,]+\.\d{2})\s+"     # annual income
                r"([\d.]+)%",               # yield
                text,
            ):
                shares = _parse_usd(m.group(1))
                unit_price = _parse_usd(m.group(2))
                market_value = _parse_usd(m.group(4))
                total_cost = _parse_usd(m.group(5))
                gain_loss = _parse_usd(m.group(6))
                income = _parse_usd(m.group(8))
                pct = m.group(7)
                yield_pct = m.group(9)

                # Look backwards in text for the instrument name
                pos = m.start()
                preceding = text[:pos]
                lines = preceding.rstrip().split("\n")
                # Get instrument name from last 1-3 non-empty lines
                name_lines = []
                for line in reversed(lines):
                    line = line.strip()
                    if not line:
                        break
                    if re.match(r"^(DUR:|CY:|PERCENT|UNITS|DESCRIPTION|Page \d)", line):
                        continue
                    if "continued" in line.lower() and "FIXED INCOME" in line.upper():
                        continue
                    name_lines.insert(0, line)
                    if len(name_lines) >= 3:
                        break

                instrument = " ".join(name_lines).strip()
                # Clean up: remove header remnants
                if any(skip in instrument.upper() for skip in [
                    "TOTAL", "DESCRIPTION", "PERCENT", "DETAILS",
                    "PAGE", "ACCOUNT", "ANALYSIS",
                ]):
                    continue

                if not instrument or len(instrument) < 3:
                    instrument = f"Unknown ({page_num + 1})"

                result.rows.append(ParsedRow(
                    data={
                        "instrument": instrument,
                        "shares": str(shares) if shares else None,
                        "unit_price": str(unit_price) if unit_price else None,
                        "market_value": str(market_value) if market_value else None,
                        "total_cost": str(total_cost) if total_cost else None,
                        "unrealized_gain_loss": str(gain_loss) if gain_loss else None,
                        "pct_of_portfolio": pct + "%",
                        "estimated_annual_income": str(income) if income else None,
                        "yield_pct": yield_pct + "%",
                        "section": current_section,
                        "sub_section": sub_section,
                    },
                    row_number=page_num + 1,
                    confidence=0.85,
                ))

            # Section totals: "Total xxx $market_value $cost $gain_loss pct% $income yield%"
            for m in re.finditer(
                r"(?:Total|TOTAL)\s+([\w\s.&-]+?)\s+"
                r"\$?(-?[\d,]+\.\d{2})\s+"     # market value
                r"\$?(-?[\d,]+\.\d{2})\s+"      # cost
                r"\$?(-?[\d,]+\.\d{2})\s+"      # gain/loss
                r"([\d.]+)%\s+"                 # pct
                r"\$?(-?[\d,]+\.\d{2})\s+"      # income
                r"([\d.]+)%",                   # yield
                text,
            ):
                section_name = m.group(1).strip()
                value = _parse_usd(m.group(2))
                if value and abs(value) > Decimal("0"):
                    result.rows.append(ParsedRow(
                        data={
                            "instrument": f"TOTAL: {section_name}",
                            "market_value": str(value),
                            "total_cost": str(_parse_usd(m.group(3))),
                            "unrealized_gain_loss": str(_parse_usd(m.group(4))),
                            "pct_of_portfolio": m.group(5) + "%",
                            "is_total": True,
                        },
                        row_number=page_num + 1,
                        confidence=0.9,
                    ))

    def validate(self, result: ParseResult) -> list[str]:
        errors = []
        if result.opening_balance and result.closing_balance:
            if result.closing_balance <= Decimal("0"):
                errors.append(f"Closing balance sospechoso: {result.closing_balance}")
        return errors

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
                        score += 0.3
                fname = filepath.name.lower()
                if "bbh" in fname:
                    score += 0.2
                if "boatview" in fname:
                    score += 0.1
                return min(score, 1.0)
        except Exception:
            return 0.0
