"""
Parser: JPMorgan – Cuenta Custodia / Mandato (Investment Management PDF).

Formato:  "01 December - 31 December 2025", "Account Number: XXX"
Páginas 1-4+: Texto legal (se salta)
Página data: Account Summary con Asset Allocation y Portfolio Activity
Páginas siguientes: Holdings por tipo (Cash, Fixed Income, Equities)

AISLADO: No comparte lógica con otros parsers.
"""

from __future__ import annotations

import re
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Optional

import pdfplumber

from parsers.base import BaseParser, ParseResult, ParsedRow, ParserStatus


# ── Helpers locales ──────────────────────────────────────────────

_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
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


def _parse_date_text(text: str) -> Optional[date]:
    """Parse '01 December 2025' or '31 December 2025'."""
    m = re.search(r"(\d{1,2})\s+(\w+)\s+(\d{4})", text)
    if not m:
        return None
    day, month_str, year = int(m.group(1)), m.group(2).lower(), int(m.group(3))
    month = _MONTHS.get(month_str)
    if not month:
        return None
    try:
        return date(year, month, day)
    except ValueError:
        return None


# ── Parser ───────────────────────────────────────────────────────

class JPMorganCustodyParser(BaseParser):
    BANK_CODE = "jpmorgan"
    ACCOUNT_TYPE = "custody"
    VERSION = "2.1.0"
    DESCRIPTION = "Parser para cartolas Mandato JPMorgan (Investment Management PDF)"
    SUPPORTED_EXTENSIONS = [".pdf"]

    _DETECTION_MARKERS = [
        "JPMorgan Chase Bank",
        "Statement of Account",
        "Investment Management",
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

        # 1) Find data pages (skip legal text)
        self._extract_period(pages, result)
        self._extract_account_number(pages, result)
        self._extract_account_summary(pages, result)
        self._extract_subaccount_summaries(pages, result)
        self._finalize_subaccount_identity(result)
        self._extract_asset_allocation(pages, result)
        self._extract_holdings(pages, result)

        if not result.account_number:
            result.status = ParserStatus.PARTIAL
            result.warnings.append("No se encontró número de cuenta")

        return result

    def _extract_period(self, pages: list[str], result: ParseResult) -> None:
        for text in pages[:15]:
            m = re.search(
                r"(\d{1,2})\s+(\w+)\s*-\s*(\d{1,2})\s+(\w+)\s+(\d{4})",
                text,
            )
            if m:
                start_day = int(m.group(1))
                start_month = _MONTHS.get(m.group(2).lower())
                end_day = int(m.group(3))
                end_month = _MONTHS.get(m.group(4).lower())
                year = int(m.group(5))
                if start_month and end_month:
                    try:
                        result.period_start = date(year, start_month, start_day)
                        result.period_end = date(year, end_month, end_day)
                        result.statement_date = result.period_end
                    except ValueError:
                        pass
                    return

    def _extract_account_number(self, pages: list[str], result: ParseResult) -> None:
        for text in pages[:15]:
            # "Account Number: MND-1483400" or "Account Number: 1179200"
            m = re.search(r"Account Number:\s*([A-Z0-9-]+)", text)
            if m:
                result.account_number = m.group(1)
                return

    def _extract_account_summary(self, pages: list[str], result: ParseResult) -> None:
        """Extract from page with 'Account Summary' header."""
        for text in pages[:20]:
            if "Account Summary" not in text:
                continue
            if "Asset Allocation" not in text:
                continue

            # Asset allocation lines: "Cash, Deposits & Short Term  9,404,989.43  5,633,705.15  -3,771,284.28"
            alloc: dict[str, dict] = {}
            for m in re.finditer(
                r"(Cash,?\s*Deposits\s*&?\s*Short\s*Term|Fixed Income|Equities)"
                r"\s+([\d,]+\.\d{2})\s+([\d,]+\.\d{2})\s+(-?[\d,]+\.\d{2})",
                text,
            ):
                alloc[m.group(1).strip()] = {
                    "beginning": str(_parse_usd(m.group(2))),
                    "ending": str(_parse_usd(m.group(3))),
                    "change": str(_parse_usd(m.group(4))),
                }

            if alloc:
                result.qualitative_data["asset_allocation"] = alloc

            # Total Market Value
            total_m = re.search(
                r"Total (?:Net )?Market Value\*?\s+([\d,]+\.\d{2})\s+([\d,]+\.\d{2})",
                text,
            )
            if total_m:
                result.opening_balance = _parse_usd(total_m.group(1))
                result.closing_balance = _parse_usd(total_m.group(2))

            # Portfolio Activity
            activity = {}
            patterns = [
                ("beginning_market_value", r"Beginning Market Value\s+([\d,]+\.\d{2})\s+([\d,]+\.\d{2})"),
                ("net_cash_contributions", r"Net Cash Contributions\s*/?\s*Withdrawals\s+(-?[\d,]+\.\d{2})\s+(-?[\d,]+\.\d{2})"),
                ("income_distributions", r"Income and Distributions\s+([\d,]+\.\d{2})\s+([\d,]+\.\d{2})"),
                ("change_investment", r"Change in Investment Value\s+(-?[\d,]+\.\d{2})\s+(-?[\d,]+\.\d{2})"),
                ("ending_market_value", r"Ending Market Value\s+([\d,]+\.\d{2})\s+([\d,]+\.\d{2})"),
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

            # Diversification percentages
            div_pcts: dict[str, str] = {}
            for m in re.finditer(r"([\d.]+)%\s+([\w\s,&]+?)(?:\n|$)", text):
                pct = m.group(1)
                name = m.group(2).strip()
                if name and len(name) > 2:
                    div_pcts[name] = pct + "%"
            if div_pcts:
                result.qualitative_data["diversification"] = div_pcts

            break

    def _extract_subaccount_summaries(
        self, pages: list[str], result: ParseResult
    ) -> None:
        """
        Extrae subcuentas internas (ej: 1179200/1412600/1483400) desde
        sus páginas 'Account Summary' propias.
        """
        accounts: list[dict] = []
        monthly: list[dict] = []
        seen: set[str] = set()
        current_account: Optional[str] = None

        for text in pages:
            acct_m = re.search(r"Account Number:\s*([A-Z0-9-]+)", text)
            if acct_m:
                current_account = acct_m.group(1)

            if not current_account or not re.fullmatch(r"\d{7}", current_account):
                continue
            if current_account in seen:
                continue
            if "Account Summary" not in text or "Portfolio Activity" not in text:
                continue

            beginning = self._extract_current_period_value(
                text, r"Beginning Market Value\s+([\d,]+\.\d{2})"
            )
            ending = self._extract_current_period_value(
                text, r"Ending Market Value\s+([\d,]+\.\d{2})"
            )
            net_contributions = self._extract_current_period_value(
                text, r"Net Cash Contributions\s*/\s*Withdrawals\s+(-?[\d,]+\.\d{2})"
            )
            income = self._extract_current_period_value(
                text, r"Income and Distributions\s+([\d,]+\.\d{2})"
            )
            change = self._extract_current_period_value(
                text, r"Change in Investment Value\s+(-?[\d,]+\.\d{2})"
            )

            if beginning is None and ending is None:
                continue

            seen.add(current_account)
            accounts.append({
                "account_number": current_account,
                "beginning_value": str(beginning) if beginning is not None else None,
                "ending_value": str(ending) if ending is not None else None,
                "change": str(change) if change is not None else None,
            })

            utilidad = Decimal("0")
            if income is not None:
                utilidad += income
            if change is not None:
                utilidad += change

            monthly.append({
                "account_number": current_account,
                "net_contributions": str(net_contributions) if net_contributions is not None else None,
                "income_distributions": str(income) if income is not None else None,
                "change_investment": str(change) if change is not None else None,
                "ending_value_with_accrual": str(ending) if ending is not None else None,
                "ending_value_without_accrual": str(ending) if ending is not None else None,
                "utilidad": str(utilidad),
            })

        if accounts:
            result.qualitative_data["accounts"] = accounts
        if monthly:
            result.qualitative_data["account_monthly_activity"] = monthly

    @staticmethod
    def _extract_current_period_value(text: str, pattern: str) -> Optional[Decimal]:
        m = re.search(pattern, text)
        if not m:
            return None
        return _parse_usd(m.group(1))

    def _finalize_subaccount_identity(self, result: ParseResult) -> None:
        extracted = [
            x.get("account_number")
            for x in result.qualitative_data.get("account_monthly_activity", [])
            if x.get("account_number")
        ]
        if extracted:
            result.account_numbers = list(dict.fromkeys(extracted))
            if len(result.account_numbers) > 1:
                result.account_number = "Varios"
            else:
                result.account_number = result.account_numbers[0]

    def _extract_asset_allocation(self, pages: list[str], result: ParseResult) -> None:
        """Extract 'Portfolio Diversification' page."""
        for text in pages[:20]:
            if "Portfolio Diversification" not in text:
                continue

            # Currency breakdown: "U.S. Dollar USD 1.67% 55.67% ..."
            currencies: dict[str, str] = {}
            for m in re.finditer(
                r"(\w[\w\s.]+?)\s+(USD|EUR|JPY|GBP|CAD|CHF|ASI|AUD)\s+([\d.]+)%",
                text,
            ):
                currencies[m.group(2)] = m.group(3) + "%"
            if currencies:
                result.qualitative_data["currency_allocation"] = currencies

    def _extract_holdings(self, pages: list[str], result: ParseResult) -> None:
        """Extract holdings from detail pages."""
        current_section = "unknown"

        for page_num, text in enumerate(pages):
            if "Cash, Deposits & Short Term" in text and ("Detail" in text or "Cash Holdings" in text):
                current_section = "cash"
            elif "Fixed Income" in text and ("Detail" in text or "Summary" in text):
                current_section = "fixed_income"
            elif "Equities" in text and ("Detail" in text or "Summary" in text):
                current_section = "equities"

            # Holdings: "Security Name  QTY  PRICE  DATE  COST  MARKET_VALUE  GAIN/LOSS"
            # Pattern for IM format bonds:
            # BOEING CO 63,000.00 95.70 99.88 60,291.58 62,926.01 2,634.43
            for m in re.finditer(
                r"^(.{10,50}?)\s+"
                r"([\d,]+\.\d{2})\s+"    # quantity/face value
                r"([\d.]+)\s+"            # cost price
                r"([\d.]+)\s+"            # market price
                r"([\d,]+\.\d{2})\s+"     # cost value
                r"([\d,]+\.\d{2})\s+"     # market value
                r"(-?[\d,]+\.\d{2})",     # gain/loss
                text,
                re.MULTILINE,
            ):
                name = m.group(1).strip()
                if any(skip in name.lower() for skip in [
                    "total", "summary", "account", "page", "currency",
                ]):
                    continue

                market_value = _parse_usd(m.group(6))
                cost_value = _parse_usd(m.group(5))
                gain_loss = _parse_usd(m.group(7))

                result.rows.append(ParsedRow(
                    data={
                        "instrument": name,
                        "quantity": str(_parse_usd(m.group(2))),
                        "cost_price": m.group(3),
                        "market_price": m.group(4),
                        "cost_value": str(cost_value) if cost_value else None,
                        "market_value": str(market_value) if market_value else None,
                        "unrealized_gain_loss": str(gain_loss) if gain_loss else None,
                        "section": current_section,
                    },
                    row_number=page_num + 1,
                    confidence=0.8,
                ))

            # Total lines
            for m in re.finditer(
                r"Total\s+([\w\s&,]+?)\s+([\d,]+\.\d{2})\s+",
                text,
            ):
                section = m.group(1).strip()
                value = _parse_usd(m.group(2))
                if value and value > Decimal("0"):
                    result.rows.append(ParsedRow(
                        data={
                            "instrument": f"TOTAL: {section}",
                            "market_value": str(value),
                            "is_total": True,
                        },
                        row_number=page_num + 1,
                        confidence=0.9,
                    ))

    def validate(self, result: ParseResult) -> list[str]:
        errors = []
        if result.opening_balance is not None and result.closing_balance is not None:
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
                # Check multiple pages (legal text on first pages)
                for page in pdf.pages[:10]:
                    text = page.extract_text() or ""
                    if "Statement of Account" in text and "JPMorgan" in text:
                        return 0.9
                    if "Account Number:" in text and "JPMorgan Chase Bank" in text:
                        return 0.9
                # Fallback: check first page markers
                text = pdf.pages[0].extract_text() or ""
                score = 0.0
                for marker in self._DETECTION_MARKERS:
                    if marker.lower() in text.lower():
                        score += 0.2
                if "mandato" in filepath.name.lower() or "custody" in filepath.name.lower():
                    score += 0.2
                return min(score, 1.0)
        except Exception:
            return 0.0
