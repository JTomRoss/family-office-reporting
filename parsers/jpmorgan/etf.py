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
    VERSION = "2.2.0"
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

        # 5) Per-account CURRENT PERIOD activity (from individual account pages)
        self._extract_per_account_monthly_activity(pages, result)
        self._finalize_account_mapping(result)

        # 6) Holdings from detail pages
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
            if beginning == Decimal("0") and ending == Decimal("0"):
                # Excluye sub-secciones brokerage de valor cero.
                continue
            accounts.append({
                "account_number": acct,
                "beginning_value": str(beginning) if beginning is not None else None,
                "ending_value": str(ending) if ending is not None else None,
                "change": str(change) if change is not None else None,
            })

        # Total Value can be on page 1 (classic layout) or later pages (Armel/Ecoterra layout).
        # For split-layout statements, there are multiple section totals (e.g. 28% + 72%).
        # In that case, the statement total is the sum of section endings.
        opening_candidate = None
        closing_candidate = None
        closing_100pct_candidate = None
        section_totals: list[tuple[Decimal, Decimal, int]] = []
        for pg in pages[:10]:
            for m in re.finditer(
                r"Total Value\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})"
                r"(?:\s+\(?\$?[\d,]+\.\d{2}\)?)?\s+(\d{1,3})%",
                pg,
            ):
                op = _parse_usd(m.group(1))
                cl = _parse_usd(m.group(2))
                pct = int(m.group(3))
                if op is not None and cl is not None:
                    section_totals.append((op, cl, pct))
            for m in re.finditer(
                r"Total Value\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})",
                pg,
            ):
                opening_candidate = _parse_usd(m.group(1))
                closing_candidate = _parse_usd(m.group(2))
            for m in re.finditer(
                r"Total Value\s+\$?([\d,]+\.\d{2})\s+100%",
                pg,
            ):
                closing_100pct_candidate = _parse_usd(m.group(1))

        if section_totals and sum(p for _, _, p in section_totals) == 100:
            result.opening_balance = sum((op for op, _, _ in section_totals), Decimal("0"))
            result.closing_balance = sum((cl for _, cl, _ in section_totals), Decimal("0"))
        elif opening_candidate is not None:
            result.opening_balance = opening_candidate
        if not section_totals and closing_candidate is not None:
            result.closing_balance = closing_candidate
        if not section_totals and closing_100pct_candidate is not None:
            # Prefer explicit consolidated 100% total when present.
            result.closing_balance = closing_100pct_candidate

        if accounts:
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
                # Normalize balances to "Ending/Beginning Market Value" (without accruals)
                # when this section is available (classic ETF layout).
                try:
                    emv = activity.get("ending_market_value", {}).get("current_period")
                    bmv = activity.get("beginning_market_value", {}).get("current_period")
                    emv_dec = _parse_usd(emv) if emv is not None else None
                    bmv_dec = _parse_usd(bmv) if bmv is not None else None
                    if emv_dec is not None:
                        result.closing_balance = emv_dec
                    if bmv_dec is not None:
                        result.opening_balance = bmv_dec
                except Exception:
                    pass
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

    # ── Per-account monthly (current period) activity ──────────

    def _extract_per_account_monthly_activity(
        self, pages: list[str], result: ParseResult
    ) -> None:
        """
        Extract CURRENT PERIOD Portfolio Activity from individual account pages.

        Each account section has its own Account Summary with:
        - Asset allocation (beginning/ending accruals)
        - Portfolio Activity with Current Period and YTD columns
          (we take the FIRST number = current period)

        Stores list of dicts in qualitative_data["account_monthly_activity"]:
          account_number, net_contributions, income_distributions,
          change_investment, accrual_beginning, accrual_ending, utilidad
        """
        activities: list[dict] = []
        current_account: Optional[str] = None
        # Track which pages belong to an account's Account Summary
        i = 0
        while i < len(pages):
            text = pages[i]

            # Detect account number
            acct_m = re.search(r"ACCT\.\s+([A-Z0-9]+)", text)
            if acct_m:
                current_account = acct_m.group(1)

            # Only process Account Summary pages (Portfolio Activity section)
            if current_account and "Portfolio Activity" in text and "Period" in text:
                # Check this is an account-specific page, not the consolidated one
                if "Consolidated Summary" in text:
                    i += 1
                    continue

                acct_data = self._parse_account_activity_page(
                    text, current_account
                )
                if acct_data:
                    # Avoid duplicates (same account may span multiple pages)
                    existing = [
                        a for a in activities
                        if a["account_number"] == current_account
                    ]
                    if not existing:
                        activities.append(acct_data)

            i += 1

        if activities:
            result.qualitative_data["account_monthly_activity"] = activities

    def _parse_account_activity_page(
        self, text: str, account_number: str
    ) -> Optional[dict]:
        """Parse a single account's Portfolio Activity page for current-period values."""
        data: dict = {"account_number": account_number}

        # ── Accruals from Asset Allocation section ───────────────
        # Pattern: "Accruals  <beginning>  <ending>  <change>"
        # Must NOT be in the Portfolio Activity section (those only show ending)
        accrual_beginning: Optional[Decimal] = None
        accrual_ending: Optional[Decimal] = None

        # Find accruals in the Asset Allocation table (before Portfolio Activity)
        activity_pos = text.find("Portfolio Activity")
        search_text = text[:activity_pos] if activity_pos > 0 else text

        accrual_m = re.search(
            r"Accruals\s+([\d,]+\.\d{2})\s+([\d,]+\.\d{2})\s+"
            r"(\([\d,]+\.\d{2}\)|[\d,]+\.\d{2})",
            search_text,
        )
        if accrual_m:
            accrual_beginning = _parse_usd(accrual_m.group(1))
            accrual_ending = _parse_usd(accrual_m.group(2))

        data["accrual_beginning"] = str(accrual_beginning) if accrual_beginning is not None else None
        data["accrual_ending"] = str(accrual_ending) if accrual_ending is not None else None

        # Ending value with/without accrual (requerido por subcuenta en reporting).
        with_accrual_m = re.search(
            r"Market Value with Accruals\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})",
            search_text,
        )
        if with_accrual_m:
            val = _parse_usd(with_accrual_m.group(2))
            data["ending_value_with_accrual"] = str(val) if val is not None else None

        without_accrual_m = re.search(
            r"Ending Market Value\s+\$?([\d,]+\.\d{2})",
            text[activity_pos:] if activity_pos > 0 else text,
        )
        if without_accrual_m:
            val = _parse_usd(without_accrual_m.group(1))
            data["ending_value_without_accrual"] = str(val) if val is not None else None

        # ── Portfolio Activity current period values ─────────────
        # Each line has: <label>  <current_period>  <ytd>
        # We want the FIRST number (current period)

        # Net Contributions/Withdrawals — may be $xxx or ($xxx) or plain number
        # Handle dollar sign inside/outside parens: ($2,728,400.00), $2,228,400.00
        net_contrib_m = re.search(
            r"Net Contributions/Withdrawals\s+"
            r"(\$?\(?\$?[\d,]+\.\d{2}\)?)",
            text,
        )
        net_contributions: Optional[Decimal] = None
        if net_contrib_m:
            net_contributions = _parse_usd(net_contrib_m.group(1))

        # Income & Distributions — current period (first number after label)
        income_m = re.search(
            r"Income & Distributions\s+([\d,]+\.\d{2})",
            text[activity_pos:] if activity_pos > 0 else text,
        )
        income_distributions: Optional[Decimal] = None
        if income_m:
            income_distributions = _parse_usd(income_m.group(1))

        # Change In Investment Value — current period (first number)
        change_m = re.search(
            r"Change [Ii]n Investment Value\s+"
            r"(\([\d,]+\.\d{2}\)|[\d,]+\.\d{2})",
            text[activity_pos:] if activity_pos > 0 else text,
        )
        change_investment: Optional[Decimal] = None
        if change_m:
            change_investment = _parse_usd(change_m.group(1))

        # Sanity check: need at least net_contributions to be useful
        if net_contributions is None and income_distributions is None:
            return None

        data["net_contributions"] = str(net_contributions) if net_contributions is not None else None
        data["income_distributions"] = str(income_distributions) if income_distributions is not None else None
        data["change_investment"] = str(change_investment) if change_investment is not None else None

        # ── Compute utilidad ─────────────────────────────────────
        # utilidad = Income & Distributions + Change In Investment Value
        #          + accrual_ending - accrual_beginning
        utilidad = Decimal("0")
        if income_distributions is not None:
            utilidad += income_distributions
        if change_investment is not None:
            utilidad += change_investment
        if accrual_ending is not None:
            utilidad += accrual_ending
        if accrual_beginning is not None:
            utilidad -= accrual_beginning

        data["utilidad"] = str(utilidad)

        return data

    # ── Holdings detail ──────────────────────────────────────────

    def _finalize_account_mapping(self, result: ParseResult) -> None:
        """Alinea identidad de cuentas con subcuentas efectivamente extraídas."""
        extracted = [
            x.get("account_number")
            for x in result.qualitative_data.get("account_monthly_activity", [])
            if x.get("account_number")
        ]
        if extracted:
            account_set = set(extracted)
            accounts = result.qualitative_data.get("accounts", [])
            filtered_accounts = [
                a for a in accounts
                if a.get("account_number") in account_set
            ]
            if filtered_accounts:
                result.qualitative_data["accounts"] = filtered_accounts
            result.account_numbers = list(dict.fromkeys(extracted))
            if len(result.account_numbers) > 1:
                result.account_number = "Varios"
            elif result.account_numbers:
                result.account_number = result.account_numbers[0]
            return

        fallback_accounts = result.qualitative_data.get("accounts", [])
        if len(fallback_accounts) > 1:
            result.account_number = "Varios"
            result.account_numbers = [a["account_number"] for a in fallback_accounts]
        elif len(fallback_accounts) == 1:
            result.account_number = fallback_accounts[0]["account_number"]

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
            elif "Global Fixed Income" in text:
                current_section = "cash_fixed_income"
            elif "Equity" in text and "Detail" in text:
                current_section = "equity"
            elif "Global Equity" in text:
                current_section = "equity"

            # Skip non-holdings pages
            has_holdings_header = (
                ("Detail" in text)
                or ("Holdings" in text)
                or ("Global Fixed Income" in text)
                or ("Global Equity" in text)
                or ("Price" in text and "Quantity" in text and "Value" in text)
            )
            if not has_holdings_header:
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
                    # Skip header lines and table labels (avoid broad substring filters).
                    name_l = name.lower()
                    if (
                        name_l.startswith("price")
                        or name_l.startswith("quantity")
                        or name_l.startswith("total ")
                        or name_l.startswith("account ")
                        or name_l.startswith("period ")
                        or name_l.startswith("beginning market value")
                        or name_l.startswith("ending market value")
                        or name_l.startswith("summary by ")
                        or name_l.startswith("asset categories")
                    ):
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
