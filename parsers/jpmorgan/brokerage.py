"""
Parser: JPMorgan – Cuenta Brokerage (Consolidated Statement PDF).

Formato IDÉNTICO al ETF: "For the Period M/D/YY to M/D/YY"
Usa el mismo Consolidated Statement package pero taggeado como brokerage.
Detección: "Consolidated Statement" + "brokerage" en nombre de archivo.

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


# ── Helpers locales (duplicados a propósito – aislamiento) ───────

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


def _parse_date_mdy_short(text: str) -> Optional[date]:
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


_ACTIVITY_VALUE_RE = re.compile(r"-?\(?\$?[\d,]+\.\d{2}\)?")


# ── Parser ───────────────────────────────────────────────────────

class JPMorganBrokerageParser(BaseParser):
    BANK_CODE = "jpmorgan"
    ACCOUNT_TYPE = "brokerage"
    VERSION = "2.1.2"
    DESCRIPTION = "Parser para cartolas Brokerage JPMorgan (Consolidated Statement PDF)"
    SUPPORTED_EXTENSIONS = [".pdf"]

    _DETECTION_MARKERS = [
        "J.P. Morgan",
        "JPMorgan",
        "Consolidated Statement",
    ]
    _ACTIVITY_ROW_PATTERNS = [
        r"Beginning\s+Market\s+Value",
        r"Net\s+Contributions\s*/\s*Withdrawals",
        r"Income\s*(?:&|and)\s*Distributions",
        r"Change\s+[Ii]n\s+Investment\s+Value",
        r"Ending\s+Market\s+Value",
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

        self._extract_period(pages, result)
        self._extract_account_summary(pages, result)
        self._extract_consolidated_summary(pages, result)
        self._extract_account_ytd(pages, result)
        self._extract_per_account_monthly_activity(pages, result)
        self._finalize_account_mapping(result)
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

        for m in re.finditer(
            r"([A-Z0-9]{5,15})[¹²³\u00b9\u00b2\u00b3\u2071]*"
            r"\s+([\d,]+\.\d{2})\s+([\d,]+\.\d{2})\s+"
            r"(\([\d,]+\.\d{2}\)|[\d,]+\.\d{2})\s+(\d+)",
            text,
        ):
            beginning = _parse_usd(m.group(2))
            ending = _parse_usd(m.group(3))
            change = _parse_usd(m.group(4))
            if beginning == Decimal("0") and ending == Decimal("0"):
                continue
            accounts.append({
                "account_number": m.group(1),
                "beginning_value": str(beginning) if beginning is not None else None,
                "ending_value": str(ending) if ending is not None else None,
                "change": str(change) if change is not None else None,
            })

        total_m = re.search(
            r"Total Value\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})",
            text,
        )
        if total_m:
            result.opening_balance = _parse_usd(total_m.group(1))
            result.closing_balance = _parse_usd(total_m.group(2))

        if accounts:
            result.qualitative_data["accounts"] = accounts

    # ── Consolidated Summary ─────────────────────────────────────

    def _extract_consolidated_summary(self, pages: list[str], result: ParseResult) -> None:
        for text in pages[:10]:
            if "Consolidated Summary" not in text:
                continue

            asset_alloc: dict[str, dict] = {}
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
            break

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

            income: list[dict] = []
            for m in re.finditer(
                r"([A-Z0-9]{5,15})\s+"
                r"([\d,]+\.\d{2})\s+"
                r"([\d,]+\.\d{2})\s+"
                r"(\([\d,]+\.\d{2}\)|[\d,]+\.\d{2})\s+"
                r"(\([\d,]+\.\d{2}\)|[\d,]+\.\d{2})",
                text,
            ):
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

    def _extract_per_account_monthly_activity(
        self, pages: list[str], result: ParseResult
    ) -> None:
        """Extrae actividad mensual actual por subcuenta desde páginas ACCT."""
        activities: list[dict] = []
        cash_fixed_alloc_by_account = self._extract_cash_fixed_summary_by_account(pages)
        current_account: Optional[str] = None

        for text in pages:
            acct_m = re.search(r"ACCT\.\s+([A-Z0-9]+)", text)
            if acct_m:
                current_account = acct_m.group(1)

            if not current_account:
                continue
            if "Portfolio Activity" not in text or "Period" not in text:
                continue
            if "Consolidated Summary" in text:
                continue

            acct_data = self._parse_account_activity_page(text, current_account)
            if acct_data and current_account in cash_fixed_alloc_by_account and "asset_allocation" not in acct_data:
                acct_data["asset_allocation"] = cash_fixed_alloc_by_account[current_account]
            if acct_data and not any(
                a["account_number"] == current_account for a in activities
            ):
                activities.append(acct_data)

        if activities:
            result.qualitative_data["account_monthly_activity"] = activities

    def _extract_cash_fixed_summary_by_account(self, pages: list[str]) -> dict[str, dict]:
        alloc_by_account: dict[str, dict] = {}
        current_account: Optional[str] = None
        for text in pages:
            acct_m = re.search(r"ACCT\.\s+([A-Z0-9]+)", text)
            if acct_m:
                current_account = acct_m.group(1)
            if not current_account:
                continue
            if "Cash & Fixed Income Summary" not in text:
                continue
            alloc = self._extract_cash_fixed_income_summary(text)
            if alloc:
                alloc_by_account[current_account] = alloc
        return alloc_by_account

    def _parse_account_activity_page(
        self, text: str, account_number: str
    ) -> Optional[dict]:
        """Parsea Portfolio Activity (Current Period) para una subcuenta."""
        data: dict = {"account_number": account_number}
        activity_pos = text.find("Portfolio Activity")
        search_text = text[:activity_pos] if activity_pos > 0 else text

        accrual_beginning: Optional[Decimal] = None
        accrual_ending: Optional[Decimal] = None
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

        with_accrual_m = re.search(
            r"Market Value with Accruals\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})",
            search_text,
        )
        if with_accrual_m:
            val = _parse_usd(with_accrual_m.group(2))
            data["ending_value_with_accrual"] = str(val) if val is not None else None
        cash_fixed_alloc = self._extract_cash_fixed_income_summary(search_text)
        if cash_fixed_alloc:
            data["asset_allocation"] = cash_fixed_alloc

        activity_text = text[activity_pos:] if activity_pos > 0 else text

        without_accrual_m = re.search(
            r"Ending Market Value\s+\$?([\d,]+\.\d{2})",
            activity_text,
        )
        if without_accrual_m:
            val = _parse_usd(without_accrual_m.group(1))
            data["ending_value_without_accrual"] = str(val) if val is not None else None

        interpretation_notes: list[str] = []
        net_contributions, net_contributions_ytd, net_blank_current = self._extract_activity_values(
            activity_text,
            r"Net\s+Contributions\s*/\s*Withdrawals",
            single_value_means_ytd=False,
        )
        if net_blank_current:
            interpretation_notes.append(
                "Net Contributions/Withdrawals mensual en blanco interpretado como 0; YTD se conserva solo como control."
            )

        income_distributions, income_distributions_ytd, income_blank_current = self._extract_activity_values(
            activity_text,
            r"Income\s*(?:&|and)\s*Distributions",
            single_value_means_ytd=True,
        )
        if income_blank_current:
            interpretation_notes.append(
                "Income & Distributions mensual en blanco interpretado como 0; YTD se conserva solo como control."
            )

        change_investment, change_investment_ytd, change_blank_current = self._extract_activity_values(
            activity_text,
            r"Change\s+[Ii]n\s+Investment\s+Value",
            single_value_means_ytd=True,
        )
        if change_blank_current:
            interpretation_notes.append(
                "Change in Investment Value mensual en blanco interpretado como 0; YTD se conserva solo como control."
            )

        if net_contributions is None and income_distributions is None:
            return None

        data["net_contributions"] = str(net_contributions) if net_contributions is not None else None
        data["net_contributions_ytd"] = (
            str(net_contributions_ytd) if net_contributions_ytd is not None else None
        )
        data["income_distributions"] = str(income_distributions) if income_distributions is not None else None
        data["income_distributions_ytd"] = (
            str(income_distributions_ytd) if income_distributions_ytd is not None else None
        )
        data["change_investment"] = str(change_investment) if change_investment is not None else None
        data["change_investment_ytd"] = (
            str(change_investment_ytd) if change_investment_ytd is not None else None
        )

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
        if interpretation_notes:
            data["interpretation_notes"] = interpretation_notes
        return data

    @staticmethod
    def _extract_cash_fixed_income_summary(text: str) -> Optional[dict[str, dict[str, object]]]:
        """
        Extrae el bloque por categoria desde "Cash & Fixed Income Summary".
        """
        if "Cash & Fixed Income Summary" not in text:
            return None

        alloc: dict[str, dict[str, object]] = {}
        pattern = re.compile(
            r"(?im)^(Cash|Short\s+Term|Non[\-\s]*US\s+Fixed\s+Income)\s+"
            r"([\d,]+\.\d{2})\s+"
            r"([\d,]+\.\d{2})\s+"
            r"(\([\d,]+\.\d{2}\)|[\d,]+\.\d{2})\s+"
            r"(\d+)%(?:[ \t]+[^\n\r]*)?$"
        )
        for match in pattern.finditer(text):
            raw_label = re.sub(r"\s+", " ", match.group(1)).strip()
            label_norm = raw_label.lower().replace("-", " ")
            if label_norm == "short term":
                label = "Short Term"
            elif label_norm == "non us fixed income":
                label = "Non-US Fixed Income"
            else:
                label = "Cash"

            beginning = _parse_usd(match.group(2))
            ending = _parse_usd(match.group(3))
            change = _parse_usd(match.group(4))
            alloc[label] = {
                "beginning": str(beginning) if beginning is not None else None,
                "ending": str(ending) if ending is not None else None,
                "change": str(change) if change is not None else None,
                "allocation_pct": int(match.group(5)),
                "value": str(ending) if ending is not None else None,
            }

        return alloc or None

    @staticmethod
    def _extract_activity_values(
        text: str,
        label_pattern: str,
        *,
        single_value_means_ytd: bool,
    ) -> tuple[Optional[Decimal], Optional[Decimal], bool]:
        """
        Extrae columnas Current Period / YTD desde una fila de Portfolio Activity.

        Si la fila tiene un solo monto, se interpreta como YTD y el valor mensual
        en blanco se trata como 0 para no rellenar mensual con YTD.
        """
        label_re = re.compile(label_pattern, re.IGNORECASE)
        match = label_re.search(text)
        if not match:
            return None, None, False

        next_start = len(text)
        for next_pattern in JPMorganBrokerageParser._ACTIVITY_ROW_PATTERNS:
            if next_pattern == label_pattern:
                continue
            next_re = re.compile(next_pattern, re.IGNORECASE)
            next_match = next_re.search(text, match.end())
            if next_match:
                next_start = min(next_start, next_match.start())

        row_block = text[match.end():next_start]
        tokens = _ACTIVITY_VALUE_RE.findall(row_block)
        amounts = [_parse_usd(token) for token in tokens]
        amounts = [amount for amount in amounts if amount is not None]
        if len(amounts) >= 2:
            return amounts[0], amounts[1], False
        if len(amounts) == 1:
            if single_value_means_ytd:
                return Decimal("0"), amounts[0], True
            return amounts[0], None, False
        if single_value_means_ytd:
            return Decimal("0"), None, True
        return None, None, False

    def _finalize_account_mapping(self, result: ParseResult) -> None:
        extracted = [
            x.get("account_number")
            for x in result.qualitative_data.get("account_monthly_activity", [])
            if x.get("account_number")
        ]
        if extracted:
            account_set = set(extracted)
            accounts = result.qualitative_data.get("accounts", [])
            filtered = [a for a in accounts if a.get("account_number") in account_set]
            if filtered:
                result.qualitative_data["accounts"] = filtered
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
        current_account: Optional[str] = None
        current_section: str = "unknown"

        for page_num, text in enumerate(pages):
            acct_m = re.search(r"ACCT\.\s+([A-Z0-9]+)", text)
            if acct_m:
                current_account = acct_m.group(1)

            has_holdings_table = (
                "Price Quantity Value Original Cost" in text
                or (
                    "Price Quantity Value" in text
                    and "Adjusted Cost" in text
                )
            )

            if "Cash & Fixed Income" in text:
                current_section = "cash_fixed_income"
            elif has_holdings_table and re.search(r"\bEquity\b", text):
                current_section = "equity"

            if "Detail" not in text and "Holdings" not in text and not has_holdings_table:
                continue

            for line in text.split("\n"):
                h_m = re.match(
                    r"^(.{15,60}?)\s+"
                    r"([\d,.]+)\s+"
                    r"([\d,.]+)\s+"
                    r"([\d,]+\.\d{2})\s+"
                    r"([\d,]+\.\d{2}|N/A)",
                    line.strip(),
                )
                if h_m:
                    name = h_m.group(1).strip()
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
                text = pdf.pages[0].extract_text() or ""
                score = 0.0
                for marker in self._DETECTION_MARKERS:
                    if marker.lower() in text.lower():
                        score += 0.25
                # Bonus for brokerage-specific markers
                if "brokerage" in filepath.name.lower():
                    score += 0.25
                return min(score, 1.0)
        except Exception:
            return 0.0
