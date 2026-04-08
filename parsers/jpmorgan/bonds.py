"""
Parser: JPMorgan – Cuenta Bonos (Investment Management PDF).

Formato: "01 December - 31 December 2025", "Account Number: 1531100"
Páginas 1-4: Texto legal
Página 5: Table of Contents
Página 6: Account Summary (asset allocation + portfolio activity)
Páginas 9+: Holdings con formato 3-líneas (nombre, cupón/vencimiento, ISIN)

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

class JPMorganBondsParser(BaseParser):
    BANK_CODE = "jpmorgan"
    ACCOUNT_TYPE = "bonds"
    VERSION = "2.0.2"
    DESCRIPTION = "Parser para cartolas Mandato Bonos JPMorgan (Investment Management PDF)"
    SUPPORTED_EXTENSIONS = [".pdf"]

    _DETECTION_MARKERS = [
        "JPMorgan Chase Bank",
        "Statement of Account",
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
        self._extract_account_number(pages, result)
        self._extract_account_summary(pages, result)
        self._extract_fixed_income_summary(pages, result)
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
            m = re.search(r"Account Number:\s*([A-Z0-9-]+)", text)
            if m:
                result.account_number = m.group(1)
                return

    def _extract_account_summary(self, pages: list[str], result: ParseResult) -> None:
        """Extract from 'Account Summary' page with Asset Allocation and Portfolio Activity."""
        for text in pages[:15]:
            if "Account Summary" not in text:
                continue
            if "Asset Allocation" not in text and "Portfolio Activity" not in text:
                continue

            # Asset allocation: "Cash, Deposits & Short Term 1,314,748.12 1,217,852.19 -96,895.93"
            alloc: dict[str, dict] = {}
            for m in re.finditer(
                r"(Cash,?\s*Deposits?\s*&?\s*Short\s*Term|Fixed Income|Equities)"
                r"\s+([\d,]+\.\d{2})\s+([\d,]+\.\d{2})\s+(-?[\d,]+\.\d{2})",
                text,
            ):
                alloc[m.group(1).strip()] = {
                    "beginning": str(_parse_usd(m.group(2))),
                    "ending": str(_parse_usd(m.group(3))),
                    "change": str(_parse_usd(m.group(4))),
                }
            if alloc:
                self._apply_cash_holdings_override_for_ecoterra_1100(
                    pages=pages,
                    result=result,
                    alloc=alloc,
                )
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

    @staticmethod
    def _normalized_label(label: str) -> str:
        return re.sub(r"[^a-z0-9]", "", str(label or "").lower())

    def _extract_total_cash_holdings(self, pages: list[str]) -> Optional[Decimal]:
        """
        Extrae `Total Cash Holdings` desde la sección Cash, Deposits & Short Term.
        """
        pattern = re.compile(r"Total\s+Cash\s+Holdings\s+([\d,]+\.\d{2})", re.IGNORECASE)
        for text in pages[:25]:
            if "Cash Holdings" not in text:
                continue
            m = pattern.search(text)
            if m:
                return _parse_usd(m.group(1))
        return None

    def _extract_total_short_term_investments(self, pages: list[str]) -> Optional[Decimal]:
        """
        Extrae `Total Short Term Investments` para reclasificarlo como IG FI en 1531100.
        """
        pattern = re.compile(
            r"Total\s+Short\s+Term\s+Investments\s+[\d,]+\.\d{2}\s+([\d,]+\.\d{2})",
            re.IGNORECASE,
        )
        # Fallback two-col: skip beginning, capture ending (same as primary pattern).
        # Fallback one-col: capture the only value when no second column is present.
        fallback_two_col = re.compile(
            r"Total\s+Short\s+Term\s+Investments\s+[\d,]+\.\d{2}\s+([\d,]+\.\d{2})",
            re.IGNORECASE,
        )
        fallback_one_col = re.compile(
            r"Total\s+Short\s+Term\s+Investments\s+([\d,]+\.\d{2})(?!\s*[\d,]+\.\d{2})",
            re.IGNORECASE,
        )
        line_pattern = re.compile(
            r"(?:^|\n)\s*Short\s+Term\s+Investments\s+([\d,]+\.\d{2})(?:\s|$)",
            re.IGNORECASE,
        )
        for text in pages[:25]:
            if "Short Term Investments" not in text:
                continue
            m = pattern.search(text)
            if not m:
                m = fallback_two_col.search(text)
            if not m:
                m = fallback_one_col.search(text)
            if not m:
                m = line_pattern.search(text)
            if m:
                return _parse_usd(m.group(1))
        return None

    def _apply_cash_holdings_override_for_ecoterra_1100(
        self,
        *,
        pages: list[str],
        result: ParseResult,
        alloc: dict[str, dict],
    ) -> None:
        """
        Ajuste acotado al motor JPM bonds para Ecoterra Internacional 1100:
        caja = Total Cash Holdings (sin incluir Short Term Investments).
        """
        if str(result.account_number or "").strip() != "1531100":
            return
        cash_holdings = self._extract_total_cash_holdings(pages)
        if cash_holdings is None:
            return

        cash_key = None
        for key in alloc.keys():
            key_norm = self._normalized_label(key)
            if "cash" in key_norm and "deposit" in key_norm and "shortterm" in key_norm:
                cash_key = key
                break
        if cash_key is None:
            return

        cash_payload = alloc.get(cash_key) if isinstance(alloc.get(cash_key), dict) else {}
        beginning_value = _parse_usd(str(cash_payload.get("beginning") or ""))
        cash_payload["ending"] = str(cash_holdings)
        if beginning_value is not None:
            cash_payload["change"] = str(cash_holdings - beginning_value)
        alloc[cash_key] = cash_payload

        # En esta cuenta, Short Term Investments se reporta dentro de RF (IG),
        # no en caja.
        short_term_total = self._extract_total_short_term_investments(pages)
        if short_term_total is None:
            return

        fixed_key = None
        for key in alloc.keys():
            key_norm = self._normalized_label(key)
            if "fixedincome" in key_norm:
                fixed_key = key
                break
        if fixed_key is None:
            return

        fixed_payload = alloc.get(fixed_key) if isinstance(alloc.get(fixed_key), dict) else {}
        fixed_beginning = _parse_usd(str(fixed_payload.get("beginning") or ""))
        fixed_ending = _parse_usd(str(fixed_payload.get("ending") or "")) or Decimal("0")
        updated_fixed = fixed_ending + short_term_total
        fixed_payload["ending"] = str(updated_fixed)
        if fixed_beginning is not None:
            fixed_payload["change"] = str(updated_fixed - fixed_beginning)
        alloc[fixed_key] = fixed_payload

    def _extract_fixed_income_summary(self, pages: list[str], result: ParseResult) -> None:
        """Extract Fixed Income summary page with maturity breakdown."""
        for text in pages[:20]:
            if "Fixed Income" not in text or "Summary by Maturity" not in text:
                continue

            maturity: dict[str, str] = {}
            for m in re.finditer(
                r"(0-6 months|6-12 months|1-2 years|2-5 years|5-10 years|10-15 years|>15 years|Funds)"
                r"\s+([\d,]+\.\d{2})\s+([\d.]+)%",
                text,
            ):
                maturity[m.group(1)] = str(_parse_usd(m.group(2)))
            if maturity:
                result.qualitative_data["fixed_income_maturity"] = maturity

            # Total Fixed Income
            total_m = re.search(
                r"Total (?:Market Value|Fixed Income)\s+([\d,]+\.\d{2})\s+([\d.]+)%",
                text,
            )
            if total_m:
                result.qualitative_data["total_fixed_income"] = str(_parse_usd(total_m.group(1)))
            break

    def _extract_holdings(self, pages: list[str], result: ParseResult) -> None:
        """Extract bond holdings from detail pages.

        Each holding spans 2-3 lines:
        Line 1: SECURITY_NAME  QUANTITY  UNIT_COST  MARKET_PRICE  AVG_COST_USD  MARKET_VALUE_USD  GAIN_LOSS  (C)  INCOME  %
        Line 2: COUPON%  DATE_RANGE  DATE  ACCRUED_INT  0.00  (F)  YTM
        Line 3: ISIN
        """
        current_section = "unknown"

        for page_num, text in enumerate(pages):
            if "Short Term" in text and ("Investments" in text or "Holdings" in text):
                current_section = "short_term"
            elif "Fixed Income Holdings" in text:
                current_section = "fixed_income"

            # Pattern for bond holdings line 1:
            # BOEING CO 63,000.00 95.70 99.88 60,291.58 62,926.01 2,634.43 (C) 1,732.50 0.48%
            for m in re.finditer(
                r"^([A-Z][A-Z\s&/.'-]{3,40}?)\s+"
                r"([\d,]+\.\d{2})\s+"       # quantity/face value
                r"([\d.]+)\s+"               # unit cost price
                r"([\d.]+)\s+"               # market price
                r"([\d,]+\.\d{2})\s+"        # avg cost USD
                r"([\d,]+\.\d{2})\s+"        # market value USD
                r"(-?[\d,]+\.\d{2})\s+"      # gain/loss
                r"\(C\)\s+"                  # capital gain marker
                r"([\d,.]+)\s+"              # income
                r"([\d.]+)%",                # % of portfolio
                text,
                re.MULTILINE,
            ):
                name = m.group(1).strip()
                if any(skip in name.lower() for skip in [
                    "total", "summary", "account", "page", "description",
                    "security", "status", "nominal",
                ]):
                    continue

                quantity = _parse_usd(m.group(2))
                cost_value = _parse_usd(m.group(5))
                market_value = _parse_usd(m.group(6))
                gain_loss = _parse_usd(m.group(7))
                income = _parse_usd(m.group(8))
                pct = m.group(9)

                result.rows.append(ParsedRow(
                    data={
                        "instrument": name,
                        "asset_type": "bond",
                        "quantity": str(quantity) if quantity else None,
                        "unit_cost": m.group(3),
                        "market_price": m.group(4),
                        "cost_value": str(cost_value) if cost_value else None,
                        "market_value": str(market_value) if market_value else None,
                        "unrealized_gain_loss": str(gain_loss) if gain_loss else None,
                        "estimated_annual_income": str(income) if income else None,
                        "pct_of_portfolio": pct + "%",
                        "section": current_section,
                    },
                    row_number=page_num + 1,
                    confidence=0.85,
                ))

            # Simpler fallback pattern for bonds without (C) marker
            if not any("(C)" in (text or "") for text in [text]):
                for m in re.finditer(
                    r"^([A-Z][A-Z\s&/.'-]{3,40}?)\s+"
                    r"([\d,]+\.\d{2})\s+"       # quantity
                    r"([\d.]+)\s+"               # unit cost
                    r"([\d.]+)\s+"               # market price
                    r"([\d,]+\.\d{2})\s+"        # avg cost
                    r"([\d,]+\.\d{2})\s+"        # market value
                    r"(-?[\d,]+\.\d{2})",         # gain/loss
                    text,
                    re.MULTILINE,
                ):
                    name = m.group(1).strip()
                    if any(skip in name.lower() for skip in [
                        "total", "summary", "account", "page", "short term",
                    ]):
                        continue

                    # Check if this holding already exists
                    existing = [r for r in result.rows if r.data.get("instrument") == name]
                    if existing:
                        continue

                    market_value = _parse_usd(m.group(6))
                    cost_value = _parse_usd(m.group(5))
                    gain_loss = _parse_usd(m.group(7))

                    result.rows.append(ParsedRow(
                        data={
                            "instrument": name,
                            "asset_type": "bond",
                            "quantity": str(_parse_usd(m.group(2))),
                            "cost_value": str(cost_value) if cost_value else None,
                            "market_value": str(market_value) if market_value else None,
                            "unrealized_gain_loss": str(gain_loss) if gain_loss else None,
                            "section": current_section,
                        },
                        row_number=page_num + 1,
                        confidence=0.7,
                    ))

            # Section totals
            for m in re.finditer(
                r"Total\s+([\w\s&,]+?)\s+([\d,]+\.\d{2})\s+([\d,]+\.\d{2})",
                text,
            ):
                section = m.group(1).strip()
                if any(skip in section.lower() for skip in ["page", "account"]):
                    continue
                cost = _parse_usd(m.group(2))
                value = _parse_usd(m.group(3))
                if value and value > Decimal("0"):
                    result.rows.append(ParsedRow(
                        data={
                            "instrument": f"TOTAL: {section}",
                            "cost_value": str(cost) if cost else None,
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
                for page in pdf.pages[:10]:
                    text = page.extract_text() or ""
                    if "Statement of Account" in text and "JPMorgan" in text:
                        # Check if it's bonds-specific
                        if "Fixed Income" in text or "1531100" in text:
                            return 0.9
                        return 0.5
                    if "Account Number:" in text and "JPMorgan Chase Bank" in text:
                        return 0.7
                # Filename bonus
                fname = filepath.name.lower()
                score = 0.0
                if "jpmorgan" in fname or "jpm" in fname:
                    score += 0.3
                if "bono" in fname or "bond" in fname:
                    score += 0.3
                return min(score, 1.0)
        except Exception:
            return 0.0
