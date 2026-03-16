"""
Parser: UBS Miami – Portfolio Management Program (PMP) Statement.

Formato:  "December 2025", Account: 3J 00432 P1
Página 1: Account value overview, sources of growth
Página 2: Asset allocation balance sheet
Página 3: Change in value, gains/losses, cash activity
Páginas 5-7: Equity holdings (common stock, ETFs)
Páginas 8-13: Fixed income holdings (corporate bonds)
Texto libre, sin tablas pdfplumber.

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


def _parse_date_mdy(text: str) -> Optional[date]:
    """Parse 'Dec 31, 2025' or 'December 31, 2025'."""
    for pat in [r"(\w+)\s+(\d{1,2}),?\s+(\d{4})", r"(\w+)\s+(\d{1,2}),?\s+(\d{2})"]:
        m = re.search(pat, text)
        if m:
            month_str = m.group(1).lower()
            month = _MONTHS.get(month_str)
            # Try abbreviated months
            if not month:
                for k, v in _MONTHS.items():
                    if k.startswith(month_str[:3]):
                        month = v
                        break
            if not month:
                continue
            year = int(m.group(3))
            if year < 100:
                year += 2000
            try:
                return date(year, month, int(m.group(2)))
            except ValueError:
                continue
    return None


# ── Parser ───────────────────────────────────────────────────────

class UBSMiamiCustodyParser(BaseParser):
    BANK_CODE = "ubs_miami"
    ACCOUNT_TYPE = "custody"
    VERSION = "2.1.2"
    DESCRIPTION = "Parser para cartolas UBS Miami PMP (Portfolio Management Program)"
    SUPPORTED_EXTENSIONS = [".pdf"]

    _DETECTION_MARKERS = [
        "UBS Financial Services",
        "Portfolio Management Program",
        "Coral Gables",
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
        self._extract_asset_allocation(pages, result)
        self._extract_change_in_value(pages, result)
        self._emit_account_monthly_activity(result)
        self._extract_equity_holdings(pages, result)
        self._extract_fi_holdings(pages, result)

        if not result.account_number:
            result.status = ParserStatus.PARTIAL
            result.warnings.append("No se encontró número de cuenta")

        return result

    def _extract_overview(self, pages: list[str], result: ParseResult) -> None:
        """Page 1: Account info and total value."""
        text = pages[0] if pages else ""

        # Account number: "3J 00432 P1"
        m = re.search(r"Account number:\s*(.+?)(?:\n|$)", text)
        if m:
            result.account_number = m.group(1).strip()

        # Account name
        m = re.search(r"Account name:\s*(.+?)(?:\n|Friendly)", text)
        if m:
            result.qualitative_data["account_name"] = m.group(1).strip()

        # Period: "December 2025" (header code varies: ANQ/CNQ/AFG/CFG/etc.).
        m = re.search(
            r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})\b",
            text,
            flags=re.IGNORECASE,
        )
        if m:
            month_str = m.group(1).lower()
            month = _MONTHS.get(month_str)
            year = int(m.group(2))
            if month:
                try:
                    import calendar
                    last_day = calendar.monthrange(year, month)[1]
                    result.period_end = date(year, month, last_day)
                    result.period_start = date(year, month, 1)
                    result.statement_date = result.period_end
                except (ValueError, ImportError):
                    pass

        # Account value: "Value of your account $91,920,968.16 $92,398,627.15"
        m = re.search(r"Value of your account\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})", text)
        if m:
            result.opening_balance = _parse_usd(m.group(1))
            result.closing_balance = _parse_usd(m.group(2))

        # Sources of growth
        growth: dict[str, str] = {}
        patterns = [
            ("year_end_prior", r"at year end \d{4}\s+\$?([\d,]+\.\d{2})"),
            ("net_deposits_withdrawals", r"Net deposits and\s*\n?\s*withdrawals\s+(-?\$?[\d,]+\.\d{2})"),
            ("dividend_interest", r"Dividend and\s*\n?\s*interest income\s+\$?([\d,]+\.\d{2})"),
            ("change_accrued_interest", r"Change in value of\s*\n?\s*accrued interest\s+(-?\$?[\d,]+\.\d{2})"),
            ("change_market_value", r"Change in\s*\n?\s*market value\s+(-?\$?[\d,]+\.\d{2})"),
        ]
        for key, pat in patterns:
            m = re.search(pat, text)
            if m:
                growth[key] = str(_parse_usd(m.group(1)))
        if growth:
            result.qualitative_data["sources_of_growth"] = growth

    def _extract_asset_allocation(self, pages: list[str], result: ParseResult) -> None:
        """Page 2: Asset allocation balance sheet."""
        for text in pages[:5]:
            if "Summary of your assets" not in text:
                continue

            alloc: dict[str, dict] = {}
            for m in re.finditer(
                r"([A-G])\s+(Cash and money balances|Cash alternatives|Equities|Fixed income|"
                r"Non-traditional|Commodities|Other)\s+"
                r"([\d,]+\.\d{2})\s+([\d.]+)%",
                text,
            ):
                alloc[m.group(2)] = {
                    "value": str(_parse_usd(m.group(3))),
                    "pct": m.group(4) + "%",
                }

            if alloc:
                result.qualitative_data["asset_allocation"] = alloc

            # Total assets
            total_m = re.search(r"Total assets\s+\$?([\d,]+\.\d{2})", text)
            if total_m:
                result.qualitative_data["total_assets"] = str(_parse_usd(total_m.group(1)))

            # Market indices
            indices: dict[str, dict] = {}
            for m in re.finditer(
                r"(S&P 500|Russell 3000|MSCI.*?|Barclays.*?)\s+(-?[\d.]+)%\s+(-?[\d.]+)%",
                text,
            ):
                indices[m.group(1).strip()] = {
                    "month": m.group(2) + "%",
                    "ytd": m.group(3) + "%",
                }
            if indices:
                result.qualitative_data["market_indices"] = indices
            break

    def _extract_change_in_value(self, pages: list[str], result: ParseResult) -> None:
        """Page 3: Change in account value and gains/losses."""
        for text in pages[:5]:
            if "Change in the value of your account" not in text:
                continue

            changes = {}
            patterns = [
                ("opening_value", r"Opening account value\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})"),
                (
                    "withdrawals_fees",
                    r"Withdrawals and fees[\s\S]*?investments transferred[\s\S]*?out\s+(-?[\d,]+\.\d{2})\s+(-?[\d,]+\.\d{2})",
                ),
                ("dividend_interest", r"Dividend and interest income\s+([\d,]+\.\d{2})\s+([\d,]+\.\d{2})"),
                (
                    "change_accrued",
                    r"Change in value of accrued[\s\S]*?interest\s+(-?[\d,]+\.\d{2})\s+(-?[\d,]+\.\d{2})",
                ),
                ("change_market", r"Change in market value\s+(-?[\d,]+\.\d{2})\s+(-?[\d,]+\.\d{2})"),
                ("closing_value", r"Closing account value\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})"),
            ]
            for key, pat in patterns:
                m = re.search(pat, text)
                if m:
                    changes[key] = {
                        "month": str(_parse_usd(m.group(1))),
                        "ytd": str(_parse_usd(m.group(2))),
                    }
            if changes:
                result.qualitative_data["value_changes"] = changes

            # Gains and losses
            gains: dict[str, dict] = {}
            for m in re.finditer(
                r"(Short term|Long term)\s+([\d,.-]+)\s+([\d,.-]+)\s+([\d,.-]+)",
                text,
            ):
                gains[m.group(1).lower()] = {
                    "realized_month": str(_parse_usd(m.group(2))),
                    "realized_ytd": str(_parse_usd(m.group(3))),
                    "unrealized": str(_parse_usd(m.group(4))),
                }
            if gains:
                result.qualitative_data["gains_losses"] = gains

            # Income breakdown
            income = {}
            for m in re.finditer(
                r"(Taxable dividends|Taxable interest)\s+([\d,]+\.\d{2})\s+([\d,]+\.\d{2})",
                text,
            ):
                income[m.group(1)] = {
                    "month": str(_parse_usd(m.group(2))),
                    "ytd": str(_parse_usd(m.group(3))),
                }
            if income:
                result.qualitative_data["income_breakdown"] = income
            break

    def _emit_account_monthly_activity(self, result: ParseResult) -> None:
        """Emite movimientos/utilidad estandarizados para DataLoadingService."""
        if not result.account_number:
            return

        changes = result.qualitative_data.get("value_changes", {})
        growth = result.qualitative_data.get("sources_of_growth", {})

        def _month_val(key: str) -> Optional[Decimal]:
            block = changes.get(key)
            if isinstance(block, dict):
                raw = block.get("month")
                if raw is not None:
                    return _parse_usd(str(raw))
            return None

        net_contributions = _month_val("withdrawals_fees")
        if net_contributions is None:
            raw = growth.get("net_deposits_withdrawals")
            if raw is not None:
                net_contributions = _parse_usd(str(raw))

        div_int = _month_val("dividend_interest")
        if div_int is None and growth.get("dividend_interest") is not None:
            div_int = _parse_usd(str(growth.get("dividend_interest")))

        chg_accr = _month_val("change_accrued")
        if chg_accr is None and growth.get("change_accrued_interest") is not None:
            chg_accr = _parse_usd(str(growth.get("change_accrued_interest")))

        chg_mkt = _month_val("change_market")
        if chg_mkt is None and growth.get("change_market_value") is not None:
            chg_mkt = _parse_usd(str(growth.get("change_market_value")))

        utilidad = None
        if any(v is not None for v in (div_int, chg_accr, chg_mkt)):
            utilidad = (div_int or Decimal("0")) + (chg_accr or Decimal("0")) + (chg_mkt or Decimal("0"))
        elif (
            result.opening_balance is not None
            and result.closing_balance is not None
            and net_contributions is not None
        ):
            utilidad = result.closing_balance - result.opening_balance - net_contributions

        if net_contributions is None and utilidad is None:
            return

        result.qualitative_data["account_monthly_activity"] = [
            {
                "account_number": result.account_number,
                "beginning_value": (
                    str(result.opening_balance) if result.opening_balance is not None else None
                ),
                "ending_value_with_accrual": (
                    str(result.closing_balance) if result.closing_balance is not None else None
                ),
                "ending_value_without_accrual": (
                    str(result.closing_balance) if result.closing_balance is not None else None
                ),
                "net_contributions": (
                    str(net_contributions) if net_contributions is not None else None
                ),
                "utilidad": str(utilidad) if utilidad is not None else None,
                "source": "ubs_miami_change_in_value",
            }
        ]

    def _extract_equity_holdings(self, pages: list[str], result: ParseResult) -> None:
        """Pages 5-7: Equity positions (common stock, ETFs)."""
        in_equities = False

        for page_num, text in enumerate(pages):
            if "Equities" in text:
                in_equities = True
            if in_equities and "Fixed income" in text and "Corporate bonds" in text:
                in_equities = False

            if not in_equities:
                continue

            # "Security total" lines:
            # Security total 9,100.000 490.071 4,459,650.15 4,574,115.00 114,464.85
            for m in re.finditer(
                r"Security total\s+"
                r"([\d,]+\.\d{3})\s+"      # shares
                r"([\d,]+\.\d{3})\s+"       # avg price
                r"([\d,]+\.\d{2})\s+"       # cost basis
                r"([\d,]+\.\d{2})\s+"       # value
                r"(-?[\d,]+\.\d{2})",        # unrealized
                text,
            ):
                # Get instrument name from lines before
                pos = m.start()
                preceding = text[:pos]
                lines = preceding.strip().split("\n")

                instrument = "Unknown Equity"
                for line in reversed(lines):
                    line = line.strip()
                    if re.match(r"^(Trade date:|EAI:|Security total|Purchase|Number|Holding|continued|ab |Portfolio|December|Account)", line):
                        continue
                    if line and len(line) > 5 and not line[0].isdigit():
                        instrument = line.split("  ")[0].strip()
                        break

                shares = _parse_usd(m.group(1))
                cost = _parse_usd(m.group(3))
                value = _parse_usd(m.group(4))
                gain = _parse_usd(m.group(5))

                result.rows.append(ParsedRow(
                    data={
                        "instrument": instrument,
                        "asset_type": "equity",
                        "shares": str(shares) if shares else None,
                        "avg_price": m.group(2),
                        "cost_basis": str(cost) if cost else None,
                        "market_value": str(value) if value else None,
                        "unrealized_gain_loss": str(gain) if gain else None,
                        "section": "equities",
                    },
                    row_number=page_num + 1,
                    confidence=0.85,
                ))

            # Single-lot equities (no "Security total", just one trade date line ending with value)
            # E.g.: "Trade date: Dec 6, 24 20,268.000 189.199 3,834,686.08 3,834,686.08 254.678 5,161,813.70 1,327,127.62 1,327,127.62 LT"
            # These are harder to parse consistently, skip for now and rely on "Security total"

            # Section total: "Total $26,377,050.02 $26,377,050.02 $31,223,313.39 $4,846,263.37"
            total_m = re.search(
                r"Total\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})",
                text,
            )
            if total_m:
                result.rows.append(ParsedRow(
                    data={
                        "instrument": "TOTAL: Equities ETFs",
                        "cost_basis": str(_parse_usd(total_m.group(1))),
                        "market_value": str(_parse_usd(total_m.group(3))),
                        "unrealized_gain_loss": str(_parse_usd(total_m.group(4))),
                        "is_total": True,
                        "section": "equities",
                    },
                    row_number=page_num + 1,
                    confidence=0.9,
                ))

    def _extract_fi_holdings(self, pages: list[str], result: ParseResult) -> None:
        """Pages 8-13: Fixed income positions (corporate bonds)."""
        in_fi = False

        for page_num, text in enumerate(pages):
            if "Fixed income" in text and "Corporate bonds" in text:
                in_fi = True
            if in_fi and "Account activity" in text:
                in_fi = False
                break

            if not in_fi:
                continue

            # Bond pattern: Trade date line with face value, purchase price, cost, price, value, unrealized
            # Standard single-lot bond:
            # "Mar 06, 19 800,000.000 102.345 818,760.00 100.072 800,576.00 -18,184.00 LT"
            for m in re.finditer(
                r"(?:Security total|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{2,4})"
                r"\s+([\d,]+\.\d{3})\s+"          # face value
                r"(?:[\d,.]+\s+)?"                 # purchase price (may be absent for security total)
                r"([\d,]+\.\d{2})\s+"              # cost basis
                r"(?:[\d,.]+\s+)?"                 # price per share (may be absent)
                r"([\d,]+\.\d{2})\s+"              # value
                r"(-?[\d,]+\.\d{2})",               # unrealized
                text,
            ):
                # This is too broad. Let me use a different approach.
                pass

            # Better: look for bond names (RATE XX.XXX% MATURES MM/DD/YY)
            # Then find "Security total" or single trade line
            bond_names: list[tuple[str, int]] = []
            for m in re.finditer(r"^([A-Z][A-Z\s&/.'()-]+?)(?:\n|$)", text, re.MULTILINE):
                name = m.group(1).strip()
                if len(name) > 8 and not any(skip in name for skip in [
                    "RATE", "MATURES", "ACCRUED", "CUSIP", "Moody", "EAI",
                    "Trade", "Security", "Total", "Purchase", "Holding",
                    "Portfolio", "Account", "continued", "December",
                    "Prices", "Cost basis", "Unrealized",
                ]):
                    bond_names.append((name, m.start()))

            # Find "Security total" lines for bonds
            for m in re.finditer(
                r"Security total\s+"
                r"([\d,]+\.\d{3})\s+"       # face value
                r"([\d,]+\.\d{2})\s+"        # cost basis
                r"([\d,]+\.\d{2})\s+"        # value
                r"(-?[\d,]+\.\d{2})",         # unrealized
                text,
            ):
                # Find nearest bond name before this position
                instrument = "Unknown Bond"
                for name, pos in reversed(bond_names):
                    if pos < m.start():
                        instrument = name
                        break

                face = _parse_usd(m.group(1))
                cost = _parse_usd(m.group(2))
                value = _parse_usd(m.group(3))
                gain = _parse_usd(m.group(4))

                result.rows.append(ParsedRow(
                    data={
                        "instrument": instrument,
                        "asset_type": "bond",
                        "face_value": str(face) if face else None,
                        "cost_basis": str(cost) if cost else None,
                        "market_value": str(value) if value else None,
                        "unrealized_gain_loss": str(gain) if gain else None,
                        "section": "fixed_income",
                    },
                    row_number=page_num + 1,
                    confidence=0.8,
                ))

            # Single-lot bonds (only one trade date, no "Security total")
            # Pattern: "TradeDate FACE_VALUE PURCHASE_PRICE COST_BASIS PRICE VALUE UNREALIZED LT/ST"
            for m in re.finditer(
                r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{2,4}\s+"
                r"([\d,]+\.\d{3})\s+"          # face
                r"([\d,.]+)\s+"                 # purchase price
                r"([\d,]+\.\d{2})\s+"           # cost
                r"([\d,.]+)\s+"                 # price
                r"([\d,]+\.\d{2})\s+"           # value
                r"(-?[\d,]+\.\d{2})\s+"         # unrealized
                r"(LT|ST)",
                text,
            ):
                cost = _parse_usd(m.group(3))
                value = _parse_usd(m.group(5))
                gain = _parse_usd(m.group(6))
                face = _parse_usd(m.group(1))

                # Find nearest bond name
                instrument = "Unknown Bond"
                pos = m.start()
                for name, name_pos in reversed(bond_names):
                    if name_pos < pos:
                        instrument = name
                        break

                # Only add if not already captured by "Security total"
                existing = [r for r in result.rows
                            if r.data.get("instrument") == instrument
                            and r.data.get("section") == "fixed_income"]
                if not existing:
                    result.rows.append(ParsedRow(
                        data={
                            "instrument": instrument,
                            "asset_type": "bond",
                            "face_value": str(face) if face else None,
                            "cost_basis": str(cost) if cost else None,
                            "market_value": str(value) if value else None,
                            "unrealized_gain_loss": str(gain) if gain else None,
                            "holding_period": m.group(7),
                            "section": "fixed_income",
                        },
                        row_number=page_num + 1,
                        confidence=0.75,
                    ))

            # FI section total
            total_m = re.search(
                r"Total\s+\$?([\d,]+\.\d{3})\s+"    # total face
                r"\$?([\d,]+\.\d{2})\s+"             # total cost
                r"\$?([\d,]+\.\d{2})\s+"             # total value
                r"\$?(-?[\d,]+\.\d{2})",              # total unrealized
                text,
            )
            if total_m and "Fixed income" in text:
                result.rows.append(ParsedRow(
                    data={
                        "instrument": "TOTAL: Fixed Income",
                        "face_value": str(_parse_usd(total_m.group(1))),
                        "cost_basis": str(_parse_usd(total_m.group(2))),
                        "market_value": str(_parse_usd(total_m.group(3))),
                        "unrealized_gain_loss": str(_parse_usd(total_m.group(4))),
                        "is_total": True,
                        "section": "fixed_income",
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
                        score += 0.25
                fname = filepath.name.lower()
                if "ubs" in fname and "miami" in fname:
                    score += 0.25
                elif "ubs" in fname:
                    score += 0.1
                return min(score, 1.0)
        except Exception:
            return 0.0
