"""
UBS Miami mandate report parser (bank-isolated).
"""

from __future__ import annotations

import calendar
import re
from datetime import date
from pathlib import Path
from typing import Any

import pdfplumber

from parsers.base import BaseParser, ParseResult, ParsedRow, ParserStatus


_MONTHS_EN = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}
_MONTHS_ES_SHORT = {
    "ene": 1,
    "feb": 2,
    "mar": 3,
    "abr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "ago": 8,
    "sep": 9,
    "sept": 9,
    "oct": 10,
    "nov": 11,
    "dic": 12,
}


def _to_float(raw: str | None, *, comma_decimal: bool = False) -> float | None:
    if raw is None:
        return None
    cleaned = str(raw).strip().replace("%", "")
    if comma_decimal:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    else:
        cleaned = cleaned.replace(",", "")
    cleaned = re.sub(r"[^0-9.\-]", "", cleaned)
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _last_day(year: int, month: int) -> date:
    return date(year, month, calendar.monthrange(year, month)[1])


class UBSMiamiMandateReportParser(BaseParser):
    BANK_CODE = "ubs_miami"
    ACCOUNT_TYPE = "report_mandato"
    VERSION = "1.2.0"
    DESCRIPTION = "Parser aislado UBS Miami para reportes de mandato"
    SUPPORTED_EXTENSIONS = [".pdf"]

    def parse(self, filepath: Path) -> ParseResult:
        source_hash = self.compute_file_hash(filepath)
        pages = self._extract_pages(filepath)
        text = "\n".join(pages)
        filename = filepath.name

        if "duration&yield" in filename.lower():
            return self._parse_duration_yield_file(
                source_hash=source_hash,
                text=text,
            )

        period_end = self._extract_period_end(text=text, filename=filename)
        allocation = self._extract_allocation(text=text)

        status = ParserStatus.SUCCESS if allocation else ParserStatus.PARTIAL
        warnings: list[str] = []
        if not allocation:
            warnings.append("No se extrajo asset allocation en reporte UBS Miami.")

        rows = [
            ParsedRow(
                row_number=i + 1,
                data={"asset_class": label, **(payload if isinstance(payload, dict) else {"value": payload})},
            )
            for i, (label, payload) in enumerate(allocation.items())
            if not str(label).startswith("__")
        ]
        account_number = self._extract_account_number(text=text) or "Varios"

        return ParseResult(
            status=status,
            parser_name=self.get_parser_name(),
            parser_version=self.VERSION,
            source_file_hash=source_hash,
            account_number=account_number,
            bank_code=self.BANK_CODE,
            statement_date=period_end,
            period_end=period_end,
            currency="USD",
            rows=rows,
            qualitative_data={"asset_allocation": allocation} if allocation else {},
            warnings=warnings,
            raw_text_preview=text[:1200],
        )

    def validate(self, result: ParseResult) -> list[str]:
        errors: list[str] = []
        alloc = result.qualitative_data.get("asset_allocation")
        metrics_series = result.qualitative_data.get("fixed_income_metrics_by_month")
        if (not isinstance(alloc, dict) or not alloc) and not (isinstance(metrics_series, list) and metrics_series):
            errors.append("Sin asset allocation ni serie duration/yield en UBS Miami report_mandato")
        return errors

    def detect(self, filepath: Path) -> float:
        if filepath.suffix.lower() not in self.SUPPORTED_EXTENSIONS:
            return 0.0
        name = filepath.name.lower()
        if "ubs miami duration&yield" in name:
            return 0.99
        if "boatview limited" in name and re.search(r"\d{2}-\d{2}-\d{4}", name):
            return 0.98
        if "ubs" in name and "miami" in name:
            return 0.9
        return 0.35

    @staticmethod
    def _extract_pages(filepath: Path) -> list[str]:
        pages: list[str] = []
        with pdfplumber.open(str(filepath)) as pdf:
            for page in pdf.pages:
                pages.append(page.extract_text() or "")
        return pages

    @staticmethod
    def _extract_account_number(*, text: str) -> str | None:
        # Typical report line: "3X XX432  Boatview ..."
        m = re.search(r"\b3X\s+XX(\d{3})\b", text, flags=re.IGNORECASE)
        if not m:
            return None
        # Keep core digits as hint; loader can resolve by document account_id fallback.
        return m.group(1)

    def _extract_period_end(self, *, text: str, filename: str) -> date | None:
        m = re.search(r"As of\s+([A-Za-z]+)\s+(\d{1,2}),\s*([12]\d{3})", text, flags=re.IGNORECASE)
        if m:
            month = _MONTHS_EN.get(m.group(1).lower())
            if month:
                return date(int(m.group(3)), month, int(m.group(2)))

        m = re.search(r"\b(0?[1-9]|1[0-2])[/-]([0-2]?\d|3[01])[/-]([12]\d{3})\b", filename)
        if m:
            return date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
        return None

    @staticmethod
    def _extract_pct_row(text: str, label: str) -> float | None:
        m = re.search(rf"{label}\s+[\d,]+\.\d+\s+([\d.]+)\b", text, flags=re.IGNORECASE)
        if not m:
            return None
        return _to_float(m.group(1))

    def _extract_allocation(self, *, text: str) -> dict[str, dict[str, Any]]:
        allocation: dict[str, dict[str, Any]] = {}

        fixed_income = self._extract_pct_row(text, r"Fixed Income")
        corporate_ig = self._extract_pct_row(text, r"Corporate IG Credit")
        corporate_hy = self._extract_pct_row(text, r"Corporate High Yield")
        fixed_em = self._extract_pct_row(text, r"Emerging Markets")

        equity = self._extract_pct_row(text, r"Equity")
        us_equity = None
        us_equity_match = re.search(
            r"Equity\s+[\d,]+\.\d+\s+[\d.]+[\s\S]{0,220}US\s+[\d,]+\.\d+\s+([\d.]+)",
            text,
            flags=re.IGNORECASE,
        )
        if us_equity_match:
            us_equity = _to_float(us_equity_match.group(1))
        # International in this report belongs to both FI and Equity in different sections.
        # For equity split, use explicit "International 15.86" near Equity section.
        eq_international = None
        eq_international_match = re.search(r"Equity\s+[\d,]+\.\d+\s+[\d.]+[\s\S]{0,220}International\s+[\d,]+\.\d+\s+([\d.]+)", text, flags=re.IGNORECASE)
        if eq_international_match:
            eq_international = _to_float(eq_international_match.group(1))

        # Rule already approved for UBS Miami:
        # fixed-income emerging market is treated as High Yield.
        hy = None
        if corporate_hy is not None or fixed_em is not None:
            hy = (corporate_hy or 0.0) + (fixed_em or 0.0)

        ig = None
        if fixed_income is not None and hy is not None:
            ig = max(fixed_income - hy, 0.0)
        elif corporate_ig is not None:
            ig = corporate_ig

        if ig is not None:
            allocation["Investment Grade Fixed Income"] = {"value": ig, "unit": "%"}
        if hy is not None:
            allocation["High Yield Fixed Income"] = {"value": hy, "unit": "%"}
        if us_equity is not None:
            allocation["US Equities"] = {"value": us_equity, "unit": "%"}
        if equity is not None and us_equity is not None:
            allocation["Non US Equities"] = {"value": max(equity - us_equity, 0.0), "unit": "%"}
        elif eq_international is not None:
            allocation["Non US Equities"] = {"value": eq_international, "unit": "%"}

        return allocation

    def _parse_duration_yield_file(
        self,
        *,
        source_hash: str,
        text: str,
    ) -> ParseResult:
        rows: list[dict[str, Any]] = []
        metrics_series: list[dict[str, Any]] = []
        for raw_line in text.splitlines():
            line = raw_line.strip().lower()
            if not line:
                continue
            m = re.search(r"\b([a-z]{3,4})-([0-9]{2})\s+([0-9]+,[0-9])\s+([0-9]+,[0-9])%", line)
            if not m:
                continue
            month = _MONTHS_ES_SHORT.get(m.group(1))
            if month is None:
                continue
            year = 2000 + int(m.group(2))
            duration = _to_float(m.group(3), comma_decimal=True)
            yld = _to_float(m.group(4), comma_decimal=True)
            if duration is None and yld is None:
                continue
            metrics_series.append(
                {
                    "year": year,
                    "month": month,
                    "fixed_income_duration": duration,
                    "fixed_income_yield": yld,
                    "yield_unit": "%",
                    "source": "ubs_miami_duration_yield_table",
                }
            )
            rows.append(
                {
                    "period": f"{year}-{month:02d}",
                    "duration": duration,
                    "yield": yld,
                }
            )

        metrics_series.sort(key=lambda x: (x["year"], x["month"]))
        latest_period = metrics_series[-1] if metrics_series else None
        period_end = _last_day(latest_period["year"], latest_period["month"]) if latest_period else None

        allocation: dict[str, Any] = {}
        if latest_period:
            allocation["__mandate_metrics"] = {
                "fixed_income_duration": {
                    "value": latest_period.get("fixed_income_duration"),
                    "unit": "years",
                    "source": "ubs_miami_duration_yield_table_latest",
                },
                "fixed_income_yield": {
                    "value": latest_period.get("fixed_income_yield"),
                    "unit": "%",
                    "source": "ubs_miami_duration_yield_table_latest",
                },
            }

        parsed_rows = [
            ParsedRow(
                row_number=i + 1,
                data=row,
            )
            for i, row in enumerate(rows)
        ]
        status = ParserStatus.SUCCESS if metrics_series else ParserStatus.PARTIAL
        warnings: list[str] = []
        if not metrics_series:
            warnings.append("No se extrajo serie duration/yield en UBS Miami.")

        qualitative_data: dict[str, Any] = {"fixed_income_metrics_by_month": metrics_series}
        if allocation:
            qualitative_data["asset_allocation"] = allocation

        return ParseResult(
            status=status,
            parser_name=self.get_parser_name(),
            parser_version=self.VERSION,
            source_file_hash=source_hash,
            account_number="Varios",
            bank_code=self.BANK_CODE,
            statement_date=period_end,
            period_end=period_end,
            currency="USD",
            rows=parsed_rows,
            qualitative_data=qualitative_data,
            warnings=warnings,
            raw_text_preview=text[:1200],
        )
