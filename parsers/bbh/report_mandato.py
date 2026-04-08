"""
BBH mandate report parser (bank-isolated).
"""

from __future__ import annotations

import calendar
import re
from datetime import date
from pathlib import Path
from typing import Any

import pdfplumber

from parsers.base import BaseParser, ParseResult, ParsedRow, ParserStatus


_MONTHS = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}


def _safe_float(raw: str | None) -> float | None:
    if not raw:
        return None
    cleaned = str(raw).strip().replace("%", "").replace(",", "")
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _last_day(year: int, month: int) -> date:
    return date(year, month, calendar.monthrange(year, month)[1])


class BBHMandateReportParser(BaseParser):
    BANK_CODE = "bbh"
    ACCOUNT_TYPE = "report_mandato"
    VERSION = "1.2.0"
    DESCRIPTION = "Parser aislado BBH para reportes de mandato"
    SUPPORTED_EXTENSIONS = [".pdf"]

    def parse(self, filepath: Path) -> ParseResult:
        source_hash = self.compute_file_hash(filepath)
        pages = self._extract_pages(filepath)
        text = "\n".join(pages)

        period_end = self._extract_period_end(text=text, filename=filepath.name)
        allocation = self._extract_allocation(text=text)
        metrics = self._extract_fixed_income_metrics(text=text)

        if metrics:
            allocation.setdefault("__mandate_metrics", {}).update(metrics)

        rows = [
            ParsedRow(
                row_number=i + 1,
                data={"asset_class": label, **(payload if isinstance(payload, dict) else {"value": payload})},
            )
            for i, (label, payload) in enumerate(allocation.items())
            if not str(label).startswith("__")
        ]

        status = ParserStatus.SUCCESS if allocation else ParserStatus.PARTIAL
        warnings: list[str] = []
        if not allocation:
            warnings.append("No se extrajo asset allocation en reporte BBH.")

        account_number = self._extract_account_number(text) or "Varios"
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
        if not isinstance(alloc, dict) or not alloc:
            errors.append("Asset allocation vacio en BBH report_mandato")
        return errors

    def detect(self, filepath: Path) -> float:
        if filepath.suffix.lower() not in self.SUPPORTED_EXTENSIONS:
            return 0.0
        name = filepath.name.lower()
        if "bbh" in name and ("report" in name or "investment review" in name):
            return 0.98
        if "boatview" in name and "investment review" in name:
            return 0.75
        return 0.35

    @staticmethod
    def _extract_pages(filepath: Path) -> list[str]:
        pages: list[str] = []
        with pdfplumber.open(str(filepath)) as pdf:
            for page in pdf.pages:
                pages.append(page.extract_text() or "")
        return pages

    @staticmethod
    def _extract_account_number(text: str) -> str | None:
        m = re.search(r"Account Number:\s*([A-Z0-9\-]+)", text, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip()
        m = re.search(r"MND[-\s]*([0-9]{4,7})", text, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip()
        return None

    def _extract_period_end(self, *, text: str, filename: str) -> date | None:
        m = re.search(
            r"as of\s+(\d{1,2})\s+([A-Za-z]+),\s*([12]\d{3})",
            text,
            flags=re.IGNORECASE,
        )
        if m:
            month = _MONTHS.get(m.group(2).strip().lower())
            if month:
                return date(int(m.group(3)), month, int(m.group(1)))

        m = re.search(r"\b([A-Za-z]+)\s+([12]\d{3})\b", filename, flags=re.IGNORECASE)
        if m:
            month = _MONTHS.get(m.group(1).strip().lower())
            if month:
                return _last_day(int(m.group(2)), month)

        m = re.search(r"^\D*([12]\d{3})[\s._-]+(0?[1-9]|1[0-2])\b", filename)
        if m:
            return _last_day(int(m.group(1)), int(m.group(2)))

        return None

    @staticmethod
    def _extract_fixed_income_metrics(*, text: str) -> dict[str, Any]:
        metrics: dict[str, Any] = {}
        ytm = re.search(r"Weighted Average YTM\s+([\d.]+)%", text, flags=re.IGNORECASE)
        if ytm:
            metrics["fixed_income_yield"] = {
                "value": _safe_float(ytm.group(1)),
                "unit": "%",
                "source": "bbh_report_weighted_average_ytm",
            }
        duration = re.search(r"Weighted Average Duration\s+([\d.]+)", text, flags=re.IGNORECASE)
        if duration:
            metrics["fixed_income_duration"] = {
                "value": _safe_float(duration.group(1)),
                "unit": "years",
                "source": "bbh_report_weighted_average_duration",
            }
        return metrics

    @staticmethod
    def _pct_from_parenthesized(text: str, label: str) -> float | None:
        pattern = rf"{label}\s*\(([\d.]+)%\)"
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if not m:
            return None
        return _safe_float(m.group(1))

    @staticmethod
    def _pct_from_table_row(text: str, label: str) -> float | None:
        pattern = rf"^\s*{label}\s+\$?[\d,]+(?:\.\d+)?\s+([\d.]+)%"
        m = re.search(pattern, text, flags=re.IGNORECASE | re.MULTILINE)
        if not m:
            return None
        return _safe_float(m.group(1))

    def _extract_allocation(self, *, text: str) -> dict[str, dict[str, Any]]:
        allocation: dict[str, dict[str, Any]] = {}

        # BBH deck style table.
        equity_pct = self._pct_from_table_row(text, "Equity")
        fixed_cash_pct = self._pct_from_table_row(text, r"Cash & Fixed Income")
        cash_pct = self._pct_from_table_row(text, "Cash")
        hy_pct = _safe_float(
            (re.search(r"% High Yield \(of total portfolio\)\s+([\d.]+)%", text, flags=re.IGNORECASE) or [None, None])[1]
        )

        us_large = self._pct_from_table_row(text, r"U\.S\. Large Cap")
        us_mid_small = self._pct_from_table_row(text, r"U\.S\. Mid/Small Cap")
        non_us_developed = self._pct_from_table_row(text, r"Non-U\.S\. Developed")
        emerging = self._pct_from_table_row(text, "Emerging Markets")
        global_eq = self._pct_from_table_row(text, "Global")

        # JPM-style investment review fallback.
        if equity_pct is None:
            equity_pct = self._pct_from_parenthesized(text, "Equity")
        if fixed_cash_pct is None:
            fixed_cash_pct = self._pct_from_parenthesized(text, "Fixed Income & Cash")
        if cash_pct is None:
            cash_pct = self._pct_from_parenthesized(text, "Cash")
        if us_large is None:
            us_large = self._pct_from_parenthesized(text, "US Large Cap Equity")
        euro_large = self._pct_from_parenthesized(text, "European Large Cap Equity")
        japan_large = self._pct_from_parenthesized(text, "Japanese Large Cap Equity")
        asia_ex_japan = self._pct_from_parenthesized(text, "Asia ex-Japan Equity")
        other_eq = self._pct_from_parenthesized(text, "Other Equity")

        fixed_income_pct = None
        if fixed_cash_pct is not None and cash_pct is not None:
            fixed_income_pct = max(fixed_cash_pct - cash_pct, 0.0)
        elif fixed_cash_pct is not None:
            fixed_income_pct = fixed_cash_pct

        if hy_pct is not None:
            allocation["High Yield Fixed Income"] = {"value": hy_pct, "unit": "%"}
        if fixed_income_pct is not None and hy_pct is not None:
            allocation["Investment Grade Fixed Income"] = {
                "value": max(fixed_income_pct - hy_pct, 0.0),
                "unit": "%",
            }

        us_eq = 0.0
        has_us = False
        if us_large is not None:
            us_eq += us_large
            has_us = True
        if us_mid_small is not None:
            us_eq += us_mid_small
            has_us = True
        if has_us:
            allocation["US Equities"] = {"value": us_eq, "unit": "%"}

        non_us_eq = 0.0
        has_non_us = False
        for pct in (non_us_developed, emerging, euro_large, japan_large, asia_ex_japan, other_eq):
            if pct is None:
                continue
            non_us_eq += pct
            has_non_us = True
        if has_non_us:
            allocation["Non US Equities"] = {"value": non_us_eq, "unit": "%"}
        if global_eq is not None:
            allocation["Global Equity"] = {"value": global_eq, "unit": "%"}

        return allocation
