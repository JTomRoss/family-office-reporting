"""
Goldman Sachs mandate report parser (bank-isolated).
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


def _to_float(raw: str | None) -> float | None:
    if not raw:
        return None
    cleaned = str(raw).replace(",", "").replace("%", "").replace("$", "").strip()
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _last_day(year: int, month: int) -> date:
    return date(year, month, calendar.monthrange(year, month)[1])


class GoldmanSachsMandateReportParser(BaseParser):
    BANK_CODE = "goldman_sachs"
    ACCOUNT_TYPE = "report_mandato"
    VERSION = "1.1.1"
    DESCRIPTION = "Parser aislado Goldman Sachs para reportes de mandato"
    SUPPORTED_EXTENSIONS = [".pdf"]

    def parse(self, filepath: Path) -> ParseResult:
        source_hash = self.compute_file_hash(filepath)
        text = self._extract_text(filepath)

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
            warnings.append("No se extrajo asset allocation en reporte GS.")

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
            errors.append("Asset allocation vacio en Goldman report_mandato")
        return errors

    def detect(self, filepath: Path) -> float:
        if filepath.suffix.lower() not in self.SUPPORTED_EXTENSIONS:
            return 0.0
        name = filepath.name.lower()
        if "gs" in name and ("boatview" in name or "telmar" in name):
            return 0.98
        if "goldman" in name:
            return 0.9
        return 0.35

    @staticmethod
    def _extract_text(filepath: Path) -> str:
        pages: list[str] = []
        with pdfplumber.open(str(filepath)) as pdf:
            for page in pdf.pages:
                pages.append(page.extract_text() or "")
        return "\n".join(pages)

    @staticmethod
    def _extract_account_number(text: str) -> str | None:
        m = re.search(r"Account Number:\s*([A-Z0-9\-]+)", text, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip()
        m = re.search(r"Telmar & Boatview.*?X{3,}([0-9]{4})", text, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip()
        return None

    def _extract_period_end(self, *, text: str, filename: str) -> date | None:
        m = re.search(r"As of\s+([A-Za-z]{3,9})\s+(\d{1,2}),\s*([12]\d{3})", text, flags=re.IGNORECASE)
        if m:
            month = _MONTHS.get(m.group(1).lower())
            if month:
                return date(int(m.group(3)), month, int(m.group(2)))

        m = re.search(r"\b([A-Za-z]{3,9})\s+([12]\d{3})\b", filename, flags=re.IGNORECASE)
        if m:
            month = _MONTHS.get(m.group(1).lower())
            if month:
                return _last_day(int(m.group(2)), month)

        m = re.search(r"^\D*([12]\d{3})[\s._-]+(0?[1-9]|1[0-2])\b", filename)
        if m:
            return _last_day(int(m.group(1)), int(m.group(2)))
        return None

    @staticmethod
    def _extract_labeled_amount(text: str, label: str) -> float | None:
        pattern = rf"{label}\s+\$?([\d,]+(?:\.\d+)?)\s+([\d.]+)%"
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if not m:
            return None
        return _to_float(m.group(1))

    @staticmethod
    def _extract_labeled_percent(text: str, label: str) -> float | None:
        pattern = rf"{label}\s+\$?[\d,]+(?:\.\d+)?\s+([\d.]+)%"
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if not m:
            return None
        return _to_float(m.group(1))

    def _extract_fixed_income_metrics(self, *, text: str) -> dict[str, Any]:
        metrics: dict[str, Any] = {}
        ytm = re.search(r"Market Yield To Worst\s+([\d.]+)%", text, flags=re.IGNORECASE)
        if ytm:
            metrics["fixed_income_yield"] = {
                "value": _to_float(ytm.group(1)),
                "unit": "%",
                "source": "gs_fixed_income_overview_market_ytw",
            }
        duration = re.search(r"Option Adjusted Duration\s+([\d.]+)\s+years", text, flags=re.IGNORECASE)
        if duration:
            metrics["fixed_income_duration"] = {
                "value": _to_float(duration.group(1)),
                "unit": "years",
                "source": "gs_fixed_income_overview_oas_duration",
            }
        return metrics

    def _extract_allocation(self, *, text: str) -> dict[str, dict[str, Any]]:
        allocation: dict[str, dict[str, Any]] = {}

        def _subasset_payload(percent: float | None, amount: float | None) -> dict[str, Any] | None:
            if percent is not None:
                return {"value": percent, "unit": "%"}
            if amount is not None:
                return {"value": amount}
            return None

        ig_pct = self._extract_labeled_percent(text, r"Investment Grade Fixed Income")
        hy_pct = self._extract_labeled_percent(text, r"Other Fixed Income")
        us_eq_pct = self._extract_labeled_percent(text, r"US Equity")
        non_us_pct = self._extract_labeled_percent(text, r"Non-US Equity")
        global_pct = self._extract_labeled_percent(text, r"Global Equity")

        ig_amount = self._extract_labeled_amount(text, r"Investment Grade Fixed Income")
        other_fi_amount = self._extract_labeled_amount(text, r"Other Fixed Income")
        us_eq_amount = self._extract_labeled_amount(text, r"US Equity")
        non_us_eq_amount = self._extract_labeled_amount(text, r"Non-US Equity")
        global_eq_amount = self._extract_labeled_amount(text, r"Global Equity")

        ig_payload = _subasset_payload(ig_pct, ig_amount)
        if ig_payload is not None:
            allocation["Investment Grade Fixed Income"] = ig_payload

        hy_payload = _subasset_payload(hy_pct, other_fi_amount)
        if hy_payload is not None:
            allocation["High Yield Fixed Income"] = hy_payload

        us_payload = _subasset_payload(us_eq_pct, us_eq_amount)
        if us_payload is not None:
            allocation["US Equities"] = us_payload

        non_us_payload = _subasset_payload(non_us_pct, non_us_eq_amount)
        if non_us_payload is not None:
            allocation["Non US Equities"] = non_us_payload

        global_payload = _subasset_payload(global_pct, global_eq_amount)
        if global_payload is not None:
            allocation["Global Equity"] = global_payload

        return allocation
