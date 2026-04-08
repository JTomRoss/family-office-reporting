"""
UBS Switzerland mandate report parser (bank-isolated).
"""

from __future__ import annotations

import calendar
import re
from datetime import date
from pathlib import Path
from typing import Any

import pdfplumber

from parsers.base import BaseParser, ParseResult, ParsedRow, ParserStatus


def _to_float(raw: str | None) -> float | None:
    if raw is None:
        return None
    cleaned = str(raw).replace(",", "").replace("%", "").strip()
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _last_day(year: int, month: int) -> date:
    return date(year, month, calendar.monthrange(year, month)[1])


class UBSMandateReportParser(BaseParser):
    BANK_CODE = "ubs"
    ACCOUNT_TYPE = "report_mandato"
    VERSION = "1.2.0"
    DESCRIPTION = "Parser aislado UBS Suiza para reportes de mandato"
    SUPPORTED_EXTENSIONS = [".pdf"]

    def parse(self, filepath: Path) -> ParseResult:
        source_hash = self.compute_file_hash(filepath)
        pages = self._extract_pages(filepath)
        text = "\n".join(pages)

        period_end = self._extract_period_end(text=text, filename=filepath.name)
        account_number = self._extract_account_number(text=text) or "Varios"
        allocation = self._extract_allocation(text=text)
        metrics = self._extract_fixed_income_metrics(text=text)
        if metrics:
            allocation.setdefault("__mandate_metrics", {}).update(metrics)

        status = ParserStatus.SUCCESS if allocation else ParserStatus.PARTIAL
        warnings: list[str] = []
        if not allocation:
            warnings.append("No se extrajo asset allocation en reporte UBS.")

        rows = [
            ParsedRow(
                row_number=i + 1,
                data={"asset_class": label, **(payload if isinstance(payload, dict) else {"value": payload})},
            )
            for i, (label, payload) in enumerate(allocation.items())
            if not str(label).startswith("__")
        ]

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
            errors.append("Asset allocation vacio en UBS report_mandato")
        return errors

    def detect(self, filepath: Path) -> float:
        if filepath.suffix.lower() not in self.SUPPORTED_EXTENSIONS:
            return 0.0
        name = filepath.name.lower()
        if "reporting" in name and ("ubs" in str(filepath).lower() or "560" in name):
            return 0.98
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
        # Typical header "560552, 206". Keep the unique 6-digit core for loader matching.
        m = re.search(r"\b(\d{6})\s*,\s*\d{3}\b", text)
        if m:
            return m.group(1)
        m = re.search(r"Account Number:\s*([A-Z0-9\-]+)", text, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip()
        return None

    def _extract_period_end(self, *, text: str, filename: str) -> date | None:
        m = re.search(r"as of\s+(\d{2})\.(\d{2})\.(\d{4})", text, flags=re.IGNORECASE)
        if m:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))

        m = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", filename)
        if m:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))

        m = re.search(r"^\D*([12]\d{3})[\s._-]+(0?[1-9]|1[0-2])\b", filename)
        if m:
            return _last_day(int(m.group(1)), int(m.group(2)))
        return None

    @staticmethod
    def _extract_pct_line(text: str, label: str) -> float | None:
        m = re.search(rf"{label}\s+[\d,]+\s+([\d.]+)%", text, flags=re.IGNORECASE)
        if not m:
            # Table without market value column: "Liquidity 1.94% 1.00% ..."
            m = re.search(rf"{label}\s+([\d.]+)%", text, flags=re.IGNORECASE)
        if not m:
            return None
        return _to_float(m.group(1))

    def _extract_allocation(self, *, text: str) -> dict[str, dict[str, Any]]:
        allocation: dict[str, dict[str, Any]] = {}

        # From "Assets by Categories against Benchmark UBS" / "Overview Asset Allocation".
        high_grade = self._extract_pct_line(text, r"High Grade Bonds")
        corporate = self._extract_pct_line(text, r"Corporate Bonds")
        high_yield = self._extract_pct_line(text, r"High Yield Bonds")
        eq_us = self._extract_pct_line(text, r"Equities US")
        eq_emu = self._extract_pct_line(text, r"Equities EMU")
        eq_emma = self._extract_pct_line(text, r"Equities EMMA")
        eq_uk = self._extract_pct_line(text, r"Equities UK")
        eq_japan = self._extract_pct_line(text, r"Equities Japan")
        eq_global = self._extract_pct_line(text, r"Equities Global")
        eq_switzerland = self._extract_pct_line(text, r"Equities Switzerland")

        ig = 0.0
        has_ig = False
        for value in (high_grade, corporate):
            if value is None:
                continue
            ig += value
            has_ig = True

        non_us_eq = 0.0
        has_non_us_eq = False
        for value in (eq_emu, eq_emma, eq_uk, eq_japan, eq_switzerland):
            if value is None:
                continue
            non_us_eq += value
            has_non_us_eq = True
        if has_ig:
            allocation["Investment Grade Fixed Income"] = {"value": ig, "unit": "%"}
        if high_yield is not None:
            allocation["High Yield Fixed Income"] = {"value": high_yield, "unit": "%"}
        if eq_us is not None:
            allocation["US Equities"] = {"value": eq_us, "unit": "%"}
        if has_non_us_eq:
            allocation["Non US Equities"] = {"value": non_us_eq, "unit": "%"}
        if eq_global is not None:
            allocation["Global Equity"] = {"value": eq_global, "unit": "%"}

        return allocation

    @staticmethod
    def _extract_fixed_income_metrics(*, text: str) -> dict[str, Any]:
        metrics: dict[str, Any] = {}
        # Section "Duration & Yield to Maturity - Direct Investments"
        block_match = re.search(
            r"Duration\s*&\s*Yield to Maturity\s*-\s*Direct Investments(?P<block>[\s\S]{0,500})",
            text,
            flags=re.IGNORECASE,
        )
        block = block_match.group("block") if block_match else text
        total_line = re.search(r"Total\s+([0-9.]+)\s+([0-9.]+)%", block, flags=re.IGNORECASE)
        if total_line:
            duration_val = _to_float(total_line.group(1))
            yield_val = _to_float(total_line.group(2))
            if duration_val is not None:
                metrics["fixed_income_duration"] = {
                    "value": duration_val,
                    "unit": "years",
                    "source": "ubs_direct_investments_duration",
                }
            if yield_val is not None:
                metrics["fixed_income_yield"] = {
                    "value": yield_val,
                    "unit": "%",
                    "source": "ubs_direct_investments_ytm",
                }
        return metrics
