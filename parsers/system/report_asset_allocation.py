"""
Parser: PDF Reporte de Asset Allocation (genérico).

Objetivo:
- Extraer distribución de asset allocation desde reportes PDF.
- Alimentar monthly_closings.asset_allocation_json vía DataLoadingService.
"""

from __future__ import annotations

import re
from datetime import date
from decimal import Decimal
from pathlib import Path

import pdfplumber

from parsers.base import BaseParser, ParseResult, ParsedRow, ParserStatus


class AssetAllocationReportParser(BaseParser):
    BANK_CODE = "system"
    ACCOUNT_TYPE = "report_asset_allocation"
    VERSION = "1.0.0"
    DESCRIPTION = "Parser genérico para reportes PDF de asset allocation"
    SUPPORTED_EXTENSIONS = [".pdf"]

    _DATE_PATTERNS = [
        re.compile(r"\b(\d{4})[-/](\d{2})[-/](\d{2})\b"),
        re.compile(r"\b(\d{2})[-/](\d{2})[-/](\d{4})\b"),
    ]
    _ACCOUNT_PATTERNS = [
        re.compile(r"account(?:\s*number)?\s*[:#]?\s*([A-Z0-9\-]+)", re.IGNORECASE),
        re.compile(r"cuenta\s*[:#]?\s*([A-Z0-9\-]+)", re.IGNORECASE),
    ]
    _CURRENCY_PATTERN = re.compile(r"\b(USD|CLP|EUR|CHF|GBP)\b")
    _ALLOC_PATTERN = re.compile(
        r"^\s*([A-Za-z][A-Za-z0-9 ,\-/&\(\)\.]+?)\s+([0-9][0-9\.,]*)\s*(%|USD|CLP|EUR|CHF|GBP)?\s*$"
    )

    def parse(self, filepath: Path) -> ParseResult:
        file_hash = self.compute_file_hash(filepath)
        text = self._extract_text(filepath)
        if not text:
            return ParseResult(
                status=ParserStatus.ERROR,
                parser_name=self.get_parser_name(),
                parser_version=self.VERSION,
                source_file_hash=file_hash,
                errors=["No se pudo extraer texto del PDF"],
            )

        statement_date = self._extract_date(text)
        account_number = self._extract_account_number(text)
        currency = self._extract_currency(text)
        allocation = self._extract_asset_allocation(text)

        rows = []
        for idx, (name, payload) in enumerate(allocation.items(), start=1):
            rows.append(
                ParsedRow(
                    row_number=idx,
                    data={
                        "asset_class": name,
                        "value": str(payload.get("value")) if payload.get("value") is not None else None,
                        "unit": payload.get("unit"),
                    },
                )
            )

        status = ParserStatus.SUCCESS if allocation else ParserStatus.PARTIAL
        warnings = [] if allocation else ["No se detectaron líneas de asset allocation"]
        return ParseResult(
            status=status,
            parser_name=self.get_parser_name(),
            parser_version=self.VERSION,
            source_file_hash=file_hash,
            account_number=account_number,
            statement_date=statement_date,
            period_end=statement_date,
            currency=currency or "USD",
            rows=rows,
            qualitative_data={"asset_allocation": allocation},
            warnings=warnings,
            raw_text_preview=text[:1000],
        )

    def validate(self, result: ParseResult) -> list[str]:
        errors: list[str] = []
        alloc = result.qualitative_data.get("asset_allocation")
        if not isinstance(alloc, dict) or not alloc:
            errors.append("Asset allocation vacío")
        return errors

    def detect(self, filepath: Path) -> float:
        if filepath.suffix.lower() not in self.SUPPORTED_EXTENSIONS:
            return 0.0
        name = filepath.stem.lower()
        if "allocation" in name or "asset" in name or "reporte" in name or "report" in name:
            return 0.85
        try:
            text = self._extract_text(filepath, max_pages=2).lower()
            if "asset allocation" in text or "allocation" in text:
                return 0.7
        except Exception:
            return 0.1
        return 0.1

    @staticmethod
    def _extract_text(filepath: Path, max_pages: int | None = None) -> str:
        chunks: list[str] = []
        with pdfplumber.open(str(filepath)) as pdf:
            pages = pdf.pages if max_pages is None else pdf.pages[:max_pages]
            for page in pages:
                chunks.append(page.extract_text() or "")
        return "\n".join(chunks)

    def _extract_date(self, text: str) -> date | None:
        for pat in self._DATE_PATTERNS:
            m = pat.search(text)
            if not m:
                continue
            a, b, c = m.groups()
            try:
                if len(a) == 4:
                    return date(int(a), int(b), int(c))
                return date(int(c), int(b), int(a))
            except ValueError:
                continue
        return None

    def _extract_account_number(self, text: str) -> str | None:
        for pat in self._ACCOUNT_PATTERNS:
            m = pat.search(text)
            if m:
                return m.group(1).strip()
        return None

    def _extract_currency(self, text: str) -> str | None:
        m = self._CURRENCY_PATTERN.search(text)
        return m.group(1).upper() if m else None

    def _extract_asset_allocation(self, text: str) -> dict[str, dict]:
        allocation: dict[str, dict] = {}
        for line in text.splitlines():
            match = self._ALLOC_PATTERN.match(line.strip())
            if not match:
                continue
            name, raw_val, unit = match.groups()
            # Evitar capturar encabezados genéricos
            if name.lower() in {"asset allocation", "allocation", "total"}:
                continue
            normalized = raw_val.replace(",", "")
            try:
                value = Decimal(normalized)
            except Exception:
                continue
            label = " ".join(name.split())
            allocation[label] = {
                "value": float(value),
                "unit": unit or "USD",
            }
        return allocation

