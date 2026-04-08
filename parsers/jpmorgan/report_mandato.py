"""
JPMorgan mandate report parser (bank-isolated).
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
    "ene": 1,
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
    "dic": 12,
}

_OCR_READER = None
_OCR_AVAILABLE: bool | None = None


def _to_float(raw: str | None) -> float | None:
    if raw is None:
        return None
    cleaned = str(raw).replace(",", ".").replace("%", "").strip()
    cleaned = re.sub(r"[^0-9.\-]", "", cleaned)
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _last_day(year: int, month: int) -> date:
    return date(year, month, calendar.monthrange(year, month)[1])


class JPMorganMandateReportParser(BaseParser):
    BANK_CODE = "jpmorgan"
    ACCOUNT_TYPE = "report_mandato"
    VERSION = "1.3.1"
    DESCRIPTION = "Parser aislado JPMorgan para reportes de mandato"
    SUPPORTED_EXTENSIONS = [".pdf"]

    def parse(self, filepath: Path) -> ParseResult:
        source_hash = self.compute_file_hash(filepath)
        pages = self._extract_pages(filepath)
        text = "\n".join(pages)

        period_end = self._extract_period_end(text=text, filename=filepath.name)
        allocation = self._extract_allocation(text=text)
        if self._looks_like_complementario(text=text, filename=filepath.name):
            ocr_text = self._extract_complementario_ocr_text(filepath)
            if ocr_text:
                allocation.update(self._extract_allocation_from_complementario_ocr_text(text=ocr_text))
                metrics = self._extract_fixed_income_metrics_from_complementario_ocr_text(
                    text=ocr_text,
                    allocation=allocation,
                )
                if metrics:
                    allocation.setdefault("__mandate_metrics", {}).update(metrics)

        metrics = self._extract_fixed_income_metrics(text=text, allocation=allocation)
        if metrics:
            allocation.setdefault("__mandate_metrics", {}).update(metrics)

        status = ParserStatus.SUCCESS if allocation else ParserStatus.PARTIAL
        warnings: list[str] = []
        if not allocation:
            warnings.append("No se extrajo asset allocation en reporte JPM.")

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
        if not isinstance(alloc, dict) or not alloc:
            errors.append("Asset allocation vacio en JPM report_mandato")
        return errors

    def detect(self, filepath: Path) -> float:
        if filepath.suffix.lower() not in self.SUPPORTED_EXTENSIONS:
            return 0.0
        name = filepath.name.lower()
        if "jpm" in name and "complementario" in name:
            return 0.99
        if "investment review" in name or "mandato" in name:
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
        m = re.search(r"Account Number:\s*([A-Z0-9\-]+)", text, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip()
        m = re.search(r"MND[-\s]*([0-9]{4,7})", text, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip()
        return None

    @staticmethod
    def _looks_like_complementario(*, text: str, filename: str) -> bool:
        source = f"{filename}\n{text}".lower()
        return (
            "complementario" in source
            or "portfolio positioning" in source
            or "resultados a" in source
        )

    def _extract_period_end(self, *, text: str, filename: str) -> date | None:
        # "as of 28 February, 2026"
        m = re.search(r"as of\s+(\d{1,2})\s+([A-Za-z]+),\s*([12]\d{3})", text, flags=re.IGNORECASE)
        if m:
            month = _MONTHS.get(m.group(2).lower())
            if month:
                return date(int(m.group(3)), month, int(m.group(1)))

        # "Resultados a 31 de Enero de 2025"
        m = re.search(r"Resultados?\s+a\s+(\d{1,2})\s+de\s+([A-Za-z]+)\s+de\s+([12]\d{3})", text, flags=re.IGNORECASE)
        if m:
            month = _MONTHS.get(m.group(2).lower())
            if month:
                return date(int(m.group(3)), month, int(m.group(1)))

        # "02/28/2026" in message header.
        m = re.search(r"\b(0?[1-9]|1[0-2])[/-](0?[1-9]|[12]\d|3[01])[/-]([12]\d{3})\b", text)
        if m:
            month = int(m.group(1))
            day = int(m.group(2))
            year = int(m.group(3))
            try:
                return date(year, month, day)
            except ValueError:
                pass

        # Filename "2026 02 ..."
        m = re.search(r"^\D*([12]\d{3})[\s._-]+(0?[1-9]|1[0-2])\b", filename)
        if m:
            return _last_day(int(m.group(1)), int(m.group(2)))

        # Filename with month name.
        m = re.search(r"\b([A-Za-z]{3,9})\s+([12]\d{3})\b", filename, flags=re.IGNORECASE)
        if m:
            month = _MONTHS.get(m.group(1).lower())
            if month:
                return _last_day(int(m.group(2)), month)
        return None

    @staticmethod
    def _extract_weight(label: str, text: str) -> float | None:
        # Uses Boat column in "Bench Boat" rows (second percentage).
        pattern = rf"^\s*{label}\s+([0-9.,]+)%\s+([0-9.,]+)%"
        m = re.search(pattern, text, flags=re.IGNORECASE | re.MULTILINE)
        if not m:
            return None
        return _to_float(m.group(2))

    @staticmethod
    def _ocr_reader():
        global _OCR_READER, _OCR_AVAILABLE
        if _OCR_AVAILABLE is False:
            return None
        if _OCR_READER is not None:
            return _OCR_READER
        try:
            import easyocr
        except Exception:
            _OCR_AVAILABLE = False
            return None
        try:
            _OCR_READER = easyocr.Reader(["en"], gpu=False, verbose=False)
            _OCR_AVAILABLE = True
        except Exception:
            _OCR_AVAILABLE = False
            return None
        return _OCR_READER

    @classmethod
    def _extract_complementario_ocr_text(cls, filepath: Path) -> str | None:
        reader = cls._ocr_reader()
        if reader is None:
            return None
        try:
            import fitz
            import numpy as np
            from PIL import Image, ImageOps
        except Exception:
            return None

        doc = None
        try:
            doc = fitz.open(str(filepath))
            if len(doc) == 0:
                return None
            page = doc[0]
            # Focus OCR on the complementario metrics block only.
            clip = fitz.Rect(20, 150, 560, 560)
            pix = page.get_pixmap(dpi=220, alpha=False, clip=clip)
            image = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            gray = ImageOps.grayscale(image)
            ocr_items = reader.readtext(
                np.array(gray),
                detail=1,
                paragraph=False,
                allowlist="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789.%-() ",
            )

            grouped_lines: list[dict[str, Any]] = []
            for bbox, raw_text, _confidence in ocr_items:
                text = str(raw_text or "").strip()
                if not text:
                    continue
                ys = [point[1] for point in bbox]
                xs = [point[0] for point in bbox]
                item = {
                    "text": text,
                    "x": min(xs),
                    "y": sum(ys) / len(ys),
                }
                if not grouped_lines or abs(item["y"] - grouped_lines[-1]["y"]) > 18:
                    grouped_lines.append({"y": item["y"], "items": [item]})
                else:
                    grouped_lines[-1]["items"].append(item)

            lines: list[str] = []
            for group in grouped_lines:
                ordered = sorted(group["items"], key=lambda row: row["x"])
                line = " ".join(part["text"] for part in ordered).strip()
                if line:
                    lines.append(line)

            candidate = "\n".join(lines)
            return candidate or None
        except Exception:
            return None
        finally:
            if doc is not None:
                doc.close()

    @staticmethod
    def _ocr_lines(text: str) -> list[str]:
        return [re.sub(r"\s+", " ", line).strip() for line in str(text or "").splitlines() if str(line).strip()]

    @staticmethod
    def _normalize_ocr_numeric_token(raw: str) -> float | None:
        token = (
            str(raw or "")
            .strip()
            .upper()
            .replace("O", "0")
            .replace("Q", "0")
            .replace("I", "1")
            .replace("L", "1")
            .replace("S", "5")
        )
        token = re.sub(r"[^0-9.\-]", "", token)
        if not token or token in {"-", ".", "-."}:
            return None
        if token.count(".") > 1:
            first, *rest = token.split(".")
            token = first + "." + "".join(rest)
        if "." in token:
            whole, frac = token.split(".", 1)
            while len(frac) > 2 and frac.endswith(("9", "6")):
                frac = frac[:-1]
            token = f"{whole}.{frac}"
        else:
            while len(token) > 1:
                try:
                    numeric = abs(float(token))
                except ValueError:
                    return None
                if numeric <= 100 or not token.endswith(("9", "6")):
                    break
                token = token[:-1]
        try:
            return float(token)
        except ValueError:
            return None

    @classmethod
    def _extract_ocr_numeric_values(cls, line: str) -> list[float]:
        values: list[float] = []
        for raw in re.findall(r"-?[0-9OQILSPS&.%]+", str(line or ""), flags=re.IGNORECASE):
            value = cls._normalize_ocr_numeric_token(raw)
            if value is not None:
                values.append(value)
        return values

    @staticmethod
    def _extract_numeric_like_tokens(line: str) -> list[str]:
        return [
            raw
            for raw in re.findall(r"-?[0-9A-Z.%]+", str(line or "").upper())
            if re.search(r"\d", raw)
        ]

    @staticmethod
    def _normalize_ocr_percentage_token(raw: str, *, label: str | None = None) -> float | None:
        token = (
            str(raw or "")
            .strip()
            .upper()
            .replace("O", "0")
            .replace("Q", "0")
            .replace("I", "1")
            .replace("L", "1")
            .replace("S", "5")
        )
        token = re.sub(r"[^0-9.\-]", "", token)
        if not token or token in {"-", ".", "-."}:
            return None
        if "." in token:
            try:
                return float(token)
            except ValueError:
                return None

        negative = token.startswith("-")
        digits = token[1:] if negative else token
        digits = digits.lstrip("0") or "0"
        while len(digits) > 1 and int(digits) > 100:
            digits = digits[:-1]
        if str(label or "").strip().lower() == "cash" and len(digits) > 1 and int(digits) >= 10:
            digits = digits[:-1]
        try:
            value = float(digits)
            return -value if negative else value
        except ValueError:
            return None

    @staticmethod
    def _normalize_ocr_yield_token(raw: str) -> float | None:
        token = (
            str(raw or "")
            .strip()
            .upper()
            .replace("O", "0")
            .replace("Q", "0")
            .replace("I", "1")
            .replace("L", "1")
            .replace("S", "5")
        )
        token = re.sub(r"[^0-9.\-]", "", token)
        if not token or token in {"-", ".", "-."}:
            return None
        if "." in token:
            try:
                return float(token)
            except ValueError:
                return None

        negative = token.startswith("-")
        digits = token[1:] if negative else token
        digits = digits.lstrip("0") or "0"
        if len(digits) >= 3:
            normalized = f"{digits[0]}.{digits[1:3]}"
        else:
            normalized = digits
        try:
            value = float(normalized)
            return -value if negative else value
        except ValueError:
            return None

    @classmethod
    def _extract_allocation_from_complementario_ocr_text(cls, *, text: str) -> dict[str, dict[str, Any]]:
        alloc: dict[str, dict[str, Any]] = {}
        lines = cls._ocr_lines(text)
        in_positioning = False
        for line in lines:
            lower = line.lower()
            if "portfolio positioning" in lower:
                in_positioning = True
                continue
            if not in_positioning:
                continue
            if "duration" in lower:
                break

            for raw_label, bucket in (
                ("hg", "Investment Grade Fixed Income"),
                ("hy", "High Yield Fixed Income"),
            ):
                if not re.match(rf"^{raw_label}\b", lower):
                    continue
                values = [
                    cls._normalize_ocr_percentage_token(token, label=raw_label)
                    for token in cls._extract_numeric_like_tokens(line)
                ]
                values = [value for value in values if value is not None]
                if len(values) >= 2:
                    alloc[bucket] = {"value": values[1], "unit": "%"}

        if alloc:
            return alloc

        for index, line in enumerate(lines):
            if "portfolio positioning" not in line.lower():
                continue
            numeric_lines: list[list[float]] = []
            for candidate in lines[index + 1 : index + 6]:
                values = cls._extract_ocr_numeric_values(candidate)
                if len(values) >= 4:
                    numeric_lines.append(values[:4])
            if len(numeric_lines) < 2:
                continue
            boat = numeric_lines[1]
            alloc["Investment Grade Fixed Income"] = {"value": boat[1], "unit": "%"}
            alloc["High Yield Fixed Income"] = {"value": boat[2], "unit": "%"}
            return alloc
        return alloc

    @classmethod
    def _extract_fixed_income_metrics_from_complementario_ocr_text(
        cls,
        *,
        text: str,
        allocation: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        metrics: dict[str, Any] = {}
        lines = cls._ocr_lines(text)
        in_duration = False
        for line in lines:
            lower = line.lower()
            if not in_duration and "duration" in lower:
                in_duration = True
                continue
            if not in_duration:
                continue
            if "yield" in lower:
                break
            if "total duration blended" not in lower:
                continue

            values = [
                cls._normalize_ocr_numeric_token(token)
                for token in cls._extract_numeric_like_tokens(line)
            ]
            values = [value for value in values if value is not None]
            if len(values) >= 2:
                metrics["fixed_income_duration"] = {
                    "value": values[1],
                    "unit": "years",
                    "source": "jpm_complementario_ocr_total_duration_blended",
                }
                break

        if "fixed_income_duration" not in metrics:
            for index, line in enumerate(lines):
                if "duration" not in line.lower():
                    continue
                numeric_lines: list[list[float]] = []
                for candidate in lines[index + 1 : index + 6]:
                    values = cls._extract_ocr_numeric_values(candidate)
                    if len(values) >= 3:
                        numeric_lines.append(values)
                if len(numeric_lines) >= 2 and len(numeric_lines[1]) >= 3:
                    metrics["fixed_income_duration"] = {
                        "value": numeric_lines[1][2],
                        "unit": "years",
                        "source": "jpm_complementario_ocr_total_duration_blended",
                    }
                break

        hg_weight = allocation.get("Investment Grade Fixed Income", {}).get("value")
        hy_weight = allocation.get("High Yield Fixed Income", {}).get("value")
        hg_yield: float | None = None
        hy_yield: float | None = None
        in_yield = False
        for line in lines:
            lower = line.lower()
            if "yield" in lower:
                in_yield = True
                continue
            if not in_yield:
                continue
            if "source" in lower:
                break
            if re.match(r"^hg\b", lower):
                values = [
                    cls._normalize_ocr_yield_token(token)
                    for token in cls._extract_numeric_like_tokens(line)
                ]
                values = [value for value in values if value is not None]
                if len(values) >= 2:
                    hg_yield = values[1]
            elif re.match(r"^hy\b", lower):
                values = [
                    cls._normalize_ocr_yield_token(token)
                    for token in cls._extract_numeric_like_tokens(line)
                ]
                values = [value for value in values if value is not None]
                if len(values) >= 2:
                    hy_yield = values[1]

        if hg_yield is None or hy_yield is None:
            for index, line in enumerate(lines):
                if "yield" not in line.lower():
                    continue
                numeric_lines = []
                for candidate in lines[index + 1 : index + 5]:
                    values = cls._extract_ocr_numeric_values(candidate)
                    if len(values) >= 2:
                        numeric_lines.append(values[:2])
                if len(numeric_lines) >= 2:
                    hg_yield, hy_yield = numeric_lines[1]
                break

        if (
            isinstance(hg_weight, (int, float))
            and isinstance(hy_weight, (int, float))
            and isinstance(hg_yield, (int, float))
            and isinstance(hy_yield, (int, float))
        ):
            total = float(hg_weight) + float(hy_weight)
            if total > 0:
                metrics["fixed_income_yield"] = {
                    "value": ((float(hg_weight) * float(hg_yield)) + (float(hy_weight) * float(hy_yield))) / total,
                    "unit": "%",
                    "source": "jpm_complementario_ocr_weighted_hg_hy",
                }

        return metrics

    @staticmethod
    def _extract_allocation_from_investment_review(text: str) -> dict[str, dict[str, Any]]:
        alloc: dict[str, dict[str, Any]] = {}

        def pct(label: str) -> float | None:
            m = re.search(rf"{label}\s*\(([\d.]+)%\)", text, flags=re.IGNORECASE)
            if not m:
                return None
            return _to_float(m.group(1))

        us_eq = pct("US Large Cap Equity")
        eu_eq = pct("European Large Cap Equity")
        jp_eq = pct("Japanese Large Cap Equity")
        asia_eq = pct("Asia ex-Japan Equity")
        other_eq = pct("Other Equity")
        if us_eq is not None:
            alloc["US Equities"] = {"value": us_eq, "unit": "%"}

        non_us = 0.0
        has_non_us = False
        for value in (eu_eq, jp_eq, asia_eq, other_eq):
            if value is None:
                continue
            non_us += value
            has_non_us = True
        if has_non_us:
            alloc["Non US Equities"] = {"value": non_us, "unit": "%"}

        return alloc

    @staticmethod
    def _extract_allocation_from_investment_review_breakdown(text: str) -> dict[str, dict[str, Any]]:
        alloc: dict[str, dict[str, Any]] = {}
        block_match = re.search(
            r"Allocation Breakdown By Account Type(?:\s*\([^)]+\))?",
            text,
            flags=re.IGNORECASE,
        )
        if not block_match:
            return alloc

        block = text[block_match.end():]
        end_match = re.search(
            r"\n(?:Please see|Important Information)\b",
            block,
            flags=re.IGNORECASE,
        )
        if end_match:
            block = block[: end_match.start()]

        def pct(label: str) -> float | None:
            m = re.search(
                rf"^\s*{label}\s+([0-9.,]+)%(?:\s+([0-9.,]+)%)?\s*$",
                block,
                flags=re.IGNORECASE | re.MULTILINE,
            )
            if not m:
                return None
            # Prefer the right-most percentage (Asset Allocation column).
            return _to_float(m.group(2) or m.group(1))

        us_eq = pct("US Large Cap Equity")
        eu_eq = pct("European Large Cap Equity")
        jp_eq = pct("Japanese Large Cap Equity")
        asia_eq = pct(r"Asia ex[-\s]*Japan Equity")
        other_eq = pct("Other Equity")
        if us_eq is not None:
            alloc["US Equities"] = {"value": us_eq, "unit": "%"}

        non_us = 0.0
        has_non_us = False
        for value in (eu_eq, jp_eq, asia_eq, other_eq):
            if value is None:
                continue
            non_us += value
            has_non_us = True
        if has_non_us:
            alloc["Non US Equities"] = {"value": non_us, "unit": "%"}

        return alloc

    def _extract_allocation(self, *, text: str) -> dict[str, dict[str, Any]]:
        # Investment Review table is SSOT for percentages when present.
        table_alloc = self._extract_allocation_from_investment_review_breakdown(text)
        if table_alloc:
            return table_alloc

        alloc: dict[str, dict[str, Any]] = {}

        # Complementario style (EQ/HG/HY + Cash).
        hg = self._extract_weight("HG", text)
        hy = self._extract_weight("HY", text)
        cash = self._extract_weight("Cash", text)
        if cash is None and any(v is not None for v in (hg, hy)):
            # Legacy complementario layouts may show only one percentage for Cash.
            cash_m = re.search(
                r"^\s*Cash\s+([0-9.,]+)%\s*$",
                text,
                flags=re.IGNORECASE | re.MULTILINE,
            )
            cash = _to_float(cash_m.group(1)) if cash_m else None

        if hg is not None:
            alloc["Investment Grade Fixed Income"] = {"value": hg, "unit": "%"}
        if hy is not None:
            alloc["High Yield Fixed Income"] = {"value": hy, "unit": "%"}

        # Investment review style fallback.
        if not alloc:
            alloc = self._extract_allocation_from_investment_review(text)

        return alloc

    @staticmethod
    def _extract_fixed_income_metrics(
        *,
        text: str,
        allocation: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        metrics: dict[str, Any] = {}

        duration = re.search(r"Dura(?:ci[oó]n|cin)\s+Cartera\s+JPM\s+([0-9.,]+)", text, flags=re.IGNORECASE)
        if duration:
            metrics["fixed_income_duration"] = {
                "value": _to_float(duration.group(1)),
                "unit": "years",
                "source": "jpm_complementario_duration_cartera",
            }

        hg_yield = re.search(r"HG\s+JPM\s+([0-9.,]+)%?", text, flags=re.IGNORECASE)
        hy_yield = re.search(r"HY\s+JPM\s+([0-9.,]+)%?", text, flags=re.IGNORECASE)
        hg_weight = allocation.get("Investment Grade Fixed Income", {}).get("value")
        hy_weight = allocation.get("High Yield Fixed Income", {}).get("value")
        hg_y = _to_float(hg_yield.group(1)) if hg_yield else None
        hy_y = _to_float(hy_yield.group(1)) if hy_yield else None

        if hg_y is not None and hy_y is not None and isinstance(hg_weight, (int, float)) and isinstance(hy_weight, (int, float)):
            total = float(hg_weight) + float(hy_weight)
            if total > 0:
                weighted = (float(hg_weight) * hg_y + float(hy_weight) * hy_y) / total
                metrics["fixed_income_yield"] = {
                    "value": weighted,
                    "unit": "%",
                    "source": "jpm_complementario_weighted_hg_hy",
                }

        return metrics
