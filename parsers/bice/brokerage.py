"""
Parser: BICE – BICE Inversiones Corredores de Bolsa (Cartola PDF).

Formato chileno: puntos=miles, coma=decimales.
Página 1: Resumen inversiones ($ y US$), Patrimonio
Página 2: Detalle inversiones en $ (CLP) – table-based
Página 3: Detalle inversiones en US$ – table-based
Página 4: Detalle carteras (acciones, fondos mutuos)
Páginas 5-6: Detalle movimientos
Página 7: Glosario

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

def _parse_clp(text: str) -> Optional[Decimal]:
    """Parse Chilean peso: '3.960.449.291' → 3960449291."""
    if not text or text.strip() in ("", "0", "N/A", "--"):
        return None
    s = text.strip().replace("$", "").strip()
    negative = s.startswith("-")
    if negative:
        s = s[1:]
    # Chilean: dots are thousands, comma is decimal
    s = s.replace(".", "").replace(",", ".")
    try:
        val = Decimal(s)
        return -val if negative else val
    except (InvalidOperation, ValueError):
        return None


def _parse_usd_cl(text: str) -> Optional[Decimal]:
    """Parse USD in Chilean format: '5.280.912,75' → 5280912.75."""
    if not text or text.strip() in ("", "0,00", "0", "N/A", "--"):
        return None
    s = text.strip().replace("US$", "").replace("$", "").strip()
    negative = s.startswith("-")
    if negative:
        s = s[1:]
    s = s.replace(".", "").replace(",", ".")
    try:
        val = Decimal(s)
        return -val if negative else val
    except (InvalidOperation, ValueError):
        return None


def _parse_date_cl(text: str) -> Optional[date]:
    """Parse '01-12-2025' or '31-12-2025' (DD-MM-YYYY)."""
    m = re.search(r"(\d{2})-(\d{2})-(\d{4})", text)
    if not m:
        return None
    try:
        return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    except ValueError:
        return None


def _safe_cell(row: list, idx: int) -> str:
    """Safely get cell value from table row."""
    if idx < len(row) and row[idx] is not None:
        return str(row[idx]).strip()
    return ""


# ── Parser ───────────────────────────────────────────────────────

class BICEBrokerageParser(BaseParser):
    BANK_CODE = "bice"
    ACCOUNT_TYPE = "brokerage"
    VERSION = "2.0.0"
    DESCRIPTION = "Parser para cartolas BICE Inversiones (formato chileno, CLP/USD)"
    SUPPORTED_EXTENSIONS = [".pdf"]

    _DETECTION_MARKERS = [
        "BICE Inversiones",
        "BICE",
        "Corredores de Bolsa",
    ]

    def parse(self, filepath: Path) -> ParseResult:
        file_hash = self.compute_file_hash(filepath)

        result = ParseResult(
            status=ParserStatus.SUCCESS,
            parser_name=self.get_parser_name(),
            parser_version=self.VERSION,
            source_file_hash=file_hash,
            bank_code=self.BANK_CODE,
            currency="CLP",  # Primary currency is CLP
        )

        try:
            with pdfplumber.open(filepath) as pdf:
                pages_text: list[str] = []
                pages_tables: list[list] = []
                for page in pdf.pages:
                    pages_text.append(page.extract_text() or "")
                    pages_tables.append(page.extract_tables() or [])
        except Exception as e:
            result.status = ParserStatus.ERROR
            result.errors.append(f"Error abriendo PDF: {e}")
            return result

        if not pages_text:
            result.status = ParserStatus.ERROR
            result.errors.append("PDF vacío o ilegible")
            return result

        result.raw_text_preview = pages_text[0][:500]

        self._extract_header(pages_text, result)
        self._extract_summary(pages_text, pages_tables, result)
        self._extract_holdings_clp(pages_tables, result)
        self._extract_holdings_usd(pages_tables, result)
        self._extract_portfolio_detail(pages_tables, result)

        return result

    def _extract_header(self, pages: list[str], result: ParseResult) -> None:
        """Extract client info, period, RUT."""
        text = pages[0] if pages else ""

        # RUT
        m = re.search(r"Rut:\s*([\d.]+-[\dkK])", text)
        if m:
            result.qualitative_data["rut"] = m.group(1)

        # Client name
        m = re.search(r"Cliente:\s*(.+?)(?:\s+Rut:)", text)
        if m:
            result.qualitative_data["client_name"] = m.group(1).strip()

        # Period: "01-12-2025 al 31-12-2025"
        m = re.search(r"Per[ií]odo:\s*(\d{2}-\d{2}-\d{4})\s+al\s+(\d{2}-\d{2}-\d{4})", text)
        if m:
            result.period_start = _parse_date_cl(m.group(1))
            result.period_end = _parse_date_cl(m.group(2))
            result.statement_date = result.period_end

        # Account number - use RUT as account identifier for BICE
        if "rut" in result.qualitative_data:
            result.account_number = result.qualitative_data["rut"]

    def _extract_summary(
        self, pages: list[str], pages_tables: list[list], result: ParseResult
    ) -> None:
        """Page 1: Summary table with Activos/Patrimonio."""
        text = pages[0] if pages else ""

        # Try table extraction first (TABLE 4 from analysis)
        if pages_tables and pages_tables[0]:
            for table in pages_tables[0]:
                for row in table:
                    if not row:
                        continue
                    label = _safe_cell(row, 0) or _safe_cell(row, 2)
                    label_lower = label.lower()

                    if "patrimonio" in label_lower and len(row) >= 4:
                        clp_val = _parse_clp(_safe_cell(row, 1) or _safe_cell(row, 3))
                        usd_val = _parse_usd_cl(_safe_cell(row, 2) or _safe_cell(row, 4))
                        if clp_val:
                            result.closing_balance = clp_val
                        result.qualitative_data["patrimonio_clp"] = str(clp_val) if clp_val else None
                        result.qualitative_data["patrimonio_usd"] = str(usd_val) if usd_val else None

                    elif "total activos" in label_lower and len(row) >= 4:
                        clp_val = _parse_clp(_safe_cell(row, 1) or _safe_cell(row, 3))
                        usd_val = _parse_usd_cl(_safe_cell(row, 2) or _safe_cell(row, 4))
                        result.qualitative_data["total_activos_clp"] = str(clp_val) if clp_val else None
                        result.qualitative_data["total_activos_usd"] = str(usd_val) if usd_val else None

        # Fallback: text-based extraction
        if not result.closing_balance:
            m = re.search(r"Patrimonio\s+([\d.]+)\s+([\d.,]+)", text)
            if m:
                result.closing_balance = _parse_clp(m.group(1))
                result.qualitative_data["patrimonio_usd"] = str(_parse_usd_cl(m.group(2)))

        # Asset breakdown from summary
        assets_summary: dict[str, dict] = {}
        for m in re.finditer(
            r"(Acciones|Fondos Mutuos|Renta Fija|Disponible en Caja|Dep[oó]sitos a Plazo)\s+"
            r"([\d.]+)\s+([\d.,]+)\s+(\d+)",
            text,
        ):
            assets_summary[m.group(1)] = {
                "clp": str(_parse_clp(m.group(2))),
                "usd": str(_parse_usd_cl(m.group(3))),
            }
        if assets_summary:
            result.qualitative_data["asset_summary"] = assets_summary

        # Investor profile
        m = re.search(r"Perfil de Inversionista:\s*(\w+)", text)
        if m:
            result.qualitative_data["investor_profile"] = m.group(1)

    def _extract_holdings_clp(self, pages_tables: list[list], result: ParseResult) -> None:
        """Page 2: CLP holdings from tables."""
        if len(pages_tables) < 2:
            return

        for table in pages_tables[1]:
            if not table or len(table) < 3:
                continue

            # Check if this is a CLP investments table
            header_text = " ".join(str(c) for c in table[0] if c) if table[0] else ""
            if "inversiones en $" not in header_text.lower() and "inversiones en" not in header_text.lower():
                continue

            for row in table[3:]:  # Skip header rows
                if not row or len(row) < 7:
                    continue
                instrument = _safe_cell(row, 0)
                if not instrument or instrument.lower() in ("", "patrimonio"):
                    continue
                if "total" in instrument.lower() or "subtotal" in instrument.lower():
                    continue

                opening = _parse_clp(_safe_cell(row, 1))
                purchases = _parse_clp(_safe_cell(row, 2))
                sales = _parse_clp(_safe_cell(row, 3))
                change = _parse_clp(_safe_cell(row, 4))
                closing = _parse_clp(_safe_cell(row, 5))
                pct = _safe_cell(row, 6)

                result.rows.append(ParsedRow(
                    data={
                        "instrument": instrument,
                        "currency": "CLP",
                        "opening_value": str(opening) if opening else None,
                        "purchases": str(purchases) if purchases else None,
                        "sales": str(sales) if sales else None,
                        "change_in_value": str(change) if change else None,
                        "closing_value": str(closing) if closing else None,
                        "pct_of_portfolio": pct,
                        "section": "clp_investments",
                    },
                    row_number=2,
                    confidence=0.9,
                ))

    def _extract_holdings_usd(self, pages_tables: list[list], result: ParseResult) -> None:
        """Page 3: USD holdings from tables."""
        if len(pages_tables) < 3:
            return

        for table in pages_tables[2]:
            if not table or len(table) < 3:
                continue

            header_text = " ".join(str(c) for c in table[0] if c) if table[0] else ""
            if "us$" not in header_text.lower() and "usd" not in header_text.lower():
                continue

            for row in table[3:]:
                if not row or len(row) < 7:
                    continue
                instrument = _safe_cell(row, 0)
                if not instrument or instrument.lower() in ("", "patrimonio"):
                    continue
                if "total" in instrument.lower() or "subtotal" in instrument.lower():
                    continue

                opening = _parse_usd_cl(_safe_cell(row, 1))
                purchases = _parse_usd_cl(_safe_cell(row, 2))
                sales = _parse_usd_cl(_safe_cell(row, 3))
                change = _parse_usd_cl(_safe_cell(row, 4))
                closing = _parse_usd_cl(_safe_cell(row, 5))
                pct = _safe_cell(row, 6)

                result.rows.append(ParsedRow(
                    data={
                        "instrument": instrument,
                        "currency": "USD",
                        "opening_value": str(opening) if opening else None,
                        "purchases": str(purchases) if purchases else None,
                        "sales": str(sales) if sales else None,
                        "change_in_value": str(change) if change else None,
                        "closing_value": str(closing) if closing else None,
                        "pct_of_portfolio": pct,
                        "section": "usd_investments",
                    },
                    row_number=3,
                    confidence=0.9,
                ))

    def _extract_portfolio_detail(self, pages_tables: list[list], result: ParseResult) -> None:
        """Page 4: Portfolio detail (stocks, mutual funds)."""
        if len(pages_tables) < 4:
            return

        for table in pages_tables[3]:
            if not table or len(table) < 3:
                continue

            header_text = " ".join(str(c) for c in table[0] if c) if table[0] else ""

            # Renta Variable (stocks)
            if "renta variable" in header_text.lower():
                for row in table[3:]:
                    if not row or len(row) < 8:
                        continue
                    instrument = _safe_cell(row, 0)
                    if not instrument or "subtotal" in instrument.lower():
                        continue

                    quantity = _parse_clp(_safe_cell(row, 2))     # Libre
                    buy_price = _parse_usd_cl(_safe_cell(row, 5))  # Precio Compra
                    last_price = _parse_usd_cl(_safe_cell(row, 6))  # Último
                    market_value = _parse_clp(_safe_cell(row, 8))   # Valor de Mercado

                    result.rows.append(ParsedRow(
                        data={
                            "instrument": instrument,
                            "asset_type": "equity",
                            "currency": "CLP",
                            "quantity": str(quantity) if quantity else None,
                            "purchase_price": str(buy_price) if buy_price else None,
                            "last_price": str(last_price) if last_price else None,
                            "market_value": str(market_value) if market_value else None,
                            "section": "renta_variable",
                        },
                        row_number=4,
                        confidence=0.85,
                    ))

            # Fondos Mutuos
            if "fondos mutuos" in header_text.lower():
                is_usd = "us$" in header_text.lower()
                for row in table[3:]:
                    if not row or len(row) < 6:
                        continue
                    instrument = _safe_cell(row, 0)
                    if not instrument or "subtotal" in instrument.lower():
                        continue

                    nav = _parse_usd_cl(_safe_cell(row, 3))  # Valor Cuota
                    shares = _parse_usd_cl(_safe_cell(row, 4))  # Cuotas Libres
                    balance = _parse_usd_cl(_safe_cell(row, 6)) if is_usd else _parse_clp(_safe_cell(row, 6))

                    result.rows.append(ParsedRow(
                        data={
                            "instrument": instrument,
                            "asset_type": "mutual_fund",
                            "currency": "USD" if is_usd else "CLP",
                            "nav_per_share": str(nav) if nav else None,
                            "shares": str(shares) if shares else None,
                            "market_value": str(balance) if balance else None,
                            "section": "fondos_mutuos_usd" if is_usd else "fondos_mutuos_clp",
                        },
                        row_number=4,
                        confidence=0.85,
                    ))

    def validate(self, result: ParseResult) -> list[str]:
        errors = []
        if result.closing_balance is not None and result.closing_balance <= Decimal("0"):
            errors.append(f"Patrimonio sospechoso: {result.closing_balance}")
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
                        score += 0.3
                if "bice" in filepath.name.lower():
                    score += 0.2
                if "rut:" in text.lower() or "cartola" in text.lower():
                    score += 0.1
                return min(score, 1.0)
        except Exception:
            return 0.0
