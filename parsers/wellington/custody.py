"""
Parser: Wellington Management – Client Statement (PDF multi-fondo).

Formato: Un PDF con N páginas, cada página es un fondo distinto del mismo
cliente (ej. Boatview Limited, cuenta 576371). Cada página tiene una tabla
de transacciones con una fila "Closing Balance" cuya última columna (Balance)
contiene el valor USD del fondo al cierre del período.

Lógica de extracción:
- Recorre todas las páginas del PDF.
- En cada página busca la línea "Closing Balance" en el texto extraído.
- Suma todos los valores Balance de esas líneas → valor total de la cartola.

Nota de implementación: pdfplumber no extrae tablas de este formato PDF
(retorna lista vacía). Toda la extracción es texto plano vía regex.
Los labels en el PDF aparecen sin espacios internos ("AccountNumber:",
"ClientName:", "StatementPeriod:") debido a cómo el PDF embebe las fuentes.

Identificadores de detección:
- Encabezado: "Client Statement"
- Emisor: "Wellington Management Funds"
- Administrador: "Wellington Global TA" / State Street

AISLADO: No comparte lógica con ningún otro parser.
"""

from __future__ import annotations

import re
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Optional

import pdfplumber

from parsers.base import BaseParser, ParseResult, ParsedRow, ParserStatus


# ── Helpers locales ──────────────────────────────────────────────────────────

def _parse_usd(text: str) -> Optional[Decimal]:
    """
    Parsea un número USD con formato anglosajón: '21,233,634.72' → Decimal.
    Ignora strings vacíos o puramente cero.
    """
    if not text:
        return None
    s = text.strip().replace("$", "").replace(",", "").strip()
    if not s or s in ("-", "0.00", "0"):
        return None
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def _parse_date_wellington(text: str) -> Optional[date]:
    """Parsea '28-Feb-2022' → date(2022, 2, 28)."""
    m = re.search(r"(\d{1,2})-([A-Za-z]{3})-(\d{4})", text)
    if not m:
        return None
    months = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    }
    month = months.get(m.group(2).lower())
    if not month:
        return None
    try:
        return date(int(m.group(3)), month, int(m.group(1)))
    except ValueError:
        return None


# ── Parser ───────────────────────────────────────────────────────────────────

class WellingtonCustodyParser(BaseParser):
    BANK_CODE = "wellington"
    ACCOUNT_TYPE = "custody"
    VERSION = "1.0.0"
    DESCRIPTION = (
        "Parser para cartolas Wellington Management Funds – Client Statement "
        "(multi-fondo, suma Closing Balance de todas las páginas, USD)"
    )
    SUPPORTED_EXTENSIONS = [".pdf"]

    # Marcadores de detección.
    # pdfplumber colapsa espacios en este PDF, por lo que los labels aparecen
    # sin espacios internos: "ClientStatement", "WellingtonManagementFunds".
    # La comparación se hace contra el texto normalizado (sin espacios).
    _MARKERS_REQUIRED = [
        "clientstatement",           # "Client Statement"
        "wellingtonmanagementfunds", # "Wellington Management Funds"
    ]
    _MARKERS_SUPPORTING = [
        "wellingtonglobalta",  # "Wellington Global TA"
        "statestreet",         # "State Street"
    ]

    # Patrón para la línea de Closing Balance.
    # Texto real: "28-Feb-2022 Closing Balance 9.8706 2,151,200.000 21,233,634.72"
    # El `\s` antes del grupo capturado obliga al backtracking greedy a anclar
    # sobre el espacio que precede al número completo, evitando que capture solo
    # los últimos dígitos (ej. "4.72" en lugar de "21,233,634.72").
    _RE_CLOSING = re.compile(
        r"Closing Balance[^\n]*\s([\d,]+\.\d{2})\s*$",
        re.MULTILINE,
    )

    def parse(self, filepath: Path) -> ParseResult:
        file_hash = self.compute_file_hash(filepath)

        result = ParseResult(
            status=ParserStatus.SUCCESS,
            parser_name=self.get_parser_name(),
            parser_version=self.VERSION,
            source_file_hash=file_hash,
            bank_code=self.BANK_CODE,
            currency="USD",
        )

        try:
            with pdfplumber.open(filepath) as pdf:
                pages = pdf.pages
                if not pages:
                    result.status = ParserStatus.ERROR
                    result.errors.append("PDF vacío o ilegible")
                    return result

                first_text = pages[0].extract_text() or ""
                result.raw_text_preview = first_text[:500]

                # Metadatos del encabezado (página 1)
                self._extract_header(first_text, result)

                # Recorrer todas las páginas y sumar Closing Balances
                total = Decimal("0")
                found_any = False

                for page_num, page in enumerate(pages, start=1):
                    text = page.extract_text() or ""
                    closing_val = self._extract_closing_balance(text)
                    fund_name = self._extract_fund_name(text)

                    if closing_val is not None:
                        total += closing_val
                        found_any = True
                        result.rows.append(ParsedRow(
                            data={
                                "fund_name": fund_name,
                                "closing_balance_usd": str(closing_val),
                                "page": page_num,
                            },
                            row_number=page_num,
                            confidence=0.95,
                        ))
                    else:
                        result.warnings.append(
                            f"Pág {page_num}: no se encontró Closing Balance"
                            + (f" ({fund_name})" if fund_name else "")
                        )

                if not found_any:
                    result.status = ParserStatus.ERROR
                    result.errors.append(
                        "No se encontró ninguna fila 'Closing Balance' en el PDF"
                    )
                    return result

                result.closing_balance = total
                result.qualitative_data["fund_count"] = len(result.rows)
                result.qualitative_data["total_closing_balance_usd"] = str(total)

        except Exception as e:
            result.status = ParserStatus.ERROR
            result.errors.append(f"Error procesando PDF: {e}")

        return result

    def _extract_header(self, text: str, result: ParseResult) -> None:
        """
        Extrae número de cuenta, período y cliente de la primera página.

        Labels en el texto real (sin espacios): "AccountNumber:", "ClientName:",
        "StatementPeriod:", "Fund Name:" (este último sí tiene espacio).
        """
        # Número de cuenta: "AccountNumber: 576371"
        m = re.search(r"AccountNumber[:\s]+(\d+)", text)
        if m:
            result.account_number = m.group(1).strip()

        # Nombre del cliente: "ClientName: BoatviewLimited StatementPeriod:"
        # Tomar lo que hay entre "ClientName:" y "StatementPeriod:"
        m = re.search(r"ClientName[:\s]+(.+?)\s+StatementPeriod", text)
        if m:
            result.qualitative_data["client_name"] = m.group(1).strip()

        # Período: "StatementPeriod: 01-Feb-2022to28-Feb-2022"
        # Dos fechas separadas por "to" sin espacios
        m = re.search(
            r"StatementPeriod[:\s]+"
            r"(\d{1,2}-[A-Za-z]{3}-\d{4})"
            r"to"
            r"(\d{1,2}-[A-Za-z]{3}-\d{4})",
            text,
        )
        if m:
            result.period_start = _parse_date_wellington(m.group(1))
            result.period_end = _parse_date_wellington(m.group(2))
            result.statement_date = result.period_end

    def _extract_closing_balance(self, text: str) -> Optional[Decimal]:
        """
        Extrae el valor de Closing Balance de una página vía texto plano.

        Texto real: "28-Feb-2022 Closing Balance 9.8706 2,151,200.000 21,233,634.72"
        Captura el último número con 2 decimales al final de la línea.
        """
        m = self._RE_CLOSING.search(text)
        if m:
            return _parse_usd(m.group(1))
        return None

    def _extract_fund_name(self, text: str) -> str:
        """
        Extrae el nombre del fondo de la página.
        Texto real: "Fund Name: WellingtonGlobal CreditPlusFund"
        """
        m = re.search(r"Fund Name[:\s]+(.+?)(?=\n|Share Class)", text, re.DOTALL)
        if m:
            return m.group(1).strip()
        return ""

    def validate(self, result: ParseResult) -> list[str]:
        errors = []
        if result.closing_balance is not None and result.closing_balance <= Decimal("0"):
            errors.append(
                f"Valor total sospechoso: {result.closing_balance} (debe ser positivo)"
            )
        if not result.rows:
            errors.append("No se extrajeron filas de fondos")
        return errors

    def detect(self, filepath: Path) -> float:
        if filepath.suffix.lower() != ".pdf":
            return 0.0
        try:
            with pdfplumber.open(filepath) as pdf:
                if not pdf.pages:
                    return 0.0
                raw = pdf.pages[0].extract_text() or ""

                # Normalizar: minúsculas y sin espacios para robustez ante PDFs
                # que colapsan espacios entre palabras ("WellingtonManagementFunds").
                norm = raw.lower().replace(" ", "")

                # Marcadores obligatorios: si falta uno, confianza cero
                for marker in self._MARKERS_REQUIRED:
                    if marker not in norm:
                        return 0.0

                score = 0.7  # Base alta cuando ambos markers obligatorios están presentes

                for marker in self._MARKERS_SUPPORTING:
                    if marker in norm:
                        score += 0.1

                if "wellington" in filepath.name.lower():
                    score += 0.1

                return min(score, 1.0)
        except Exception:
            return 0.0
