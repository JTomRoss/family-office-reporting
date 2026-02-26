"""
Parser: JPMorgan – Cuenta Custodia (Cartola PDF).

Extrae posiciones, movimientos y totales de cartolas mensuales JPM.
AISLADO: No comparte lógica con otros parsers.
"""

from pathlib import Path
from decimal import Decimal
from typing import Optional

from parsers.base import BaseParser, ParseResult, ParsedRow, ParserStatus


class JPMorganCustodyParser(BaseParser):
    BANK_CODE = "jpmorgan"
    ACCOUNT_TYPE = "custody"
    VERSION = "1.0.0"
    DESCRIPTION = "Parser para cartolas de custodia JPMorgan (PDF)"
    SUPPORTED_EXTENSIONS = [".pdf"]

    # ── Marcadores de detección en el PDF ────────────────────────
    _DETECTION_MARKERS = [
        "J.P. Morgan",
        "JPMorgan",
        "Custody Account Statement",
    ]

    def parse(self, filepath: Path) -> ParseResult:
        """
        Parsea cartola PDF de custodia JPMorgan.

        TODO: Implementar con pdfplumber.
        Stub retorna estructura vacía correcta.
        """
        file_hash = self.compute_file_hash(filepath)

        # STUB: Estructura real a implementar con pdfplumber
        result = ParseResult(
            status=ParserStatus.SUCCESS,
            parser_name=self.get_parser_name(),
            parser_version=self.VERSION,
            source_file_hash=file_hash,
            bank_code=self.BANK_CODE,
            warnings=["STUB: Parser no implementado aún. Retorna datos vacíos."],
        )

        return result

    def validate(self, result: ParseResult) -> list[str]:
        """
        Validación interna JPMorgan custody:
        - opening + credits - debits ≈ closing
        - Todos los instrumentos tienen market_value
        """
        errors = []

        if result.opening_balance is not None and result.closing_balance is not None:
            credits = result.total_credits or Decimal("0")
            debits = result.total_debits or Decimal("0")
            expected = result.opening_balance + credits - debits
            diff = abs(expected - result.closing_balance)
            if diff > Decimal("0.01"):
                errors.append(
                    f"Balance check failed: open({result.opening_balance}) + "
                    f"credits({credits}) - debits({debits}) = {expected}, "
                    f"but closing = {result.closing_balance} (diff={diff})"
                )

        return errors

    def detect(self, filepath: Path) -> float:
        """Detecta si es una cartola JPM custody."""
        if filepath.suffix.lower() != ".pdf":
            return 0.0

        try:
            import pdfplumber
            with pdfplumber.open(filepath) as pdf:
                if not pdf.pages:
                    return 0.0
                first_page_text = pdf.pages[0].extract_text() or ""
                score = 0.0
                for marker in self._DETECTION_MARKERS:
                    if marker.lower() in first_page_text.lower():
                        score += 0.3
                return min(score, 1.0)
        except Exception:
            return 0.0
