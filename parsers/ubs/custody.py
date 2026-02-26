"""
Parser: UBS Suiza – Cuenta Custodia (Cartola PDF).

AISLADO: No comparte lógica con JPMorgan ni Goldman Sachs.
"""

from pathlib import Path
from decimal import Decimal

from parsers.base import BaseParser, ParseResult, ParserStatus


class UBSSwitzerlandCustodyParser(BaseParser):
    BANK_CODE = "ubs"
    ACCOUNT_TYPE = "custody"
    VERSION = "1.0.0"
    DESCRIPTION = "Parser para cartolas de custodia UBS Suiza (PDF)"
    SUPPORTED_EXTENSIONS = [".pdf"]

    _DETECTION_MARKERS = [
        "UBS",
        "Switzerland",
        "Custody",
    ]

    def parse(self, filepath: Path) -> ParseResult:
        """
        Parsea cartola PDF UBS Suiza.

        TODO: Implementar con pdfplumber.
        """
        file_hash = self.compute_file_hash(filepath)

        result = ParseResult(
            status=ParserStatus.SUCCESS,
            parser_name=self.get_parser_name(),
            parser_version=self.VERSION,
            source_file_hash=file_hash,
            bank_code=self.BANK_CODE,
            warnings=["STUB: Parser no implementado aún."],
        )

        return result

    def validate(self, result: ParseResult) -> list[str]:
        """Validación interna UBS."""
        errors = []
        # Validar balance check
        if result.opening_balance is not None and result.closing_balance is not None:
            credits = result.total_credits or Decimal("0")
            debits = result.total_debits or Decimal("0")
            expected = result.opening_balance + credits - debits
            diff = abs(expected - result.closing_balance)
            if diff > Decimal("0.01"):
                errors.append(
                    f"UBS balance check: expected {expected}, got {result.closing_balance}"
                )
        return errors

    def detect(self, filepath: Path) -> float:
        if filepath.suffix.lower() != ".pdf":
            return 0.0
        try:
            import pdfplumber
            with pdfplumber.open(filepath) as pdf:
                if not pdf.pages:
                    return 0.0
                text = pdf.pages[0].extract_text() or ""
                score = 0.0
                for marker in self._DETECTION_MARKERS:
                    if marker.lower() in text.lower():
                        score += 0.3
                return min(score, 1.0)
        except Exception:
            return 0.0
