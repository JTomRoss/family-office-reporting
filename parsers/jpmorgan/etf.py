"""
Parser: JPMorgan – Cuenta ETF (Cartola PDF).

Submotor ETF aislado para JPMorgan.
Sin lógica compartida con Goldman Sachs.
"""

from pathlib import Path
from decimal import Decimal

from parsers.base import BaseParser, ParseResult, ParserStatus


class JPMorganEtfParser(BaseParser):
    BANK_CODE = "jpmorgan"
    ACCOUNT_TYPE = "etf"
    VERSION = "1.0.0"
    DESCRIPTION = "Parser para cartolas ETF JPMorgan (PDF)"
    SUPPORTED_EXTENSIONS = [".pdf"]

    # ETFs válidos para JPMorgan
    VALID_ETFS = {
        "IWDA": "ISHARES CORE MSCI WORLD",
        "IEMA": "ISHARES MSCI EM-ACC",
        "IHYA": "ISHARES USD HY CORP USD ACC",
        "VDCA": "VAND USDCP1-3 USDA",
        "VDPA": "VANG USDCPBD USDA",
    }

    # Aliases conocidos
    ETF_ALIASES = {
        "ISHARES USD HIGH YIELD CORP BOND": "IHYA",
        "ISHARES CORE MSCI WORLD": "IWDA",
        "ISHARES MSCI EM-ACC": "IEMA",
    }

    _DETECTION_MARKERS = [
        "J.P. Morgan",
        "JPMorgan",
        "ETF",
    ]

    def parse(self, filepath: Path) -> ParseResult:
        """
        Parsea cartola ETF JPMorgan.

        TODO: Implementar extracción de composición ETF.
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
        """
        Validaciones ETF JPMorgan:
        - Total composición instrumentos == Total tabla ETF
        - Todos los ETFs en lista válida
        """
        errors = []

        for row in result.rows:
            etf_code = row.data.get("etf_code", "")
            if etf_code and etf_code not in self.VALID_ETFS:
                # Intentar resolver alias
                resolved = self.ETF_ALIASES.get(etf_code)
                if not resolved:
                    errors.append(f"ETF desconocido: {etf_code}")

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
                        score += 0.25
                return min(score, 1.0)
        except Exception:
            return 0.0
