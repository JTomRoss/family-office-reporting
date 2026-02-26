"""
Parser: Excel – Precios Diarios (FX + activos).
"""

from pathlib import Path
import pandas as pd

from parsers.base import BaseExcelParser, ParseResult, ParsedRow, ParserStatus


class DailyPricesParser(BaseExcelParser):
    BANK_CODE = "system"
    ACCOUNT_TYPE = "daily_prices"
    VERSION = "1.0.0"
    DESCRIPTION = "Parser para Excel/CSV de precios diarios"
    SUPPORTED_EXTENSIONS = [".xlsx", ".xls", ".csv"]

    _EXPECTED_COLUMNS = [
        "price_date", "instrument_code", "instrument_type",
        "price", "currency", "source",
    ]

    _COLUMN_ALIASES = {
        "fecha": "price_date",
        "date": "price_date",
        "codigo": "instrument_code",
        "code": "instrument_code",
        "ticker": "instrument_code",
        "tipo": "instrument_type",
        "type": "instrument_type",
        "precio": "price",
        "moneda": "currency",
        "ccy": "currency",
        "fuente": "source",
    }

    def get_expected_columns(self) -> list[str]:
        return self._EXPECTED_COLUMNS

    def map_columns(self, raw_columns: list[str]) -> dict[str, str]:
        mapping = {}
        for col in raw_columns:
            normalized = col.strip().lower().replace(" ", "_")
            if normalized in self._EXPECTED_COLUMNS:
                mapping[col] = normalized
            elif normalized in self._COLUMN_ALIASES:
                mapping[col] = self._COLUMN_ALIASES[normalized]
        return mapping

    def parse(self, filepath: Path) -> ParseResult:
        file_hash = self.compute_file_hash(filepath)
        try:
            df = pd.read_csv(filepath) if filepath.suffix.lower() == ".csv" else pd.read_excel(filepath)
            col_mapping = self.map_columns(list(df.columns))
            df = df.rename(columns=col_mapping)

            rows = [
                ParsedRow(data=row.to_dict(), row_number=idx + 2)
                for idx, row in df.iterrows()
            ]

            return ParseResult(
                status=ParserStatus.SUCCESS,
                parser_name=self.get_parser_name(),
                parser_version=self.VERSION,
                source_file_hash=file_hash,
                rows=rows,
            )
        except Exception as e:
            return ParseResult(
                status=ParserStatus.ERROR,
                parser_name=self.get_parser_name(),
                parser_version=self.VERSION,
                source_file_hash=file_hash,
                errors=[str(e)],
            )

    def validate(self, result: ParseResult) -> list[str]:
        errors = []
        for row in result.rows:
            if not row.data.get("instrument_code"):
                errors.append(f"Fila {row.row_number}: instrument_code vacío")
            price = row.data.get("price")
            if price is not None and float(price) <= 0:
                errors.append(f"Fila {row.row_number}: precio <= 0")
        return errors

    def detect(self, filepath: Path) -> float:
        if filepath.suffix.lower() not in self.SUPPORTED_EXTENSIONS:
            return 0.0
        name = filepath.stem.lower()
        if "precio" in name or "price" in name or "fx" in name:
            return 0.9
        return 0.1
