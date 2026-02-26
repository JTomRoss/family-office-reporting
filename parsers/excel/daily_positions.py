"""
Parser: Excel – Posiciones Diarias.
"""

from pathlib import Path
import pandas as pd

from parsers.base import BaseExcelParser, ParseResult, ParsedRow, ParserStatus


class DailyPositionsParser(BaseExcelParser):
    BANK_CODE = "system"
    ACCOUNT_TYPE = "daily_positions"
    VERSION = "1.0.0"
    DESCRIPTION = "Parser para Excel/CSV de posiciones diarias"
    SUPPORTED_EXTENSIONS = [".xlsx", ".xls", ".csv"]

    _EXPECTED_COLUMNS = [
        "account_number", "position_date", "instrument_code",
        "instrument_name", "instrument_type", "isin",
        "quantity", "market_price", "market_value",
        "cost_basis", "unrealized_pnl", "currency",
        "market_value_usd", "accrued_interest",
    ]

    _COLUMN_ALIASES = {
        "cuenta": "account_number",
        "fecha": "position_date",
        "date": "position_date",
        "codigo": "instrument_code",
        "code": "instrument_code",
        "ticker": "instrument_code",
        "nombre": "instrument_name",
        "name": "instrument_name",
        "tipo": "instrument_type",
        "type": "instrument_type",
        "cantidad": "quantity",
        "qty": "quantity",
        "precio": "market_price",
        "price": "market_price",
        "valor_mercado": "market_value",
        "mv": "market_value",
        "costo": "cost_basis",
        "cost": "cost_basis",
        "pnl": "unrealized_pnl",
        "moneda": "currency",
        "ccy": "currency",
        "valor_usd": "market_value_usd",
        "accrual": "accrued_interest",
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
            if filepath.suffix.lower() == ".csv":
                df = pd.read_csv(filepath)
            else:
                df = pd.read_excel(filepath)

            col_mapping = self.map_columns(list(df.columns))
            df = df.rename(columns=col_mapping)

            rows = []
            for idx, row in df.iterrows():
                rows.append(ParsedRow(
                    data=row.to_dict(),
                    row_number=idx + 2,
                ))

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
            if not row.data.get("account_number"):
                errors.append(f"Fila {row.row_number}: account_number vacío")
            if not row.data.get("instrument_code"):
                errors.append(f"Fila {row.row_number}: instrument_code vacío")
        return errors

    def detect(self, filepath: Path) -> float:
        if filepath.suffix.lower() not in self.SUPPORTED_EXTENSIONS:
            return 0.0
        name = filepath.stem.lower()
        if "posicion" in name or "position" in name:
            return 0.9
        return 0.1
