"""
Parser: Excel – Movimientos Diarios.
"""

from pathlib import Path
import pandas as pd

from parsers.base import BaseExcelParser, ParseResult, ParsedRow, ParserStatus


class DailyMovementsParser(BaseExcelParser):
    BANK_CODE = "system"
    ACCOUNT_TYPE = "daily_movements"
    VERSION = "1.0.0"
    DESCRIPTION = "Parser para Excel/CSV de movimientos diarios"
    SUPPORTED_EXTENSIONS = [".xlsx", ".xls", ".csv"]

    _EXPECTED_COLUMNS = [
        "account_number", "movement_date", "settlement_date",
        "movement_type", "instrument_code", "instrument_name",
        "description", "quantity", "price",
        "gross_amount", "net_amount", "fees", "tax",
        "currency", "amount_usd",
    ]

    _COLUMN_ALIASES = {
        "cuenta": "account_number",
        "fecha": "movement_date",
        "date": "movement_date",
        "fecha_liquidacion": "settlement_date",
        "tipo": "movement_type",
        "type": "movement_type",
        "codigo": "instrument_code",
        "code": "instrument_code",
        "nombre": "instrument_name",
        "descripcion": "description",
        "cantidad": "quantity",
        "precio": "price",
        "monto_bruto": "gross_amount",
        "monto_neto": "net_amount",
        "comision": "fees",
        "impuesto": "tax",
        "moneda": "currency",
        "ccy": "currency",
        "monto_usd": "amount_usd",
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
            if not row.data.get("account_number"):
                errors.append(f"Fila {row.row_number}: account_number vacío")
            if not row.data.get("movement_date"):
                errors.append(f"Fila {row.row_number}: movement_date vacío")
        return errors

    def detect(self, filepath: Path) -> float:
        if filepath.suffix.lower() not in self.SUPPORTED_EXTENSIONS:
            return 0.0
        name = filepath.stem.lower()
        if "movimiento" in name or "movement" in name or "transaction" in name:
            return 0.9
        return 0.1
