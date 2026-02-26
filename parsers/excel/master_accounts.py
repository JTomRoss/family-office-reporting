"""
Parser: Excel – Maestro de Cuentas (SSOT).

Este es el Single Source of Truth para metadata de cuentas.
Al cargar:
- Auto-completa metadata en otras cargas
- Detecta errores de clasificación
- Corrige inconsistencias
"""

from pathlib import Path
from decimal import Decimal

import pandas as pd

from parsers.base import BaseExcelParser, ParseResult, ParsedRow, ParserStatus


class MasterAccountsParser(BaseExcelParser):
    BANK_CODE = "system"
    ACCOUNT_TYPE = "master_accounts"
    VERSION = "1.0.0"
    DESCRIPTION = "Parser para Excel maestro de cuentas (SSOT)"
    SUPPORTED_EXTENSIONS = [".xlsx", ".xls", ".csv"]

    # Columnas esperadas (nombres normalizados)
    _EXPECTED_COLUMNS = [
        "account_number",
        "bank_code",
        "bank_name",
        "account_type",
        "entity_name",
        "entity_type",
        "currency",
        "country",
        "mandate_type",
        "is_active",
    ]

    # Mapeo flexible de nombres de columna
    _COLUMN_ALIASES = {
        "numero_cuenta": "account_number",
        "nro_cuenta": "account_number",
        "account": "account_number",
        "banco": "bank_code",
        "bank": "bank_code",
        "nombre_banco": "bank_name",
        "tipo_cuenta": "account_type",
        "type": "account_type",
        "sociedad": "entity_name",
        "titular": "entity_name",
        "entity": "entity_name",
        "tipo_entidad": "entity_type",
        "moneda": "currency",
        "ccy": "currency",
        "pais": "country",
        "mandato": "mandate_type",
        "activa": "is_active",
        "active": "is_active",
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

            # Mapear columnas
            col_mapping = self.map_columns(list(df.columns))
            df = df.rename(columns=col_mapping)

            # Verificar columnas obligatorias
            missing = set(self._EXPECTED_COLUMNS[:7]) - set(df.columns)
            warnings = []
            if missing:
                warnings.append(f"Columnas obligatorias faltantes: {missing}")

            rows = []
            for idx, row in df.iterrows():
                parsed_row = ParsedRow(
                    data=row.to_dict(),
                    row_number=idx + 2,  # +2 por header + 0-based
                )
                # Validar datos de la fila
                if pd.isna(row.get("account_number")):
                    parsed_row.warnings.append("account_number vacío")
                    parsed_row.confidence = 0.3
                if pd.isna(row.get("bank_code")):
                    parsed_row.warnings.append("bank_code vacío")
                    parsed_row.confidence = 0.5

                rows.append(parsed_row)

            return ParseResult(
                status=ParserStatus.SUCCESS,
                parser_name=self.get_parser_name(),
                parser_version=self.VERSION,
                source_file_hash=file_hash,
                rows=rows,
                warnings=warnings,
            )

        except Exception as e:
            return ParseResult(
                status=ParserStatus.ERROR,
                parser_name=self.get_parser_name(),
                parser_version=self.VERSION,
                source_file_hash=file_hash,
                errors=[f"Error parseando maestro: {str(e)}"],
            )

    def validate(self, result: ParseResult) -> list[str]:
        errors = []
        seen_accounts = set()
        for row in result.rows:
            acct = row.data.get("account_number")
            if acct and acct in seen_accounts:
                errors.append(f"Cuenta duplicada en maestro: {acct}")
            if acct:
                seen_accounts.add(acct)
        return errors

    def detect(self, filepath: Path) -> float:
        if filepath.suffix.lower() not in self.SUPPORTED_EXTENSIONS:
            return 0.0
        # Heurística: nombre del archivo contiene "maestro" o "master"
        name = filepath.stem.lower()
        if "maestro" in name or "master" in name:
            return 0.9
        return 0.1
