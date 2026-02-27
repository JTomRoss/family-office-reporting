"""
Parser: Excel – Maestro de Cuentas (SSOT).

Este es el Single Source of Truth para metadata de cuentas.
Al cargar:
- Auto-completa metadata en otras cargas
- Detecta errores de clasificación
- Corrige inconsistencias

Diseño robusto:
- Normaliza columnas (quita tildes, espacios extra, preposiciones)
- Acepta columnas extra sin error (las guarda en metadata_json)
- Transforma valores reales (nombre banco → código, tipo cuenta → enum)
- Deriva entity_type de columnas Portafolio/Personal si entity_type no existe
"""

import json
import unicodedata
from pathlib import Path

import pandas as pd

from parsers.base import BaseExcelParser, ParseResult, ParsedRow, ParserStatus


def _strip_accents(text: str) -> str:
    """Quita tildes/diacríticos: Número → Numero, Código → Codigo."""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


class MasterAccountsParser(BaseExcelParser):
    BANK_CODE = "system"
    ACCOUNT_TYPE = "master_accounts"
    VERSION = "2.0.0"
    DESCRIPTION = "Parser para Excel maestro de cuentas (SSOT) – mapeo flexible"
    SUPPORTED_EXTENSIONS = [".xlsx", ".xls", ".csv"]

    # ── Columnas internas que el sistema necesita ────────────────
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
        "person_name",
        "internal_code",
    ]

    # ── Mapeo flexible: clave = nombre normalizado (sin tildes,
    #    lowercase, _ en vez de espacios, sin preposiciones "de").
    #    Se intenta match exacto, luego parcial (startswith/contains).
    _COLUMN_ALIASES: dict[str, str] = {
        # account_number
        "numero_cuenta": "account_number",
        "numero_de_cuenta": "account_number",
        "nro_cuenta": "account_number",
        "account_number": "account_number",
        "account": "account_number",
        "cuenta": "account_number",
        # bank_code / bank_name
        "banco": "bank_code",
        "bank": "bank_code",
        "bank_code": "bank_code",
        "nombre_banco": "bank_name",
        "bank_name": "bank_name",
        # account_type
        "tipo_cuenta": "account_type",
        "tipo_de_cuenta": "account_type",
        "account_type": "account_type",
        "type": "account_type",
        # entity_name
        "sociedad": "entity_name",
        "titular": "entity_name",
        "entity_name": "entity_name",
        "entity": "entity_name",
        "nombre_entidad": "entity_name",
        # entity_type
        "tipo_entidad": "entity_type",
        "tipo_de_entidad": "entity_type",
        "entity_type": "entity_type",
        # currency
        "moneda": "currency",
        "currency": "currency",
        "ccy": "currency",
        # country
        "pais": "country",
        "country": "country",
        # mandate
        "mandato": "mandate_type",
        "mandate_type": "mandate_type",
        "mandate": "mandate_type",
        # is_active
        "activa": "is_active",
        "active": "is_active",
        "is_active": "is_active",
        # person_name
        "nombre_persona": "person_name",
        "person_name": "person_name",
        "persona": "person_name",
        # internal_code
        "codigo_interno": "internal_code",
        "internal_code": "internal_code",
        "codigo": "internal_code",
        # Portafolio / Personal (columnas especiales que se procesan aparte)
        "portafolio": "_portafolio",
        "personal": "_personal",
        # Columnas a ignorar
        "direccion": "_ignore",
        "ruta": "_ignore",
        "path": "_ignore",
    }

    # ── Normalización de nombres de banco → bank_code ────────────
    _BANK_NAME_TO_CODE: dict[str, str] = {
        "jpmorgan": "jpmorgan",
        "jp morgan": "jpmorgan",
        "j.p. morgan": "jpmorgan",
        "goldman sachs": "goldman_sachs",
        "goldman": "goldman_sachs",
        "bbh": "bbh",
        "brown brothers harriman": "bbh",
        "ubs miami": "ubs_miami",
        "ubs suiza": "ubs",
        "ubs ubs suiza": "ubs",
        "ubs": "ubs",
        "bice": "bice",
    }

    # ── Normalización de tipo de cuenta → account_type enum ──────
    _ACCOUNT_TYPE_MAP: dict[str, str] = {
        "etf": "etf",
        "brokerage": "brokerage",
        "mandato": "mandato",
        "mandatos": "mandato",
        "custodia": "custody",
        "custody": "custody",
        "corriente": "current",
        "cuenta corriente": "current",
        "checking": "checking",
        "checking account": "checking",
        "ahorro": "savings",
        "savings": "savings",
        "inversion": "investment",
        "investment": "investment",
        "bonos": "bonds",
        "bonds": "bonds",
        "modulo bonos": "bonds",
        "modulo_bonos": "bonds",
    }

    def get_expected_columns(self) -> list[str]:
        return self._EXPECTED_COLUMNS

    def _normalize_col_name(self, name: str) -> str:
        """Normaliza nombre de columna: sin tildes, lowercase, _ en vez de espacios."""
        s = _strip_accents(name.strip()).lower()
        s = s.replace(" ", "_")
        return s

    def map_columns(self, raw_columns: list[str]) -> dict[str, str]:
        """Mapea columnas del Excel a nombres internos, tolerando extras."""
        mapping = {}
        for col in raw_columns:
            norm = self._normalize_col_name(col)
            # Match exacto en aliases
            if norm in self._COLUMN_ALIASES:
                mapping[col] = self._COLUMN_ALIASES[norm]
            # Match exacto en expected
            elif norm in self._EXPECTED_COLUMNS:
                mapping[col] = norm
            else:
                # Columna no reconocida → guardar como _extra_<nombre>
                mapping[col] = f"_extra_{norm}"
        return mapping

    def _normalize_bank(self, raw_bank: str) -> tuple[str, str]:
        """
        Convierte nombre de banco a (bank_code, bank_name).

        Ej: 'Goldman Sachs' → ('goldman_sachs', 'Goldman Sachs')
        """
        if not raw_bank or pd.isna(raw_bank):
            return ("unknown", "")
        bank_str = str(raw_bank).strip()
        lookup = bank_str.lower()
        code = self._BANK_NAME_TO_CODE.get(lookup, lookup.replace(" ", "_"))
        return (code, bank_str)

    def _normalize_account_type(self, raw_type: str) -> str:
        """Convierte tipo de cuenta legible a valor del enum."""
        if not raw_type or pd.isna(raw_type):
            return "custody"
        lookup = _strip_accents(str(raw_type).strip()).lower()
        return self._ACCOUNT_TYPE_MAP.get(lookup, lookup.replace(" ", "_"))

    def _derive_entity_type(self, row_data: dict) -> str:
        """Deriva entity_type de columnas _portafolio / _personal."""
        # Prioridad: entity_type explícito > derivado de columnas
        if "entity_type" in row_data:
            val = str(row_data["entity_type"]).strip().lower()
            if val in ("sociedad", "persona"):
                return val

        personal = str(row_data.get("_personal", "")).strip().lower()
        portafolio = str(row_data.get("_portafolio", "")).strip().lower()

        if personal in ("si", "sí", "yes", "true", "1"):
            return "persona"
        if portafolio in ("si", "sí", "yes", "true", "1"):
            return "sociedad"
        return "sociedad"  # default

    def _build_account_row(self, raw_row: dict) -> dict:
        """
        Transforma una fila raw del Excel en un dict listo para Account.

        Normaliza bancos, tipos, entity_type. Campos extra van a metadata_json.
        """
        # Normalizar banco
        bank_code, bank_name = self._normalize_bank(raw_row.get("bank_code"))
        if not bank_name:
            bank_name = raw_row.get("bank_name", bank_code)

        # Normalizar tipo de cuenta
        account_type = self._normalize_account_type(raw_row.get("account_type"))

        # Derivar entity_type
        entity_type = self._derive_entity_type(raw_row)

        # Campos extra → metadata_json
        extra = {}
        for key, val in raw_row.items():
            if key.startswith("_extra_") and val is not None and not (isinstance(val, float) and pd.isna(val)):
                extra[key.replace("_extra_", "")] = str(val)

        acct = {
            "account_number": raw_row.get("account_number"),
            "bank_code": bank_code,
            "bank_name": str(bank_name),
            "account_type": account_type,
            "entity_name": str(raw_row.get("entity_name", "")).strip(),
            "entity_type": entity_type,
            "currency": str(raw_row.get("currency", "USD")).strip(),
            "country": str(raw_row.get("country", "")).strip() if raw_row.get("country") else "",
            "mandate_type": raw_row.get("mandate_type"),
            "is_active": raw_row.get("is_active", True),
        }

        # Campos opcionales: solo incluir si tienen valor real (no NaN)
        pn = raw_row.get("person_name")
        if pn is not None and not (isinstance(pn, float) and pd.isna(pn)):
            acct["person_name"] = str(pn).strip()

        ic = raw_row.get("internal_code")
        if ic is not None and not (isinstance(ic, float) and pd.isna(ic)):
            acct["internal_code"] = str(ic).strip()

        # Si es mandato, auto-detectar mandate_type
        if account_type == "mandato" and not acct["mandate_type"]:
            acct["mandate_type"] = "discretionary"

        # Guardar extras en metadata_json si hay
        if extra:
            acct["metadata_json"] = json.dumps(extra, ensure_ascii=False)

        return acct

    def parse(self, filepath: Path) -> ParseResult:
        file_hash = self.compute_file_hash(filepath)

        try:
            if filepath.suffix.lower() == ".csv":
                df = pd.read_csv(filepath)
            else:
                df = pd.read_excel(filepath)

            # ── 1. Mapear columnas ───────────────────────────────
            col_mapping = self.map_columns(list(df.columns))
            df = df.rename(columns=col_mapping)

            warnings = []

            # Verificar columnas mínimas (account_number es imprescindible)
            has_account = "account_number" in df.columns
            if not has_account:
                # Intentar detectar: columna que contiene "cuenta" o números
                for orig_col, mapped in col_mapping.items():
                    if mapped.startswith("_extra_") and "cuenta" in mapped.lower():
                        df = df.rename(columns={mapped: "account_number"})
                        has_account = True
                        warnings.append(f"Columna '{orig_col}' interpretada como account_number")
                        break

            if not has_account:
                return ParseResult(
                    status=ParserStatus.ERROR,
                    parser_name=self.get_parser_name(),
                    parser_version=self.VERSION,
                    source_file_hash=file_hash,
                    errors=["No se encontró columna de número de cuenta. "
                            f"Columnas encontradas: {list(col_mapping.keys())}"],
                )

            # Info sobre columnas mapeadas
            mapped_cols = {v for v in col_mapping.values() if not v.startswith("_")}
            extra_cols = [k for k, v in col_mapping.items() if v.startswith("_extra_")]
            ignored_cols = [k for k, v in col_mapping.items() if v == "_ignore"]
            if extra_cols:
                warnings.append(f"Columnas extra (guardadas en metadata): {extra_cols}")
            if ignored_cols:
                warnings.append(f"Columnas ignoradas: {ignored_cols}")

            # ── 2. Procesar filas ────────────────────────────────
            rows = []
            for idx, raw_row in df.iterrows():
                row_data = raw_row.to_dict()

                # Verificar account_number
                acct_num = row_data.get("account_number")
                if acct_num is None or (isinstance(acct_num, float) and pd.isna(acct_num)):
                    warnings.append(f"Fila {idx + 2}: account_number vacío, omitida")
                    continue

                # Convertir a string
                row_data["account_number"] = str(int(acct_num) if isinstance(acct_num, float) else acct_num).strip()

                # Normalizar y construir fila
                account_data = self._build_account_row(row_data)

                parsed_row = ParsedRow(
                    data=account_data,
                    row_number=idx + 2,
                )

                # Validación de fila
                if not account_data.get("entity_name"):
                    parsed_row.warnings.append("entity_name vacío")
                    parsed_row.confidence = 0.7

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
        name = filepath.stem.lower()
        if "maestro" in name or "master" in name or "cuentas" in name:
            return 0.9
        return 0.1
