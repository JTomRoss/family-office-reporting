"""
FO Reporting – Interfaz base de Parser (Plugin System).

Todos los parsers DEBEN heredar de BaseParser e implementar sus métodos abstractos.
Cada parser vive aislado en: parsers/<banco>/<tipo_cuenta>.py

Principios:
- Un parser = un banco + un tipo de cuenta
- Sin lógica compartida entre parsers de distintos bancos
- Versionado obligatorio
- Hash del código fuente para trazabilidad
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Optional
import hashlib
import inspect


class ParserStatus(str, Enum):
    SUCCESS = "success"
    PARTIAL = "partial"  # Se extrajo algo pero con warnings
    ERROR = "error"


@dataclass
class ParsedRow:
    """Una fila genérica de datos parseados."""
    data: dict[str, Any]
    row_number: Optional[int] = None
    confidence: float = 1.0  # 0.0 - 1.0
    warnings: list[str] = field(default_factory=list)


@dataclass
class ParseResult:
    """Resultado completo de un parsing."""
    status: ParserStatus
    parser_name: str
    parser_version: str
    source_file_hash: str

    # Datos extraídos
    rows: list[ParsedRow] = field(default_factory=list)

    # Metadatos del statement
    account_number: Optional[str] = None
    bank_code: Optional[str] = None
    statement_date: Optional[date] = None
    period_start: Optional[date] = None
    period_end: Optional[date] = None
    currency: Optional[str] = None

    # Totales extraídos (para validación)
    opening_balance: Optional[Decimal] = None
    closing_balance: Optional[Decimal] = None
    total_credits: Optional[Decimal] = None
    total_debits: Optional[Decimal] = None

    # Datos cualitativos (asset allocation, etc.)
    qualitative_data: dict[str, Any] = field(default_factory=dict)

    # Errores y warnings
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    # Trazabilidad
    parsed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    raw_text_preview: Optional[str] = None  # Primeras líneas del raw para debug

    @property
    def is_success(self) -> bool:
        return self.status == ParserStatus.SUCCESS

    @property
    def row_count(self) -> int:
        return len(self.rows)


class BaseParser(ABC):
    """
    Interfaz base para todos los parsers.

    Convenciones:
    - Cada subclase define BANK_CODE, ACCOUNT_TYPE, VERSION
    - parse() recibe un Path y retorna ParseResult
    - validate() verifica consistencia interna del resultado
    - get_source_hash() retorna SHA-256 del código fuente del parser
    """

    # ── Metadata obligatoria (sobreescribir en cada parser) ──────
    BANK_CODE: str = ""
    ACCOUNT_TYPE: str = ""
    VERSION: str = "0.0.0"
    DESCRIPTION: str = ""

    # ── Tipos de archivo soportados ──────────────────────────────
    SUPPORTED_EXTENSIONS: list[str] = [".pdf"]

    def __init__(self):
        if not self.BANK_CODE or not self.ACCOUNT_TYPE:
            raise ValueError(
                f"Parser {self.__class__.__name__} debe definir BANK_CODE y ACCOUNT_TYPE"
            )

    @abstractmethod
    def parse(self, filepath: Path) -> ParseResult:
        """
        Parsea un archivo y retorna el resultado estructurado.

        Args:
            filepath: Ruta al archivo a parsear.

        Returns:
            ParseResult con todos los datos extraídos.
        """
        ...

    @abstractmethod
    def validate(self, result: ParseResult) -> list[str]:
        """
        Valida consistencia interna del resultado.

        Ej:
        - opening + credits - debits == closing
        - Todas las filas tienen campos requeridos
        - Suma de partes == total

        Returns:
            Lista de errores encontrados (vacía si OK).
        """
        ...

    @abstractmethod
    def detect(self, filepath: Path) -> float:
        """
        Detecta si este parser puede procesar el archivo dado.

        Returns:
            Confianza entre 0.0 (no es para este parser) y 1.0 (seguro).
        """
        ...

    def get_parser_name(self) -> str:
        """Nombre canónico del parser: parsers.<banco>.<tipo>"""
        return f"parsers.{self.BANK_CODE}.{self.ACCOUNT_TYPE}"

    def safe_parse(self, filepath: Path) -> ParseResult:
        """
        Wrapper de parse() que ejecuta validación de contrato automática.

        USAR ESTE MÉTODO en lugar de parse() directamente.
        Garantiza que el ParseResult cumple los campos mínimos requeridos.
        """
        result = self.parse(filepath)
        contract_errors = self.validate_contract(result)
        if contract_errors:
            result.warnings.extend(contract_errors)
            if result.status == ParserStatus.SUCCESS:
                result.status = ParserStatus.PARTIAL
        return result

    def validate_contract(self, result: ParseResult) -> list[str]:
        """
        Validación de contrato: verifica que el ParseResult tiene
        los campos mínimos requeridos para ser procesado por el sistema.

        Esta validación es AUTOMÁTICA y NO se puede sobreescribir.
        Es independiente de validate() que cada parser implementa.

        Returns:
            Lista de errores de contrato (vacía si cumple).
        """
        errors = []

        # Parser metadata
        if not result.parser_name:
            errors.append("CONTRACT: parser_name es requerido")
        if not result.parser_version:
            errors.append("CONTRACT: parser_version es requerido")
        if not result.source_file_hash:
            errors.append("CONTRACT: source_file_hash es requerido")

        # Si es SUCCESS, debe tener datos mínimos
        if result.status == ParserStatus.SUCCESS:
            if not result.account_number:
                errors.append("CONTRACT: account_number es requerido para SUCCESS")
            if not result.currency:
                errors.append("CONTRACT: currency es requerido para SUCCESS")
            if result.statement_date is None and result.row_count == 0:
                errors.append(
                    "CONTRACT: SUCCESS requiere statement_date o al menos 1 fila"
                )

        # Cada fila debe tener data no vacía
        for i, row in enumerate(result.rows):
            if not row.data:
                errors.append(f"CONTRACT: fila {i} tiene data vacía")

        return errors

    def get_source_hash(self) -> str:
        """SHA-256 del código fuente de este parser para trazabilidad."""
        source = inspect.getsource(self.__class__)
        return hashlib.sha256(source.encode()).hexdigest()

    @staticmethod
    def compute_file_hash(filepath: Path) -> str:
        """SHA-256 del archivo para idempotencia."""
        sha256 = hashlib.sha256()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__} "
            f"bank={self.BANK_CODE} type={self.ACCOUNT_TYPE} v={self.VERSION}>"
        )


class BaseExcelParser(BaseParser):
    """
    Extensión de BaseParser para archivos Excel/CSV.
    Los parsers de Excel (posiciones, movimientos, precios, maestro)
    heredan de aquí.
    """

    SUPPORTED_EXTENSIONS: list[str] = [".xlsx", ".xls", ".csv"]

    @abstractmethod
    def get_expected_columns(self) -> list[str]:
        """Columnas esperadas en el archivo."""
        ...

    @abstractmethod
    def map_columns(self, raw_columns: list[str]) -> dict[str, str]:
        """
        Mapeo de columnas del archivo a columnas internas.

        Returns:
            Dict {columna_archivo: columna_interna}
        """
        ...
