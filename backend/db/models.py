"""
FO Reporting – Modelos de Base de Datos (SQLAlchemy ORM).

Esquema diseñado para:
- Trazabilidad completa (audit trail)
- Idempotencia (SHA-256 checksums)
- Versionado de parsers
- Conciliación diaria vs mensual
"""

from datetime import datetime, date, timezone
from decimal import Decimal
from enum import Enum as PyEnum
from typing import Optional


def _utcnow() -> datetime:
    """Timezone-aware UTC now. Reemplaza datetime.utcnow (deprecated 3.12+)."""
    return datetime.now(timezone.utc)


# ═══════════════════════════════════════════════════════════════════
# ENUMS – Valores válidos para campos de texto en la BD.
# Evitan insertar basura y documentan los valores aceptados.
# ═══════════════════════════════════════════════════════════════════

class AccountType(str, PyEnum):
    CUSTODY = "custody"
    CURRENT = "current"
    SAVINGS = "savings"
    INVESTMENT = "investment"
    ETF = "etf"
    BROKERAGE = "brokerage"
    MANDATO = "mandato"
    BONDS = "bonds"
    CHECKING = "checking"


class EntityType(str, PyEnum):
    SOCIEDAD = "sociedad"
    PERSONA = "persona"


class MandateType(str, PyEnum):
    DISCRETIONARY = "discretionary"
    ADVISORY = "advisory"
    EXECUTION_ONLY = "execution_only"


class DocumentStatus(str, PyEnum):
    UPLOADED = "uploaded"
    PROCESSING = "processing"
    PARSED = "parsed"
    VALIDATED = "validated"
    ERROR = "error"


class FileType(str, PyEnum):
    PDF_CARTOLA = "pdf_cartola"
    PDF_REPORT = "pdf_report"
    EXCEL_POSITIONS = "excel_positions"
    EXCEL_MOVEMENTS = "excel_movements"
    EXCEL_PRICES = "excel_prices"
    EXCEL_MASTER = "excel_master"
    CSV = "csv"


class MovementType(str, PyEnum):
    BUY = "buy"
    SELL = "sell"
    DIVIDEND = "dividend"
    INTEREST = "interest"
    FEE = "fee"
    TRANSFER_IN = "transfer_in"
    TRANSFER_OUT = "transfer_out"
    FX = "fx"
    COUPON = "coupon"
    OTHER = "other"


class ReconciliationStatusEnum(str, PyEnum):
    MATCHED = "matched"
    MINOR_DIFF = "minor_diff"
    MAJOR_DIFF = "major_diff"
    MISSING_DAILY = "missing_daily"
    MISSING_MONTHLY = "missing_monthly"


class ValidationSeverity(str, PyEnum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class ValidationType(str, PyEnum):
    PARSE = "parse"
    LOAD = "load"
    RECONCILE = "reconcile"
    CALCULATE = "calculate"
    MASTER_CHECK = "master_check"
    IDEMPOTENCY = "idempotency"

from sqlalchemy import (
    String,
    Text,
    Integer,
    Float,
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    UniqueConstraint,
    Index,
    CheckConstraint,
    Numeric,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base declarativa para todos los modelos."""
    pass


# ╔══════════════════════════════════════════════════════════════════╗
# ║  1. ACCOUNTS – Maestro de cuentas (Single Source of Truth)      ║
# ╚══════════════════════════════════════════════════════════════════╝

class Account(Base):
    """
    Maestro de cuentas. Fuente: Excel maestro.
    Es el SSOT para metadata de cuentas.
    """
    __tablename__ = "accounts"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    account_number: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    # Número de cuenta REAL (ej: U28375001). UNIQUE.
    identification_number: Mapped[Optional[str]] = mapped_column(String(50))
    # Dígito corto de identificación (ej: 5001). NO unique, puede repetirse.
    # Es el que se muestra en tablas de reporting.
    bank_code: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    bank_name: Mapped[str] = mapped_column(String(200), nullable=False)
    account_type: Mapped[str] = mapped_column(String(50), nullable=False)
    # Tipos: "custody", "current", "savings", "investment", "etf"
    entity_name: Mapped[str] = mapped_column(String(200), nullable=False)
    # Sociedad / persona titular
    entity_type: Mapped[str] = mapped_column(String(50), nullable=False)
    # "sociedad", "persona"
    currency: Mapped[str] = mapped_column(String(10), nullable=False)
    country: Mapped[Optional[str]] = mapped_column(String(100), default="")
    mandate_type: Mapped[Optional[str]] = mapped_column(String(100))
    # "discretionary", "advisory", "execution_only"
    person_name: Mapped[Optional[str]] = mapped_column(String(200))
    # Nombre persona (cuando entity_type=persona)
    internal_code: Mapped[Optional[str]] = mapped_column(String(100))
    # Código interno de referencia
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    metadata_json: Mapped[Optional[str]] = mapped_column(Text)
    # JSON libre para campos extra del maestro

    # Audit
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=_utcnow, onupdate=_utcnow
    )
    source_file_hash: Mapped[Optional[str]] = mapped_column(String(64))
    # Hash del Excel maestro del que se cargó

    # Relationships
    raw_documents: Mapped[list["RawDocument"]] = relationship(back_populates="account")
    daily_positions: Mapped[list["DailyPosition"]] = relationship(back_populates="account")
    daily_movements: Mapped[list["DailyMovement"]] = relationship(back_populates="account")
    monthly_closings: Mapped[list["MonthlyClosing"]] = relationship(back_populates="account")
    normalized_monthly_metrics: Mapped[list["MonthlyMetricNormalized"]] = relationship(
        back_populates="account"
    )

    __table_args__ = (
        Index("ix_accounts_bank_entity", "bank_code", "entity_name"),
        CheckConstraint(
            f"account_type IN ({', '.join(repr(e.value) for e in AccountType)})",
            name="ck_accounts_account_type",
        ),
        CheckConstraint(
            f"entity_type IN ({', '.join(repr(e.value) for e in EntityType)})",
            name="ck_accounts_entity_type",
        ),
    )


# ╔══════════════════════════════════════════════════════════════════╗
# ║  2. RAW_DOCUMENTS – Documentos fuente originales                ║
# ╚══════════════════════════════════════════════════════════════════╝

class RawDocument(Base):
    """
    Registro de cada documento ingresado al sistema.
    El archivo físico se guarda en data/raw/{bank}/{type}/{filename}.
    Idempotencia: sha256_hash unique.
    """
    __tablename__ = "raw_documents"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    filename: Mapped[str] = mapped_column(String(500), nullable=False)
    filepath: Mapped[str] = mapped_column(String(1000), nullable=False)
    file_type: Mapped[str] = mapped_column(String(20), nullable=False)
    # "pdf_cartola", "pdf_report", "excel_positions", "excel_movements",
    # "excel_prices", "excel_master", "csv"
    sha256_hash: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    file_size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)

    # Clasificación
    bank_code: Mapped[Optional[str]] = mapped_column(String(50), index=True)
    account_id: Mapped[Optional[int]] = mapped_column(ForeignKey("accounts.id"))
    period_year: Mapped[Optional[int]] = mapped_column(Integer)
    period_month: Mapped[Optional[int]] = mapped_column(Integer)

    # Estado de procesamiento
    status: Mapped[str] = mapped_column(String(30), default="uploaded")
    # "uploaded", "processing", "parsed", "validated", "error"
    error_message: Mapped[Optional[str]] = mapped_column(Text)

    # Trazabilidad
    uploaded_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
    processed_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    parser_version_id: Mapped[Optional[int]] = mapped_column(ForeignKey("parser_versions.id"))

    # Relationships
    account: Mapped[Optional["Account"]] = relationship(back_populates="raw_documents")
    parser_version: Mapped[Optional["ParserVersion"]] = relationship()
    parsed_statements: Mapped[list["ParsedStatement"]] = relationship(
        back_populates="raw_document", cascade="all, delete-orphan"
    )
    validation_logs: Mapped[list["ValidationLog"]] = relationship(
        back_populates="raw_document", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("ix_raw_docs_bank_period", "bank_code", "period_year", "period_month"),
        CheckConstraint(
            f"status IN ({', '.join(repr(e.value) for e in DocumentStatus)})",
            name="ck_raw_docs_status",
        ),
        CheckConstraint(
            f"file_type IN ({', '.join(repr(e.value) for e in FileType)})",
            name="ck_raw_docs_file_type",
        ),
    )


# ╔══════════════════════════════════════════════════════════════════╗
# ║  3. PARSER_VERSIONS – Registro de versiones de parsers          ║
# ╚══════════════════════════════════════════════════════════════════╝

class ParserVersion(Base):
    """
    Cada vez que un parser procesa un documento, se registra qué versión usó.
    Permite reproducibilidad y debugging.
    """
    __tablename__ = "parser_versions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    parser_name: Mapped[str] = mapped_column(String(200), nullable=False)
    # Ej: "parsers.jpmorgan.custody"
    version: Mapped[str] = mapped_column(String(50), nullable=False)
    # Ej: "1.0.0"
    source_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    # SHA-256 del código fuente del parser
    registered_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
    description: Mapped[Optional[str]] = mapped_column(Text)

    __table_args__ = (
        UniqueConstraint("parser_name", "version", name="uq_parser_name_version"),
    )


# ╔══════════════════════════════════════════════════════════════════╗
# ║  4. PARSED_STATEMENTS – Resultado de parsing de cartolas        ║
# ╚══════════════════════════════════════════════════════════════════╝

class ParsedStatement(Base):
    """
    Datos extraídos de una cartola bancaria (PDF).
    Estructura intermedia entre raw y los datos finales.
    """
    __tablename__ = "parsed_statements"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    raw_document_id: Mapped[int] = mapped_column(
        ForeignKey("raw_documents.id"), nullable=False
    )
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"), nullable=False)

    # Periodo
    statement_date: Mapped[date] = mapped_column(Date, nullable=False)
    period_start: Mapped[date] = mapped_column(Date, nullable=False)
    period_end: Mapped[date] = mapped_column(Date, nullable=False)

    # Valores principales
    opening_balance: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))
    closing_balance: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))
    total_credits: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))
    total_debits: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))
    currency: Mapped[str] = mapped_column(String(10), nullable=False)

    # Datos parseados completos en JSON (toda la tabla extraída)
    parsed_data_json: Mapped[Optional[str]] = mapped_column(Text)
    # Contiene: posiciones, movimientos, líneas de detalle, etc.

    # Trazabilidad
    parsed_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
    parser_version_id: Mapped[int] = mapped_column(ForeignKey("parser_versions.id"))

    # Relationships
    raw_document: Mapped["RawDocument"] = relationship(back_populates="parsed_statements")
    parser_version: Mapped["ParserVersion"] = relationship()

    __table_args__ = (
        UniqueConstraint(
            "raw_document_id", "account_id", "statement_date",
            name="uq_parsed_stmt_doc_acct_date"
        ),
        Index("ix_parsed_stmt_period", "account_id", "period_start", "period_end"),
    )


# ╔══════════════════════════════════════════════════════════════════╗
# ║  5. DAILY_POSITIONS – Posiciones diarias (Excel/CSV)            ║
# ╚══════════════════════════════════════════════════════════════════╝

class DailyPosition(Base):
    """
    Posiciones diarias cargadas desde Excel/CSV.
    Una fila por instrumento por cuenta por fecha.
    """
    __tablename__ = "daily_positions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"), nullable=False)
    position_date: Mapped[date] = mapped_column(Date, nullable=False)

    # Instrumento
    instrument_code: Mapped[str] = mapped_column(String(100), nullable=False)
    instrument_name: Mapped[Optional[str]] = mapped_column(String(500))
    instrument_type: Mapped[Optional[str]] = mapped_column(String(50))
    # "equity", "bond", "etf", "cash", "fund", "structured", "other"
    isin: Mapped[Optional[str]] = mapped_column(String(12))

    # Valores
    quantity: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 6))
    market_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 6))
    market_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))
    cost_basis: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))
    unrealized_pnl: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))
    currency: Mapped[str] = mapped_column(String(10), nullable=False)
    market_value_usd: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))

    # Accrual (para cálculo profit JPM ETF)
    accrued_interest: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))

    # Trazabilidad
    source_file_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    loaded_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)

    # Relationships
    account: Mapped["Account"] = relationship(back_populates="daily_positions")

    __table_args__ = (
        UniqueConstraint(
            "account_id", "position_date", "instrument_code",
            name="uq_daily_pos_acct_date_inst"
        ),
        Index("ix_daily_pos_date", "position_date"),
        Index("ix_daily_pos_acct_date", "account_id", "position_date"),
    )


# ╔══════════════════════════════════════════════════════════════════╗
# ║  6. DAILY_MOVEMENTS – Movimientos diarios (Excel/CSV)           ║
# ╚══════════════════════════════════════════════════════════════════╝

class DailyMovement(Base):
    """
    Movimientos / transacciones diarias.
    """
    __tablename__ = "daily_movements"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"), nullable=False)
    movement_date: Mapped[date] = mapped_column(Date, nullable=False)
    settlement_date: Mapped[Optional[date]] = mapped_column(Date)

    # Detalle
    movement_type: Mapped[str] = mapped_column(String(50), nullable=False)
    # "buy", "sell", "dividend", "interest", "fee", "transfer_in",
    # "transfer_out", "fx", "coupon", "other"
    instrument_code: Mapped[Optional[str]] = mapped_column(String(100))
    instrument_name: Mapped[Optional[str]] = mapped_column(String(500))
    description: Mapped[Optional[str]] = mapped_column(Text)

    # Valores
    quantity: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 6))
    price: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 6))
    gross_amount: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))
    net_amount: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))
    fees: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))
    tax: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))
    currency: Mapped[str] = mapped_column(String(10), nullable=False)
    amount_usd: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))

    # Trazabilidad
    source_file_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    loaded_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)

    # Relationships
    account: Mapped["Account"] = relationship(back_populates="daily_movements")

    __table_args__ = (
        Index("ix_daily_mov_date", "movement_date"),
        Index("ix_daily_mov_acct_date", "account_id", "movement_date"),
    )


# ╔══════════════════════════════════════════════════════════════════╗
# ║  7. DAILY_PRICES – Precios FX y activos (Excel/CSV)             ║
# ╚══════════════════════════════════════════════════════════════════╝

class DailyPrice(Base):
    """
    Precios diarios de instrumentos y tipos de cambio.
    """
    __tablename__ = "daily_prices"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    price_date: Mapped[date] = mapped_column(Date, nullable=False)
    instrument_code: Mapped[str] = mapped_column(String(100), nullable=False)
    instrument_type: Mapped[str] = mapped_column(String(50), nullable=False)
    # "fx", "equity", "bond", "etf", "fund", "index"

    # Valores
    price: Mapped[Decimal] = mapped_column(Numeric(20, 8), nullable=False)
    currency: Mapped[str] = mapped_column(String(10), nullable=False)
    source: Mapped[Optional[str]] = mapped_column(String(100))
    # "bloomberg", "bank_feed", "manual"

    # Trazabilidad
    source_file_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    loaded_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)

    __table_args__ = (
        UniqueConstraint(
            "price_date", "instrument_code",
            name="uq_daily_price_date_inst"
        ),
        Index("ix_daily_price_date", "price_date"),
    )


# ╔══════════════════════════════════════════════════════════════════╗
# ║  8. MONTHLY_CLOSINGS – Cierres mensuales (cartolas = VERDAD)    ║
# ╚══════════════════════════════════════════════════════════════════╝

class MonthlyClosing(Base):
    """
    Cierre mensual oficial por cuenta.
    Fuente: cartola bancaria (PDF parseado).
    Esta tabla es la VERDAD para conciliación.
    """
    __tablename__ = "monthly_closings"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"), nullable=False)
    closing_date: Mapped[date] = mapped_column(Date, nullable=False)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    month: Mapped[int] = mapped_column(Integer, nullable=False)

    # Valores cierre
    total_assets: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))
    total_liabilities: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))
    net_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))
    currency: Mapped[str] = mapped_column(String(10), nullable=False)
    net_value_usd: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))

    # Income y performance (para cálculos)
    income: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))
    change_in_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))
    total_return: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))

    # Accrual (para JPM)
    accrual: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))

    # Metadata cualitativa (del PDF reporting)
    asset_allocation_json: Mapped[Optional[str]] = mapped_column(Text)
    geography_json: Mapped[Optional[str]] = mapped_column(Text)
    currency_allocation_json: Mapped[Optional[str]] = mapped_column(Text)

    # Trazabilidad
    source_document_id: Mapped[Optional[int]] = mapped_column(ForeignKey("raw_documents.id"))
    loaded_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)

    # Relationships
    account: Mapped["Account"] = relationship(back_populates="monthly_closings")
    reconciliations: Mapped[list["Reconciliation"]] = relationship(back_populates="monthly_closing")

    __table_args__ = (
        UniqueConstraint(
            "account_id", "year", "month",
            name="uq_monthly_closing_acct_period"
        ),
        Index("ix_monthly_closing_period", "year", "month"),
    )


# ╔══════════════════════════════════════════════════════════════════╗
# ║  8b. MONTHLY_METRICS_NORMALIZED – Capa normalizada mensual      ║
# ╚══════════════════════════════════════════════════════════════════╝

class MonthlyMetricNormalized(Base):
    """
    Capa canónica mensual para consumo de reporting.
    Guarda explícitamente ending with/without accrual para evitar ambigüedad.
    """
    __tablename__ = "monthly_metrics_normalized"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"), nullable=False)
    closing_date: Mapped[date] = mapped_column(Date, nullable=False)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    month: Mapped[int] = mapped_column(Integer, nullable=False)

    # Métricas normalizadas (canónicas)
    ending_value_with_accrual: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))
    ending_value_without_accrual: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))
    accrual_ending: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))
    cash_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))
    movements_net: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))
    profit_period: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))
    currency: Mapped[str] = mapped_column(String(10), nullable=False)

    # Trazabilidad
    source_document_id: Mapped[Optional[int]] = mapped_column(ForeignKey("raw_documents.id"))
    loaded_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)

    # Relationships
    account: Mapped["Account"] = relationship(back_populates="normalized_monthly_metrics")

    __table_args__ = (
        UniqueConstraint(
            "account_id", "year", "month",
            name="uq_norm_monthly_metric_acct_period",
        ),
        Index("ix_norm_monthly_metric_period", "year", "month"),
    )


# ╔══════════════════════════════════════════════════════════════════╗
# ║  9. RECONCILIATIONS – Resultado de conciliación                 ║
# ╚══════════════════════════════════════════════════════════════════╝

class Reconciliation(Base):
    """
    Resultado de conciliación entre datos diarios y cierre mensual.
    La cartola manda como verdad.
    """
    __tablename__ = "reconciliations"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    monthly_closing_id: Mapped[int] = mapped_column(
        ForeignKey("monthly_closings.id"), nullable=False
    )
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"), nullable=False)
    reconciliation_date: Mapped[date] = mapped_column(Date, nullable=False)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    month: Mapped[int] = mapped_column(Integer, nullable=False)

    # Valores comparados
    daily_total: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))
    monthly_total: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))
    difference: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))
    difference_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 6))

    # Estado
    status: Mapped[str] = mapped_column(String(30), nullable=False)
    # "matched", "minor_diff", "major_diff", "missing_daily", "missing_monthly"
    threshold_used: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 6))
    currency: Mapped[str] = mapped_column(String(10), nullable=False)

    # Detalle de diferencias
    details_json: Mapped[Optional[str]] = mapped_column(Text)
    # JSON con breakdown de diferencias por instrumento

    notes: Mapped[Optional[str]] = mapped_column(Text)
    resolved: Mapped[bool] = mapped_column(Boolean, default=False)
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    resolved_by: Mapped[Optional[str]] = mapped_column(String(100))

    # Trazabilidad
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)

    # Relationships
    monthly_closing: Mapped["MonthlyClosing"] = relationship(back_populates="reconciliations")

    __table_args__ = (
        UniqueConstraint(
            "account_id", "year", "month",
            name="uq_reconciliation_acct_period"
        ),
        CheckConstraint(
            f"status IN ({', '.join(repr(e.value) for e in ReconciliationStatusEnum)})",
            name="ck_reconciliation_status",
        ),
    )


# ╔══════════════════════════════════════════════════════════════════╗
# ║  10. VALIDATION_LOGS – Logs de validación y auditoría           ║
# ╚══════════════════════════════════════════════════════════════════╝

class ValidationLog(Base):
    """
    Log de cada validación ejecutada.
    Sirve como audit trail completo del sistema.
    """
    __tablename__ = "validation_logs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    raw_document_id: Mapped[Optional[int]] = mapped_column(ForeignKey("raw_documents.id"))
    account_id: Mapped[Optional[int]] = mapped_column(ForeignKey("accounts.id"))

    # Contexto
    validation_type: Mapped[str] = mapped_column(String(50), nullable=False)
    # "parse", "load", "reconcile", "calculate", "master_check", "idempotency"
    severity: Mapped[str] = mapped_column(String(20), nullable=False)
    # "info", "warning", "error", "critical"
    message: Mapped[str] = mapped_column(Text, nullable=False)
    details_json: Mapped[Optional[str]] = mapped_column(Text)
    # JSON con contexto adicional

    # Trazabilidad
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
    source_module: Mapped[Optional[str]] = mapped_column(String(200))
    # Ej: "parsers.jpmorgan.custody", "calculations.profit"

    # Relationships
    raw_document: Mapped[Optional["RawDocument"]] = relationship(back_populates="validation_logs")

    __table_args__ = (
        Index("ix_validation_log_type_sev", "validation_type", "severity"),
        Index("ix_validation_log_date", "created_at"),
        CheckConstraint(
            f"severity IN ({', '.join(repr(e.value) for e in ValidationSeverity)})",
            name="ck_validation_log_severity",
        ),
        CheckConstraint(
            f"validation_type IN ({', '.join(repr(e.value) for e in ValidationType)})",
            name="ck_validation_log_type",
        ),
    )


# ╔══════════════════════════════════════════════════════════════════╗
# ║  11. ETF_COMPOSITIONS – Composición de ETFs por submotor        ║
# ╚══════════════════════════════════════════════════════════════════╝

class EtfComposition(Base):
    """
    Composición de ETFs por instrumento.
    Submotor aislado: JPMorgan y Goldman Sachs independientes.
    """
    __tablename__ = "etf_compositions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"), nullable=False)
    bank_code: Mapped[str] = mapped_column(String(50), nullable=False)
    # "jpmorgan", "goldman_sachs"
    report_date: Mapped[date] = mapped_column(Date, nullable=False)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    month: Mapped[int] = mapped_column(Integer, nullable=False)

    # ETF Instrumento
    etf_code: Mapped[str] = mapped_column(String(50), nullable=False)
    etf_name: Mapped[str] = mapped_column(String(500), nullable=False)
    isin: Mapped[Optional[str]] = mapped_column(String(12))

    # Valores
    quantity: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 6))
    market_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))
    weight_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 6))
    currency: Mapped[str] = mapped_column(String(10), nullable=False)
    market_value_usd: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))

    # Trazabilidad
    source_document_id: Mapped[Optional[int]] = mapped_column(ForeignKey("raw_documents.id"))
    loaded_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)

    __table_args__ = (
        UniqueConstraint(
            "account_id", "bank_code", "year", "month", "etf_code",
            name="uq_etf_comp_acct_bank_period_etf"
        ),
        Index("ix_etf_comp_bank_period", "bank_code", "year", "month"),
    )


# ╔══════════════════════════════════════════════════════════════════╗
# ║  12. CACHE_METADATA – Control de cache pre-calculado            ║
# ╚══════════════════════════════════════════════════════════════════╝

class CacheMetadata(Base):
    """
    Control de cache Parquet pre-calculados.
    Permite invalidación selectiva.
    """
    __tablename__ = "cache_metadata"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    cache_key: Mapped[str] = mapped_column(String(500), unique=True, nullable=False)
    # Ej: "summary_2025_all", "etf_jpmorgan_2025_q1"
    filepath: Mapped[str] = mapped_column(String(1000), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    data_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    # Hash de los datos fuente para detectar invalidación
    is_valid: Mapped[bool] = mapped_column(Boolean, default=True)
