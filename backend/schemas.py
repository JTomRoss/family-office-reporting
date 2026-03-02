"""
FO Reporting – Pydantic Schemas (API contracts).

Estos schemas definen la interfaz entre Frontend y Backend.
La UI nunca toca modelos SQLAlchemy directamente.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from pydantic import BaseModel, Field


# ═══════════════════════════════════════════════════════════════════
# ACCOUNTS
# ═══════════════════════════════════════════════════════════════════

class AccountBase(BaseModel):
    account_number: str
    identification_number: Optional[str] = None
    bank_code: str
    bank_name: str
    account_type: str
    entity_name: str
    entity_type: str
    currency: str
    country: Optional[str] = ""
    mandate_type: Optional[str] = None
    person_name: Optional[str] = None
    internal_code: Optional[str] = None
    is_active: bool = True


class AccountCreate(AccountBase):
    pass


class AccountResponse(AccountBase):
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


# ═══════════════════════════════════════════════════════════════════
# DOCUMENTS
# ═══════════════════════════════════════════════════════════════════

class DocumentUploadResponse(BaseModel):
    id: int
    filename: str
    sha256_hash: str
    file_type: str
    status: str
    is_duplicate: bool = False
    message: str = ""
    existing_metadata: Optional[dict] = None  # Metadata del doc existente si es duplicado


class DocumentListItem(BaseModel):
    id: int
    filename: str
    file_type: str
    bank_code: Optional[str]
    period_year: Optional[int]
    period_month: Optional[int]
    status: str
    uploaded_at: datetime

    model_config = {"from_attributes": True}


class ProcessingProgress(BaseModel):
    document_id: int
    filename: str
    progress_pct: int = Field(ge=0, le=100)
    status: str
    message: str = ""


# ═══════════════════════════════════════════════════════════════════
# MONTHLY CLOSINGS
# ═══════════════════════════════════════════════════════════════════

class MonthlyClosingResponse(BaseModel):
    id: int
    account_id: int
    closing_date: date
    year: int
    month: int
    total_assets: Optional[Decimal]
    net_value: Optional[Decimal]
    currency: str
    net_value_usd: Optional[Decimal]
    income: Optional[Decimal]
    change_in_value: Optional[Decimal]
    total_return: Optional[Decimal]

    model_config = {"from_attributes": True}


# ═══════════════════════════════════════════════════════════════════
# RECONCILIATION
# ═══════════════════════════════════════════════════════════════════

class ReconciliationResponse(BaseModel):
    id: int
    account_id: int
    year: int
    month: int
    daily_total: Optional[Decimal]
    monthly_total: Optional[Decimal]
    difference: Optional[Decimal]
    difference_pct: Optional[Decimal]
    status: str
    currency: str
    resolved: bool

    model_config = {"from_attributes": True}


# ═══════════════════════════════════════════════════════════════════
# SUMMARY / DASHBOARD
# ═══════════════════════════════════════════════════════════════════

class FilterParams(BaseModel):
    """Parámetros de filtro comunes para todas las pestañas."""
    years: list[int] = []
    months: list[int] = []
    bank_codes: list[str] = []
    entity_names: list[str] = []
    account_types: list[str] = []
    currencies: list[str] = []
    fecha: Optional[str] = None  # "YYYY-MM" para filtro ETF


class SummaryRow(BaseModel):
    """Fila de tabla resumen."""
    entity_name: str
    bank_code: str
    account_number: str
    currency: str
    month_values: dict[str, Optional[Decimal]]  # "2025-01" → valor
    ytd_return: Optional[Decimal]


class SummaryResponse(BaseModel):
    """Respuesta completa de pestaña Resumen."""
    rows: list[SummaryRow]
    totals: dict[str, Optional[Decimal]]
    filter_options: dict[str, list[str]]
    active_filters: FilterParams


# ═══════════════════════════════════════════════════════════════════
# ETF
# ═══════════════════════════════════════════════════════════════════

class EtfCompositionRow(BaseModel):
    etf_code: str
    etf_name: str
    bank_code: str
    quantity: Optional[Decimal]
    market_value: Optional[Decimal]
    weight_pct: Optional[Decimal]
    currency: str


class EtfSummaryResponse(BaseModel):
    compositions: list[EtfCompositionRow]
    total_value: Optional[Decimal]
    by_bank: dict[str, Decimal]  # bank_code → total


# ═══════════════════════════════════════════════════════════════════
# PARSERS
# ═══════════════════════════════════════════════════════════════════

class ParserInfo(BaseModel):
    bank_code: str
    account_type: str
    class_name: str
    version: str
    description: str
    supported_extensions: list[str]


# ═══════════════════════════════════════════════════════════════════
# VALIDATION LOGS
# ═══════════════════════════════════════════════════════════════════

class ValidationLogResponse(BaseModel):
    id: int
    validation_type: str
    severity: str
    message: str
    created_at: datetime
    source_module: Optional[str]

    model_config = {"from_attributes": True}


# ═══════════════════════════════════════════════════════════════════
# HEALTH
# ═══════════════════════════════════════════════════════════════════

class HealthResponse(BaseModel):
    status: str = "ok"
    version: str
    database: str = "connected"
    parsers_loaded: int = 0
    git_hash: Optional[str] = None
