"""
FO Reporting – Schemas de validación para campos JSON en la BD.

PROBLEMA QUE RESUELVE:
Los campos *_json en los modelos SQLAlchemy (metadata_json, asset_allocation_json,
parsed_data_json, etc.) son Text libre. Sin validación, cada parser puede meter
una estructura distinta → bugs silenciosos cuando la UI o cálculos los leen.

SOLUCIÓN:
Pydantic models que validan la estructura ANTES de serializar a JSON.
Funciones serialize() / deserialize() que siempre pasan por validación.

USO:
    # Guardar
    alloc = AssetAllocation(items=[AssetAllocationItem(category="equity", weight_pct=Decimal("60"))])
    model.asset_allocation_json = serialize_json(alloc)

    # Leer
    alloc = deserialize_json(model.asset_allocation_json, AssetAllocation)
"""

import json
from decimal import Decimal
from typing import Optional, TypeVar, Type
from pydantic import BaseModel, Field, model_validator


T = TypeVar("T", bound=BaseModel)


# ═══════════════════════════════════════════════════════════════════
# SERIALIZACIÓN / DESERIALIZACIÓN SEGURA
# ═══════════════════════════════════════════════════════════════════

def serialize_json(model: BaseModel) -> str:
    """Serializa un Pydantic model a JSON string para guardar en BD."""
    return model.model_dump_json()


def deserialize_json(json_str: Optional[str], schema: Type[T]) -> Optional[T]:
    """
    Deserializa JSON string de BD a Pydantic model con validación.

    Returns:
        Instancia validada del schema, o None si json_str es None/vacío.

    Raises:
        pydantic.ValidationError si el JSON no cumple el schema.
    """
    if not json_str:
        return None
    return schema.model_validate_json(json_str)


def safe_deserialize_json(json_str: Optional[str], schema: Type[T]) -> Optional[T]:
    """
    Como deserialize_json pero NO lanza excepción.
    Retorna None si la validación falla (útil para datos legacy).
    """
    if not json_str:
        return None
    try:
        return schema.model_validate_json(json_str)
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════════
# ACCOUNT METADATA
# ═══════════════════════════════════════════════════════════════════

class AccountMetadata(BaseModel):
    """Schema para Account.metadata_json"""
    contact_name: Optional[str] = None
    contact_email: Optional[str] = None
    notes: Optional[str] = None
    tags: list[str] = Field(default_factory=list)
    custom_fields: dict[str, str] = Field(default_factory=dict)


# ═══════════════════════════════════════════════════════════════════
# ASSET ALLOCATION (MonthlyClosing)
# ═══════════════════════════════════════════════════════════════════

class AssetAllocationItem(BaseModel):
    """Una categoría de asset allocation."""
    category: str  # "equity", "fixed_income", "cash", "alternatives", "other"
    weight_pct: Decimal = Field(ge=Decimal("0"), le=Decimal("100"))
    market_value: Optional[Decimal] = None
    currency: str = "USD"


class AssetAllocation(BaseModel):
    """Schema para MonthlyClosing.asset_allocation_json"""
    items: list[AssetAllocationItem] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_total_weight(self):
        total = sum(item.weight_pct for item in self.items)
        if self.items and abs(total - Decimal("100")) > Decimal("1"):
            raise ValueError(
                f"Asset allocation suma {total}%, debería ser ~100%"
            )
        return self


# ═══════════════════════════════════════════════════════════════════
# GEOGRAPHY ALLOCATION (MonthlyClosing)
# ═══════════════════════════════════════════════════════════════════

class GeographyItem(BaseModel):
    """Una región geográfica."""
    region: str  # "North America", "Europe", "Asia Pacific", etc.
    weight_pct: Decimal = Field(ge=Decimal("0"), le=Decimal("100"))
    market_value: Optional[Decimal] = None


class GeographyAllocation(BaseModel):
    """Schema para MonthlyClosing.geography_json"""
    items: list[GeographyItem] = Field(default_factory=list)


# ═══════════════════════════════════════════════════════════════════
# CURRENCY ALLOCATION (MonthlyClosing)
# ═══════════════════════════════════════════════════════════════════

class CurrencyAllocationItem(BaseModel):
    """Una moneda en la composición."""
    currency: str  # "USD", "EUR", "CHF", "CLP"
    weight_pct: Decimal = Field(ge=Decimal("0"), le=Decimal("100"))
    market_value: Optional[Decimal] = None


class CurrencyAllocation(BaseModel):
    """Schema para MonthlyClosing.currency_allocation_json"""
    items: list[CurrencyAllocationItem] = Field(default_factory=list)


# ═══════════════════════════════════════════════════════════════════
# PARSED DATA (ParsedStatement)
# ═══════════════════════════════════════════════════════════════════

class ParsedPositionRow(BaseModel):
    """Una posición extraída de un statement."""
    instrument_code: str
    instrument_name: Optional[str] = None
    quantity: Optional[Decimal] = None
    market_value: Optional[Decimal] = None
    currency: str = "USD"
    weight_pct: Optional[Decimal] = None


class ParsedMovementRow(BaseModel):
    """Un movimiento extraído de un statement."""
    date: str  # ISO format
    description: Optional[str] = None
    movement_type: Optional[str] = None
    amount: Optional[Decimal] = None
    currency: str = "USD"


class ParsedStatementData(BaseModel):
    """Schema para ParsedStatement.parsed_data_json"""
    positions: list[ParsedPositionRow] = Field(default_factory=list)
    movements: list[ParsedMovementRow] = Field(default_factory=list)
    summary: dict[str, Optional[Decimal]] = Field(default_factory=dict)
    raw_tables: list[list[str]] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


# ═══════════════════════════════════════════════════════════════════
# RECONCILIATION DETAILS
# ═══════════════════════════════════════════════════════════════════

class InstrumentDiff(BaseModel):
    """Diferencia por instrumento en reconciliación."""
    instrument_code: str
    instrument_name: Optional[str] = None
    daily_value: Optional[Decimal] = None
    monthly_value: Optional[Decimal] = None
    difference: Optional[Decimal] = None
    note: Optional[str] = None


class ReconciliationDetails(BaseModel):
    """Schema para Reconciliation.details_json"""
    instrument_diffs: list[InstrumentDiff] = Field(default_factory=list)
    calculation_method: str = "standard"
    notes: list[str] = Field(default_factory=list)


# ═══════════════════════════════════════════════════════════════════
# VALIDATION LOG DETAILS
# ═══════════════════════════════════════════════════════════════════

class ValidationLogDetails(BaseModel):
    """Schema para ValidationLog.details_json"""
    expected: Optional[str] = None
    actual: Optional[str] = None
    context: dict[str, str] = Field(default_factory=dict)
    affected_rows: list[int] = Field(default_factory=list)
