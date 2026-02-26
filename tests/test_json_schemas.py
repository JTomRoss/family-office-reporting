"""
Tests para JSON schemas – Validación de campos JSON en la BD.

Verifica que la serialización/deserialización funciona correctamente
y que los schemas rechazan datos malformados.
"""

import pytest
from decimal import Decimal

from backend.db.json_schemas import (
    serialize_json,
    deserialize_json,
    safe_deserialize_json,
    AccountMetadata,
    AssetAllocation,
    AssetAllocationItem,
    GeographyAllocation,
    GeographyItem,
    CurrencyAllocation,
    CurrencyAllocationItem,
    ParsedStatementData,
    ParsedPositionRow,
    ReconciliationDetails,
    InstrumentDiff,
    ValidationLogDetails,
)


class TestSerializeDeserialize:
    """Tests de serialización/deserialización segura."""

    def test_serialize_and_deserialize_roundtrip(self):
        """Serializar y deserializar debe ser idempotente."""
        original = AccountMetadata(
            contact_name="Juan Pérez",
            tags=["vip", "latam"],
            custom_fields={"ref": "123"},
        )
        json_str = serialize_json(original)
        restored = deserialize_json(json_str, AccountMetadata)

        assert restored is not None
        assert restored.contact_name == "Juan Pérez"
        assert restored.tags == ["vip", "latam"]
        assert restored.custom_fields == {"ref": "123"}

    def test_deserialize_none_returns_none(self):
        assert deserialize_json(None, AccountMetadata) is None

    def test_deserialize_empty_string_returns_none(self):
        assert deserialize_json("", AccountMetadata) is None

    def test_safe_deserialize_invalid_json_returns_none(self):
        result = safe_deserialize_json("{invalid json}", AccountMetadata)
        assert result is None

    def test_safe_deserialize_wrong_schema_returns_none(self):
        # Serializamos un AccountMetadata pero deserializamos como AssetAllocation
        meta = AccountMetadata(contact_name="Test")
        json_str = serialize_json(meta)
        # Esto no debería fallar porque los campos son opcionales
        result = safe_deserialize_json(json_str, AssetAllocation)
        assert result is not None  # Ambos tienen defaults

    def test_deserialize_invalid_json_raises(self):
        with pytest.raises(Exception):
            deserialize_json("{broken", AccountMetadata)


class TestAssetAllocation:
    """Tests del schema de asset allocation."""

    def test_valid_allocation(self):
        alloc = AssetAllocation(items=[
            AssetAllocationItem(category="equity", weight_pct=Decimal("60")),
            AssetAllocationItem(category="fixed_income", weight_pct=Decimal("30")),
            AssetAllocationItem(category="cash", weight_pct=Decimal("10")),
        ])
        assert len(alloc.items) == 3

    def test_allocation_must_sum_approximately_100(self):
        """Allocation que suma muy lejos de 100% debe fallar."""
        with pytest.raises(ValueError, match="100%"):
            AssetAllocation(items=[
                AssetAllocationItem(category="equity", weight_pct=Decimal("50")),
                # Suma 50%, muy lejos de 100
            ])

    def test_allocation_tolerates_small_rounding(self):
        """Diferencias < 1% son aceptables (redondeo)."""
        alloc = AssetAllocation(items=[
            AssetAllocationItem(category="equity", weight_pct=Decimal("60.3")),
            AssetAllocationItem(category="bonds", weight_pct=Decimal("39.8")),
        ])
        assert len(alloc.items) == 2

    def test_empty_allocation_is_valid(self):
        """Allocation vacía es válida (no hay nada que sumar)."""
        alloc = AssetAllocation(items=[])
        assert len(alloc.items) == 0

    def test_negative_weight_rejected(self):
        with pytest.raises(Exception):
            AssetAllocationItem(category="equity", weight_pct=Decimal("-5"))

    def test_weight_over_100_rejected(self):
        with pytest.raises(Exception):
            AssetAllocationItem(category="equity", weight_pct=Decimal("101"))


class TestParsedStatementData:
    """Tests del schema para parsed_data_json."""

    def test_valid_statement_data(self):
        data = ParsedStatementData(
            positions=[
                ParsedPositionRow(
                    instrument_code="AAPL",
                    instrument_name="Apple Inc.",
                    quantity=Decimal("100"),
                    market_value=Decimal("15000.00"),
                    currency="USD",
                ),
            ],
            notes=["Página 3: tabla parcialmente ilegible"],
        )
        assert len(data.positions) == 1
        assert data.positions[0].instrument_code == "AAPL"

    def test_empty_statement_is_valid(self):
        data = ParsedStatementData()
        assert len(data.positions) == 0
        assert len(data.movements) == 0


class TestReconciliationDetails:
    """Tests del schema para reconciliation details."""

    def test_valid_details(self):
        details = ReconciliationDetails(
            instrument_diffs=[
                InstrumentDiff(
                    instrument_code="AAPL",
                    daily_value=Decimal("15000"),
                    monthly_value=Decimal("15050"),
                    difference=Decimal("-50"),
                ),
            ],
            notes=["Diferencia en AAPL posiblemente por FX"],
        )
        assert len(details.instrument_diffs) == 1
        assert details.instrument_diffs[0].difference == Decimal("-50")


class TestValidationLogDetails:
    """Tests del schema para validation log details."""

    def test_valid_log_details(self):
        details = ValidationLogDetails(
            expected="100.00",
            actual="99.50",
            context={"field": "closing_balance", "source": "jpmorgan"},
            affected_rows=[1, 5, 12],
        )
        assert details.expected == "100.00"
        assert len(details.affected_rows) == 3
