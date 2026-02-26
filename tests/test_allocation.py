"""
Tests para calculations/allocation.py
"""

import pytest
from decimal import Decimal

from calculations.allocation import (
    weight_pct,
    validate_allocation_sums_to_100,
    etf_composition_check,
    mandate_allocation_pct,
)


class TestWeightPct:
    def test_basic(self):
        result = weight_pct(Decimal("250"), Decimal("1000"))
        assert result == Decimal("25.000000")

    def test_zero_total(self):
        assert weight_pct(Decimal("100"), Decimal("0")) is None

    def test_full_weight(self):
        result = weight_pct(Decimal("1000"), Decimal("1000"))
        assert result == Decimal("100.000000")


class TestAllocationSum:
    def test_exact_100(self):
        weights = [Decimal("30"), Decimal("40"), Decimal("30")]
        is_valid, diff = validate_allocation_sums_to_100(weights)
        assert is_valid
        assert diff == Decimal("0")

    def test_within_tolerance(self):
        weights = [Decimal("33.34"), Decimal("33.33"), Decimal("33.33")]
        is_valid, diff = validate_allocation_sums_to_100(weights)
        assert is_valid

    def test_outside_tolerance(self):
        weights = [Decimal("30"), Decimal("30"), Decimal("30")]
        is_valid, diff = validate_allocation_sums_to_100(weights)
        assert not is_valid
        assert diff == Decimal("10")


class TestEtfCompositionCheck:
    def test_matching(self):
        values = [Decimal("100"), Decimal("200"), Decimal("300")]
        is_valid, diff = etf_composition_check(values, Decimal("600"))
        assert is_valid
        assert diff == Decimal("0")

    def test_mismatch(self):
        values = [Decimal("100"), Decimal("200")]
        is_valid, diff = etf_composition_check(values, Decimal("400"))
        assert not is_valid
        assert diff == Decimal("100")

    def test_empty(self):
        is_valid, diff = etf_composition_check([], Decimal("0"))
        assert is_valid


class TestMandateAllocation:
    def test_basic(self):
        result = mandate_allocation_pct(Decimal("500000"), Decimal("2000000"))
        assert result == Decimal("25.000000")

    def test_zero_portfolio(self):
        assert mandate_allocation_pct(Decimal("100"), Decimal("0")) is None
