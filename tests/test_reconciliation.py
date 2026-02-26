"""
Tests para calculations/reconciliation.py
"""

import pytest
from decimal import Decimal

from calculations.reconciliation import (
    reconcile_monthly,
    reconcile_by_instrument,
    ReconciliationStatus,
)


class TestReconcileMonthly:
    def test_exact_match(self):
        result = reconcile_monthly(
            daily_total=Decimal("1000000"),
            monthly_total=Decimal("1000000"),
            account_id=1, year=2025, month=6,
        )
        assert result.status == ReconciliationStatus.MATCHED
        assert result.difference == Decimal("0")

    def test_minor_diff(self):
        result = reconcile_monthly(
            daily_total=Decimal("1000000.005"),
            monthly_total=Decimal("1000000"),
            threshold_pct=Decimal("0.01"),
            account_id=1, year=2025, month=6,
        )
        assert result.status == ReconciliationStatus.MINOR_DIFF

    def test_major_diff(self):
        result = reconcile_monthly(
            daily_total=Decimal("1050000"),
            monthly_total=Decimal("1000000"),
            threshold_pct=Decimal("0.01"),
            account_id=1, year=2025, month=6,
        )
        assert result.status == ReconciliationStatus.MAJOR_DIFF
        assert result.difference == Decimal("50000")

    def test_missing_daily(self):
        result = reconcile_monthly(
            daily_total=None,
            monthly_total=Decimal("1000000"),
            account_id=1, year=2025, month=6,
        )
        assert result.status == ReconciliationStatus.MISSING_DAILY

    def test_missing_monthly(self):
        result = reconcile_monthly(
            daily_total=Decimal("1000000"),
            monthly_total=None,
            account_id=1, year=2025, month=6,
        )
        assert result.status == ReconciliationStatus.MISSING_MONTHLY


class TestReconcileByInstrument:
    def test_all_match(self):
        daily = [
            {"instrument_code": "AAPL", "market_value": "1000"},
            {"instrument_code": "MSFT", "market_value": "2000"},
        ]
        monthly = [
            {"instrument_code": "AAPL", "market_value": "1000"},
            {"instrument_code": "MSFT", "market_value": "2000"},
        ]
        diffs = reconcile_by_instrument(daily, monthly)
        assert len(diffs) == 0

    def test_difference_detected(self):
        daily = [{"instrument_code": "AAPL", "market_value": "1000"}]
        monthly = [{"instrument_code": "AAPL", "market_value": "1100"}]
        diffs = reconcile_by_instrument(daily, monthly)
        assert len(diffs) == 1
        assert diffs[0]["difference"] == Decimal("-100")

    def test_missing_in_monthly(self):
        daily = [
            {"instrument_code": "AAPL", "market_value": "1000"},
            {"instrument_code": "NEW", "market_value": "500"},
        ]
        monthly = [{"instrument_code": "AAPL", "market_value": "1000"}]
        diffs = reconcile_by_instrument(daily, monthly)
        assert len(diffs) == 1
        assert diffs[0]["instrument"] == "NEW"
        assert diffs[0]["in_monthly_only"] is False
