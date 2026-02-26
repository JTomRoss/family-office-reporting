"""
Tests obligatorios para calculations/profit.py

Cubre:
- profit_jpm_etf (fórmula JPMorgan ETF)
- profit_ubs_switzerland (fórmula UBS Suiza)
- monthly_return_pct
- ytd_return_pct
- total_portfolio_value

Incluye property-based tests con hypothesis.
"""

import pytest
from decimal import Decimal
from hypothesis import given, strategies as st

from calculations.profit import (
    profit_jpm_etf,
    profit_ubs_switzerland,
    monthly_return_pct,
    ytd_return_pct,
    total_portfolio_value,
)


# ═══════════════════════════════════════════════════════════════════
# PROFIT JPM ETF
# Fórmula: Income + Change_in_Value + (accrual_mes - accrual_mes_prev)
# ═══════════════════════════════════════════════════════════════════

class TestProfitJpmEtf:
    def test_basic_positive(self):
        result = profit_jpm_etf(
            income=Decimal("1000"),
            change_in_value=Decimal("500"),
            accrual_current=Decimal("200"),
            accrual_previous=Decimal("150"),
        )
        # 1000 + 500 + (200 - 150) = 1550
        assert result == Decimal("1550")

    def test_negative_change(self):
        result = profit_jpm_etf(
            income=Decimal("100"),
            change_in_value=Decimal("-300"),
            accrual_current=Decimal("50"),
            accrual_previous=Decimal("50"),
        )
        # 100 + (-300) + (50 - 50) = -200
        assert result == Decimal("-200")

    def test_zero_accrual_change(self):
        result = profit_jpm_etf(
            income=Decimal("500"),
            change_in_value=Decimal("200"),
            accrual_current=Decimal("100"),
            accrual_previous=Decimal("100"),
        )
        # 500 + 200 + 0 = 700
        assert result == Decimal("700")

    def test_all_zeros(self):
        result = profit_jpm_etf(
            income=Decimal("0"),
            change_in_value=Decimal("0"),
            accrual_current=Decimal("0"),
            accrual_previous=Decimal("0"),
        )
        assert result == Decimal("0")

    def test_large_numbers(self):
        result = profit_jpm_etf(
            income=Decimal("1000000.50"),
            change_in_value=Decimal("2500000.25"),
            accrual_current=Decimal("50000.75"),
            accrual_previous=Decimal("45000.50"),
        )
        # 1000000.50 + 2500000.25 + (50000.75 - 45000.50) = 3505001.00
        assert result == Decimal("3505001.00")

    def test_negative_accrual_change(self):
        result = profit_jpm_etf(
            income=Decimal("100"),
            change_in_value=Decimal("0"),
            accrual_current=Decimal("20"),
            accrual_previous=Decimal("80"),
        )
        # 100 + 0 + (20 - 80) = 40
        assert result == Decimal("40")

    @given(
        income=st.decimals(min_value=-1_000_000, max_value=1_000_000, places=2),
        civ=st.decimals(min_value=-1_000_000, max_value=1_000_000, places=2),
        acc_curr=st.decimals(min_value=0, max_value=1_000_000, places=2),
        acc_prev=st.decimals(min_value=0, max_value=1_000_000, places=2),
    )
    def test_property_matches_formula(self, income, civ, acc_curr, acc_prev):
        """Property: resultado siempre == income + civ + (acc_curr - acc_prev)"""
        result = profit_jpm_etf(income, civ, acc_curr, acc_prev)
        expected = income + civ + (acc_curr - acc_prev)
        assert result == expected


# ═══════════════════════════════════════════════════════════════════
# PROFIT UBS SUIZA
# Fórmula: total_assets_mes - movimientos_mes - total_assets_mes_prev
# ═══════════════════════════════════════════════════════════════════

class TestProfitUbsSwitzerland:
    def test_basic_positive(self):
        result = profit_ubs_switzerland(
            total_assets_current=Decimal("1100000"),
            movements=Decimal("50000"),
            total_assets_previous=Decimal("1000000"),
        )
        # 1100000 - 50000 - 1000000 = 50000
        assert result == Decimal("50000")

    def test_negative_profit(self):
        result = profit_ubs_switzerland(
            total_assets_current=Decimal("900000"),
            movements=Decimal("0"),
            total_assets_previous=Decimal("1000000"),
        )
        # 900000 - 0 - 1000000 = -100000
        assert result == Decimal("-100000")

    def test_with_withdrawals(self):
        """Retiros son movimientos negativos."""
        result = profit_ubs_switzerland(
            total_assets_current=Decimal("950000"),
            movements=Decimal("-100000"),  # Retiro de 100k
            total_assets_previous=Decimal("1000000"),
        )
        # 950000 - (-100000) - 1000000 = 50000
        assert result == Decimal("50000")

    def test_all_zeros(self):
        result = profit_ubs_switzerland(
            total_assets_current=Decimal("0"),
            movements=Decimal("0"),
            total_assets_previous=Decimal("0"),
        )
        assert result == Decimal("0")

    def test_no_movement(self):
        result = profit_ubs_switzerland(
            total_assets_current=Decimal("1050000"),
            movements=Decimal("0"),
            total_assets_previous=Decimal("1000000"),
        )
        # 1050000 - 0 - 1000000 = 50000
        assert result == Decimal("50000")

    @given(
        current=st.decimals(min_value=0, max_value=100_000_000, places=2),
        movements=st.decimals(min_value=-10_000_000, max_value=10_000_000, places=2),
        previous=st.decimals(min_value=0, max_value=100_000_000, places=2),
    )
    def test_property_matches_formula(self, current, movements, previous):
        result = profit_ubs_switzerland(current, movements, previous)
        expected = current - movements - previous
        assert result == expected


# ═══════════════════════════════════════════════════════════════════
# MONTHLY RETURN
# ═══════════════════════════════════════════════════════════════════

class TestMonthlyReturn:
    def test_positive_return(self):
        result = monthly_return_pct(
            profit=Decimal("50000"),
            total_assets_previous=Decimal("1000000"),
        )
        assert result == Decimal("5.000000")

    def test_negative_return(self):
        result = monthly_return_pct(
            profit=Decimal("-20000"),
            total_assets_previous=Decimal("1000000"),
        )
        assert result == Decimal("-2.000000")

    def test_zero_base_returns_none(self):
        result = monthly_return_pct(
            profit=Decimal("100"),
            total_assets_previous=Decimal("0"),
        )
        assert result is None

    def test_zero_profit(self):
        result = monthly_return_pct(
            profit=Decimal("0"),
            total_assets_previous=Decimal("1000000"),
        )
        assert result == Decimal("0")


# ═══════════════════════════════════════════════════════════════════
# YTD RETURN (chain-linking)
# ═══════════════════════════════════════════════════════════════════

class TestYtdReturn:
    def test_single_month(self):
        result = ytd_return_pct([Decimal("5")])
        assert result == Decimal("5")

    def test_two_months_compound(self):
        result = ytd_return_pct([Decimal("10"), Decimal("10")])
        # (1.10 * 1.10 - 1) * 100 = 21.00
        assert result == Decimal("21.00")

    def test_empty_list(self):
        result = ytd_return_pct([])
        assert result == Decimal("0")

    def test_mixed_positive_negative(self):
        result = ytd_return_pct([Decimal("10"), Decimal("-5")])
        # (1.10 * 0.95 - 1) * 100 = 4.50
        assert result == Decimal("4.50")

    def test_twelve_months_flat(self):
        """12 meses de 1% → compuesto ≈ 12.68%"""
        returns = [Decimal("1")] * 12
        result = ytd_return_pct(returns)
        assert Decimal("12.6") < result < Decimal("12.7")


# ═══════════════════════════════════════════════════════════════════
# TOTAL PORTFOLIO VALUE
# ═══════════════════════════════════════════════════════════════════

class TestTotalPortfolioValue:
    def test_basic_sum(self):
        values = [Decimal("100"), Decimal("200"), Decimal("300")]
        assert total_portfolio_value(values) == Decimal("600")

    def test_empty(self):
        assert total_portfolio_value([]) == Decimal("0")

    def test_single(self):
        assert total_portfolio_value([Decimal("42.50")]) == Decimal("42.50")
