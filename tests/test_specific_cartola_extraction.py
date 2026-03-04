from decimal import Decimal
from pathlib import Path

import pytest

from parsers.jpmorgan.brokerage import JPMorganBrokerageParser
from parsers.jpmorgan.custody import JPMorganCustodyParser
from parsers.jpmorgan.etf import JPMorganEtfParser
from parsers.bbh.custody import BBHCustodyParser
from parsers.goldman_sachs.custody import GoldmanSachsCustodyParser
from parsers.ubs.custody import UBSSwitzerlandCustodyParser
from parsers.ubs_miami.custody import UBSMiamiCustodyParser
from parsers.base import ParseResult, ParserStatus


def _cartola_path(filename: str) -> Path:
    return Path(__file__).resolve().parents[1] / "Documentos" / "Cartolas" / filename


def _require(path: Path) -> None:
    if not path.exists():
        pytest.skip(f"Fixture PDF not found: {path}")


def _as_decimal(value: str | None) -> Decimal | None:
    return Decimal(value) if value is not None else None


def test_ubs_suiza_uses_selected_portfolio_and_profit_fallback():
    path = _cartola_path("202512 Boatview UBS Suiza (Portfolio 1) - Mandato.pdf")
    _require(path)

    result = UBSSwitzerlandCustodyParser().safe_parse(path)
    assert result.is_success
    assert result.account_number == "206-560552-01"
    assert result.balances.get("selected_portfolio", {}).get("portfolio") == "Portfolio01"

    monthly = result.qualitative_data.get("account_monthly_activity", [])
    assert len(monthly) == 1
    row = monthly[0]
    assert _as_decimal(row.get("ending_value_with_accrual")) == Decimal("52974")
    assert _as_decimal(row.get("ending_value_without_accrual")) == Decimal("52974")
    assert _as_decimal(row.get("net_contributions")) == Decimal("-640")
    assert _as_decimal(row.get("utilidad")) == Decimal("96")


def test_ubs_suiza_quarterly_performance_emits_history_activity():
    path = _cartola_path("202512 Boatview UBS SW (206-560552-02) 511UBS SW_P2.pdf")
    _require(path)

    result = UBSSwitzerlandCustodyParser().safe_parse(path)
    assert result.is_success
    assert result.account_number == "206-560552-02"

    history = result.qualitative_data.get("account_monthly_activity_history", [])
    assert len(history) >= 12

    mar = next(
        (
            row for row in history
            if row.get("period_year") == 2025 and row.get("period_month") == 3
        ),
        None,
    )
    assert mar is not None
    assert _as_decimal(mar.get("net_contributions")) == Decimal("-1428")
    assert _as_decimal(mar.get("utilidad")) == Decimal("-831817")

    current = result.qualitative_data.get("account_monthly_activity", [])
    assert len(current) == 1
    assert _as_decimal(current[0].get("net_contributions")) == Decimal("0")
    assert _as_decimal(current[0].get("utilidad")) == Decimal("606973")


def test_jpm_etf_extracts_two_real_subaccounts():
    path = _cartola_path("20251231-statements-0007-ETF - JPMorgan.pdf")
    _require(path)

    result = JPMorganEtfParser().safe_parse(path)
    assert result.is_success
    assert result.account_number == "Varios"
    assert result.account_numbers == ["B99719001", "E31070007"]

    monthly = {
        row["account_number"]: row
        for row in result.qualitative_data.get("account_monthly_activity", [])
    }
    assert set(monthly.keys()) == {"B99719001", "E31070007"}
    assert _as_decimal(monthly["B99719001"]["ending_value_with_accrual"]) == Decimal("18885468.69")
    assert _as_decimal(monthly["E31070007"]["ending_value_with_accrual"]) == Decimal("13130284.90")


def test_jpm_brokerage_extracts_two_real_subaccounts():
    path = _cartola_path("20251231-statements-9001-Brokerage - JPMorgan.pdf")
    _require(path)

    result = JPMorganBrokerageParser().safe_parse(path)
    assert result.is_success
    assert result.account_number == "Varios"
    assert result.account_numbers == ["B99719001", "E31070007"]

    monthly = {
        row["account_number"]: row
        for row in result.qualitative_data.get("account_monthly_activity", [])
    }
    assert set(monthly.keys()) == {"B99719001", "E31070007"}


def test_jpm_mandato_extracts_three_subaccounts():
    path = _cartola_path("20251231-statements-2600-Mandato - JPMorgan.pdf")
    _require(path)

    result = JPMorganCustodyParser().safe_parse(path)
    assert result.is_success
    assert result.account_number == "Varios"
    assert set(result.account_numbers) == {"1179200", "1412600", "1483400"}

    monthly = {
        row["account_number"]: row
        for row in result.qualitative_data.get("account_monthly_activity", [])
    }
    assert set(monthly.keys()) == {"1179200", "1412600", "1483400"}
    assert _as_decimal(monthly["1412600"]["ending_value_with_accrual"]) == Decimal("148531792.87")
    assert _as_decimal(monthly["1483400"]["ending_value_with_accrual"]) == Decimal("188557765.96")


def test_bbh_mandato_extracts_real_account_number():
    path = _cartola_path("202512 Boatview - Mandato - BBH.pdf")
    _require(path)

    result = BBHCustodyParser().safe_parse(path)
    assert result.is_success
    assert result.account_number in {"7085", "7101"}
    assert result.statement_date is not None
    monthly = result.qualitative_data.get("account_monthly_activity", [])
    assert len(monthly) == 1
    assert monthly[0]["account_number"] in {"7085", "7101"}
    assert _as_decimal(monthly[0]["utilidad"]) is not None
    assert _as_decimal(monthly[0]["net_contributions"]) is not None


def test_goldman_mandato_has_statement_dates_and_balances():
    path = _cartola_path("202512 Boatview - Mandato - GoldmanSachs.pdf")
    _require(path)

    result = GoldmanSachsCustodyParser().safe_parse(path)
    assert result.is_success
    assert result.account_number == "451-9"
    assert result.statement_date is not None
    assert result.period_start is not None
    assert result.period_end is not None
    assert result.closing_balance is not None
    monthly = result.qualitative_data.get("account_monthly_activity", [])
    assert len(monthly) == 1
    assert monthly[0]["account_number"] == "451-9"
    assert _as_decimal(monthly[0]["net_contributions"]) == Decimal("0.00")
    assert _as_decimal(monthly[0]["utilidad"]) == Decimal("1106908.06")


def test_ubs_miami_emits_monthly_activity_contract():
    path = _cartola_path("202512 Boatview UBS Miami (432) - Mandato.pdf")
    _require(path)

    result = UBSMiamiCustodyParser().safe_parse(path)
    assert result.is_success
    assert result.statement_date is not None
    assert result.opening_balance is not None
    assert result.closing_balance is not None

    monthly = result.qualitative_data.get("account_monthly_activity", [])
    assert len(monthly) == 1
    row = monthly[0]
    assert row["account_number"]
    assert _as_decimal(row["net_contributions"]) is not None
    assert _as_decimal(row["utilidad"]) is not None


def test_ubs_miami_change_market_preserves_negative_sign():
    parser = UBSMiamiCustodyParser()
    result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name=parser.get_parser_name(),
        parser_version=parser.VERSION,
        source_file_hash="test-hash",
        bank_code="ubs_miami",
        currency="USD",
    )
    result.account_number = "3J 00432 P1"
    result.opening_balance = Decimal("83781797.36")
    result.closing_balance = Decimal("83066420.41")

    sample_page = """Change in the value of your account
Opening account value $83,781,797.36 $83,450,851.99
Withdrawals and fees, including investments transferred out -3,757.73 -43,608.16
Dividend and interest income 0.00 47,150.94
Change in value of accrued interest 0.00 525.72
Change in market value -711,619.22 1,217,284.77
Closing account value $83,066,420.41 $92,398,627.15
"""
    parser._extract_change_in_value([sample_page], result)
    parser._emit_account_monthly_activity(result)

    monthly = result.qualitative_data.get("account_monthly_activity", [])
    assert len(monthly) == 1
    row = monthly[0]
    assert _as_decimal(row["net_contributions"]) == Decimal("-3757.73")
    # utilidad = 0 + 0 + (-711,619.22)
    assert _as_decimal(row["utilidad"]) == Decimal("-711619.22")
