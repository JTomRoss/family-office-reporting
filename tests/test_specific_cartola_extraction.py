from decimal import Decimal
from datetime import date
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
    assert _as_decimal(mar.get("net_contributions")) == Decimal("-1435")
    assert _as_decimal(mar.get("utilidad")) == Decimal("-831817")

    current = result.qualitative_data.get("account_monthly_activity", [])
    assert len(current) == 1
    assert _as_decimal(current[0].get("net_contributions")) == Decimal("0")
    assert _as_decimal(current[0].get("utilidad")) == Decimal("606973")
    selected = result.balances.get("selected_portfolio", {})
    assert selected.get("portfolio") == "Portfolio02"
    assert _as_decimal(selected.get("asset_classes", {}).get("liquidity")) == Decimal("951473")
    assert _as_decimal(selected.get("asset_classes", {}).get("bonds")) == Decimal("53411634")
    assert _as_decimal(selected.get("asset_classes", {}).get("equities")) == Decimal("37633194")
    assert _as_decimal(selected.get("net_assets")) == Decimal("91996301")
    alloc = result.qualitative_data.get("asset_allocation", {})
    assert _as_decimal(alloc.get("Liquidity", {}).get("total")) == Decimal("951473")
    assert _as_decimal(alloc.get("Bonds", {}).get("total")) == Decimal("53411634")
    assert _as_decimal(alloc.get("Equities", {}).get("total")) == Decimal("37633194")


def test_ubs_suiza_statement_date_fallback_from_filename():
    parser = UBSSwitzerlandCustodyParser()
    stmt = parser._statement_date_from_filename(
        "202501 Boatview UBS SW (206-560552-02) 511UBS SW_P2.pdf"
    )
    assert stmt == date(2025, 1, 31)


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


def test_jpm_etf_keeps_proceeds_from_pending_sales_as_holding():
    parser = JPMorganEtfParser()
    result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name=parser.get_parser_name(),
        parser_version=parser.VERSION,
        source_file_hash="test-hash",
        bank_code="jpmorgan",
        currency="USD",
    )
    pages = [
        "\n".join(
            [
                "ACCT. U28375001",
                "Cash & Fixed Income Detail",
                "Price Quantity Value Original Cost",
                "PROCEEDS FROM PENDING SALES 1.00 50,000.00 50,000.00 50,000.00",
            ]
        )
    ]

    parser._extract_holdings(pages, result)
    rows = [r.data for r in result.rows if not r.data.get("is_total")]
    proceeds = next((r for r in rows if r.get("instrument") == "PROCEEDS FROM PENDING SALES"), None)
    assert proceeds is not None
    assert _as_decimal(proceeds.get("market_value")) == Decimal("50000.00")


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


def test_jpm_brokerage_keeps_change_investment_in_profit_for_security_transfers():
    parser = JPMorganBrokerageParser()
    row = parser._parse_account_activity_page(
        "\n".join(
            [
                "ACCT. E63535000",
                "Account Summary",
                "Accruals 12.06 12.40 0.34",
                "Market Value with Accruals 16,349.18 16,361.63",
                "Portfolio Activity",
                "Current Period Value Year-to-Date Value",
                "Beginning Market Value 16,337.12 14,076.19",
                "Net Contributions/Withdrawals (5,894.95) (7,018.61)",
                "Income & Distributions 12.11 3,396.70",
                "Change In Investment Value 5,894.95 5,894.95",
                "Ending Market Value 16,349.23 16,349.23",
            ]
        ),
        "E63535000",
    )
    assert row is not None
    assert _as_decimal(row["net_contributions"]) == Decimal("-5894.95")
    assert _as_decimal(row["change_investment"]) == Decimal("5894.95")
    assert _as_decimal(row["utilidad"]) == Decimal("5907.40")
    notes = row.get("interpretation_notes", [])
    assert not any("duplicar Net Contributions/Withdrawals" in note for note in notes)


def test_jpm_brokerage_single_value_uses_ytd_only_and_monthly_zero():
    parser = JPMorganBrokerageParser()
    row = parser._parse_account_activity_page(
        "\n".join(
            [
                "ACCT. E92671008",
                "Account Summary",
                "Portfolio Activity",
                "Current Period Value Year-to-Date Value",
                "Beginning Market Value 3.69 0.00",
                "Net Contributions/Withdrawals 0.00 0.00",
                "Income & Distributions 5.91",
                "Ending Market Value 3.69",
            ]
        ),
        "E92671008",
    )
    assert row is not None
    assert _as_decimal(row["net_contributions"]) == Decimal("0")
    assert _as_decimal(row["income_distributions"]) == Decimal("0")
    assert _as_decimal(row["income_distributions_ytd"]) == Decimal("5.91")
    assert _as_decimal(row["utilidad"]) == Decimal("0")
    notes = row.get("interpretation_notes", [])
    assert any("Income & Distributions mensual en blanco interpretado como 0" in note for note in notes)


def test_jpm_brokerage_single_net_contribution_value_stays_monthly():
    parser = JPMorganBrokerageParser()
    row = parser._parse_account_activity_page(
        "\n".join(
            [
                "ACCT. E63535000",
                "Account Summary",
                "Accruals 12.06 12.40 0.34",
                "Market Value with Accruals 16,349.18 16,361.63",
                "Portfolio Activity",
                "Current Period Value Year-to-Date Value",
                "Beginning Market Value 16,337.12 14,076.19",
                "Net Contributions/Withdrawals ($5,894.95)",
                "Income & Distributions 12.11 3,396.70",
                "Change In Investment Value 5,894.95 5,894.95",
                "Ending Market Value 16,349.23 16,349.23",
            ]
        ),
        "E63535000",
    )
    assert row is not None
    assert _as_decimal(row["net_contributions"]) == Decimal("-5894.95")
    assert row.get("net_contributions_ytd") is None
    assert _as_decimal(row["utilidad"]) == Decimal("5907.40")


def test_jpm_brokerage_reads_negative_net_contributions_with_minus_sign():
    parser = JPMorganBrokerageParser()
    row = parser._parse_account_activity_page(
        "\n".join(
            [
                "ACCT. E92671008",
                "Account Summary",
                "Accruals 2.22 0.00 (2.22)",
                "Market Value with Accruals 2.20 0.00",
                "Portfolio Activity",
                "Current Period Value Year-to-Date Value",
                "Beginning Market Value 2.20 0.00",
                "Net Contributions / Withdrawals -2.20 -2.20",
                "Income & Distributions 2.22 2.22",
                "Ending Market Value 0.00 0.00",
            ]
        ),
        "E92671008",
    )
    assert row is not None
    assert _as_decimal(row["net_contributions"]) == Decimal("-2.20")
    assert _as_decimal(row["net_contributions_ytd"]) == Decimal("-2.20")
    assert _as_decimal(row["utilidad"]) == Decimal("0.00")


def test_jpm_brokerage_reads_net_contributions_when_amounts_wrap_to_next_line():
    parser = JPMorganBrokerageParser()
    row = parser._parse_account_activity_page(
        "\n".join(
            [
                "ACCT. E37222008",
                "Account Summary",
                "Accruals 4,590.62 11,773.45 7,182.83",
                "Market Value with Accruals 4,055,259.59 2,027,944.29",
                "Portfolio Activity",
                "Current Period Value Year-to-Date Value",
                "Beginning Market Value 4,050,668.97 742,968.98",
                "Net Contributions / Withdrawals",
                "-2,038,883.00 1,274,776.69",
                "Income & Distributions 4,590.62 8,196.36",
                "Change In Investment Value -205.75 -205.83",
                "Ending Market Value 2,016,170.84 2,016,170.84",
            ]
        ),
        "E37222008",
    )
    assert row is not None
    assert _as_decimal(row["net_contributions"]) == Decimal("-2038883.00")
    assert _as_decimal(row["net_contributions_ytd"]) == Decimal("1274776.69")
    assert _as_decimal(row["change_investment"]) == Decimal("-205.75")
    assert _as_decimal(row["utilidad"]) == Decimal("11567.70")


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
    alloc_1412600 = monthly["1412600"].get("asset_allocation", {})
    assert _as_decimal(alloc_1412600.get("Cash, Deposits & Short Term", {}).get("ending")) == Decimal("2811109.86")
    assert _as_decimal(alloc_1412600.get("Fixed Income", {}).get("ending")) == Decimal("145720683.04")
    assert _as_decimal(alloc_1412600.get("Equities", {}).get("ending")) in {None, Decimal("0")}


def test_jpm_mandato_net_cash_with_parentheses_is_parsed():
    parser = JPMorganCustodyParser()
    result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name=parser.get_parser_name(),
        parser_version=parser.VERSION,
        source_file_hash="test-hash",
        bank_code="jpmorgan",
        currency="USD",
    )
    pages = [
        "\n".join(
            [
                "Account Number: 1412600",
                "Account Summary",
                "Portfolio Activity",
                "Beginning Market Value 100.00",
                "Net Cash Contributions / Withdrawals ($2,728,400.00)",
                "Income and Distributions 100.00",
                "Change in Investment Value 200.00",
                "Ending Market Value 300.00",
            ]
        )
    ]

    parser._extract_subaccount_summaries(pages, result)
    monthly = result.qualitative_data.get("account_monthly_activity", [])
    assert len(monthly) == 1
    assert monthly[0]["account_number"] == "1412600"
    assert _as_decimal(monthly[0]["net_contributions"]) == Decimal("-2728400.00")


def test_jpm_mandato_feb_2025_extracts_negative_net_cash_for_1412600():
    path = _cartola_path("202502 Boatview JPM NY Mandato (2600 y 3400).pdf")
    _require(path)

    result = JPMorganCustodyParser().safe_parse(path)
    assert result.is_success

    monthly = {
        row["account_number"]: row
        for row in result.qualitative_data.get("account_monthly_activity", [])
    }
    assert "1412600" in monthly
    assert _as_decimal(monthly["1412600"]["net_contributions"]) == Decimal("-3000000.00")
    assert _as_decimal(monthly["1412600"]["net_contributions_ytd"]) == Decimal("-3000000.00")


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
