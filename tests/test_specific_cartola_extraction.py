from decimal import Decimal
from datetime import date
from pathlib import Path

import pytest

from parsers.jpmorgan.brokerage import JPMorganBrokerageParser
from parsers.jpmorgan.custody import JPMorganCustodyParser
from parsers.jpmorgan.etf import JPMorganEtfParser
from parsers.bbh.custody import BBHCustodyParser
from parsers.goldman_sachs.custody import GoldmanSachsCustodyParser
from parsers.goldman_sachs.etf import GoldmanSachsEtfParser
from parsers.ubs.custody import UBSSwitzerlandCustodyParser
from parsers.ubs_miami.custody import UBSMiamiCustodyParser
from parsers.base import ParseResult, ParserStatus


def _cartola_path(filename: str) -> Path:
    return Path(__file__).resolve().parents[1] / "Documentos" / "Cartolas" / filename


def _raw_cartola_path(filename: str) -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "raw" / "ubs" / "pdf_cartola" / filename


def _goldman_raw_cartola_path(filename: str) -> Path:
    return (
        Path(__file__).resolve().parents[1]
        / "data"
        / "raw"
        / "goldman_sachs"
        / "pdf_cartola"
        / filename
    )


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


def test_ubs_suiza_jan_2025_boatview_portfolio_02_keeps_zero_closing_balance():
    path = _raw_cartola_path("202501 Boatview UBS SW (206-560552-02) 511UBS SW_P2.pdf")
    _require(path)

    result = UBSSwitzerlandCustodyParser().safe_parse(path)
    assert result.is_success
    assert result.account_number == "206-560552-02"
    assert result.closing_balance == Decimal("0")

    selected = result.balances.get("selected_portfolio", {})
    assert selected.get("portfolio") == "Portfolio02"
    assert _as_decimal(selected.get("net_assets")) == Decimal("0")

    monthly = result.qualitative_data.get("account_monthly_activity", [])
    assert len(monthly) == 1
    assert _as_decimal(monthly[0].get("ending_value_with_accrual")) == Decimal("0")


@pytest.mark.parametrize(
    ("filename", "expected_liquidity"),
    [
        ("202505 Boatview UBS SW (206-560552-01) 511UBS SW BR (60F)_P1.pdf", Decimal("54236")),
        ("202508 Boatview UBS SW (206-560552-01) 511UBS SW BR (60F) P1.pdf", Decimal("53977")),
        ("202511 Boatview UBS SW (206-560552-01) 511UBS SW BR (60F) P1.pdf", Decimal("53646")),
    ],
)
def test_ubs_suiza_portfolio_01_liquidity_row_keeps_total_column(filename, expected_liquidity):
    path = _raw_cartola_path(filename)
    _require(path)

    result = UBSSwitzerlandCustodyParser().safe_parse(path)
    assert result.is_success
    assert result.account_number == "206-560552-01"
    assert result.closing_balance == expected_liquidity

    selected = result.balances.get("selected_portfolio", {})
    assert selected.get("portfolio") == "Portfolio01"
    assert _as_decimal(selected.get("asset_classes", {}).get("liquidity")) == expected_liquidity

    alloc = result.qualitative_data.get("asset_allocation", {})
    assert _as_decimal(alloc.get("Liquidity", {}).get("total")) == expected_liquidity


@pytest.mark.parametrize(
    ("filename", "expected_net_assets", "expected_bonds", "expected_equities"),
    [
        (
            "202310 Telmar UBS SW Mandato (0402 60P).pdf",
            Decimal("68433704"),
            Decimal("43964829"),
            Decimal("23478819"),
        ),
        (
            "202311 Telmar UBS SW Mandato (0402 60P).pdf",
            Decimal("72201365"),
            Decimal("45557933"),
            Decimal("25731409"),
        ),
    ],
)
def test_ubs_suiza_telmar_2023_total_assets_ignores_graph_axis_noise(
    filename,
    expected_net_assets,
    expected_bonds,
    expected_equities,
):
    path = _raw_cartola_path(filename)
    _require(path)

    result = UBSSwitzerlandCustodyParser().safe_parse(path)
    assert result.is_success
    assert result.account_number == "206-560402-01"
    assert result.closing_balance == expected_net_assets

    selected = result.balances.get("selected_portfolio", {})
    assert selected.get("portfolio") == "Portfolio01"
    assert _as_decimal(selected.get("net_assets")) == expected_net_assets
    assert _as_decimal(selected.get("asset_classes", {}).get("bonds")) == expected_bonds
    assert _as_decimal(selected.get("asset_classes", {}).get("equities")) == expected_equities

    monthly = result.qualitative_data.get("account_monthly_activity", [])
    assert len(monthly) == 1
    assert _as_decimal(monthly[0].get("ending_value_with_accrual")) == expected_net_assets

    alloc = result.qualitative_data.get("asset_allocation", {})
    assert _as_decimal(alloc.get("Bonds", {}).get("total")) == expected_bonds
    assert _as_decimal(alloc.get("Equities", {}).get("total")) == expected_equities


def test_ubs_suiza_extracts_negative_current_and_previous_net_assets_from_detail():
    parser = UBSSwitzerlandCustodyParser()
    current, previous = parser._extract_current_previous_from_detail(
        "\n".join(
            [
                "Net assets of your portfolio valued in USD Details of your portfolio",
                "Period Net assets",
                "31.10.2024 -2 359",
                "29.12.2023 -424",
            ]
        ),
        statement_date=date(2024, 10, 31),
    )
    assert current == Decimal("-2359")
    assert previous == Decimal("-424")


def test_ubs_suiza_extracts_current_net_assets_from_date_range_detail():
    parser = UBSSwitzerlandCustodyParser()
    current, previous = parser._extract_current_previous_from_detail(
        "\n".join(
            [
                "Net assets Performance Reference currency USD",
                "Period (per end of period) TWR value",
                "31.12.2024-31.01.2025 2 039 -0.49% -10 yourclientadvisorwillbehappytohelp.",
                "08.11.2024-31.12.2024 2 049 -0.87% -20",
            ]
        ),
        statement_date=date(2025, 1, 31),
    )
    assert current == Decimal("2039")
    assert previous == Decimal("2049")


def test_ubs_suiza_monthly_row_parsing_handles_footnote_year_suffix():
    parsed = UBSSwitzerlandCustodyParser._parse_monthly_row_line(
        "29October20211 239504248 44912083 0 -30 0.00% -30 0.00%"
    )
    assert parsed is not None
    assert parsed["period_iso"] == "2021-10-29"
    assert parsed["final_value"] == Decimal("239504248")
    assert parsed["inflows"] == Decimal("44912083")
    assert parsed["outflows"] == Decimal("0")
    assert parsed["performance_value"] == Decimal("-30")


def test_ubs_suiza_monthly_row_parsing_keeps_thousands_groups_separate():
    parsed = UBSSwitzerlandCustodyParser._parse_monthly_row_line(
        "31 October 2024 95 914 300 000 -300 000 218 0.22% 2 756 2.92%"
    )
    assert parsed is not None
    assert parsed["final_value"] == Decimal("95914")
    assert parsed["inflows"] == Decimal("300000")
    assert parsed["outflows"] == Decimal("-300000")
    assert parsed["performance_value"] == Decimal("218")
    assert parsed["cumulative_value"] == Decimal("2756")


def test_ubs_suiza_monthly_row_parsing_handles_large_spaced_values():
    parsed = UBSSwitzerlandCustodyParser._parse_monthly_row_line(
        "31 December 2021 260 092 798 282 688 492 -296 052 078 -55 142 -0.02% -47 887 -0.02%"
    )
    assert parsed is not None
    assert parsed["final_value"] == Decimal("260092798")
    assert parsed["inflows"] == Decimal("282688492")
    assert parsed["outflows"] == Decimal("-296052078")
    assert parsed["performance_value"] == Decimal("-55142")
    assert parsed["cumulative_value"] == Decimal("-47887")


def test_ubs_suiza_nov_2021_uses_previous_month_closing_not_inception_start():
    path = _raw_cartola_path("202111 MI - UBS Sw (9943).pdf")
    _require(path)

    result = UBSSwitzerlandCustodyParser().safe_parse(path)
    assert result.is_success
    assert result.account_number == "206-579943-01"
    assert result.opening_balance == Decimal("239504248")
    monthly = result.qualitative_data.get("account_monthly_activity", [])
    assert len(monthly) == 1
    assert _as_decimal(monthly[0].get("net_contributions")) == Decimal("33999993")
    assert _as_decimal(monthly[0].get("utilidad")) == Decimal("7286")


def test_ubs_suiza_oct_2021_prefers_exact_statement_date_row_within_month():
    path = _raw_cartola_path("202110 MI - UBS Sw (9943).pdf")
    _require(path)

    result = UBSSwitzerlandCustodyParser().safe_parse(path)
    assert result.is_success
    assert result.account_number == "206-579943-01"
    assert result.statement_date == date(2021, 10, 29)
    assert result.opening_balance == Decimal("194592195")
    assert result.closing_balance == Decimal("239504248")

    monthly = result.qualitative_data.get("account_monthly_activity", [])
    assert len(monthly) == 1
    assert _as_decimal(monthly[0].get("net_contributions")) == Decimal("44912083")
    assert _as_decimal(monthly[0].get("utilidad")) == Decimal("-30")


def test_ubs_suiza_dec_2021_keeps_current_month_closing_balance_bounded():
    path = _raw_cartola_path("202112 MI - UBS Sw (9943).pdf")
    _require(path)

    result = UBSSwitzerlandCustodyParser().safe_parse(path)
    assert result.is_success
    assert result.account_number == "206-579943-01"
    assert result.opening_balance == Decimal("273511527")
    assert result.closing_balance == Decimal("260092798")


def test_ubs_suiza_dec_2024_does_not_overstate_october_or_net_contributions():
    path = _raw_cartola_path("202412 MI - UBS Sw (9943).pdf")
    _require(path)

    result = UBSSwitzerlandCustodyParser().safe_parse(path)
    assert result.is_success
    monthly = result.qualitative_data.get("account_monthly_activity", [])
    assert len(monthly) == 1
    assert _as_decimal(monthly[0].get("ending_value_with_accrual")) == Decimal("345799")
    assert _as_decimal(monthly[0].get("net_contributions")) == Decimal("249433")
    assert _as_decimal(monthly[0].get("utilidad")) == Decimal("207")

    history = result.qualitative_data.get("account_monthly_activity_history", [])
    oct_row = next(
        row for row in history
        if row.get("period_year") == 2024 and row.get("period_month") == 10
    )
    assert _as_decimal(oct_row.get("ending_value_with_accrual")) == Decimal("95914")
    assert _as_decimal(oct_row.get("net_contributions")) == Decimal("0")


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


def test_jpm_brokerage_extracts_cash_fixed_income_summary_categories():
    parser = JPMorganBrokerageParser()
    row = parser._parse_account_activity_page(
        "\n".join(
            [
                "ACCT. B99719001",
                "For the Period 2/1/26 to 2/28/26",
                "Cash & Fixed Income Summary",
                "Asset Categories Beginning Market Value Ending Market Value Change In Value Allocation",
                "Cash 15,920,368.36 15,967,852.51 47,484.15 85%",
                "Short Term 328.56 328.81 0.25 1%",
                "Non-US Fixed Income 2,590,600.32 2,574,871.20 (15,729.12) 14%",
                "Total Value 18,511,297.24 18,543,052.52 31,755.28 100%",
                "Portfolio Activity",
                "Current Period Value Year-to-Date Value",
                "Beginning Market Value 18,511,297.24 18,954,678.10",
                "Net Contributions/Withdrawals 0.00 (443,380.86)",
                "Income & Distributions 348.81 57,539.81",
                "Change In Investment Value 31,406.47 92,656.03",
                "Ending Market Value 18,543,052.52 18,543,052.52",
            ]
        ),
        "B99719001",
    )
    assert row is not None
    alloc = row.get("asset_allocation", {})
    assert _as_decimal(alloc.get("Cash", {}).get("ending")) == Decimal("15967852.51")
    assert _as_decimal(alloc.get("Short Term", {}).get("ending")) == Decimal("328.81")
    assert _as_decimal(alloc.get("Non-US Fixed Income", {}).get("ending")) == Decimal("2574871.20")


def test_jpm_brokerage_merges_cash_fixed_summary_from_separate_page():
    parser = JPMorganBrokerageParser()
    result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name=parser.get_parser_name(),
        parser_version=parser.VERSION,
        source_file_hash="test-hash-merge-summary",
        bank_code="jpmorgan",
        currency="USD",
    )
    pages = [
        "\n".join(
            [
                "ACCT. B99719001",
                "For the Period 2/1/26 to 2/28/26",
                "Cash & Fixed Income Summary",
                "Cash 15,920,368.36 15,967,852.51 47,484.15 85%",
                "Short Term 328.56 328.81 0.25 1%",
                "Non-US Fixed Income 2,590,600.32 2,574,871.20 (15,729.12) 14%",
            ]
        ),
        "\n".join(
            [
                "ACCT. B99719001",
                "For the Period 2/1/26 to 2/28/26",
                "Portfolio Activity",
                "Current Period Value Year-to-Date Value",
                "Beginning Market Value 18,511,297.24 18,954,678.10",
                "Net Contributions/Withdrawals 0.00 (443,380.86)",
                "Income & Distributions 348.81 57,539.81",
                "Change In Investment Value 31,406.47 92,656.03",
                "Ending Market Value 18,543,052.52 18,543,052.52",
            ]
        ),
    ]
    parser._extract_per_account_monthly_activity(pages, result)
    monthly = result.qualitative_data.get("account_monthly_activity", [])
    assert len(monthly) == 1
    alloc = monthly[0].get("asset_allocation", {})
    assert _as_decimal(alloc.get("Cash", {}).get("ending")) == Decimal("15967852.51")
    assert _as_decimal(alloc.get("Short Term", {}).get("ending")) == Decimal("328.81")
    assert _as_decimal(alloc.get("Non-US Fixed Income", {}).get("ending")) == Decimal("2574871.20")


def test_jpm_brokerage_summary_parses_non_us_line_with_trailing_text():
    parser = JPMorganBrokerageParser()
    alloc = parser._extract_cash_fixed_income_summary(
        "\n".join(
            [
                "Cash & Fixed Income Summary",
                "Cash 16,258,536.38 15,920,368.36 (338,168.02) 85%",
                "Short Term 0.00 328.56 328.56 1%",
                "Non-US Fixed Income 2,577,373.29 2,590,600.32 13,227.03 14% Short Term",
                "Total Value $18,836,237.54 $18,511,297.24 ($324,940.30) 100%",
            ]
        )
    )
    assert alloc is not None
    assert _as_decimal(alloc.get("Non-US Fixed Income", {}).get("ending")) == Decimal("2590600.32")


def test_jpm_brokerage_extracts_holdings_from_table_without_detail_keyword():
    parser = JPMorganBrokerageParser()
    result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name=parser.get_parser_name(),
        parser_version=parser.VERSION,
        source_file_hash="test-hash-holdings-no-detail",
        bank_code="jpmorgan",
        currency="USD",
    )
    pages = [
        "\n".join(
            [
                "ACCT. D16567000",
                "For the Period 2/1/26 to 2/28/26",
                "Adjusted Cost Unrealized Est. Annual Inc.",
                "Price Quantity Value Original Cost Gain/Loss Accrued Div. Yield",
                "Global Equity",
                "ISHARES CORE MSCI WORLD 134.27 12,564.000 1,686,968.28 1,565,738.66 121,229.62",
            ]
        )
    ]
    parser._extract_holdings(pages, result)
    rows = [row.data for row in result.rows if not row.data.get("is_total")]
    world = next((row for row in rows if row.get("instrument") == "ISHARES CORE MSCI WORLD"), None)
    assert world is not None
    assert world.get("section") == "equity"
    assert _as_decimal(world.get("market_value")) == Decimal("1686968.28")


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


@pytest.mark.parametrize(
    ("filename", "expected_closing"),
    [
        ("201711 Telmar - GS.pdf", Decimal("100605235.03")),
        ("201802 Telmar - GS.pdf", Decimal("100445378.50")),
    ],
)
def test_goldman_legacy_telmar_avoids_double_counting_main_overview_as_subportfolio(
    filename,
    expected_closing,
):
    path = _goldman_raw_cartola_path(filename)
    _require(path)

    result = GoldmanSachsCustodyParser().safe_parse(path)
    assert result.is_success
    assert result.account_number == "097-4"
    assert result.closing_balance == expected_closing

    alloc = result.qualitative_data.get("asset_allocation", {})
    assert _as_decimal(alloc.get("TOTAL PORTFOLIO", {}).get("market_value")) == expected_closing


def test_goldman_boatview_jan_2026_keeps_cash_umbrella_from_cartola():
    path = _goldman_raw_cartola_path("202601 Boatview - GS (Mandato_).pdf")
    _require(path)

    result = GoldmanSachsCustodyParser().safe_parse(path)
    assert result.is_success
    assert result.account_number == "451-9"

    alloc = result.qualitative_data.get("asset_allocation", {})
    assert _as_decimal(alloc.get("CASH, DEPOSITS & MONEY MARKET FUNDS", {}).get("market_value")) == Decimal("38057935.83")
    assert _as_decimal(alloc.get("FIXED INCOME", {}).get("market_value")) == Decimal("133979750.72")
    assert _as_decimal(alloc.get("PUBLIC EQUITY", {}).get("market_value")) == Decimal("104103888.43")


def test_goldman_etf_recovers_spdr_bloomberg_alias_from_legacy_layout():
    path = _goldman_raw_cartola_path("202410 Boatview - GS (ETF).pdf")
    _require(path)

    result = GoldmanSachsEtfParser().safe_parse(path)
    assert result.is_success

    names = {
        str((row.data or {}).get("instrument") or "").strip()
        for row in result.rows
    }
    assert "SSGA SPDR ETFS EU I PB L C-SPD ETF ON BLOOMBERG" in names


@pytest.mark.parametrize(
    (
        "filename",
        "expected_account",
        "expected_opening",
        "expected_movements",
        "expected_profit",
        "expected_closing",
    ),
    [
        (
            "202305 Telmar - GS.pdf",
            "097-4",
            Decimal("111232679.35"),
            Decimal("-0.10"),
            Decimal("-555931.41"),
            Decimal("110676747.84"),
        ),
        (
            "202406 Telmar - GS.pdf",
            "097-4",
            Decimal("123379607.32"),
            Decimal("0"),
            Decimal("2264393.55"),
            Decimal("125644000.87"),
        ),
        (
            "202306 Telmar - GS.pdf",
            "097-4",
            Decimal("110676747.84"),
            Decimal("0.00"),
            Decimal("2506847.47"),
            Decimal("113183595.31"),
        ),
        (
            "202305 Boatview - GS.pdf",
            "214-9",
            Decimal("47521970.93"),
            Decimal("0"),
            Decimal("-320516.68"),
            Decimal("47201454.25"),
        ),
        (
            "202306 Boatview - GS.pdf",
            "214-9",
            Decimal("47201454.25"),
            Decimal("0.00"),
            Decimal("1110421.42"),
            Decimal("48311875.67"),
        ),
        (
            "202406 Boatview - GS.pdf",
            "214-9",
            Decimal("52860140.28"),
            Decimal("0"),
            Decimal("1020179.83"),
            Decimal("53880320.11"),
        ),
    ],
)
def test_goldman_mandato_falls_back_to_subportfolio_consolidation(
    filename,
    expected_account,
    expected_opening,
    expected_movements,
    expected_profit,
    expected_closing,
):
    path = _goldman_raw_cartola_path(filename)
    _require(path)

    result = GoldmanSachsCustodyParser().safe_parse(path)
    assert result.is_success
    assert result.account_number == expected_account
    assert result.opening_balance == expected_opening
    assert result.closing_balance == expected_closing

    monthly = result.qualitative_data.get("account_monthly_activity", [])
    assert len(monthly) == 1
    row = monthly[0]
    assert row["account_number"] == expected_account
    assert _as_decimal(row["beginning_value"]) == expected_opening
    assert _as_decimal(row["ending_value_with_accrual"]) == expected_closing
    assert _as_decimal(row["net_contributions"]) == expected_movements
    assert _as_decimal(row["utilidad"]) == expected_profit

    alloc = result.qualitative_data.get("asset_allocation", {})
    assert "FIXED INCOME" in alloc
    assert "PUBLIC EQUITY" in alloc


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
