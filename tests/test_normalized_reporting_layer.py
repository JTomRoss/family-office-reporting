from __future__ import annotations

import json
from datetime import date
from decimal import Decimal

import pytest

from backend.db.models import (
    Account,
    EtfComposition,
    MonthlyClosing,
    MonthlyMetricNormalized,
    ParserVersion,
    ParsedStatement,
    RawDocument,
)
from backend.routers.data import (
    PERSONAL_ENTITY_NAMES,
    _build_health_report,
    get_etf,
    get_mandates,
    get_normalization_quality,
    get_personal,
    get_summary,
)
from backend.schemas import FilterParams, HealthAuditParams


def _mk_account(
    db_session,
    *,
    account_number: str,
    bank_code: str,
    account_type: str,
    entity_name: str,
) -> Account:
    acct = Account(
        account_number=account_number,
        identification_number=account_number,
        bank_code=bank_code,
        bank_name=bank_code.replace("_", " ").title(),
        account_type=account_type,
        entity_name=entity_name,
        entity_type="sociedad",
        currency="USD",
        country="US",
    )
    db_session.add(acct)
    db_session.flush()
    return acct


def _mk_parser_version(db_session, *, name: str) -> ParserVersion:
    parser_version = ParserVersion(
        parser_name=name,
        version="test",
        source_hash="0" * 64,
        description="test parser version",
    )
    db_session.add(parser_version)
    db_session.flush()
    return parser_version


def test_summary_prefers_normalized_monthly_metrics(db_session):
    acct = _mk_account(
        db_session,
        account_number="NORM-SUM-001",
        bank_code="jpmorgan",
        account_type="mandato",
        entity_name="Entity Norm",
    )
    db_session.add_all(
        [
            MonthlyClosing(
                account_id=acct.id,
                closing_date=date(2024, 12, 31),
                year=2024,
                month=12,
                net_value=Decimal("90.00"),
                income=Decimal("1.00"),
                change_in_value=Decimal("1.00"),
                currency="USD",
            ),
            MonthlyClosing(
                account_id=acct.id,
                closing_date=date(2025, 1, 31),
                year=2025,
                month=1,
                net_value=Decimal("110.00"),
                income=Decimal("2.00"),
                change_in_value=Decimal("2.00"),
                currency="USD",
            ),
            MonthlyMetricNormalized(
                account_id=acct.id,
                closing_date=date(2024, 12, 31),
                year=2024,
                month=12,
                ending_value_with_accrual=Decimal("100.00"),
                ending_value_without_accrual=Decimal("95.00"),
                cash_value=Decimal("20.00"),
                movements_net=Decimal("0.00"),
                profit_period=Decimal("0.00"),
                currency="USD",
            ),
            MonthlyMetricNormalized(
                account_id=acct.id,
                closing_date=date(2025, 1, 31),
                year=2025,
                month=1,
                ending_value_with_accrual=Decimal("130.00"),
                ending_value_without_accrual=Decimal("124.00"),
                cash_value=Decimal("22.00"),
                movements_net=Decimal("5.00"),
                profit_period=Decimal("25.00"),
                currency="USD",
            ),
        ]
    )
    db_session.commit()

    payload = get_summary(FilterParams(years=[2025]), db_session)
    consolidated = {row["fecha"]: row for row in payload["consolidated_rows"]}
    jan = consolidated["2025-01"]

    assert jan["ending_value"] == 130.0
    assert jan["caja"] == 22.0
    assert jan["movimientos"] == 5.0
    assert jan["utilidad"] == 25.0
    assert jan["rent_mensual_pct"] == 25.0
    assert jan["rent_mensual_sin_caja_pct"] == 31.25


def test_summary_zeroes_negative_ubs_return(db_session):
    acct = _mk_account(
        db_session,
        account_number="UBS-NEG-001",
        bank_code="ubs",
        account_type="mandato",
        entity_name="Entity UBS",
    )
    db_session.add_all(
        [
            MonthlyClosing(
                account_id=acct.id,
                closing_date=date(2024, 12, 31),
                year=2024,
                month=12,
                net_value=Decimal("-100.00"),
                income=Decimal("0.00"),
                change_in_value=Decimal("0.00"),
                currency="USD",
            ),
            MonthlyClosing(
                account_id=acct.id,
                closing_date=date(2025, 1, 31),
                year=2025,
                month=1,
                net_value=Decimal("-120.00"),
                income=Decimal("-20.00"),
                change_in_value=Decimal("0.00"),
                currency="USD",
            ),
            MonthlyMetricNormalized(
                account_id=acct.id,
                closing_date=date(2024, 12, 31),
                year=2024,
                month=12,
                ending_value_with_accrual=Decimal("-100.00"),
                ending_value_without_accrual=Decimal("-100.00"),
                cash_value=Decimal("0.00"),
                movements_net=Decimal("0.00"),
                profit_period=Decimal("0.00"),
                currency="USD",
            ),
            MonthlyMetricNormalized(
                account_id=acct.id,
                closing_date=date(2025, 1, 31),
                year=2025,
                month=1,
                ending_value_with_accrual=Decimal("-120.00"),
                ending_value_without_accrual=Decimal("-120.00"),
                cash_value=Decimal("0.00"),
                movements_net=Decimal("0.00"),
                profit_period=Decimal("-20.00"),
                currency="USD",
            ),
        ]
    )
    db_session.commit()

    payload = get_summary(FilterParams(years=[2025], bank_codes=["ubs"]), db_session)
    consolidated = {row["fecha"]: row for row in payload["consolidated_rows"]}
    detail = {row["fecha"]: row for row in payload["rows"]}

    assert consolidated["2025-01"]["rent_mensual_pct"] == 0.0
    assert detail["2025-01"]["rent_mensual_pct"] == 0.0


def test_mandates_prefers_normalized_asset_allocation(db_session):
    acct = _mk_account(
        db_session,
        account_number="NORM-MAND-ALLOC-001",
        bank_code="jpmorgan",
        account_type="mandato",
        entity_name="Entity Alloc",
    )
    db_session.add(
        MonthlyClosing(
            account_id=acct.id,
            closing_date=date(2025, 1, 31),
            year=2025,
            month=1,
            net_value=Decimal("100.00"),
            income=Decimal("0.00"),
            change_in_value=Decimal("0.00"),
            asset_allocation_json=json.dumps(
                {
                    "Cash": {"value": "40.00"},
                    "Equities": {"value": "60.00"},
                }
            ),
            currency="USD",
        )
    )
    db_session.flush()
    db_session.add(
        MonthlyMetricNormalized(
            account_id=acct.id,
            closing_date=date(2025, 1, 31),
            year=2025,
            month=1,
            ending_value_with_accrual=Decimal("100.00"),
            ending_value_without_accrual=Decimal("100.00"),
            cash_value=Decimal("10.00"),
            movements_net=Decimal("0.00"),
            profit_period=Decimal("0.00"),
            asset_allocation_json=json.dumps(
                {
                    "Cash": {"value": "10.00"},
                    "Equities": {"value": "90.00"},
                }
            ),
            currency="USD",
        )
    )
    db_session.commit()

    payload = get_mandates(FilterParams(years=[2025], fecha="2025-01"), db_session)

    by_month = {row["fecha"]: row for row in payload["asset_allocation"]}
    assert by_month["2025-01"]["Cash"] == 10.0
    assert by_month["2025-01"]["Equities"] == 90.0
    assert payload["aa_by_bank"]["jpmorgan"]["Cash"] == 10.0
    assert payload["aa_by_bank"]["jpmorgan"]["Equities"] == 90.0


def test_mandates_etf_alloc_is_always_without_personal_and_exposes_cash(db_session):
    mandate = _mk_account(
        db_session,
        account_number="MAND-KPI-001",
        bank_code="jpmorgan",
        account_type="mandato",
        entity_name="Mandate Entity",
    )
    etf_visible = _mk_account(
        db_session,
        account_number="ETF-KPI-001",
        bank_code="jpmorgan",
        account_type="etf",
        entity_name="Telmar",
    )
    etf_personal = _mk_account(
        db_session,
        account_number="ETF-KPI-002",
        bank_code="jpmorgan",
        account_type="etf",
        entity_name="RaÃƒÂ­ces LP",
    )
    db_session.add_all(
        [
            MonthlyClosing(
                account_id=mandate.id,
                closing_date=date(2025, 1, 31),
                year=2025,
                month=1,
                net_value=Decimal("100.00"),
                income=Decimal("0.00"),
                change_in_value=Decimal("0.00"),
                asset_allocation_json=json.dumps(
                    {
                        "Cash": {"value": "25.00"},
                        "Equities": {"value": "75.00"},
                    }
                ),
                currency="USD",
            ),
            MonthlyMetricNormalized(
                account_id=mandate.id,
                closing_date=date(2025, 1, 31),
                year=2025,
                month=1,
                ending_value_with_accrual=Decimal("100.00"),
                ending_value_without_accrual=Decimal("100.00"),
                cash_value=Decimal("25.00"),
                movements_net=Decimal("0.00"),
                profit_period=Decimal("0.00"),
                asset_allocation_json=json.dumps(
                    {
                        "Cash": {"value": "25.00"},
                        "Equities": {"value": "75.00"},
                    }
                ),
                currency="USD",
            ),
            EtfComposition(
                account_id=etf_visible.id,
                bank_code="jpmorgan",
                report_date=date(2025, 1, 31),
                year=2025,
                month=1,
                etf_code="IWDA",
                etf_name="IWDA",
                quantity=Decimal("1"),
                market_value=Decimal("75.00"),
                weight_pct=Decimal("75.0"),
                currency="USD",
            ),
            EtfComposition(
                account_id=etf_visible.id,
                bank_code="jpmorgan",
                report_date=date(2025, 1, 31),
                year=2025,
                month=1,
                etf_code="MM",
                etf_name="Money Market",
                quantity=Decimal("1"),
                market_value=Decimal("25.00"),
                weight_pct=Decimal("25.0"),
                currency="USD",
            ),
            EtfComposition(
                account_id=etf_personal.id,
                bank_code="jpmorgan",
                report_date=date(2025, 1, 31),
                year=2025,
                month=1,
                etf_code="IWDA",
                etf_name="IWDA",
                quantity=Decimal("1"),
                market_value=Decimal("999.00"),
                weight_pct=Decimal("100.0"),
                currency="USD",
            ),
        ]
    )
    etf_personal.entity_name = next(iter(PERSONAL_ENTITY_NAMES))
    db_session.commit()

    payload = get_mandates(FilterParams(years=[2025], fecha="2025-01"), db_session)

    mandate_row = next(row for row in payload["banks_by_month"] if row["bank_code"] == "jpmorgan")
    assert mandate_row["cash_value"] == 25.0
    assert payload["aa_by_bank"]["etf_portfolio"]["Cash, Deposits & Money Market"] == 25.0
    assert payload["aa_by_bank"]["etf_portfolio"]["Equities"] == 75.0


def test_mandates_sin_caja_reprices_returns_and_hides_cash_bucket(db_session):
    mandate = _mk_account(
        db_session,
        account_number="MAND-SC-001",
        bank_code="jpmorgan",
        account_type="mandato",
        entity_name="Mandate Sin Caja",
    )
    etf_account = _mk_account(
        db_session,
        account_number="ETF-SC-001",
        bank_code="jpmorgan",
        account_type="etf",
        entity_name="Telmar",
    )
    db_session.add_all(
        [
            MonthlyClosing(
                account_id=mandate.id,
                closing_date=date(2024, 12, 31),
                year=2024,
                month=12,
                net_value=Decimal("100.00"),
                income=Decimal("0.00"),
                change_in_value=Decimal("0.00"),
                asset_allocation_json=json.dumps(
                    {
                        "Cash": {"value": "20.00"},
                        "Equities": {"value": "80.00"},
                    }
                ),
                currency="USD",
            ),
            MonthlyClosing(
                account_id=mandate.id,
                closing_date=date(2025, 1, 31),
                year=2025,
                month=1,
                net_value=Decimal("130.00"),
                income=Decimal("25.00"),
                change_in_value=Decimal("5.00"),
                asset_allocation_json=json.dumps(
                    {
                        "Cash": {"value": "30.00"},
                        "Equities": {"value": "100.00"},
                    }
                ),
                currency="USD",
            ),
            MonthlyMetricNormalized(
                account_id=mandate.id,
                closing_date=date(2024, 12, 31),
                year=2024,
                month=12,
                ending_value_with_accrual=Decimal("100.00"),
                ending_value_without_accrual=Decimal("100.00"),
                cash_value=Decimal("20.00"),
                movements_net=Decimal("0.00"),
                profit_period=Decimal("0.00"),
                asset_allocation_json=json.dumps(
                    {
                        "Cash": {"value": "20.00"},
                        "Equities": {"value": "80.00"},
                    }
                ),
                currency="USD",
            ),
            MonthlyMetricNormalized(
                account_id=mandate.id,
                closing_date=date(2025, 1, 31),
                year=2025,
                month=1,
                ending_value_with_accrual=Decimal("130.00"),
                ending_value_without_accrual=Decimal("100.00"),
                cash_value=Decimal("30.00"),
                movements_net=Decimal("5.00"),
                profit_period=Decimal("25.00"),
                asset_allocation_json=json.dumps(
                    {
                        "Cash": {"value": "30.00"},
                        "Equities": {"value": "100.00"},
                    }
                ),
                currency="USD",
            ),
            MonthlyClosing(
                account_id=etf_account.id,
                closing_date=date(2024, 12, 31),
                year=2024,
                month=12,
                net_value=Decimal("40.00"),
                income=Decimal("0.00"),
                change_in_value=Decimal("0.00"),
                currency="USD",
            ),
            MonthlyClosing(
                account_id=etf_account.id,
                closing_date=date(2025, 1, 31),
                year=2025,
                month=1,
                net_value=Decimal("60.00"),
                income=Decimal("15.00"),
                change_in_value=Decimal("5.00"),
                currency="USD",
            ),
            MonthlyMetricNormalized(
                account_id=etf_account.id,
                closing_date=date(2024, 12, 31),
                year=2024,
                month=12,
                ending_value_with_accrual=Decimal("40.00"),
                ending_value_without_accrual=Decimal("30.00"),
                cash_value=Decimal("10.00"),
                movements_net=Decimal("0.00"),
                profit_period=Decimal("0.00"),
                currency="USD",
            ),
            MonthlyMetricNormalized(
                account_id=etf_account.id,
                closing_date=date(2025, 1, 31),
                year=2025,
                month=1,
                ending_value_with_accrual=Decimal("60.00"),
                ending_value_without_accrual=Decimal("45.00"),
                cash_value=Decimal("15.00"),
                movements_net=Decimal("5.00"),
                profit_period=Decimal("15.00"),
                currency="USD",
            ),
            EtfComposition(
                account_id=etf_account.id,
                bank_code="jpmorgan",
                report_date=date(2025, 1, 31),
                year=2025,
                month=1,
                etf_code="IWDA",
                etf_name="IWDA",
                quantity=Decimal("1"),
                market_value=Decimal("45.00"),
                weight_pct=Decimal("75.0"),
                currency="USD",
            ),
            EtfComposition(
                account_id=etf_account.id,
                bank_code="jpmorgan",
                report_date=date(2025, 1, 31),
                year=2025,
                month=1,
                etf_code="MM",
                etf_name="Money Market",
                quantity=Decimal("1"),
                market_value=Decimal("15.00"),
                weight_pct=Decimal("25.0"),
                currency="USD",
            ),
        ]
    )
    db_session.commit()

    payload = get_mandates(FilterParams(years=[2024, 2025], sin_caja=True), db_session)

    by_month = {row["fecha"]: row for row in payload["asset_allocation"]}
    returns_by_bank = {row["bank_code"]: row for row in payload["returns_table"]}

    assert "Cash" not in by_month["2025-01"]
    assert by_month["2025-01"]["Equities"] == 100.0
    assert payload["aa_by_bank"]["jpmorgan"]["Equities"] == 100.0
    assert payload["aa_by_bank"]["etf_portfolio"]["Cash, Deposits & Money Market"] == 0.0
    assert payload["aa_by_bank"]["etf_portfolio"]["Equities"] == 100.0
    assert returns_by_bank["jpmorgan"]["2025-01_monthly"] == 31.25
    assert returns_by_bank["jpmorgan"]["2025-01_ytd"] == 31.25
    assert payload["etf_total_returns"]["2025-01_monthly"] == 50.0
    assert payload["etf_total_returns"]["2025-01_ytd"] == 50.0


def test_personal_exposes_sibling_accounts_separately_when_one_normalized_value_is_zero(db_session):
    mandate = _mk_account(
        db_session,
        account_number="206-560552-02",
        bank_code="ubs",
        account_type="mandato",
        entity_name="Boatview",
    )
    brokerage = _mk_account(
        db_session,
        account_number="206-560552-01",
        bank_code="ubs",
        account_type="brokerage",
        entity_name="Boatview",
    )
    db_session.add_all(
        [
            MonthlyClosing(
                account_id=mandate.id,
                closing_date=date(2025, 1, 31),
                year=2025,
                month=1,
                net_value=Decimal("54185.00"),
                change_in_value=Decimal("-40.00"),
                currency="USD",
            ),
            MonthlyMetricNormalized(
                account_id=mandate.id,
                closing_date=date(2025, 1, 31),
                year=2025,
                month=1,
                ending_value_with_accrual=Decimal("0.00"),
                ending_value_without_accrual=Decimal("0.00"),
                movements_net=Decimal("-40.00"),
                profit_period=Decimal("0.00"),
                currency="USD",
            ),
            MonthlyClosing(
                account_id=brokerage.id,
                closing_date=date(2025, 1, 31),
                year=2025,
                month=1,
                net_value=Decimal("54185.00"),
                income=Decimal("124.00"),
                change_in_value=Decimal("-448.00"),
                currency="USD",
            ),
            MonthlyMetricNormalized(
                account_id=brokerage.id,
                closing_date=date(2025, 1, 31),
                year=2025,
                month=1,
                ending_value_with_accrual=Decimal("54185.00"),
                ending_value_without_accrual=Decimal("54185.00"),
                movements_net=Decimal("-448.00"),
                profit_period=Decimal("124.00"),
                currency="USD",
            ),
        ]
    )
    db_session.commit()

    payload = get_personal(
        FilterParams(entity_names=["Boatview"], years=[2025], months=[1]),
        db_session,
    )
    rows = {
        (row["banco"], row["id"], row["tipo_cuenta"]): row
        for row in payload["entities_table"]
        if row["banco"] == "ubs"
    }

    assert rows[("ubs", "206-560552-02", "mandato")]["net_value"] == 0.0
    assert rows[("ubs", "206-560552-02", "mandato")]["movimientos"] == -40.0
    assert rows[("ubs", "206-560552-01", "brokerage")]["net_value"] == 54185.0
    assert rows[("ubs", "206-560552-01", "brokerage")]["movimientos"] == -448.0


def test_etf_control_uses_normalized_ending_without_accrual(db_session):
    acct = _mk_account(
        db_session,
        account_number="NORM-ETF-001",
        bank_code="jpmorgan",
        account_type="etf",
        entity_name="Telmar",
    )
    db_session.add_all(
        [
            MonthlyClosing(
                account_id=acct.id,
                closing_date=date(2025, 12, 31),
                year=2025,
                month=12,
                net_value=Decimal("100.00"),
                accrual=Decimal("0.00"),
                currency="USD",
            ),
            MonthlyMetricNormalized(
                account_id=acct.id,
                closing_date=date(2025, 12, 31),
                year=2025,
                month=12,
                ending_value_with_accrual=Decimal("100.00"),
                ending_value_without_accrual=Decimal("95.00"),
                movements_net=Decimal("0.00"),
                profit_period=Decimal("0.00"),
                currency="USD",
            ),
            EtfComposition(
                account_id=acct.id,
                bank_code="jpmorgan",
                report_date=date(2025, 12, 31),
                year=2025,
                month=12,
                etf_code="IWDA",
                etf_name="IWDA",
                quantity=Decimal("1"),
                market_value=Decimal("60.00"),
                weight_pct=Decimal("63.1579"),
                currency="USD",
            ),
            EtfComposition(
                account_id=acct.id,
                bank_code="jpmorgan",
                report_date=date(2025, 12, 31),
                year=2025,
                month=12,
                etf_code="MM",
                etf_name="Money Market",
                quantity=Decimal("1"),
                market_value=Decimal("35.00"),
                weight_pct=Decimal("36.8421"),
                currency="USD",
            ),
        ]
    )
    db_session.commit()

    payload = get_etf(
        FilterParams(
            years=[2025],
            fecha="2025-12",
            entity_names=["Telmar"],
            bank_codes=["jpmorgan"],
        ),
        db_session,
    )
    assert payload["control_expected"]["Telmar"] == 95.0
    assert payload["control_expected"]["Total"] == 95.0


def test_etf_normalizes_p_jpm_li_liq_to_money_market(db_session):
    acct = _mk_account(
        db_session,
        account_number="NORM-ETF-002",
        bank_code="jpmorgan",
        account_type="etf",
        entity_name="Telmar",
    )
    db_session.add_all(
        [
            EtfComposition(
                account_id=acct.id,
                bank_code="jpmorgan",
                report_date=date(2025, 12, 31),
                year=2025,
                month=12,
                etf_code="LIQ",
                etf_name="P JPM LI-LIQ LVNAV FD - USD - W -",
                quantity=Decimal("1"),
                market_value=Decimal("50.00"),
                weight_pct=Decimal("50.0"),
                currency="USD",
            ),
            EtfComposition(
                account_id=acct.id,
                bank_code="jpmorgan",
                report_date=date(2025, 12, 31),
                year=2025,
                month=12,
                etf_code="IWDA",
                etf_name="IWDA",
                quantity=Decimal("1"),
                market_value=Decimal("50.00"),
                weight_pct=Decimal("50.0"),
                currency="USD",
            ),
        ]
    )
    db_session.commit()

    payload = get_etf(
        FilterParams(
            years=[2025],
            fecha="2025-12",
            entity_names=["Telmar"],
            bank_codes=["jpmorgan"],
        ),
        db_session,
    )
    instruments = payload["instruments_table"]
    assert "Money Market" in instruments
    assert "P JPM LI-LIQ LVNAV FD - USD - W -" not in instruments
    assert instruments["Money Market"]["Total"] == 50.0


def test_etf_pct_table_uses_column_totals_per_society(db_session):
    telmar = _mk_account(
        db_session,
        account_number="NORM-ETF-003",
        bank_code="jpmorgan",
        account_type="etf",
        entity_name="Telmar",
    )
    armel = _mk_account(
        db_session,
        account_number="NORM-ETF-004",
        bank_code="jpmorgan",
        account_type="etf",
        entity_name="Armel Holdings",
    )
    db_session.add_all(
        [
            EtfComposition(
                account_id=telmar.id,
                bank_code="jpmorgan",
                report_date=date(2025, 12, 31),
                year=2025,
                month=12,
                etf_code="IWDA",
                etf_name="IWDA",
                quantity=Decimal("1"),
                market_value=Decimal("60.00"),
                weight_pct=Decimal("60.0"),
                currency="USD",
            ),
            EtfComposition(
                account_id=telmar.id,
                bank_code="jpmorgan",
                report_date=date(2025, 12, 31),
                year=2025,
                month=12,
                etf_code="VDPA",
                etf_name="VDPA",
                quantity=Decimal("1"),
                market_value=Decimal("40.00"),
                weight_pct=Decimal("40.0"),
                currency="USD",
            ),
            EtfComposition(
                account_id=armel.id,
                bank_code="jpmorgan",
                report_date=date(2025, 12, 31),
                year=2025,
                month=12,
                etf_code="IWDA",
                etf_name="IWDA",
                quantity=Decimal("1"),
                market_value=Decimal("30.00"),
                weight_pct=Decimal("30.0"),
                currency="USD",
            ),
            EtfComposition(
                account_id=armel.id,
                bank_code="jpmorgan",
                report_date=date(2025, 12, 31),
                year=2025,
                month=12,
                etf_code="VDPA",
                etf_name="VDPA",
                quantity=Decimal("1"),
                market_value=Decimal("70.00"),
                weight_pct=Decimal("70.0"),
                currency="USD",
            ),
        ]
    )
    db_session.commit()

    payload = get_etf(
        FilterParams(
            years=[2025],
            fecha="2025-12",
            bank_codes=["jpmorgan"],
            entity_names=["Telmar", "Armel Holdings"],
        ),
        db_session,
    )
    pct = payload["instruments_pct_table"]
    assert pct["IWDA"]["Telmar"] == 60.0
    assert pct["VDPA"]["Telmar"] == 40.0
    assert pct["IWDA"]["Armel Holdings"] == 30.0
    assert pct["VDPA"]["Armel Holdings"] == 70.0
    assert pct["IWDA"]["Total"] == 45.0
    assert pct["VDPA"]["Total"] == 55.0


def test_etf_pct_table_reweights_without_cash_when_sin_caja(db_session):
    acct = _mk_account(
        db_session,
        account_number="NORM-ETF-005",
        bank_code="jpmorgan",
        account_type="etf",
        entity_name="Telmar",
    )
    db_session.add_all(
        [
            EtfComposition(
                account_id=acct.id,
                bank_code="jpmorgan",
                report_date=date(2025, 12, 31),
                year=2025,
                month=12,
                etf_code="IWDA",
                etf_name="IWDA",
                quantity=Decimal("1"),
                market_value=Decimal("60.00"),
                weight_pct=Decimal("60.0"),
                currency="USD",
            ),
            EtfComposition(
                account_id=acct.id,
                bank_code="jpmorgan",
                report_date=date(2025, 12, 31),
                year=2025,
                month=12,
                etf_code="MM",
                etf_name="Money Market",
                quantity=Decimal("1"),
                market_value=Decimal("40.00"),
                weight_pct=Decimal("40.0"),
                currency="USD",
            ),
        ]
    )
    db_session.commit()

    payload = get_etf(
        FilterParams(
            years=[2025],
            fecha="2025-12",
            entity_names=["Telmar"],
            bank_codes=["jpmorgan"],
            sin_caja=True,
        ),
        db_session,
    )
    pct = payload["instruments_pct_table"]
    assert pct["IWDA"]["Telmar"] == 100.0
    assert pct["IWDA"]["Total"] == 100.0


def test_etf_sin_caja_affects_all_visible_outputs(db_session):
    acct = _mk_account(
        db_session,
        account_number="NORM-ETF-005B",
        bank_code="jpmorgan",
        account_type="etf",
        entity_name="Telmar",
    )
    db_session.add_all(
        [
            EtfComposition(
                account_id=acct.id,
                bank_code="jpmorgan",
                report_date=date(2025, 1, 31),
                year=2025,
                month=1,
                etf_code="IWDA",
                etf_name="IWDA",
                quantity=Decimal("1"),
                market_value=Decimal("90.00"),
                weight_pct=Decimal("90.0"),
                currency="USD",
            ),
            EtfComposition(
                account_id=acct.id,
                bank_code="jpmorgan",
                report_date=date(2025, 1, 31),
                year=2025,
                month=1,
                etf_code="MM",
                etf_name="Money Market",
                quantity=Decimal("1"),
                market_value=Decimal("10.00"),
                weight_pct=Decimal("10.0"),
                currency="USD",
            ),
            EtfComposition(
                account_id=acct.id,
                bank_code="jpmorgan",
                report_date=date(2025, 2, 28),
                year=2025,
                month=2,
                etf_code="IWDA",
                etf_name="IWDA",
                quantity=Decimal("1"),
                market_value=Decimal("100.00"),
                weight_pct=Decimal("83.3333"),
                currency="USD",
            ),
            EtfComposition(
                account_id=acct.id,
                bank_code="jpmorgan",
                report_date=date(2025, 2, 28),
                year=2025,
                month=2,
                etf_code="MM",
                etf_name="Money Market",
                quantity=Decimal("1"),
                market_value=Decimal("20.00"),
                weight_pct=Decimal("16.6667"),
                currency="USD",
            ),
            MonthlyClosing(
                account_id=acct.id,
                closing_date=date(2025, 1, 31),
                year=2025,
                month=1,
                net_value=Decimal("100.00"),
                change_in_value=Decimal("15.00"),
                currency="USD",
            ),
            MonthlyClosing(
                account_id=acct.id,
                closing_date=date(2025, 2, 28),
                year=2025,
                month=2,
                net_value=Decimal("120.00"),
                change_in_value=Decimal("15.00"),
                currency="USD",
            ),
            MonthlyMetricNormalized(
                account_id=acct.id,
                closing_date=date(2025, 1, 31),
                year=2025,
                month=1,
                ending_value_with_accrual=Decimal("100.00"),
                ending_value_without_accrual=Decimal("100.00"),
                movements_net=Decimal("15.00"),
                profit_period=Decimal("0.00"),
                currency="USD",
            ),
            MonthlyMetricNormalized(
                account_id=acct.id,
                closing_date=date(2025, 2, 28),
                year=2025,
                month=2,
                ending_value_with_accrual=Decimal("120.00"),
                ending_value_without_accrual=Decimal("120.00"),
                movements_net=Decimal("15.00"),
                profit_period=Decimal("0.00"),
                currency="USD",
            ),
        ]
    )
    db_session.commit()

    payload = get_etf(
        FilterParams(
            years=[2025],
            fecha="2025-02",
            entity_names=["Telmar"],
            bank_codes=["jpmorgan"],
            sin_caja=True,
        ),
        db_session,
    )

    assert "Money Market" not in payload["instruments_table"]
    assert payload["composition_by_instrument"] == [{"label": "IWDA", "value": 100.0}]

    montos_by_soc = {row["sociedad"]: row for row in payload["society_montos_table"]}
    assert montos_by_soc["Telmar"]["01"] == 90.0
    assert montos_by_soc["Telmar"]["02"] == 100.0
    assert montos_by_soc["Total"]["01"] == 90.0
    assert montos_by_soc["Total"]["02"] == 100.0

    movs_by_soc = {row["sociedad"]: row for row in payload["society_movements_table"]}
    assert movs_by_soc["Telmar"]["01"] == 5.0
    assert movs_by_soc["Telmar"]["02"] == 5.0
    assert movs_by_soc["Total"]["01"] == 5.0
    assert movs_by_soc["Total"]["02"] == 5.0

    returns_by_soc = {row["sociedad"]: row for row in payload["society_returns_monthly"]}
    assert returns_by_soc["Telmar"]["01"] is None
    assert returns_by_soc["Telmar"]["02"] == pytest.approx(5.5556, rel=1e-4)
    assert returns_by_soc["Total"]["02"] == pytest.approx(5.5556, rel=1e-4)


def test_etf_total_row_matches_summary_without_cash(db_session):
    acct = _mk_account(
        db_session,
        account_number="NORM-ETF-TOTAL-001",
        bank_code="jpmorgan",
        account_type="etf",
        entity_name="Telmar",
    )
    db_session.add_all(
        [
            MonthlyClosing(
                account_id=acct.id,
                closing_date=date(2024, 12, 31),
                year=2024,
                month=12,
                net_value=Decimal("100.00"),
                change_in_value=Decimal("0.00"),
                currency="USD",
            ),
            MonthlyClosing(
                account_id=acct.id,
                closing_date=date(2025, 1, 31),
                year=2025,
                month=1,
                net_value=Decimal("120.00"),
                change_in_value=Decimal("10.00"),
                currency="USD",
            ),
            MonthlyMetricNormalized(
                account_id=acct.id,
                closing_date=date(2024, 12, 31),
                year=2024,
                month=12,
                ending_value_with_accrual=Decimal("100.00"),
                ending_value_without_accrual=Decimal("100.00"),
                cash_value=Decimal("40.00"),
                movements_net=Decimal("0.00"),
                profit_period=Decimal("0.00"),
                currency="USD",
            ),
            MonthlyMetricNormalized(
                account_id=acct.id,
                closing_date=date(2025, 1, 31),
                year=2025,
                month=1,
                ending_value_with_accrual=Decimal("120.00"),
                ending_value_without_accrual=Decimal("120.00"),
                cash_value=Decimal("50.00"),
                movements_net=Decimal("10.00"),
                profit_period=Decimal("0.00"),
                currency="USD",
            ),
            EtfComposition(
                account_id=acct.id,
                bank_code="jpmorgan",
                report_date=date(2024, 12, 31),
                year=2024,
                month=12,
                etf_code="IWDA",
                etf_name="IWDA",
                quantity=Decimal("1"),
                market_value=Decimal("60.00"),
                weight_pct=Decimal("60.0"),
                currency="USD",
            ),
            EtfComposition(
                account_id=acct.id,
                bank_code="jpmorgan",
                report_date=date(2024, 12, 31),
                year=2024,
                month=12,
                etf_code="MM",
                etf_name="Money Market",
                quantity=Decimal("1"),
                market_value=Decimal("40.00"),
                weight_pct=Decimal("40.0"),
                currency="USD",
            ),
            EtfComposition(
                account_id=acct.id,
                bank_code="jpmorgan",
                report_date=date(2025, 1, 31),
                year=2025,
                month=1,
                etf_code="IWDA",
                etf_name="IWDA",
                quantity=Decimal("1"),
                market_value=Decimal("70.00"),
                weight_pct=Decimal("58.3333"),
                currency="USD",
            ),
            EtfComposition(
                account_id=acct.id,
                bank_code="jpmorgan",
                report_date=date(2025, 1, 31),
                year=2025,
                month=1,
                etf_code="MM",
                etf_name="Money Market",
                quantity=Decimal("1"),
                market_value=Decimal("50.00"),
                weight_pct=Decimal("41.6667"),
                currency="USD",
            ),
        ]
    )
    db_session.commit()

    etf_payload = get_etf(
        FilterParams(
            years=[2025],
            fecha="2025-01",
            entity_names=["Telmar"],
            bank_codes=["jpmorgan"],
            sin_caja=True,
        ),
        db_session,
    )
    summary_payload = get_summary(
        FilterParams(
            years=[2025],
            entity_names=["Telmar"],
            bank_codes=["jpmorgan"],
            account_types=["etf"],
        ),
        db_session,
    )

    total_row = next(row for row in etf_payload["society_returns_monthly"] if row["sociedad"] == "Total")
    jan_summary = next(
        row
        for row in summary_payload["consolidated_rows"]
        if row["fecha"] == "2025-01"
    )
    assert total_row["01"] == jan_summary["rent_mensual_sin_caja_pct"]


def test_etf_movements_table_aggregates_monthly_movements_by_society(db_session):
    telmar = _mk_account(
        db_session,
        account_number="NORM-ETF-006",
        bank_code="jpmorgan",
        account_type="etf",
        entity_name="Telmar",
    )
    armel = _mk_account(
        db_session,
        account_number="NORM-ETF-007",
        bank_code="jpmorgan",
        account_type="etf",
        entity_name="Armel Holdings",
    )
    db_session.add_all(
        [
            MonthlyClosing(
                account_id=telmar.id,
                closing_date=date(2025, 1, 31),
                year=2025,
                month=1,
                net_value=Decimal("100.00"),
                change_in_value=Decimal("1.00"),
                currency="USD",
            ),
            MonthlyClosing(
                account_id=telmar.id,
                closing_date=date(2025, 2, 28),
                year=2025,
                month=2,
                net_value=Decimal("110.00"),
                change_in_value=Decimal("1.50"),
                currency="USD",
            ),
            MonthlyClosing(
                account_id=armel.id,
                closing_date=date(2025, 1, 31),
                year=2025,
                month=1,
                net_value=Decimal("200.00"),
                change_in_value=Decimal("-2.00"),
                currency="USD",
            ),
            MonthlyClosing(
                account_id=armel.id,
                closing_date=date(2025, 2, 28),
                year=2025,
                month=2,
                net_value=Decimal("205.00"),
                change_in_value=Decimal("3.00"),
                currency="USD",
            ),
            MonthlyMetricNormalized(
                account_id=telmar.id,
                closing_date=date(2025, 1, 31),
                year=2025,
                month=1,
                ending_value_with_accrual=Decimal("100.00"),
                ending_value_without_accrual=Decimal("100.00"),
                movements_net=Decimal("5.00"),
                profit_period=Decimal("0.00"),
                currency="USD",
            ),
            MonthlyMetricNormalized(
                account_id=telmar.id,
                closing_date=date(2025, 2, 28),
                year=2025,
                month=2,
                ending_value_with_accrual=Decimal("110.00"),
                ending_value_without_accrual=Decimal("110.00"),
                movements_net=Decimal("7.50"),
                profit_period=Decimal("0.00"),
                currency="USD",
            ),
        ]
    )
    db_session.commit()

    payload = get_etf(
        FilterParams(
            years=[2025],
            fecha="2025-02",
            bank_codes=["jpmorgan"],
            entity_names=["Telmar", "Armel Holdings"],
        ),
        db_session,
    )
    by_soc = {row["sociedad"]: row for row in payload["society_movements_table"]}

    assert by_soc["Telmar"]["01"] == 5.0
    assert by_soc["Telmar"]["02"] == 7.5
    assert by_soc["Armel Holdings"]["01"] == -2.0
    assert by_soc["Armel Holdings"]["02"] == 3.0
    assert by_soc["Total"]["01"] == 3.0
    assert by_soc["Total"]["02"] == 10.5


def test_normalization_quality_reports_coverage_and_missing_rows(db_session):
    acct = _mk_account(
        db_session,
        account_number="NORM-QA-001",
        bank_code="ubs",
        account_type="mandato",
        entity_name="QA Entity",
    )
    db_session.add_all(
        [
            MonthlyClosing(
                account_id=acct.id,
                closing_date=date(2025, 1, 31),
                year=2025,
                month=1,
                net_value=Decimal("100.00"),
                income=Decimal("1.00"),
                change_in_value=Decimal("2.00"),
                currency="USD",
            ),
            MonthlyClosing(
                account_id=acct.id,
                closing_date=date(2025, 2, 28),
                year=2025,
                month=2,
                net_value=Decimal("101.00"),
                income=Decimal("1.50"),
                change_in_value=Decimal("2.50"),
                currency="USD",
            ),
            MonthlyMetricNormalized(
                account_id=acct.id,
                closing_date=date(2025, 1, 31),
                year=2025,
                month=1,
                ending_value_with_accrual=Decimal("100.00"),
                ending_value_without_accrual=Decimal("100.00"),
                movements_net=Decimal("2.00"),
                profit_period=Decimal("1.00"),
                currency="USD",
            ),
        ]
    )
    db_session.commit()

    payload = get_normalization_quality(limit=10, db=db_session)
    assert payload["totals"]["monthly_closings"] == 2
    assert payload["totals"]["normalized_rows"] == 1
    assert payload["totals"]["coverage_pct"] == 50.0
    assert payload["missing_count"] == 1


def test_health_report_treats_missing_movements_as_zero_when_identity_implies_zero(db_session):
    acct = _mk_account(
        db_session,
        account_number="HEALTH-BRO-001",
        bank_code="jpmorgan",
        account_type="brokerage",
        entity_name="Health Brokerage",
    )
    db_session.add_all(
        [
            MonthlyClosing(
                account_id=acct.id,
                closing_date=date(2024, 12, 31),
                year=2024,
                month=12,
                net_value=Decimal("100.00"),
                income=Decimal("0.00"),
                change_in_value=Decimal("0.00"),
                currency="USD",
            ),
            MonthlyClosing(
                account_id=acct.id,
                closing_date=date(2025, 1, 31),
                year=2025,
                month=1,
                net_value=Decimal("105.00"),
                income=Decimal("5.00"),
                change_in_value=None,
                currency="USD",
            ),
        ]
    )
    db_session.commit()

    report = _build_health_report(
        db=db_session,
        filters=HealthAuditParams(years=[2025], bank_codes=["jpmorgan"], account_types=["brokerage"], limit=50),
    )

    assert report["summary"]["identity_mismatch_count"] == 0
    assert report["summary"]["missing_components_count"] == 0
    assert report["identity_issues"] == []
    assert report["missing_component_issues"] == []


def test_health_report_keeps_missing_movements_when_identity_implies_non_zero(db_session):
    acct = _mk_account(
        db_session,
        account_number="HEALTH-BRO-002",
        bank_code="jpmorgan",
        account_type="brokerage",
        entity_name="Health Brokerage NonZero",
    )
    db_session.add_all(
        [
            MonthlyClosing(
                account_id=acct.id,
                closing_date=date(2024, 12, 31),
                year=2024,
                month=12,
                net_value=Decimal("100.00"),
                income=Decimal("0.00"),
                change_in_value=Decimal("0.00"),
                currency="USD",
            ),
            MonthlyClosing(
                account_id=acct.id,
                closing_date=date(2025, 1, 31),
                year=2025,
                month=1,
                net_value=Decimal("115.00"),
                income=Decimal("5.00"),
                change_in_value=None,
                currency="USD",
            ),
        ]
    )
    db_session.commit()

    report = _build_health_report(
        db=db_session,
        filters=HealthAuditParams(years=[2025], bank_codes=["jpmorgan"], account_types=["brokerage"], limit=50),
    )

    assert report["summary"]["missing_components_count"] == 1
    assert len(report["missing_component_issues"]) == 1
    issue = report["missing_component_issues"][0]
    assert issue["entity_name"] == "Health Brokerage NonZero"
    assert issue["movements"] is None
    assert issue["missing_fields"] == ["movements"]


def test_health_report_account_ytd_maps_net_contributions_and_total_profit(db_session):
    acct = _mk_account(
        db_session,
        account_number="B99719001",
        bank_code="jpmorgan",
        account_type="brokerage",
        entity_name="Boatview",
    )
    parser_version = _mk_parser_version(db_session, name="tests.health.account_ytd")
    raw_doc = RawDocument(
        filename="202512 Boatview JPM NY Brokerage (9001).pdf",
        filepath="data/raw/jpmorgan/pdf_cartola/202512 Boatview JPM NY Brokerage (9001).pdf",
        file_type="pdf_cartola",
        sha256_hash="health-ytd-account-ytd-doc",
        file_size_bytes=1,
        bank_code="jpmorgan",
        account_id=acct.id,
        status="parsed",
    )
    db_session.add_all(
        [
            raw_doc,
            MonthlyClosing(
                account_id=acct.id,
                closing_date=date(2024, 12, 31),
                year=2024,
                month=12,
                net_value=Decimal("13389560.81"),
                income=Decimal("0.00"),
                change_in_value=Decimal("0.00"),
                currency="USD",
            ),
            MonthlyClosing(
                account_id=acct.id,
                closing_date=date(2025, 12, 31),
                year=2025,
                month=12,
                net_value=Decimal("18885468.69"),
                income=Decimal("1023877.61"),
                change_in_value=Decimal("4422799.12"),
                currency="USD",
                source_document_id=1,
            ),
        ]
    )
    db_session.flush()
    raw_doc_id = raw_doc.id
    closing = (
        db_session.query(MonthlyClosing)
        .filter(MonthlyClosing.account_id == acct.id, MonthlyClosing.year == 2025, MonthlyClosing.month == 12)
        .one()
    )
    closing.source_document_id = raw_doc_id
    db_session.add(
        ParsedStatement(
            raw_document_id=raw_doc_id,
            account_id=acct.id,
            statement_date=date(2025, 12, 31),
            period_start=date(2025, 12, 1),
            period_end=date(2025, 12, 31),
            closing_balance=Decimal("18885468.69"),
            currency="USD",
                parser_version_id=parser_version.id,
            parsed_data_json=json.dumps(
                {
                    "qualitative_data": {
                        "account_ytd": [
                            {
                                "account_number": "B99719001",
                                "beginning_value": "13389560.81",
                                "net_contributions": "4422799.12",
                                "income": "406188.90",
                                "change_investment": "617688.71",
                                "ending_value": "18885468.69",
                            }
                        ]
                    }
                }
            ),
        )
    )
    db_session.commit()

    report = _build_health_report(
        db=db_session,
        filters=HealthAuditParams(years=[2025], bank_codes=["jpmorgan"], account_types=["brokerage"], limit=50),
    )

    assert report["ytd_issues"] == []
    assert report["summary"]["ytd_movement_mismatch_count"] == 0
    assert report["summary"]["ytd_profit_mismatch_count"] == 0


def test_health_report_reads_ytd_from_normalized_columns(db_session):
    """Health reads YTD from MonthlyMetricNormalized, not from parsed JSON."""
    acct = _mk_account(
        db_session,
        account_number="HEALTH-YTD-NORM-001",
        bank_code="jpmorgan",
        account_type="brokerage",
        entity_name="Health YTD Normalized",
    )
    db_session.add_all(
        [
            MonthlyClosing(
                account_id=acct.id,
                closing_date=date(2024, 12, 31),
                year=2024,
                month=12,
                net_value=Decimal("100.00"),
                income=Decimal("0.00"),
                change_in_value=Decimal("0.00"),
                currency="USD",
            ),
            MonthlyClosing(
                account_id=acct.id,
                closing_date=date(2025, 1, 31),
                year=2025,
                month=1,
                net_value=Decimal("125.00"),
                income=Decimal("10.00"),
                change_in_value=Decimal("5.00"),
                currency="USD",
            ),
            MonthlyMetricNormalized(
                account_id=acct.id,
                closing_date=date(2025, 1, 31),
                year=2025,
                month=1,
                ending_value_with_accrual=Decimal("125.00"),
                movements_net=Decimal("5.00"),
                profit_period=Decimal("10.00"),
                movements_ytd=Decimal("99.00"),
                profit_ytd=Decimal("110.00"),
                currency="USD",
            ),
        ]
    )
    db_session.commit()

    report = _build_health_report(
        db=db_session,
        filters=HealthAuditParams(years=[2025], bank_codes=["jpmorgan"], account_types=["brokerage"], limit=50),
    )

    assert report["summary"]["ytd_movement_mismatch_count"] == 1
    assert report["summary"]["ytd_profit_mismatch_count"] == 1
    assert len(report["ytd_issues"]) == 2
