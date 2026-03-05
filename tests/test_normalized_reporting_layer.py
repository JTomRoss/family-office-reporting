from __future__ import annotations

from datetime import date
from decimal import Decimal

from backend.db.models import Account, EtfComposition, MonthlyClosing, MonthlyMetricNormalized
from backend.routers.data import get_etf, get_normalization_quality, get_summary
from backend.schemas import FilterParams


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
