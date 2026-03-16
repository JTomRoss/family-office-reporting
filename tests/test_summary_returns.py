"""
Regresiones de cálculo de rentabilidad en /data/summary.
"""

from datetime import date
from decimal import Decimal
import json

from backend.db.models import (
    Account,
    MonthlyClosing,
    MonthlyMetricNormalized,
    ParsedStatement,
    ParserVersion,
    RawDocument,
)
from backend.routers.data import get_summary
from backend.schemas import FilterParams


def test_summary_return_uses_calendar_previous_month(db_session):
    """
    Si falta el mes calendario anterior, la rentabilidad debe ser None.
    No debe usar "el último mes disponible" con salto.
    """
    account = Account(
        account_number="TEST-RET-001",
        bank_code="jpmorgan",
        bank_name="JP Morgan",
        account_type="custody",
        entity_name="Test Entity",
        entity_type="sociedad",
        currency="USD",
        country="US",
    )
    db_session.add(account)
    db_session.flush()

    db_session.add_all(
        [
            MonthlyClosing(
                account_id=account.id,
                closing_date=date(2025, 12, 31),
                year=2025,
                month=12,
                net_value=Decimal("100.00"),
                income=Decimal("10.00"),
                currency="USD",
            ),
            MonthlyClosing(
                account_id=account.id,
                closing_date=date(2027, 1, 31),
                year=2027,
                month=1,
                net_value=Decimal("130.00"),
                income=Decimal("13.00"),
                currency="USD",
            ),
            MonthlyClosing(
                account_id=account.id,
                closing_date=date(2027, 2, 28),
                year=2027,
                month=2,
                net_value=Decimal("143.00"),
                income=Decimal("14.30"),
                currency="USD",
            ),
        ]
    )
    db_session.commit()

    payload = get_summary(FilterParams(years=[2025, 2027]), db_session)

    consolidated = {row["fecha"]: row for row in payload["consolidated_rows"]}
    detail = {row["fecha"]: row for row in payload["rows"]}

    # 2027-01 no tiene 2026-12: no debe calcular rentabilidad.
    assert consolidated["2027-01"]["rent_mensual_pct"] is None
    assert detail["2027-01"]["rent_mensual_pct"] is None

    # 2027-02 sí tiene 2027-01: debe calcularse.
    # Sin change_in_value explícito, retorno es simple: (143/130 - 1)*100 = 10.0
    assert consolidated["2027-02"]["rent_mensual_pct"] == 10.0
    assert detail["2027-02"]["rent_mensual_pct"] == 10.0


def test_summary_return_sin_caja_uses_asset_allocation_cash(db_session):
    """
    Si hay caja en asset_allocation_json, rentabilidad sin caja debe usar
    el denominador (ending previo - caja previa), no el ending total.
    """
    account = Account(
        account_number="TEST-CASH-001",
        bank_code="goldman_sachs",
        bank_name="Goldman Sachs",
        account_type="mandato",
        entity_name="Test Entity",
        entity_type="sociedad",
        currency="USD",
        country="US",
    )
    db_session.add(account)
    db_session.flush()

    db_session.add_all(
        [
            MonthlyClosing(
                account_id=account.id,
                closing_date=date(2024, 12, 31),
                year=2024,
                month=12,
                net_value=Decimal("100.00"),
                income=Decimal("10.00"),
                change_in_value=Decimal("0.00"),
                asset_allocation_json=json.dumps(
                    {"CASH, DEPOSITS & MONEY MARKET FUNDS": {"market_value": "40.00"}}
                ),
                currency="USD",
            ),
            MonthlyClosing(
                account_id=account.id,
                closing_date=date(2025, 1, 31),
                year=2025,
                month=1,
                net_value=Decimal("110.00"),
                income=Decimal("10.00"),
                change_in_value=Decimal("0.00"),
                asset_allocation_json=json.dumps(
                    {"CASH, DEPOSITS & MONEY MARKET FUNDS": {"market_value": "45.00"}}
                ),
                currency="USD",
            ),
        ]
    )
    db_session.commit()

    payload = get_summary(FilterParams(years=[2025]), db_session)
    consolidated = {row["fecha"]: row for row in payload["consolidated_rows"]}
    jan = consolidated["2025-01"]

    # Rent normal = 10 / 100 = 10%
    assert jan["rent_mensual_pct"] == 10.0
    # Rent sin caja = 10 / (100 - 40) = 16.6667%
    assert jan["rent_mensual_sin_caja_pct"] == 16.6667


def test_summary_cash_uses_goldman_umbrella_row_without_double_count(db_session):
    account = Account(
        account_number="TEST-GS-CASH-001",
        bank_code="goldman_sachs",
        bank_name="Goldman Sachs",
        account_type="mandato",
        entity_name="Test Entity",
        entity_type="sociedad",
        currency="USD",
        country="US",
    )
    db_session.add(account)
    db_session.flush()

    alloc = {
        "CASH, DEPOSITS & MONEY MARKET FUNDS": {"market_value": "40.00"},
        "CASH": {"market_value": "1.00"},
        "DEPOSITS & MONEY MARKET FUNDS": {"market_value": "39.00"},
    }
    db_session.add_all(
        [
            MonthlyClosing(
                account_id=account.id,
                closing_date=date(2024, 12, 31),
                year=2024,
                month=12,
                net_value=Decimal("100.00"),
                income=Decimal("10.00"),
                change_in_value=Decimal("0.00"),
                asset_allocation_json=json.dumps(alloc),
                currency="USD",
            ),
            MonthlyClosing(
                account_id=account.id,
                closing_date=date(2025, 1, 31),
                year=2025,
                month=1,
                net_value=Decimal("110.00"),
                income=Decimal("10.00"),
                change_in_value=Decimal("0.00"),
                asset_allocation_json=json.dumps(alloc),
                currency="USD",
            ),
        ]
    )
    db_session.commit()

    payload = get_summary(FilterParams(years=[2025]), db_session)
    cons = {row["fecha"]: row for row in payload["consolidated_rows"]}
    jan = cons["2025-01"]
    # Caja debe ser 40 (umbrella), no 80 (sumando sublíneas).
    assert jan["caja"] == 40.0


def test_summary_cash_prefers_normalized_cash_over_monthly_allocation(db_session):
    """
    Si existe cash_value normalizado, Resumen debe usar esa capa canónica
    por sobre asset_allocation_json histórico.
    """
    account = Account(
        account_number="TEST-GS-CASH-002",
        bank_code="goldman_sachs",
        bank_name="Goldman Sachs",
        account_type="mandato",
        entity_name="Test Entity",
        entity_type="sociedad",
        currency="USD",
        country="US",
    )
    db_session.add(account)
    db_session.flush()

    db_session.add_all(
        [
            MonthlyClosing(
                account_id=account.id,
                closing_date=date(2024, 12, 31),
                year=2024,
                month=12,
                net_value=Decimal("100.00"),
                income=Decimal("10.00"),
                change_in_value=Decimal("0.00"),
                asset_allocation_json=json.dumps(
                    {"Cash, Deposits & Money Market": {"value": "40.00"}}
                ),
                currency="USD",
            ),
            MonthlyClosing(
                account_id=account.id,
                closing_date=date(2025, 1, 31),
                year=2025,
                month=1,
                net_value=Decimal("110.00"),
                income=Decimal("10.00"),
                change_in_value=Decimal("0.00"),
                asset_allocation_json=json.dumps(
                    {"Cash, Deposits & Money Market": {"value": "45.00"}}
                ),
                currency="USD",
            ),
        ]
    )
    db_session.flush()

    # Simula capa normalizada antigua con caja duplicada.
    db_session.add_all(
        [
            MonthlyMetricNormalized(
                account_id=account.id,
                year=2024,
                month=12,
                closing_date=date(2024, 12, 31),
                ending_value_with_accrual=Decimal("100.00"),
                ending_value_without_accrual=Decimal("100.00"),
                accrual_ending=Decimal("0.00"),
                cash_value=Decimal("80.00"),
                movements_net=Decimal("0.00"),
                profit_period=Decimal("10.00"),
                currency="USD",
            ),
            MonthlyMetricNormalized(
                account_id=account.id,
                year=2025,
                month=1,
                closing_date=date(2025, 1, 31),
                ending_value_with_accrual=Decimal("110.00"),
                ending_value_without_accrual=Decimal("110.00"),
                accrual_ending=Decimal("0.00"),
                cash_value=Decimal("90.00"),
                movements_net=Decimal("0.00"),
                profit_period=Decimal("10.00"),
                currency="USD",
            ),
        ]
    )
    db_session.commit()

    payload = get_summary(FilterParams(years=[2025]), db_session)
    consolidated = {row["fecha"]: row for row in payload["consolidated_rows"]}
    jan = consolidated["2025-01"]
    assert jan["caja"] == 90.0

    detail_jan = next(row for row in payload["rows"] if row["fecha"] == "2025-01")
    assert detail_jan["caja"] == 90.0


def test_summary_does_not_read_cash_from_parsed_statement_raw_fallback(db_session):
    account = Account(
        account_number="TEST-JPM-CASH-RAW-001",
        bank_code="jpmorgan",
        bank_name="JP Morgan",
        account_type="brokerage",
        entity_name="Test Entity",
        entity_type="sociedad",
        currency="USD",
        country="US",
    )
    db_session.add(account)
    db_session.flush()

    parser_version = ParserVersion(
        parser_name="parsers.jpmorgan.brokerage",
        version="test",
        source_hash="test-summary-no-raw-cash",
        description="test",
    )
    raw_doc = RawDocument(
        filename="20250131-test-jpm-brokerage.pdf",
        filepath="data/raw/jpmorgan/pdf_cartola/20250131-test-jpm-brokerage.pdf",
        file_type="pdf_cartola",
        sha256_hash="test-summary-no-raw-cash",
        file_size_bytes=1,
        bank_code="jpmorgan",
        account_id=account.id,
        status="parsed",
    )
    db_session.add_all([parser_version, raw_doc])
    db_session.flush()

    db_session.add(
        MonthlyClosing(
            account_id=account.id,
            closing_date=date(2025, 1, 31),
            year=2025,
            month=1,
            net_value=Decimal("459.62"),
            income=Decimal("9.62"),
            change_in_value=Decimal("0.00"),
            asset_allocation_json=None,
            source_document_id=raw_doc.id,
            currency="USD",
        )
    )
    db_session.add(
        ParsedStatement(
            raw_document_id=raw_doc.id,
            account_id=account.id,
            statement_date=date(2025, 1, 31),
            period_start=date(2025, 1, 1),
            period_end=date(2025, 1, 31),
            closing_balance=Decimal("459.62"),
            currency="USD",
            parser_version_id=parser_version.id,
            parsed_data_json=json.dumps(
                {
                    "rows": [
                        {
                            "instrument": "US DOLLAR JPM DEPOSIT SWEEP",
                            "market_value": "459.62",
                            "account_number": account.account_number,
                            "section": "cash_fixed_income",
                        }
                    ]
                }
            ),
        )
    )
    db_session.commit()

    payload = get_summary(FilterParams(years=[2025], bank_codes=["jpmorgan"]), db_session)
    jan = next(row for row in payload["rows"] if row["fecha"] == "2025-01")
    assert jan["caja"] == 0.0
