from __future__ import annotations

from datetime import date
from decimal import Decimal
import json

from backend.db.models import (
    Account,
    DailyMovement,
    DailyPosition,
    DailyPrice,
    MonthlyClosing,
    MonthlyMetricNormalized,
    ParserVersion,
    RawDocument,
)
from backend.services.data_loading_service import DataLoadingService
from parsers.base import ParseResult, ParsedRow, ParserStatus


def _mk_account(db_session, account_number: str = "ACC-001") -> Account:
    acct = Account(
        account_number=account_number,
        identification_number=account_number,
        bank_code="jpmorgan",
        bank_name="JP Morgan",
        account_type="custody",
        entity_name="Boatview",
        entity_type="sociedad",
        currency="USD",
        country="US",
    )
    db_session.add(acct)
    db_session.flush()
    return acct


def _mk_doc(db_session, file_type: str, file_hash: str = "h1") -> RawDocument:
    doc = RawDocument(
        filename=f"test_{file_type}.xlsx",
        filepath=f"data/raw/system/{file_type}/test.xlsx",
        file_type=file_type,
        sha256_hash=file_hash,
        file_size_bytes=100,
        bank_code="system",
        status="parsed",
    )
    db_session.add(doc)
    db_session.flush()
    return doc


def _mk_parser_version(db_session, parser_name: str, version: str = "1.0.0") -> ParserVersion:
    pv = ParserVersion(
        parser_name=parser_name,
        version=version,
        source_hash=f"{parser_name}:{version}",
        description=parser_name,
    )
    db_session.add(pv)
    db_session.flush()
    return pv


def test_load_operational_positions_upsert(db_session):
    acct = _mk_account(db_session)
    doc = _mk_doc(db_session, "excel_positions", "hash-pos")
    result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name="parsers.system.daily_positions",
        parser_version="1.0.0",
        source_file_hash="src-pos",
        rows=[
            ParsedRow(
                row_number=2,
                data={
                    "account_number": acct.account_number,
                    "position_date": "2025-01-31",
                    "instrument_code": "IWDA",
                    "instrument_name": "iShares World",
                    "currency": "USD",
                    "market_value": "1000.5",
                },
            )
        ],
    )
    loader = DataLoadingService(db_session)
    stats = loader.load_operational_result(result, doc, "excel_positions")
    assert stats["daily_positions"] == 1

    row = db_session.query(DailyPosition).one()
    assert row.instrument_code == "IWDA"
    assert row.market_value == Decimal("1000.5")


def test_load_operational_movements_is_idempotent_per_file_hash(db_session):
    acct = _mk_account(db_session)
    doc = _mk_doc(db_session, "excel_movements", "hash-mov")
    result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name="parsers.system.daily_movements",
        parser_version="1.0.0",
        source_file_hash="src-mov",
        rows=[
            ParsedRow(
                row_number=2,
                data={
                    "account_number": acct.account_number,
                    "movement_date": "2025-01-31",
                    "movement_type": "buy",
                    "instrument_code": "IWDA",
                    "currency": "USD",
                    "gross_amount": "100.0",
                },
            )
        ],
    )
    loader = DataLoadingService(db_session)
    stats1 = loader.load_operational_result(result, doc, "excel_movements")
    stats2 = loader.load_operational_result(result, doc, "excel_movements")

    assert stats1["daily_movements"] == 1
    assert stats2["daily_movements"] == 1
    assert db_session.query(DailyMovement).count() == 1


def test_load_operational_prices_upsert(db_session):
    doc = _mk_doc(db_session, "excel_prices", "hash-pr")
    result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name="parsers.system.daily_prices",
        parser_version="1.0.0",
        source_file_hash="src-pr",
        rows=[
            ParsedRow(
                row_number=2,
                data={
                    "price_date": "2025-01-31",
                    "instrument_code": "USDCLP",
                    "instrument_type": "fx",
                    "price": "950.1234",
                    "currency": "CLP",
                },
            )
        ],
    )
    loader = DataLoadingService(db_session)
    stats = loader.load_operational_result(result, doc, "excel_prices")
    assert stats["daily_prices"] == 1
    row = db_session.query(DailyPrice).one()
    assert row.price == Decimal("950.1234")


def test_load_asset_allocation_report_updates_monthly_closing(db_session):
    acct = _mk_account(db_session, "ACC-REPORT")
    doc = _mk_doc(db_session, "pdf_report", "hash-report")
    doc.filename = "2026 02 Reporte - Jan 2026.pdf"
    doc.account_id = acct.id
    db_session.add(
        MonthlyClosing(
            account_id=acct.id,
            closing_date=date(2026, 1, 31),
            year=2026,
            month=1,
            net_value=Decimal("100.00"),
            income=Decimal("1.00"),
            change_in_value=Decimal("2.00"),
            currency="USD",
            source_document_id=None,
        )
    )
    db_session.add(
        MonthlyMetricNormalized(
            account_id=acct.id,
            closing_date=date(2026, 1, 31),
            year=2026,
            month=1,
            ending_value_with_accrual=Decimal("100.00"),
            ending_value_without_accrual=Decimal("100.00"),
            movements_net=Decimal("2.00"),
            profit_period=Decimal("1.00"),
            cash_value=Decimal("10.00"),
            asset_allocation_json=None,
            currency="USD",
            source_document_id=None,
        )
    )
    db_session.flush()

    result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name="parsers.system.report_asset_allocation",
        parser_version="1.0.0",
        source_file_hash="src-report",
        account_number=acct.account_number,
        statement_date=date(2026, 12, 31),  # fecha ruidosa: debe priorizar filename
        period_end=date(2026, 12, 31),
        currency="USD",
        qualitative_data={
            "asset_allocation": {
                "Equity": {"value": 70.0, "unit": "%"},
                "Cash": {"value": 30.0, "unit": "%"},
            }
        },
    )

    loader = DataLoadingService(db_session)
    stats = loader.load_asset_allocation_report(result=result, raw_document=doc)
    assert stats["monthly_closings_updated"] == 1
    assert stats["skipped"] == 0
    closing = (
        db_session.query(MonthlyClosing)
        .filter(
            MonthlyClosing.account_id == acct.id,
            MonthlyClosing.year == 2026,
            MonthlyClosing.month == 1,
        )
        .one()
    )
    assert closing.asset_allocation_json is not None
    assert closing.source_document_id is None
    assert (
        db_session.query(MonthlyClosing)
        .filter(
            MonthlyClosing.account_id == acct.id,
            MonthlyClosing.year == 2026,
            MonthlyClosing.month == 12,
        )
        .count()
        == 0
    )
    normalized = (
        db_session.query(MonthlyMetricNormalized)
        .filter(
            MonthlyMetricNormalized.account_id == acct.id,
            MonthlyMetricNormalized.year == 2026,
            MonthlyMetricNormalized.month == 1,
        )
        .one()
    )
    assert normalized.cash_value == Decimal("10.0")


def test_load_asset_allocation_report_keeps_cartola_macros_and_scales_mandate_splits(db_session):
    acct = Account(
        account_number="ACC-MAND-REPORT",
        identification_number="ACC-MAND-REPORT",
        bank_code="jpmorgan",
        bank_name="JP Morgan",
        account_type="mandato",
        entity_name="Boatview",
        entity_type="sociedad",
        currency="USD",
        country="US",
    )
    db_session.add(acct)
    db_session.flush()

    doc = _mk_doc(db_session, "pdf_report", "hash-report-macro-safe")
    doc.filename = "2026 01 JPM Complementario - Reporte Mandato.pdf"
    doc.bank_code = "jpmorgan"
    doc.account_id = acct.id

    db_session.add(
        MonthlyClosing(
            account_id=acct.id,
            closing_date=date(2026, 1, 31),
            year=2026,
            month=1,
            net_value=Decimal("200.00"),
            income=Decimal("1.00"),
            change_in_value=Decimal("2.00"),
            currency="USD",
            asset_allocation_json=json.dumps(
                {
                    "Cash, Deposits & Money Market": {"value": "20"},
                    "Fixed Income": {"value": "100"},
                    "Equities": {"value": "80"},
                }
            ),
            source_document_id=None,
        )
    )
    db_session.add(
        MonthlyMetricNormalized(
            account_id=acct.id,
            closing_date=date(2026, 1, 31),
            year=2026,
            month=1,
            ending_value_with_accrual=Decimal("200.00"),
            ending_value_without_accrual=Decimal("200.00"),
            movements_net=Decimal("2.00"),
            profit_period=Decimal("1.00"),
            cash_value=Decimal("20.00"),
            asset_allocation_json=None,
            currency="USD",
            source_document_id=None,
        )
    )
    db_session.flush()

    result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name="parsers.jpmorgan.report_mandato",
        parser_version="1.1.0",
        source_file_hash="src-report-macro-safe",
        account_number=acct.account_number,
        statement_date=date(2026, 1, 31),
        period_end=date(2026, 1, 31),
        currency="USD",
        qualitative_data={
            "asset_allocation": {
                "Cash, Deposits & Money Market": {"value": 43.0, "unit": "%"},
                "Fixed Income": {"value": 57.0, "unit": "%"},
                "Equities": {"value": 42.0, "unit": "%"},
                "Investment Grade Fixed Income": {"value": 50.0, "unit": "%"},
                "High Yield Fixed Income": {"value": 7.0, "unit": "%"},
                "US Equities": {"value": 19.0, "unit": "%"},
                "Non US Equities": {"value": 23.0, "unit": "%"},
            }
        },
    )

    loader = DataLoadingService(db_session)
    stats = loader.load_asset_allocation_report(result=result, raw_document=doc)
    assert stats["monthly_closings_updated"] == 1

    closing = (
        db_session.query(MonthlyClosing)
        .filter(
            MonthlyClosing.account_id == acct.id,
            MonthlyClosing.year == 2026,
            MonthlyClosing.month == 1,
        )
        .one()
    )
    payload = json.loads(closing.asset_allocation_json or "{}")
    assert payload["Cash, Deposits & Money Market"]["value"] == "20"
    assert payload["Fixed Income"]["value"] == "100"
    assert payload["Equities"]["value"] == "80"
    assert Decimal(payload["Investment Grade Fixed Income"]["value"]) == Decimal("87.71929824561403508771929825")
    assert Decimal(payload["High Yield Fixed Income"]["value"]) == Decimal("12.28070175438596491228070175")
    assert Decimal(payload["US Equities"]["value"]) == Decimal("36.19047619047619047619047619")
    assert Decimal(payload["Non US Equities"]["value"]) == Decimal("43.80952380952380952380952381")

    normalized = (
        db_session.query(MonthlyMetricNormalized)
        .filter(
            MonthlyMetricNormalized.account_id == acct.id,
            MonthlyMetricNormalized.year == 2026,
            MonthlyMetricNormalized.month == 1,
        )
        .one()
    )
    normalized_payload = json.loads(normalized.asset_allocation_json or "{}")
    canonical = normalized_payload.get("__canonical_breakdown", {})
    assert Decimal(canonical["Cash, Deposits & Money Market"]["amount"]) == Decimal("20")
    assert Decimal(canonical["Investment Grade Fixed Income"]["amount"]) == Decimal("87.71929824561403508771929825")
    assert Decimal(canonical["High Yield Fixed Income"]["amount"]) == Decimal("12.28070175438596491228070175")
    assert Decimal(canonical["US Equities"]["amount"]) == Decimal("36.19047619047619047619047619")
    assert Decimal(canonical["Non US Equities"]["amount"]) == Decimal("43.80952380952380952380952381")


def test_load_asset_allocation_report_for_jpm_investment_review_preserves_existing_hy_split(db_session):
    acct = Account(
        account_number="ACC-JPM-MAND-REVIEW",
        identification_number="ACC-JPM-MAND-REVIEW",
        bank_code="jpmorgan",
        bank_name="JP Morgan",
        account_type="mandato",
        entity_name="Boatview",
        entity_type="sociedad",
        currency="USD",
        country="US",
    )
    db_session.add(acct)
    db_session.flush()

    doc = _mk_doc(db_session, "pdf_report", "hash-jpm-investment-review")
    doc.filename = "2026 02 Boatview Limited - February Investment Review.pdf"
    doc.bank_code = "jpmorgan"
    doc.account_id = acct.id

    db_session.add(
        MonthlyClosing(
            account_id=acct.id,
            closing_date=date(2026, 2, 28),
            year=2026,
            month=2,
            net_value=Decimal("200.00"),
            income=Decimal("1.00"),
            change_in_value=Decimal("2.00"),
            currency="USD",
            asset_allocation_json=json.dumps(
                {
                    "Cash, Deposits & Money Market": {"value": "20"},
                    "Fixed Income": {"value": "100"},
                    "Equities": {"value": "80"},
                    "Investment Grade Fixed Income": {"value": "78"},
                    "High Yield Fixed Income": {"value": "22"},
                }
            ),
            source_document_id=doc.id,
        )
    )
    db_session.flush()

    result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name="parsers.jpmorgan.report_mandato",
        parser_version="1.3.0",
        source_file_hash="src-jpm-investment-review",
        account_number=acct.account_number,
        statement_date=date(2026, 2, 28),
        period_end=date(2026, 2, 28),
        currency="USD",
        qualitative_data={
            "asset_allocation": {
                "Investment Grade Fixed Income": {"value": 55.47, "unit": "%"},
                "US Equities": {"value": 31.24, "unit": "%"},
                "Non US Equities": {"value": 11.76, "unit": "%"},
            }
        },
    )

    loader = DataLoadingService(db_session)
    stats = loader.load_asset_allocation_report(result=result, raw_document=doc)
    assert stats["monthly_closings_updated"] == 1

    closing = (
        db_session.query(MonthlyClosing)
        .filter(
            MonthlyClosing.account_id == acct.id,
            MonthlyClosing.year == 2026,
            MonthlyClosing.month == 2,
        )
        .one()
    )
    payload = json.loads(closing.asset_allocation_json or "{}")
    assert Decimal(payload["Investment Grade Fixed Income"]["value"]) == Decimal("78")
    assert Decimal(payload["High Yield Fixed Income"]["value"]) == Decimal("22")
    assert Decimal(payload["US Equities"]["value"]) == Decimal("58.12093023255813953488372093")
    assert Decimal(payload["Non US Equities"]["value"]) == Decimal("21.87906976744186046511627907")


def test_load_asset_allocation_report_applies_jpm_portfolio_report_to_sibling_mandates(db_session):
    primary = Account(
        account_number="1412600",
        identification_number="2600",
        bank_code="jpmorgan",
        bank_name="JP Morgan",
        account_type="mandato",
        entity_name="Boatview",
        entity_type="sociedad",
        currency="USD",
        country="US",
    )
    sibling = Account(
        account_number="1483400",
        identification_number="3400",
        bank_code="jpmorgan",
        bank_name="JP Morgan",
        account_type="mandato",
        entity_name="Boatview",
        entity_type="sociedad",
        currency="USD",
        country="US",
    )
    outsider = Account(
        account_number="9999999",
        identification_number="9999",
        bank_code="jpmorgan",
        bank_name="JP Morgan",
        account_type="mandato",
        entity_name="Boatview",
        entity_type="sociedad",
        currency="USD",
        country="US",
    )
    db_session.add_all([primary, sibling, outsider])
    db_session.flush()

    doc = _mk_doc(db_session, "pdf_report", "hash-jpm-sibling-split")
    doc.filename = "2026 02 JPM Complementario - Reporte Mandato.pdf"
    doc.bank_code = "jpmorgan"
    doc.account_id = primary.id

    db_session.add_all(
        [
            MonthlyClosing(
                account_id=primary.id,
                closing_date=date(2026, 2, 28),
                year=2026,
                month=2,
                net_value=Decimal("110.00"),
                income=Decimal("1.00"),
                change_in_value=Decimal("2.00"),
                currency="USD",
                asset_allocation_json=json.dumps(
                    {
                        "Cash, Deposits & Money Market": {"value": "10"},
                        "Fixed Income": {"value": "100"},
                        "Equities": {"value": "0"},
                    }
                ),
                source_document_id=doc.id,
            ),
            MonthlyClosing(
                account_id=sibling.id,
                closing_date=date(2026, 2, 28),
                year=2026,
                month=2,
                net_value=Decimal("100.00"),
                income=Decimal("1.00"),
                change_in_value=Decimal("2.00"),
                currency="USD",
                asset_allocation_json=json.dumps(
                    {
                        "Cash, Deposits & Money Market": {"value": "0"},
                        "Fixed Income": {"value": "20"},
                        "Equities": {"value": "80"},
                    }
                ),
                source_document_id=doc.id,
            ),
            MonthlyClosing(
                account_id=outsider.id,
                closing_date=date(2026, 2, 28),
                year=2026,
                month=2,
                net_value=Decimal("50.00"),
                income=Decimal("1.00"),
                change_in_value=Decimal("2.00"),
                currency="USD",
                asset_allocation_json=json.dumps(
                    {
                        "Cash, Deposits & Money Market": {"value": "0"},
                        "Fixed Income": {"value": "50"},
                        "Equities": {"value": "0"},
                    }
                ),
                source_document_id=999999,
            ),
        ]
    )
    db_session.flush()

    result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name="parsers.jpmorgan.report_mandato",
        parser_version="1.3.0",
        source_file_hash="src-jpm-sibling-split",
        account_number="Varios",
        statement_date=date(2026, 2, 28),
        period_end=date(2026, 2, 28),
        currency="USD",
        qualitative_data={
            "asset_allocation": {
                "Investment Grade Fixed Income": {"value": 43.0, "unit": "%"},
                "High Yield Fixed Income": {"value": 12.0, "unit": "%"},
                "US Equities": {"value": 31.24, "unit": "%"},
                "Non US Equities": {"value": 11.76, "unit": "%"},
            }
        },
    )

    loader = DataLoadingService(db_session)
    stats = loader.load_asset_allocation_report(result=result, raw_document=doc)
    assert stats["monthly_closings_updated"] == 2

    primary_payload = json.loads(
        db_session.query(MonthlyClosing)
        .filter(
            MonthlyClosing.account_id == primary.id,
            MonthlyClosing.year == 2026,
            MonthlyClosing.month == 2,
        )
        .one()
        .asset_allocation_json
        or "{}"
    )
    assert Decimal(primary_payload["Investment Grade Fixed Income"]["value"]) == Decimal("78.18181818181818181818181818")
    assert Decimal(primary_payload["High Yield Fixed Income"]["value"]) == Decimal("21.81818181818181818181818182")

    sibling_payload = json.loads(
        db_session.query(MonthlyClosing)
        .filter(
            MonthlyClosing.account_id == sibling.id,
            MonthlyClosing.year == 2026,
            MonthlyClosing.month == 2,
        )
        .one()
        .asset_allocation_json
        or "{}"
    )
    assert Decimal(sibling_payload["Investment Grade Fixed Income"]["value"]) == Decimal("15.63636363636363636363636364")
    assert Decimal(sibling_payload["High Yield Fixed Income"]["value"]) == Decimal("4.36363636363636363636363636")
    assert Decimal(sibling_payload["US Equities"]["value"]) == Decimal("58.12093023255813953488372093")
    assert Decimal(sibling_payload["Non US Equities"]["value"]) == Decimal("21.87906976744186046511627907")

    outsider_payload = json.loads(
        db_session.query(MonthlyClosing)
        .filter(
            MonthlyClosing.account_id == outsider.id,
            MonthlyClosing.year == 2026,
            MonthlyClosing.month == 2,
        )
        .one()
        .asset_allocation_json
        or "{}"
    )
    assert "Investment Grade Fixed Income" not in outsider_payload
    assert "High Yield Fixed Income" not in outsider_payload


def test_load_asset_allocation_report_for_mandato_ignores_report_macros_and_splits_global_equity(db_session):
    acct = Account(
        account_number="ACC-GS-MAND-REPORT",
        identification_number="ACC-GS-MAND-REPORT",
        bank_code="goldman_sachs",
        bank_name="Goldman Sachs",
        account_type="mandato",
        entity_name="Boatview",
        entity_type="sociedad",
        currency="USD",
        country="US",
    )
    db_session.add(acct)
    db_session.flush()

    doc = _mk_doc(db_session, "pdf_report", "hash-report-gs-macro-safe")
    doc.filename = "2026 02 GS - Boatview & Telmar ex Bkg - Feb 2026.pdf"
    doc.bank_code = "goldman_sachs"
    doc.account_id = acct.id

    db_session.add(
        MonthlyClosing(
            account_id=acct.id,
            closing_date=date(2026, 2, 28),
            year=2026,
            month=2,
            net_value=Decimal("600.00"),
            income=Decimal("1.00"),
            change_in_value=Decimal("2.00"),
            currency="USD",
            asset_allocation_json=json.dumps(
                {
                    "Cash, Deposits & Money Market": {"value": "100"},
                    "Fixed Income": {"value": "200"},
                    "Equities": {"value": "300"},
                }
            ),
            source_document_id=None,
        )
    )
    db_session.add(
        MonthlyMetricNormalized(
            account_id=acct.id,
            closing_date=date(2026, 2, 28),
            year=2026,
            month=2,
            ending_value_with_accrual=Decimal("600.00"),
            ending_value_without_accrual=Decimal("600.00"),
            movements_net=Decimal("2.00"),
            profit_period=Decimal("1.00"),
            cash_value=Decimal("100.00"),
            asset_allocation_json=None,
            currency="USD",
            source_document_id=None,
        )
    )
    db_session.flush()

    result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name="parsers.goldman_sachs.report_mandato",
        parser_version="1.1.1",
        source_file_hash="src-report-gs-macro-safe",
        account_number=acct.account_number,
        statement_date=date(2026, 2, 28),
        period_end=date(2026, 2, 28),
        currency="USD",
        qualitative_data={
            "asset_allocation": {
                "Cash, Deposits & Money Market": {"value": 10.0},
                "Fixed Income": {"value": 190.0},
                "Investment Grade Fixed Income": {"value": 170.0},
                "High Yield Fixed Income": {"value": 20.0},
                "Equities": {"value": 200.0},
                "US Equities": {"value": 150.0},
                "Global Equity": {"value": 30.0},
                "Non US Equities": {"value": 20.0},
            }
        },
    )

    loader = DataLoadingService(db_session)
    stats = loader.load_asset_allocation_report(result=result, raw_document=doc)
    assert stats["monthly_closings_updated"] == 1

    closing = (
        db_session.query(MonthlyClosing)
        .filter(
            MonthlyClosing.account_id == acct.id,
            MonthlyClosing.year == 2026,
            MonthlyClosing.month == 2,
        )
        .one()
    )
    payload = json.loads(closing.asset_allocation_json or "{}")
    assert payload["Cash, Deposits & Money Market"]["value"] == "100"
    assert payload["Fixed Income"]["value"] == "200"
    assert payload["Equities"]["value"] == "300"
    assert Decimal(payload["Investment Grade Fixed Income"]["value"]) == Decimal("178.9473684210526315789473684")
    assert Decimal(payload["High Yield Fixed Income"]["value"]) == Decimal("21.0526315789473684210526316")
    assert Decimal(payload["US Equities"]["value"]) == Decimal("255")
    assert Decimal(payload["Non US Equities"]["value"]) == Decimal("45")

    normalized = (
        db_session.query(MonthlyMetricNormalized)
        .filter(
            MonthlyMetricNormalized.account_id == acct.id,
            MonthlyMetricNormalized.year == 2026,
            MonthlyMetricNormalized.month == 2,
        )
        .one()
    )
    normalized_payload = json.loads(normalized.asset_allocation_json or "{}")
    canonical = normalized_payload.get("__canonical_breakdown", {})
    assert Decimal(canonical["Cash, Deposits & Money Market"]["amount"]) == Decimal("100")
    assert Decimal(canonical["Investment Grade Fixed Income"]["amount"]) == Decimal("178.9473684210526315789473684")
    assert Decimal(canonical["High Yield Fixed Income"]["amount"]) == Decimal("21.0526315789473684210526316")
    assert Decimal(canonical["US Equities"]["amount"]) == Decimal("255")
    assert Decimal(canonical["Non US Equities"]["amount"]) == Decimal("45")


def test_load_parse_result_for_mandato_preserves_report_enrichments_on_cartola_reprocess(db_session):
    acct = Account(
        account_number="ACC-MAND-CARTOLA-REPROCESS",
        identification_number="ACC-MAND-CARTOLA-REPROCESS",
        bank_code="goldman_sachs",
        bank_name="Goldman Sachs",
        account_type="mandato",
        entity_name="Boatview",
        entity_type="sociedad",
        currency="USD",
        country="US",
    )
    db_session.add(acct)
    db_session.flush()

    existing_payload = {
        "Cash, Deposits & Money Market": {"value": "20"},
        "Fixed Income": {"value": "100"},
        "Equities": {"value": "80"},
        "Investment Grade Fixed Income": {"value": "70"},
        "High Yield Fixed Income": {"value": "30"},
        "US Equities": {"value": "50"},
        "Non US Equities": {"value": "30"},
        "__mandate_metrics": {
            "fixed_income_duration": {"value": 4.2, "unit": "years"},
            "fixed_income_yield": {"value": 5.1, "unit": "%"},
        },
    }
    db_session.add(
        MonthlyClosing(
            account_id=acct.id,
            closing_date=date(2026, 2, 28),
            year=2026,
            month=2,
            net_value=Decimal("250.00"),
            income=Decimal("1.00"),
            change_in_value=Decimal("2.00"),
            currency="USD",
            asset_allocation_json=json.dumps(existing_payload),
            source_document_id=None,
        )
    )
    db_session.add(
        MonthlyMetricNormalized(
            account_id=acct.id,
            closing_date=date(2026, 2, 28),
            year=2026,
            month=2,
            ending_value_with_accrual=Decimal("250.00"),
            ending_value_without_accrual=Decimal("250.00"),
            movements_net=Decimal("2.00"),
            profit_period=Decimal("1.00"),
            cash_value=Decimal("20.00"),
            asset_allocation_json=None,
            currency="USD",
            source_document_id=None,
        )
    )
    db_session.flush()

    doc = _mk_doc(db_session, "pdf_cartola", "hash-cartola-reprocess")
    doc.filename = "202602 Boatview - GS (Mandato_).pdf"
    doc.bank_code = "goldman_sachs"
    doc.account_id = acct.id
    pv = _mk_parser_version(db_session, "parsers.goldman_sachs.custody", "2.1.1")

    result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name="parsers.goldman_sachs.custody",
        parser_version="2.1.1",
        source_file_hash="src-cartola-reprocess",
        account_number=acct.account_number,
        statement_date=date(2026, 2, 28),
        period_end=date(2026, 2, 28),
        currency="USD",
        qualitative_data={
            "asset_allocation": {
                "Cash, Deposits & Money Market": {"value": "40"},
                "Fixed Income": {"value": "120"},
                "Equities": {"value": "90"},
                "Investment Grade Fixed Income": {"value": "999"},
            },
            "account_monthly_activity": [
                {
                    "account_number": acct.account_number,
                    "ending_value_with_accrual": "250",
                    "ending_value_without_accrual": "250",
                    "net_contributions": "2",
                    "utilidad": "1",
                }
            ],
        },
    )

    loader = DataLoadingService(db_session)
    stats = loader.load_parse_result(result=result, raw_document=doc, parser_version_id=pv.id)
    assert stats["monthly_closings"] == 1

    closing = (
        db_session.query(MonthlyClosing)
        .filter(
            MonthlyClosing.account_id == acct.id,
            MonthlyClosing.year == 2026,
            MonthlyClosing.month == 2,
        )
        .one()
    )
    payload = json.loads(closing.asset_allocation_json or "{}")
    assert payload["Cash, Deposits & Money Market"]["value"] == "40"
    assert payload["Fixed Income"]["value"] == "120"
    assert payload["Equities"]["value"] == "90"
    assert Decimal(payload["Investment Grade Fixed Income"]["value"]) == Decimal("84")
    assert Decimal(payload["High Yield Fixed Income"]["value"]) == Decimal("36")
    assert Decimal(payload["US Equities"]["value"]) == Decimal("56.25")
    assert Decimal(payload["Non US Equities"]["value"]) == Decimal("33.75")
    assert payload["__mandate_metrics"]["fixed_income_duration"]["value"] == 4.2
    assert payload["__mandate_metrics"]["fixed_income_yield"]["value"] == 5.1

    normalized = (
        db_session.query(MonthlyMetricNormalized)
        .filter(
            MonthlyMetricNormalized.account_id == acct.id,
            MonthlyMetricNormalized.year == 2026,
            MonthlyMetricNormalized.month == 2,
        )
        .one()
    )
    normalized_payload = json.loads(normalized.asset_allocation_json or "{}")
    canonical = normalized_payload.get("__canonical_breakdown", {})
    assert Decimal(canonical["Cash, Deposits & Money Market"]["amount"]) == Decimal("40")
    assert Decimal(canonical["Investment Grade Fixed Income"]["amount"]) == Decimal("84")
    assert Decimal(canonical["High Yield Fixed Income"]["amount"]) == Decimal("36")
    assert Decimal(canonical["US Equities"]["amount"]) == Decimal("56.25")
    assert Decimal(canonical["Non US Equities"]["amount"]) == Decimal("33.75")


def test_load_asset_allocation_report_skips_when_monthly_closing_missing(db_session):
    acct = _mk_account(db_session, "ACC-REPORT-EMPTY")
    doc = _mk_doc(db_session, "pdf_report", "hash-report-missing")
    doc.filename = "2026 03 Reporte - Feb 2026.pdf"
    doc.account_id = acct.id
    db_session.flush()

    result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name="parsers.system.report_asset_allocation",
        parser_version="1.0.0",
        source_file_hash="src-report-missing",
        account_number=acct.account_number,
        statement_date=date(2026, 3, 31),
        period_end=date(2026, 3, 31),
        currency="USD",
        qualitative_data={"asset_allocation": {"Cash": {"value": 10.0, "unit": "%"}}},
    )

    loader = DataLoadingService(db_session)
    stats = loader.load_asset_allocation_report(result=result, raw_document=doc)
    assert stats["monthly_closings_updated"] == 0
    assert stats["skipped"] == 1
    assert (
        db_session.query(MonthlyClosing)
        .filter(MonthlyClosing.account_id == acct.id)
        .count()
        == 0
    )


def test_load_asset_allocation_report_applies_metrics_series(db_session):
    acct = Account(
        account_number="3J 00432 P1",
        identification_number="0432",
        bank_code="ubs_miami",
        bank_name="UBS Miami",
        account_type="mandato",
        entity_name="Boatview",
        entity_type="sociedad",
        currency="USD",
        country="US",
    )
    db_session.add(acct)
    db_session.flush()

    doc = _mk_doc(db_session, "pdf_report", "hash-report-series")
    doc.filename = "UBS Miami duration&yield.pdf"
    doc.bank_code = "ubs_miami"
    doc.account_id = acct.id

    for month in (1, 2):
        db_session.add(
            MonthlyClosing(
                account_id=acct.id,
                closing_date=date(2026, month, 28 if month == 2 else 31),
                year=2026,
                month=month,
                net_value=Decimal("100.00"),
                income=Decimal("1.00"),
                change_in_value=Decimal("2.00"),
                currency="USD",
                asset_allocation_json=json.dumps({"Fixed Income": {"value": "60"}}),
                source_document_id=None,
            )
        )
    db_session.flush()

    result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name="parsers.ubs_miami.report_mandato",
        parser_version="1.1.0",
        source_file_hash="src-report-series",
        account_number="Varios",
        currency="USD",
        qualitative_data={
            "fixed_income_metrics_by_month": [
                {
                    "year": 2026,
                    "month": 1,
                    "fixed_income_duration": 5.1,
                    "fixed_income_yield": 5.0,
                    "yield_unit": "%",
                    "source": "ubs_miami_duration_yield_table",
                },
                {
                    "year": 2026,
                    "month": 2,
                    "fixed_income_duration": 5.1,
                    "fixed_income_yield": 5.0,
                    "yield_unit": "%",
                    "source": "ubs_miami_duration_yield_table",
                },
            ]
        },
    )

    loader = DataLoadingService(db_session)
    stats = loader.load_asset_allocation_report(result=result, raw_document=doc)
    assert stats["monthly_closings_updated"] == 2
    assert stats["skipped"] == 0

    jan = (
        db_session.query(MonthlyClosing)
        .filter(
            MonthlyClosing.account_id == acct.id,
            MonthlyClosing.year == 2026,
            MonthlyClosing.month == 1,
        )
        .one()
    )
    payload = json.loads(jan.asset_allocation_json or "{}")
    metrics = payload.get("__mandate_metrics") or {}
    assert metrics["fixed_income_duration"]["value"] == 5.1
    assert metrics["fixed_income_yield"]["value"] == 5.0


def test_load_asset_allocation_report_resolves_ubs_mandato_from_brokerage_doc_link(db_session):
    brokerage = Account(
        account_number="206-560552-01",
        identification_number="5652",
        bank_code="ubs",
        bank_name="UBS",
        account_type="brokerage",
        entity_name="Boatview",
        entity_type="sociedad",
        currency="USD",
        country="CH",
    )
    mandato = Account(
        account_number="206-560552-02",
        identification_number="5652",
        bank_code="ubs",
        bank_name="UBS",
        account_type="mandato",
        entity_name="Boatview",
        entity_type="sociedad",
        currency="USD",
        country="CH",
    )
    db_session.add_all([brokerage, mandato])
    db_session.flush()

    doc = _mk_doc(db_session, "pdf_report", "hash-report-ubs-remap")
    doc.filename = "Reporting 28.02.2026.pdf"
    doc.bank_code = "ubs"
    doc.account_id = brokerage.id

    db_session.add(
        MonthlyClosing(
            account_id=mandato.id,
            closing_date=date(2026, 2, 28),
            year=2026,
            month=2,
            net_value=Decimal("100.00"),
            income=Decimal("1.00"),
            change_in_value=Decimal("2.00"),
            currency="USD",
            asset_allocation_json=json.dumps({"Fixed Income": {"value": "60"}}),
            source_document_id=None,
        )
    )
    db_session.add(
        MonthlyMetricNormalized(
            account_id=mandato.id,
            closing_date=date(2026, 2, 28),
            year=2026,
            month=2,
            ending_value_with_accrual=Decimal("100.00"),
            ending_value_without_accrual=Decimal("100.00"),
            movements_net=Decimal("2.00"),
            profit_period=Decimal("1.00"),
            cash_value=Decimal("0"),
            asset_allocation_json=json.dumps(
                {
                    "Cash, Deposits & Money Market": {"value": "10"},
                    "Fixed Income": {"value": "60"},
                    "Equities": {"value": "30"},
                }
            ),
            currency="USD",
            source_document_id=None,
        )
    )
    db_session.flush()

    result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name="parsers.ubs.report_mandato",
        parser_version="1.1.0",
        source_file_hash="src-report-ubs-remap",
        account_number="560552",
        statement_date=date(2026, 2, 28),
        period_end=date(2026, 2, 28),
        currency="USD",
        qualitative_data={
            "asset_allocation": {
                "High Yield Fixed Income": {"value": 10.0, "unit": "%"},
            }
        },
    )

    loader = DataLoadingService(db_session)
    stats = loader.load_asset_allocation_report(result=result, raw_document=doc)
    assert stats["monthly_closings_updated"] == 1
    assert stats["skipped"] == 0

    closing = (
        db_session.query(MonthlyClosing)
        .filter(
            MonthlyClosing.account_id == mandato.id,
            MonthlyClosing.year == 2026,
            MonthlyClosing.month == 2,
        )
        .one()
    )
    payload = json.loads(closing.asset_allocation_json or "{}")
    assert "High Yield Fixed Income" in payload

    normalized = (
        db_session.query(MonthlyMetricNormalized)
        .filter(
            MonthlyMetricNormalized.account_id == mandato.id,
            MonthlyMetricNormalized.year == 2026,
            MonthlyMetricNormalized.month == 2,
        )
        .one()
    )
    normalized_payload = json.loads(normalized.asset_allocation_json or "{}")
    assert "High Yield Fixed Income" in normalized_payload
