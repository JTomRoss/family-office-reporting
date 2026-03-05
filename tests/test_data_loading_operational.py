from __future__ import annotations

from datetime import date
from decimal import Decimal

from backend.db.models import Account, DailyMovement, DailyPosition, DailyPrice, RawDocument
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
    doc.account_id = acct.id
    db_session.flush()

    result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name="parsers.system.report_asset_allocation",
        parser_version="1.0.0",
        source_file_hash="src-report",
        account_number=acct.account_number,
        statement_date=date(2025, 1, 31),
        period_end=date(2025, 1, 31),
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
