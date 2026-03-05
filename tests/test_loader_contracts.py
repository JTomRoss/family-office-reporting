from __future__ import annotations

from datetime import date
import hashlib
from decimal import Decimal
from pathlib import Path

import pytest

from backend.db.models import Account, MonthlyClosing, ParserVersion, RawDocument
from backend.services.data_loading_service import DataLoadingService
from parsers.base import ParseResult, ParserStatus
from parsers.bbh.custody import BBHCustodyParser
from parsers.goldman_sachs.custody import GoldmanSachsCustodyParser
from parsers.ubs.custody import UBSSwitzerlandCustodyParser


def _cartola_path(filename: str) -> Path:
    return Path(__file__).resolve().parents[1] / "Documentos" / "Cartolas" / filename


def _require(path: Path) -> None:
    if not path.exists():
        pytest.skip(f"Fixture PDF not found: {path}")


def _mk_hash(seed: str) -> str:
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


def _create_account(
    db_session,
    *,
    account_number: str,
    bank_code: str,
    account_type: str,
) -> Account:
    acct = Account(
        account_number=account_number,
        identification_number=account_number,
        bank_code=bank_code,
        bank_name=bank_code.replace("_", " ").title(),
        account_type=account_type,
        entity_name="Boatview",
        entity_type="sociedad",
        currency="USD",
        country="US",
    )
    db_session.add(acct)
    db_session.flush()
    return acct


def _create_raw_document(db_session, *, filename: str, bank_code: str) -> RawDocument:
    doc = RawDocument(
        filename=filename,
        filepath=f"data/raw/{bank_code}/pdf_cartola/{filename}",
        file_type="pdf_cartola",
        sha256_hash=_mk_hash(f"{bank_code}:{filename}"),
        file_size_bytes=1,
        bank_code=bank_code,
        status="parsed",
    )
    db_session.add(doc)
    db_session.flush()
    return doc


def _create_parser_version(db_session, parser) -> ParserVersion:
    pv = ParserVersion(
        parser_name=parser.get_parser_name(),
        version=parser.VERSION,
        source_hash=parser.get_source_hash(),
        description=parser.DESCRIPTION,
    )
    db_session.add(pv)
    db_session.flush()
    return pv


def test_loader_maps_goldman_mandato_activity_to_monthly_closing(db_session):
    path = _cartola_path("202512 Boatview - Mandato - GoldmanSachs.pdf")
    _require(path)

    parser = GoldmanSachsCustodyParser()
    result = parser.safe_parse(path)
    assert result.is_success
    assert result.account_number == "451-9"

    acct = _create_account(
        db_session,
        account_number="451-9",
        bank_code="goldman_sachs",
        account_type="mandato",
    )
    doc = _create_raw_document(
        db_session,
        filename="202512 Boatview - Mandato - GoldmanSachs.pdf",
        bank_code="goldman_sachs",
    )
    pv = _create_parser_version(db_session, parser)

    loader = DataLoadingService(db_session)
    stats = loader.load_parse_result(result=result, raw_document=doc, parser_version_id=pv.id)
    assert stats["monthly_closings"] == 1
    assert not stats["errors"]

    mc = (
        db_session.query(MonthlyClosing)
        .filter(MonthlyClosing.account_id == acct.id)
        .one()
    )
    assert mc.income == Decimal("1106908.06")
    assert mc.change_in_value == Decimal("0.00")


def test_loader_maps_bbh_mandato_activity_to_monthly_closing(db_session):
    path = _cartola_path("202512 Boatview - Mandato - BBH.pdf")
    _require(path)

    parser = BBHCustodyParser()
    result = parser.safe_parse(path)
    assert result.is_success
    assert result.account_number in {"7085", "7101"}

    acct = _create_account(
        db_session,
        account_number=result.account_number,
        bank_code="bbh",
        account_type="mandato",
    )
    doc = _create_raw_document(
        db_session,
        filename="202512 Boatview - Mandato - BBH.pdf",
        bank_code="bbh",
    )
    pv = _create_parser_version(db_session, parser)

    loader = DataLoadingService(db_session)
    stats = loader.load_parse_result(result=result, raw_document=doc, parser_version_id=pv.id)
    assert stats["monthly_closings"] == 1
    assert not stats["errors"]

    mc = (
        db_session.query(MonthlyClosing)
        .filter(MonthlyClosing.account_id == acct.id)
        .one()
    )
    assert mc.income is not None
    assert mc.change_in_value is not None


def test_loader_ubs_history_backfills_prior_months(db_session):
    parser = UBSSwitzerlandCustodyParser()
    # Reproduce producción: Session autoflush deshabilitado en backend.db.session
    db_session.autoflush = False

    acct = _create_account(
        db_session,
        account_number="206-560552-02",
        bank_code="ubs",
        account_type="mandato",
    )
    doc = _create_raw_document(
        db_session,
        filename="202512 Boatview UBS SW (206-560552-02) 511UBS SW_P2.pdf",
        bank_code="ubs",
    )
    pv = _create_parser_version(db_session, parser)

    # Seed monthly closings from monthly statements (source of net_value).
    db_session.add_all(
        [
            MonthlyClosing(
                account_id=acct.id,
                closing_date=date(2024, 12, 31),
                year=2024,
                month=12,
                net_value=Decimal("90"),
                total_assets=Decimal("90"),
                currency="USD",
                income=Decimal("0"),
                change_in_value=Decimal("0"),
                source_document_id=doc.id,
            ),
            MonthlyClosing(
                account_id=acct.id,
                closing_date=date(2025, 1, 31),
                year=2025,
                month=1,
                net_value=Decimal("100"),
                total_assets=Decimal("100"),
                currency="USD",
                income=Decimal("999"),
                change_in_value=Decimal("0"),
                source_document_id=doc.id,
            ),
            MonthlyClosing(
                account_id=acct.id,
                closing_date=date(2025, 2, 28),
                year=2025,
                month=2,
                net_value=Decimal("120"),
                total_assets=Decimal("120"),
                currency="USD",
                income=Decimal("999"),
                change_in_value=Decimal("0"),
                source_document_id=doc.id,
            ),
            MonthlyClosing(
                account_id=acct.id,
                closing_date=date(2025, 3, 31),
                year=2025,
                month=3,
                net_value=Decimal("130"),
                total_assets=Decimal("130"),
                currency="USD",
                income=Decimal("999"),
                change_in_value=Decimal("0"),
                source_document_id=doc.id,
            ),
        ]
    )
    db_session.flush()

    result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name=parser.get_parser_name(),
        parser_version=parser.VERSION,
        source_file_hash=_mk_hash("ubs-history"),
        bank_code="ubs",
        account_number=acct.account_number,
        statement_date=date(2025, 12, 31),
        period_start=date(2025, 12, 1),
        period_end=date(2025, 12, 31),
        opening_balance=Decimal("91390762"),
        closing_balance=Decimal("91996301"),
        currency="USD",
        qualitative_data={
            "accounts": [
                {
                    "account_number": acct.account_number,
                    "beginning_value": "91390762",
                    "ending_value": "91996301",
                }
            ],
            "account_monthly_activity": [
                {
                    "account_number": acct.account_number,
                    "ending_value_with_accrual": "91996301",
                    "ending_value_without_accrual": "91996301",
                    "net_contributions": "0",
                    "utilidad": "606973",
                }
            ],
            "account_monthly_activity_history": [
                {
                    "account_number": acct.account_number,
                    "period_year": 2025,
                    "period_month": 1,
                    "period_end": "2025-01-31",
                    "ending_value_with_accrual": "81861001",
                    "net_contributions": "10",
                    "utilidad": "1751949",
                },
                {
                    "account_number": acct.account_number,
                    "period_year": 2025,
                    "period_month": 2,
                    "period_end": "2025-02-28",
                    "ending_value_with_accrual": "82116843",
                    "net_contributions": "5",
                    "utilidad": "255937",
                },
                {
                    "account_number": acct.account_number,
                    "period_year": 2025,
                    "period_month": 3,
                    "period_end": "2025-03-31",
                    "ending_value_with_accrual": "81282162",
                    "net_contributions": "-2",
                    "utilidad": "-831817",
                },
                {
                    "account_number": acct.account_number,
                    "period_year": 2025,
                    "period_month": 12,
                    "period_end": "2025-12-31",
                    "ending_value_with_accrual": "91996301",
                    "net_contributions": "0",
                    "utilidad": "606973",
                },
            ],
        },
    )

    loader = DataLoadingService(db_session)
    stats = loader.load_parse_result(result=result, raw_document=doc, parser_version_id=pv.id)
    assert stats["monthly_closings"] == 1
    assert not stats["errors"]

    closings = (
        db_session.query(MonthlyClosing)
        .filter(MonthlyClosing.account_id == acct.id, MonthlyClosing.year == 2025)
        .order_by(MonthlyClosing.month)
        .all()
    )
    assert len(closings) == 4

    jan = next(row for row in closings if row.month == 1)
    feb = next(row for row in closings if row.month == 2)
    march = next(row for row in closings if row.month == 3)
    assert jan.net_value == Decimal("81861001")
    assert jan.change_in_value == Decimal("10")
    assert jan.income == Decimal("1751949")
    assert feb.net_value == Decimal("82116843")
    assert feb.change_in_value == Decimal("5")
    assert feb.income == Decimal("255937")
    assert march.net_value == Decimal("81282162")
    assert march.change_in_value == Decimal("-2")
    assert march.income == Decimal("-831817")


def test_loader_bbh_prior_adjustment_updates_previous_month(db_session):
    parser = BBHCustodyParser()
    acct = _create_account(
        db_session,
        account_number="7085",
        bank_code="bbh",
        account_type="mandato",
    )
    doc = _create_raw_document(
        db_session,
        filename="bbh-feb.pdf",
        bank_code="bbh",
    )
    pv = _create_parser_version(db_session, parser)

    db_session.add(
        MonthlyClosing(
            account_id=acct.id,
            closing_date=date(2025, 1, 31),
            year=2025,
            month=1,
            net_value=Decimal("100"),
            total_assets=Decimal("100"),
            currency="USD",
            income=Decimal("5"),
            change_in_value=Decimal("59.65"),
            source_document_id=doc.id,
        )
    )
    db_session.flush()

    result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name=parser.get_parser_name(),
        parser_version=parser.VERSION,
        source_file_hash=_mk_hash("bbh-feb"),
        bank_code="bbh",
        account_number=acct.account_number,
        statement_date=date(2025, 2, 28),
        period_start=date(2025, 2, 1),
        period_end=date(2025, 2, 28),
        opening_balance=Decimal("100"),
        closing_balance=Decimal("110"),
        currency="USD",
        qualitative_data={
            "accounts": [
                {
                    "account_number": acct.account_number,
                    "beginning_value": "100",
                    "ending_value": "110",
                }
            ],
            "account_monthly_activity": [
                {
                    "account_number": acct.account_number,
                    "net_contributions": "0",
                    "net_contributions_ytd": "2210.88",
                    "prior_period_adjustments": "2151.23",
                    "utilidad": "10",
                }
            ],
        },
    )

    loader = DataLoadingService(db_session)
    stats = loader.load_parse_result(result=result, raw_document=doc, parser_version_id=pv.id)
    assert stats["monthly_closings"] == 1
    assert not stats["errors"]

    jan = (
        db_session.query(MonthlyClosing)
        .filter(MonthlyClosing.account_id == acct.id, MonthlyClosing.year == 2025, MonthlyClosing.month == 1)
        .one()
    )
    feb = (
        db_session.query(MonthlyClosing)
        .filter(MonthlyClosing.account_id == acct.id, MonthlyClosing.year == 2025, MonthlyClosing.month == 2)
        .one()
    )
    assert jan.change_in_value == Decimal("2210.88")
    assert feb.change_in_value == Decimal("0")


def test_loader_ytd_alignment_adjusts_current_month_non_bbh(db_session):
    parser = BBHCustodyParser()
    acct = _create_account(
        db_session,
        account_number="TEST-JPM-1",
        bank_code="jpmorgan",
        account_type="mandato",
    )
    doc = _create_raw_document(
        db_session,
        filename="jpm-feb.pdf",
        bank_code="jpmorgan",
    )
    pv = _create_parser_version(db_session, parser)

    db_session.add(
        MonthlyClosing(
            account_id=acct.id,
            closing_date=date(2025, 1, 31),
            year=2025,
            month=1,
            net_value=Decimal("1000"),
            total_assets=Decimal("1000"),
            currency="USD",
            income=Decimal("50"),
            change_in_value=Decimal("100"),
            source_document_id=doc.id,
        )
    )
    db_session.flush()

    result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name=parser.get_parser_name(),
        parser_version=parser.VERSION,
        source_file_hash=_mk_hash("jpm-feb"),
        bank_code="jpmorgan",
        account_number=acct.account_number,
        statement_date=date(2025, 2, 28),
        period_start=date(2025, 2, 1),
        period_end=date(2025, 2, 28),
        opening_balance=Decimal("1000"),
        closing_balance=Decimal("1110"),
        currency="USD",
        qualitative_data={
            "accounts": [
                {
                    "account_number": acct.account_number,
                    "beginning_value": "1000",
                    "ending_value": "1110",
                }
            ],
            "account_monthly_activity": [
                {
                    "account_number": acct.account_number,
                    "net_contributions": "20",
                    "net_contributions_ytd": "130",
                    "utilidad": "40",
                    "utilidad_ytd": "120",
                }
            ],
        },
    )

    loader = DataLoadingService(db_session)
    stats = loader.load_parse_result(result=result, raw_document=doc, parser_version_id=pv.id)
    assert stats["monthly_closings"] == 1
    assert not stats["errors"]

    feb = (
        db_session.query(MonthlyClosing)
        .filter(MonthlyClosing.account_id == acct.id, MonthlyClosing.year == 2025, MonthlyClosing.month == 2)
        .one()
    )
    # ytd_mov=130 vs (100+20) => +10 on current month
    assert feb.change_in_value == Decimal("30")
    # ytd_util=120 vs (50+40) => +30 on current month
    assert feb.income == Decimal("70")
