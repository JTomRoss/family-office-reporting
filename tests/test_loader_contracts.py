from __future__ import annotations

from datetime import date
import hashlib
import json
from decimal import Decimal
from pathlib import Path

import pytest

from backend.db.models import (
    Account,
    EtfComposition,
    MonthlyClosing,
    MonthlyMetricNormalized,
    ParsedStatement,
    ParserVersion,
    RawDocument,
    ValidationLog,
)
from backend.services.data_loading_service import DataLoadingService
from parsers.base import ParseResult, ParsedRow, ParserStatus
from parsers.bbh.custody import BBHCustodyParser
from parsers.goldman_sachs.custody import GoldmanSachsCustodyParser
from parsers.jpmorgan.bonds import JPMorganBondsParser
from parsers.jpmorgan.brokerage import JPMorganBrokerageParser
from parsers.jpmorgan.etf import JPMorganEtfParser
from parsers.ubs.custody import UBSSwitzerlandCustodyParser


def _cartola_path(filename: str) -> Path:
    return Path(__file__).resolve().parents[1] / "Documentos" / "Cartolas" / filename


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
    alloc = json.loads(mc.asset_allocation_json or "{}")
    assert set(alloc.keys()) == {
        "Cash, Deposits & Money Market",
        "Fixed Income",
        "Equities",
    }
    assert Decimal(alloc["Cash, Deposits & Money Market"]["value"]) == Decimal("38078891.58")
    assert Decimal(alloc["Fixed Income"]["value"]) == Decimal("133671048.88")
    assert Decimal(alloc["Equities"]["value"]) == Decimal("102110902.70")

    normalized = (
        db_session.query(MonthlyMetricNormalized)
        .filter(
            MonthlyMetricNormalized.account_id == acct.id,
            MonthlyMetricNormalized.year == mc.year,
            MonthlyMetricNormalized.month == mc.month,
        )
        .one()
    )
    assert normalized.ending_value_with_accrual == mc.net_value
    assert normalized.ending_value_without_accrual == mc.net_value
    assert normalized.movements_net == mc.change_in_value
    assert normalized.profit_period == mc.income


def test_loader_maps_goldman_raw_subportfolio_summary_to_monthly_closing(db_session):
    path = _goldman_raw_cartola_path("202305 Telmar - GS.pdf")
    _require(path)

    parser = GoldmanSachsCustodyParser()
    result = parser.safe_parse(path)
    assert result.is_success
    assert result.account_number == "097-4"

    acct = _create_account(
        db_session,
        account_number="097-4",
        bank_code="goldman_sachs",
        account_type="mandato",
    )
    doc = _create_raw_document(
        db_session,
        filename="202305 Telmar - GS.pdf",
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
    assert mc.net_value == Decimal("110676747.84")
    assert mc.change_in_value == Decimal("-0.10")
    assert mc.income == Decimal("-555931.41")

    alloc = json.loads(mc.asset_allocation_json or "{}")
    assert set(alloc.keys()) == {
        "Cash, Deposits & Money Market",
        "Fixed Income",
        "Equities",
    }
    assert Decimal(alloc["Cash, Deposits & Money Market"]["value"]) == Decimal("1192230.13")
    assert Decimal(alloc["Fixed Income"]["value"]) == Decimal("11893895.07")
    assert Decimal(alloc["Equities"]["value"]) == Decimal("37461987.07")

    normalized = (
        db_session.query(MonthlyMetricNormalized)
        .filter(
            MonthlyMetricNormalized.account_id == acct.id,
            MonthlyMetricNormalized.year == mc.year,
            MonthlyMetricNormalized.month == mc.month,
        )
        .one()
    )
    assert normalized.ending_value_with_accrual == mc.net_value
    assert normalized.movements_net == mc.change_in_value
    assert normalized.profit_period == mc.income


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
    alloc = json.loads(mc.asset_allocation_json or "{}")
    assert set(alloc.keys()) == {
        "Cash, Deposits & Money Market",
        "Fixed Income",
        "Equities",
    }
    assert Decimal(alloc["Cash, Deposits & Money Market"]["value"]) == Decimal("516437.81")
    assert Decimal(alloc["Fixed Income"]["value"]) == Decimal("51485414.23")
    assert Decimal(alloc["Equities"]["value"]) == Decimal("38198758.82")


def test_loader_prefers_account_level_asset_allocation_for_multi_account_mandate(db_session):
    acct = _create_account(
        db_session,
        account_number="1412600",
        bank_code="jpmorgan",
        account_type="mandato",
    )
    doc = _create_raw_document(
        db_session,
        filename="jpm-multi.pdf",
        bank_code="jpmorgan",
    )
    parser = BBHCustodyParser()
    pv = _create_parser_version(db_session, parser)

    result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name=parser.get_parser_name(),
        parser_version=parser.VERSION,
        source_file_hash=_mk_hash("jpm-multi"),
        bank_code="jpmorgan",
        account_number="Varios",
        account_numbers=["1179200", "1412600", "1483400"],
        statement_date=date(2025, 12, 31),
        period_start=date(2025, 12, 1),
        period_end=date(2025, 12, 31),
        closing_balance=Decimal("337089559.53"),
        currency="USD",
        qualitative_data={
            "asset_allocation": {
                "Cash, Deposits & Short Term": {"ending": "5633705.15"},
                "Fixed Income": {"ending": "187640939.13"},
                "Equities": {"ending": "143814915.29"},
            },
            "account_monthly_activity": [
                {
                    "account_number": "1412600",
                    "ending_value_with_accrual": "148531792.87",
                    "ending_value_without_accrual": "148531792.87",
                    "net_contributions": "0",
                    "utilidad": "280247.97",
                    "asset_allocation": {
                        "Cash, Deposits & Short Term": {"ending": "2811109.86"},
                        "Fixed Income": {"ending": "145720683.04"},
                    },
                }
            ],
        },
    )

    loader = DataLoadingService(db_session)
    stats = loader.load_parse_result(result=result, raw_document=doc, parser_version_id=pv.id)
    assert stats["monthly_closings"] == 1
    assert not stats["errors"]

    mc = (
        db_session.query(MonthlyClosing)
        .filter(MonthlyClosing.account_id == acct.id)
        .one()
    )
    alloc = json.loads(mc.asset_allocation_json or "{}")
    assert Decimal(alloc["Cash, Deposits & Money Market"]["value"]) == Decimal("2811109.86")
    assert Decimal(alloc["Fixed Income"]["value"]) == Decimal("145720683.04")
    assert Decimal(alloc["Equities"]["value"]) == Decimal("0")


def test_loader_etf_collapses_same_etf_code_rows_without_integrity_error(db_session):
    parser = JPMorganEtfParser()
    acct = _create_account(
        db_session,
        account_number="E30994009",
        bank_code="jpmorgan",
        account_type="etf",
    )
    doc = _create_raw_document(
        db_session,
        filename="202401 Telmar JPM NY ETF (4009).pdf",
        bank_code="jpmorgan",
    )
    pv = _create_parser_version(db_session, parser)

    result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name=parser.get_parser_name(),
        parser_version=parser.VERSION,
        source_file_hash=_mk_hash("etf-dup-code"),
        bank_code="jpmorgan",
        account_number=acct.account_number,
        statement_date=date(2024, 1, 31),
        period_start=date(2024, 1, 1),
        period_end=date(2024, 1, 31),
        opening_balance=Decimal("1000000"),
        closing_balance=Decimal("1200000"),
        currency="USD",
        rows=[
            ParsedRow(
                data={
                    "instrument": "ISHARES CORE MSCI WORLD",
                    "market_value": "11858307.51",
                    "account_number": acct.account_number,
                },
                row_number=1,
                confidence=1.0,
            ),
            ParsedRow(
                data={
                    "instrument": "ISHARES CORE MSCI WORLD UCIT",
                    "market_value": "188989.50",
                    "account_number": acct.account_number,
                },
                row_number=2,
                confidence=1.0,
            ),
        ],
        qualitative_data={
            "accounts": [
                {
                    "account_number": acct.account_number,
                    "beginning_value": "1000000",
                    "ending_value": "1200000",
                }
            ],
            "account_monthly_activity": [
                {
                    "account_number": acct.account_number,
                    "ending_value_with_accrual": "1200000",
                    "ending_value_without_accrual": "1200000",
                    "net_contributions": "0",
                    "utilidad": "10000",
                }
            ],
        },
    )

    loader = DataLoadingService(db_session)
    stats = loader.load_parse_result(result=result, raw_document=doc, parser_version_id=pv.id)
    assert stats["monthly_closings"] == 1
    assert stats["etf_compositions"] == 1
    assert not stats["errors"]

    rows = (
        db_session.query(EtfComposition)
        .filter(
            EtfComposition.account_id == acct.id,
            EtfComposition.year == 2024,
            EtfComposition.month == 1,
        )
        .all()
    )
    assert len(rows) == 1
    assert rows[0].etf_code == "ISHARES_CORE_MSCI"
    assert rows[0].market_value == Decimal("12047297.01")


def test_loader_reprocess_replaces_etf_snapshot_when_code_changes(db_session):
    parser = JPMorganEtfParser()
    acct = _create_account(
        db_session,
        account_number="E30994009",
        bank_code="jpmorgan",
        account_type="etf",
    )
    doc = _create_raw_document(
        db_session,
        filename="202410 Telmar JPM NY ETF (4009).pdf",
        bank_code="jpmorgan",
    )
    pv = _create_parser_version(db_session, parser)
    loader = DataLoadingService(db_session)

    old_result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name=parser.get_parser_name(),
        parser_version=parser.VERSION,
        source_file_hash=_mk_hash("etf-reprocess-old"),
        bank_code="jpmorgan",
        account_number=acct.account_number,
        statement_date=date(2024, 10, 31),
        period_start=date(2024, 10, 1),
        period_end=date(2024, 10, 31),
        opening_balance=Decimal("1000000"),
        closing_balance=Decimal("1000100"),
        currency="USD",
        rows=[
            ParsedRow(
                data={
                    "instrument": "OTHER INVESTMENT GRADE SECURITIES",
                    "market_value": "100.00",
                    "account_number": acct.account_number,
                },
                row_number=1,
                confidence=1.0,
            ),
        ],
        qualitative_data={
            "accounts": [
                {
                    "account_number": acct.account_number,
                    "beginning_value": "1000000",
                    "ending_value": "1000100",
                }
            ],
            "account_monthly_activity": [
                {
                    "account_number": acct.account_number,
                    "ending_value_with_accrual": "1000100",
                    "ending_value_without_accrual": "1000100",
                    "net_contributions": "0",
                    "utilidad": "100",
                }
            ],
        },
    )
    loader.load_parse_result(result=old_result, raw_document=doc, parser_version_id=pv.id)

    new_result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name=parser.get_parser_name(),
        parser_version=parser.VERSION,
        source_file_hash=_mk_hash("etf-reprocess-new"),
        bank_code="jpmorgan",
        account_number=acct.account_number,
        statement_date=date(2024, 10, 31),
        period_start=date(2024, 10, 1),
        period_end=date(2024, 10, 31),
        opening_balance=Decimal("1000000"),
        closing_balance=Decimal("1000100"),
        currency="USD",
        rows=[
            ParsedRow(
                data={
                    "instrument": "SSGA SPDR ETFS EU I PB L C-SPD ETF ON BLOOMBERG",
                    "market_value": "100.00",
                    "account_number": acct.account_number,
                },
                row_number=1,
                confidence=1.0,
            ),
        ],
        qualitative_data={
            "accounts": [
                {
                    "account_number": acct.account_number,
                    "beginning_value": "1000000",
                    "ending_value": "1000100",
                }
            ],
            "account_monthly_activity": [
                {
                    "account_number": acct.account_number,
                    "ending_value_with_accrual": "1000100",
                    "ending_value_without_accrual": "1000100",
                    "net_contributions": "0",
                    "utilidad": "100",
                }
            ],
        },
    )
    loader.load_parse_result(result=new_result, raw_document=doc, parser_version_id=pv.id)

    rows = (
        db_session.query(EtfComposition)
        .filter(
            EtfComposition.account_id == acct.id,
            EtfComposition.year == 2024,
            EtfComposition.month == 10,
        )
        .all()
    )
    assert len(rows) == 1
    assert rows[0].etf_name == "SSGA SPDR ETFS EU I PB L C-SPD ETF ON BLOOMBERG"
    assert rows[0].market_value == Decimal("100.00")


def test_loader_jpm_etf_parser_isolates_subaccounts_to_etf_type(db_session):
    acct_b = _create_account(
        db_session,
        account_number="B99719001",
        bank_code="jpmorgan",
        account_type="brokerage",
    )
    acct_e = _create_account(
        db_session,
        account_number="E31070007",
        bank_code="jpmorgan",
        account_type="etf",
    )
    doc = _create_raw_document(
        db_session,
        filename="202512 Boatview JPM NY ETF (0007).pdf",
        bank_code="jpmorgan",
    )
    parser = JPMorganEtfParser()
    pv = _create_parser_version(db_session, parser)

    result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name=parser.get_parser_name(),
        parser_version=parser.VERSION,
        source_file_hash=_mk_hash("jpm-etf-scope"),
        bank_code="jpmorgan",
        account_number="Varios",
        account_numbers=[acct_b.account_number, acct_e.account_number],
        statement_date=date(2025, 12, 31),
        period_start=date(2025, 12, 1),
        period_end=date(2025, 12, 31),
        opening_balance=Decimal("1000"),
        closing_balance=Decimal("2000"),
        currency="USD",
        qualitative_data={
            "accounts": [
                {
                    "account_number": acct_b.account_number,
                    "beginning_value": "1000",
                    "ending_value": "1500",
                },
                {
                    "account_number": acct_e.account_number,
                    "beginning_value": "2000",
                    "ending_value": "2500",
                },
            ],
            "account_monthly_activity": [
                {
                    "account_number": acct_b.account_number,
                    "ending_value_with_accrual": "1500",
                    "ending_value_without_accrual": "1500",
                    "net_contributions": "10",
                    "utilidad": "20",
                },
                {
                    "account_number": acct_e.account_number,
                    "ending_value_with_accrual": "2500",
                    "ending_value_without_accrual": "2500",
                    "net_contributions": "30",
                    "utilidad": "40",
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
        .filter(MonthlyClosing.year == 2025, MonthlyClosing.month == 12)
        .all()
    )
    assert len(closings) == 1
    assert closings[0].account_id == acct_e.id
    assert closings[0].net_value == Decimal("2500")


def test_loader_jpm_brokerage_parser_isolates_subaccounts_to_brokerage_type(db_session):
    acct_b = _create_account(
        db_session,
        account_number="B99719001",
        bank_code="jpmorgan",
        account_type="brokerage",
    )
    acct_e = _create_account(
        db_session,
        account_number="E31070007",
        bank_code="jpmorgan",
        account_type="etf",
    )
    doc = _create_raw_document(
        db_session,
        filename="202512 Boatview JPM NY Brokerage (9001).pdf",
        bank_code="jpmorgan",
    )
    parser = JPMorganBrokerageParser()
    pv = _create_parser_version(db_session, parser)

    result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name=parser.get_parser_name(),
        parser_version=parser.VERSION,
        source_file_hash=_mk_hash("jpm-brokerage-scope"),
        bank_code="jpmorgan",
        account_number="Varios",
        account_numbers=[acct_b.account_number, acct_e.account_number],
        statement_date=date(2025, 12, 31),
        period_start=date(2025, 12, 1),
        period_end=date(2025, 12, 31),
        opening_balance=Decimal("1000"),
        closing_balance=Decimal("2000"),
        currency="USD",
        qualitative_data={
            "accounts": [
                {
                    "account_number": acct_b.account_number,
                    "beginning_value": "1000",
                    "ending_value": "1500",
                },
                {
                    "account_number": acct_e.account_number,
                    "beginning_value": "2000",
                    "ending_value": "2500",
                },
            ],
            "account_monthly_activity": [
                {
                    "account_number": acct_b.account_number,
                    "ending_value_with_accrual": "1500",
                    "ending_value_without_accrual": "1500",
                    "net_contributions": "10",
                    "utilidad": "20",
                },
                {
                    "account_number": acct_e.account_number,
                    "ending_value_with_accrual": "2500",
                    "ending_value_without_accrual": "2500",
                    "net_contributions": "30",
                    "utilidad": "40",
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
        .filter(MonthlyClosing.year == 2025, MonthlyClosing.month == 12)
        .all()
    )
    assert len(closings) == 1
    assert closings[0].account_id == acct_b.id
    assert closings[0].net_value == Decimal("1500")


def test_loader_jpm_brokerage_does_not_fill_monthly_from_account_ytd(db_session):
    acct = _create_account(
        db_session,
        account_number="E92671008",
        bank_code="jpmorgan",
        account_type="brokerage",
    )
    doc = _create_raw_document(
        db_session,
        filename="20251130-statements-1008-.pdf",
        bank_code="jpmorgan",
    )
    parser = JPMorganBrokerageParser()
    pv = _create_parser_version(db_session, parser)

    result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name=parser.get_parser_name(),
        parser_version=parser.VERSION,
        source_file_hash=_mk_hash("jpm-brokerage-no-ytd-fill"),
        bank_code="jpmorgan",
        account_number=acct.account_number,
        statement_date=date(2025, 11, 30),
        period_start=date(2025, 11, 1),
        period_end=date(2025, 11, 30),
        opening_balance=Decimal("3.69"),
        closing_balance=Decimal("3.69"),
        currency="USD",
        qualitative_data={
            "accounts": [
                {
                    "account_number": acct.account_number,
                    "beginning_value": "3.69",
                    "ending_value": "3.69",
                }
            ],
            "account_monthly_activity": [
                {
                    "account_number": acct.account_number,
                    "ending_value_with_accrual": "3.69",
                    "ending_value_without_accrual": "3.69",
                    "net_contributions": "0",
                    "interpretation_notes": [
                        "Income & Distributions mensual en blanco interpretado como 0; YTD se conserva solo como control."
                    ],
                }
            ],
            "account_ytd": [
                {
                    "account_number": acct.account_number,
                    "beginning_value": "0",
                    "ending_value": "3.69",
                    "income": "5.91",
                    "change_investment": "5.91",
                }
            ],
        },
    )

    loader = DataLoadingService(db_session)
    stats = loader.load_parse_result(result=result, raw_document=doc, parser_version_id=pv.id)
    assert stats["monthly_closings"] == 1
    assert not stats["errors"]

    mc = (
        db_session.query(MonthlyClosing)
        .filter(MonthlyClosing.account_id == acct.id, MonthlyClosing.year == 2025, MonthlyClosing.month == 11)
        .one()
    )
    assert mc.net_value == Decimal("3.69")
    assert mc.change_in_value == Decimal("0")
    assert mc.income is None

    logs = (
        db_session.query(ValidationLog)
        .filter(ValidationLog.account_id == acct.id)
        .all()
    )
    assert any("mensual en blanco interpretado como 0" in (log.message or "") for log in logs)


def test_loader_jpm_brokerage_normalizes_cash_from_holdings_when_allocation_missing(db_session):
    acct = _create_account(
        db_session,
        account_number="E99087000",
        bank_code="jpmorgan",
        account_type="brokerage",
    )
    doc = _create_raw_document(
        db_session,
        filename="20260131-jpm-brokerage-cash-only.pdf",
        bank_code="jpmorgan",
    )
    parser = JPMorganBrokerageParser()
    pv = _create_parser_version(db_session, parser)

    result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name=parser.get_parser_name(),
        parser_version=parser.VERSION,
        source_file_hash=_mk_hash("jpm-brokerage-cash-holdings"),
        bank_code="jpmorgan",
        account_number=acct.account_number,
        statement_date=date(2026, 1, 31),
        period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31),
        opening_balance=Decimal("450.00"),
        closing_balance=Decimal("459.62"),
        currency="USD",
        qualitative_data={
            "accounts": [
                {
                    "account_number": acct.account_number,
                    "beginning_value": "450.00",
                    "ending_value": "459.62",
                }
            ],
            "account_monthly_activity": [
                {
                    "account_number": acct.account_number,
                    "ending_value_with_accrual": "459.62",
                    "ending_value_without_accrual": "459.62",
                    "net_contributions": "0",
                    "utilidad": "9.62",
                }
            ],
        },
        rows=[
            ParsedRow(
                data={
                    "instrument": "US DOLLAR JPM DEPOSIT SWEEP",
                    "market_value": "459.62",
                    "account_number": acct.account_number,
                    "section": "cash_fixed_income",
                },
                row_number=1,
                confidence=0.9,
            ),
            ParsedRow(
                data={
                    "instrument": "VAND USDCP1-3 USDA",
                    "market_value": "999.99",
                    "account_number": acct.account_number,
                    "section": "cash_fixed_income",
                },
                row_number=2,
                confidence=0.9,
            ),
        ],
    )

    loader = DataLoadingService(db_session)
    stats = loader.load_parse_result(result=result, raw_document=doc, parser_version_id=pv.id)
    assert stats["monthly_closings"] == 1
    assert not stats["errors"]

    normalized = (
        db_session.query(MonthlyMetricNormalized)
        .filter(
            MonthlyMetricNormalized.account_id == acct.id,
            MonthlyMetricNormalized.year == 2026,
            MonthlyMetricNormalized.month == 1,
        )
        .one()
    )
    assert json.loads(normalized.asset_allocation_json or "{}") == {
        "Caja": {"value": "459.62"},
        "RF IG Short": {"value": "999.99"},
    }
    assert normalized.cash_value == Decimal("459.62")


def test_loader_jpm_brokerage_falls_back_to_asset_allocation_total_when_closing_missing(db_session):
    acct = _create_account(
        db_session,
        account_number="E74997009",
        bank_code="jpmorgan",
        account_type="brokerage",
    )
    doc = _create_raw_document(
        db_session,
        filename="20260228-jpm-brokerage-missing-closing.pdf",
        bank_code="jpmorgan",
    )
    parser = JPMorganBrokerageParser()
    pv = _create_parser_version(db_session, parser)

    result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name=parser.get_parser_name(),
        parser_version=parser.VERSION,
        source_file_hash=_mk_hash("jpm-brokerage-missing-closing"),
        bank_code="jpmorgan",
        account_number=acct.account_number,
        statement_date=date(2026, 2, 28),
        period_start=date(2026, 2, 1),
        period_end=date(2026, 2, 28),
        opening_balance=None,
        closing_balance=None,
        currency="USD",
        qualitative_data={},
        rows=[
            ParsedRow(
                data={
                    "instrument": "US DOLLAR JPM DEPOSIT SWEEP",
                    "market_value": "9.84",
                    "account_number": acct.account_number,
                    "section": "cash_fixed_income",
                },
                row_number=1,
                confidence=0.9,
            )
        ],
    )

    loader = DataLoadingService(db_session)
    stats = loader.load_parse_result(result=result, raw_document=doc, parser_version_id=pv.id)
    assert stats["monthly_closings"] == 1
    assert not stats["errors"]

    mc = (
        db_session.query(MonthlyClosing)
        .filter(MonthlyClosing.account_id == acct.id, MonthlyClosing.year == 2026, MonthlyClosing.month == 2)
        .one()
    )
    assert mc.net_value == Decimal("9.84")
    assert json.loads(mc.asset_allocation_json or "{}") == {"Caja": {"value": "9.84"}}

    normalized = (
        db_session.query(MonthlyMetricNormalized)
        .filter(
            MonthlyMetricNormalized.account_id == acct.id,
            MonthlyMetricNormalized.year == 2026,
            MonthlyMetricNormalized.month == 2,
        )
        .one()
    )
    assert normalized.ending_value_with_accrual == Decimal("9.84")
    assert normalized.cash_value == Decimal("9.84")


def test_loader_jpm_brokerage_bucketizes_asset_allocation_from_holdings(db_session):
    acct = _create_account(
        db_session,
        account_number="B99719999",
        bank_code="jpmorgan",
        account_type="brokerage",
    )
    doc = _create_raw_document(
        db_session,
        filename="20260228-jpm-brokerage-bucketized.pdf",
        bank_code="jpmorgan",
    )
    parser = JPMorganBrokerageParser()
    pv = _create_parser_version(db_session, parser)

    result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name=parser.get_parser_name(),
        parser_version=parser.VERSION,
        source_file_hash=_mk_hash("jpm-brokerage-bucketized"),
        bank_code="jpmorgan",
        account_number=acct.account_number,
        statement_date=date(2026, 2, 28),
        period_start=date(2026, 2, 1),
        period_end=date(2026, 2, 28),
        opening_balance=Decimal("150.00"),
        closing_balance=Decimal("150.00"),
        currency="USD",
            qualitative_data={
                "accounts": [
                    {
                        "account_number": acct.account_number,
                        "beginning_value": "150.00",
                        "ending_value": "150.00",
                    }
                ],
                "asset_allocation": {
                    "Cash": {"ending": "10.00", "value": "10.00"},
                    "Short Term": {"ending": "20.00", "value": "20.00"},
                    "Non-US Fixed Income": {"ending": "50.00", "value": "50.00"},
                },
                "account_monthly_activity": [
                    {
                        "account_number": acct.account_number,
                    "ending_value_with_accrual": "150.00",
                    "ending_value_without_accrual": "150.00",
                    "net_contributions": "0",
                    "utilidad": "0",
                    "asset_allocation": {
                        "Cash": {"ending": "10.00", "value": "10.00"},
                        "Short Term": {"ending": "20.00", "value": "20.00"},
                        "Non-US Fixed Income": {"ending": "50.00", "value": "50.00"},
                    },
                }
            ],
        },
        rows=[
            ParsedRow(
                data={
                    "instrument": "IWDA",
                    "market_value": "70.00",
                    "account_number": acct.account_number,
                    "section": "equity",
                },
                row_number=1,
                confidence=0.9,
            ),
            ParsedRow(
                data={
                    "instrument": "VDCA",
                    "market_value": "3.00",
                    "account_number": acct.account_number,
                    "section": "cash_fixed_income",
                },
                row_number=2,
                confidence=0.9,
            ),
            ParsedRow(
                data={
                    "instrument": "SOME NON PARSED BOND NAME",
                    "market_value": "67.00",
                    "account_number": acct.account_number,
                    "section": "cash_fixed_income",
                },
                row_number=3,
                confidence=0.9,
            ),
            ParsedRow(
                data={
                    "instrument": "US DOLLAR JPM DEPOSIT SWEEP",
                    "market_value": "10.00",
                    "account_number": acct.account_number,
                    "section": "cash_fixed_income",
                },
                row_number=4,
                confidence=0.9,
            ),
        ],
    )

    loader = DataLoadingService(db_session)
    stats = loader.load_parse_result(result=result, raw_document=doc, parser_version_id=pv.id)
    assert stats["monthly_closings"] == 1
    assert not stats["errors"]

    mc = (
        db_session.query(MonthlyClosing)
        .filter(MonthlyClosing.account_id == acct.id, MonthlyClosing.year == 2026, MonthlyClosing.month == 2)
        .one()
    )
    alloc = json.loads(mc.asset_allocation_json or "{}")
    assert alloc == {
        "Caja": {"value": "10.00"},
        "RF IG Short": {"value": "20.00"},
        "Non US RF": {"value": "50.00"},
        "RV DM": {"value": "70.00"},
    }

    normalized = (
        db_session.query(MonthlyMetricNormalized)
        .filter(
            MonthlyMetricNormalized.account_id == acct.id,
            MonthlyMetricNormalized.year == 2026,
            MonthlyMetricNormalized.month == 2,
        )
        .one()
    )
    assert json.loads(normalized.asset_allocation_json or "{}") == alloc
    assert normalized.cash_value == Decimal("10.00")


def test_loader_refresh_backfills_jpm_cash_from_persisted_holdings(db_session):
    acct = _create_account(
        db_session,
        account_number="B99719001",
        bank_code="jpmorgan",
        account_type="brokerage",
    )
    doc = _create_raw_document(
        db_session,
        filename="20260131-jpm-brokerage-refresh.pdf",
        bank_code="jpmorgan",
    )
    parser = JPMorganBrokerageParser()
    pv = _create_parser_version(db_session, parser)

    db_session.add(
        MonthlyClosing(
            account_id=acct.id,
            closing_date=date(2026, 1, 31),
            year=2026,
            month=1,
            total_assets=Decimal("15920368.36"),
            net_value=Decimal("15920368.36"),
            currency="USD",
            income=Decimal("0"),
            change_in_value=Decimal("0"),
            source_document_id=doc.id,
        )
    )
    db_session.add(
        ParsedStatement(
            raw_document_id=doc.id,
            account_id=acct.id,
            statement_date=date(2026, 1, 31),
            period_start=date(2026, 1, 1),
            period_end=date(2026, 1, 31),
            closing_balance=Decimal("15920368.36"),
            currency="USD",
            parser_version_id=pv.id,
            parsed_data_json=json.dumps(
                {
                    "rows": [
                        {
                            "instrument": "JPM USD LIQUIDITY SWEEP C SHARE",
                            "market_value": "60622.45",
                            "account_number": acct.account_number,
                            "section": "cash_fixed_income",
                        },
                        {
                            "instrument": "JPM LI-LIQ LVNAV FD - USD - W -",
                            "market_value": "15859745.91",
                            "account_number": acct.account_number,
                            "section": "cash_fixed_income",
                        },
                    ]
                }
            ),
        )
    )
    db_session.commit()

    loader = DataLoadingService(db_session)
    loader._refresh_normalized_activity_from_monthly_closings(account=acct, year=2026)
    db_session.commit()

    normalized = (
        db_session.query(MonthlyMetricNormalized)
        .filter(
            MonthlyMetricNormalized.account_id == acct.id,
            MonthlyMetricNormalized.year == 2026,
            MonthlyMetricNormalized.month == 1,
        )
        .one()
    )
    assert normalized.cash_value == Decimal("15920368.36")


def test_loader_refresh_backfills_jpm_brokerage_asset_allocation_from_persisted_holdings(db_session):
    acct = _create_account(
        db_session,
        account_number="B99719998",
        bank_code="jpmorgan",
        account_type="brokerage",
    )
    doc = _create_raw_document(
        db_session,
        filename="20260331-jpm-brokerage-refresh-buckets.pdf",
        bank_code="jpmorgan",
    )
    parser = JPMorganBrokerageParser()
    pv = _create_parser_version(db_session, parser)

    db_session.add(
        MonthlyClosing(
            account_id=acct.id,
            closing_date=date(2026, 3, 31),
            year=2026,
            month=3,
            total_assets=Decimal("150.00"),
            net_value=Decimal("150.00"),
            currency="USD",
            income=Decimal("0"),
            change_in_value=Decimal("0"),
            asset_allocation_json=json.dumps(
                {
                    "Cash": {"value": "10.00"},
                    "Short Term": {"value": "50.00"},
                    "Non-US Fixed Income": {"value": "20.00"},
                }
            ),
            source_document_id=doc.id,
        )
    )
    db_session.add(
        ParsedStatement(
            raw_document_id=doc.id,
            account_id=acct.id,
            statement_date=date(2026, 3, 31),
            period_start=date(2026, 3, 1),
            period_end=date(2026, 3, 31),
            closing_balance=Decimal("150.00"),
            currency="USD",
            parser_version_id=pv.id,
            parsed_data_json=json.dumps(
                {
                    "rows": [
                        {
                            "instrument": "IWDA",
                            "market_value": "70.00",
                            "account_number": acct.account_number,
                            "section": "equity",
                        },
                        {
                            "instrument": "VDCA",
                            "market_value": "50.00",
                            "account_number": acct.account_number,
                            "section": "cash_fixed_income",
                        },
                        {
                            "instrument": "NON US FIXED INCOME",
                            "market_value": "20.00",
                            "account_number": acct.account_number,
                            "section": "cash_fixed_income",
                        },
                        {
                            "instrument": "US DOLLAR JPM DEPOSIT SWEEP",
                            "market_value": "10.00",
                            "account_number": acct.account_number,
                            "section": "cash_fixed_income",
                        },
                    ]
                }
            ),
        )
    )
    db_session.commit()

    loader = DataLoadingService(db_session)
    loader._refresh_normalized_activity_from_monthly_closings(account=acct, year=2026)
    db_session.commit()

    normalized = (
        db_session.query(MonthlyMetricNormalized)
        .filter(
            MonthlyMetricNormalized.account_id == acct.id,
            MonthlyMetricNormalized.year == 2026,
            MonthlyMetricNormalized.month == 3,
        )
        .one()
    )
    alloc = json.loads(normalized.asset_allocation_json or "{}")
    assert alloc == {
        "Caja": {"value": "10.00"},
        "RF IG Short": {"value": "50.00"},
        "Non US RF": {"value": "20.00"},
        "RV DM": {"value": "70.00"},
    }
    assert normalized.cash_value == Decimal("10.00")

    mc = (
        db_session.query(MonthlyClosing)
        .filter(MonthlyClosing.account_id == acct.id, MonthlyClosing.year == 2026, MonthlyClosing.month == 3)
        .one()
    )
    assert json.loads(mc.asset_allocation_json or "{}") == alloc


def test_loader_ubs_history_backfills_prior_months(db_session):
    parser = UBSSwitzerlandCustodyParser()
    # Reproduce producción: Session autoflush deshabilitado en backend.db.session
    db_session.autoflush = False

    acct = _create_account(
        db_session,
        account_number="206-560552-88",
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
    # Regla UBS: backfill trimestral no pisa net_value de meses ya cerrados.
    assert jan.net_value == Decimal("100")
    assert jan.change_in_value == Decimal("10")
    assert jan.income == Decimal("0")
    assert feb.net_value == Decimal("120")
    assert feb.change_in_value == Decimal("5")
    assert feb.income == Decimal("15")
    assert march.net_value == Decimal("130")
    assert march.change_in_value == Decimal("-2")
    assert march.income == Decimal("12")


def test_loader_ubs_quarterly_history_refines_non_quarter_movement_from_direct_month(db_session):
    parser = UBSSwitzerlandCustodyParser()
    acct = _create_account(
        db_session,
        account_number="206-560552-02",
        bank_code="ubs",
        account_type="mandato",
    )
    jan_doc = _create_raw_document(
        db_session,
        filename="202501 Boatview UBS SW (206-560552-02) 511UBS SW_P2.pdf",
        bank_code="ubs",
    )
    mar_doc = _create_raw_document(
        db_session,
        filename="202503 Boatview UBS SW (206-560552-02) 511UBS SW_P2.pdf",
        bank_code="ubs",
    )
    pv = _create_parser_version(db_session, parser)
    loader = DataLoadingService(db_session)

    db_session.add(
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
        )
    )
    db_session.flush()

    jan_result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name=parser.get_parser_name(),
        parser_version=parser.VERSION,
        source_file_hash=_mk_hash("ubs-202501-direct"),
        bank_code="ubs",
        account_number=acct.account_number,
        statement_date=date(2025, 1, 31),
        period_start=date(2025, 1, 1),
        period_end=date(2025, 1, 31),
        opening_balance=Decimal("90"),
        closing_balance=Decimal("100"),
        currency="USD",
        qualitative_data={
            "accounts": [
                {
                    "account_number": acct.account_number,
                    "beginning_value": "90",
                    "ending_value": "100",
                }
            ],
            "account_monthly_activity": [
                {
                    "account_number": acct.account_number,
                    "ending_value_with_accrual": "100",
                    "ending_value_without_accrual": "100",
                    "net_contributions": "0",
                    "utilidad": "10",
                }
            ],
        },
    )
    stats = loader.load_parse_result(result=jan_result, raw_document=jan_doc, parser_version_id=pv.id)
    assert stats["monthly_closings"] == 1
    assert not stats["errors"]

    march_result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name=parser.get_parser_name(),
        parser_version=parser.VERSION,
        source_file_hash=_mk_hash("ubs-202503-quarter"),
        bank_code="ubs",
        account_number=acct.account_number,
        statement_date=date(2025, 3, 31),
        period_start=date(2025, 3, 1),
        period_end=date(2025, 3, 31),
        opening_balance=Decimal("100"),
        closing_balance=Decimal("130"),
        currency="USD",
        qualitative_data={
            "accounts": [
                {
                    "account_number": acct.account_number,
                    "beginning_value": "120",
                    "ending_value": "130",
                }
            ],
            "account_monthly_activity": [
                {
                    "account_number": acct.account_number,
                    "ending_value_with_accrual": "130",
                    "ending_value_without_accrual": "130",
                    "net_contributions": "0",
                    "utilidad": "10",
                }
            ],
            "account_monthly_activity_history": [
                {
                    "account_number": acct.account_number,
                    "period_year": 2025,
                    "period_month": 1,
                    "period_end": "2025-01-31",
                    "ending_value_with_accrual": "100",
                    "net_contributions": "10",
                    "utilidad": "999999",
                },
                {
                    "account_number": acct.account_number,
                    "period_year": 2025,
                    "period_month": 2,
                    "period_end": "2025-02-28",
                    "ending_value_with_accrual": "120",
                    "net_contributions": "5",
                    "utilidad": "15",
                },
            ],
        },
    )
    stats = loader.load_parse_result(result=march_result, raw_document=mar_doc, parser_version_id=pv.id)
    assert stats["monthly_closings"] == 1
    assert not stats["errors"]

    jan = (
        db_session.query(MonthlyClosing)
        .filter(MonthlyClosing.account_id == acct.id, MonthlyClosing.year == 2025, MonthlyClosing.month == 1)
        .one()
    )
    assert jan.source_document_id == jan_doc.id
    assert jan.net_value == Decimal("100")
    assert jan.change_in_value == Decimal("10")
    assert jan.income == Decimal("0")

    jan_norm = (
        db_session.query(MonthlyMetricNormalized)
        .filter(
            MonthlyMetricNormalized.account_id == acct.id,
            MonthlyMetricNormalized.year == 2025,
            MonthlyMetricNormalized.month == 1,
        )
        .one()
    )
    assert jan_norm.ending_value_with_accrual == Decimal("100")
    assert jan_norm.movements_net == Decimal("10")
    assert jan_norm.profit_period == Decimal("0")


def test_loader_ubs_profit_absorbs_beginning_vs_previous_ending_gap(db_session):
    parser = UBSSwitzerlandCustodyParser()
    acct = _create_account(
        db_session,
        account_number="206-560552-02",
        bank_code="ubs",
        account_type="mandato",
    )
    feb_doc = _create_raw_document(
        db_session,
        filename="202502 Boatview UBS SW (206-560552-02) 511UBS SW_P2.pdf",
        bank_code="ubs",
    )
    mar_doc = _create_raw_document(
        db_session,
        filename="202503 Boatview UBS SW (206-560552-02) 511UBS SW_P2.pdf",
        bank_code="ubs",
    )
    pv = _create_parser_version(db_session, parser)
    loader = DataLoadingService(db_session)

    feb_result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name=parser.get_parser_name(),
        parser_version=parser.VERSION,
        source_file_hash=_mk_hash("ubs-gap-202502"),
        bank_code="ubs",
        account_number=acct.account_number,
        statement_date=date(2025, 2, 28),
        period_start=date(2025, 2, 1),
        period_end=date(2025, 2, 28),
        opening_balance=Decimal("0"),
        closing_balance=Decimal("82100670"),
        currency="USD",
        qualitative_data={
            "accounts": [
                {
                    "account_number": acct.account_number,
                    "beginning_value": "0",
                    "ending_value": "82100670",
                }
            ],
            "account_monthly_activity": [
                {
                    "account_number": acct.account_number,
                    "ending_value_with_accrual": "82100670",
                    "ending_value_without_accrual": "82100670",
                    "net_contributions": "82089481",
                    "utilidad": "11189",
                }
            ],
        },
    )
    stats = loader.load_parse_result(result=feb_result, raw_document=feb_doc, parser_version_id=pv.id)
    assert stats["monthly_closings"] == 1
    assert not stats["errors"]

    march_result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name=parser.get_parser_name(),
        parser_version=parser.VERSION,
        source_file_hash=_mk_hash("ubs-gap-202503"),
        bank_code="ubs",
        account_number=acct.account_number,
        statement_date=date(2025, 3, 31),
        period_start=date(2025, 3, 1),
        period_end=date(2025, 3, 31),
        opening_balance=Decimal("82115663"),
        closing_balance=Decimal("81278864"),
        currency="USD",
        qualitative_data={
            "accounts": [
                {
                    "account_number": acct.account_number,
                    "beginning_value": "82115663",
                    "ending_value": "81278864",
                }
            ],
            "account_monthly_activity": [
                {
                    "account_number": acct.account_number,
                    "ending_value_with_accrual": "81278864",
                    "ending_value_without_accrual": "81278864",
                    "net_contributions": "-1435",
                    "utilidad": "-834611",
                }
            ],
        },
    )
    stats = loader.load_parse_result(result=march_result, raw_document=mar_doc, parser_version_id=pv.id)
    assert stats["monthly_closings"] == 1
    assert not stats["errors"]

    march = (
        db_session.query(MonthlyClosing)
        .filter(MonthlyClosing.account_id == acct.id, MonthlyClosing.year == 2025, MonthlyClosing.month == 3)
        .one()
    )
    assert march.net_value == Decimal("81278864")
    assert march.change_in_value == Decimal("-1435")
    assert march.income == Decimal("-820371")

    march_norm = (
        db_session.query(MonthlyMetricNormalized)
        .filter(
            MonthlyMetricNormalized.account_id == acct.id,
            MonthlyMetricNormalized.year == 2025,
            MonthlyMetricNormalized.month == 3,
        )
        .one()
    )
    assert march_norm.ending_value_with_accrual == Decimal("81278864")
    assert march_norm.movements_net == Decimal("-1435")
    assert march_norm.profit_period == Decimal("-820371")


def test_loader_ubs_reprocess_previous_month_realigns_following_profit(db_session):
    parser = UBSSwitzerlandCustodyParser()
    acct = _create_account(
        db_session,
        account_number="206-560402-02",
        bank_code="ubs",
        account_type="brokerage",
    )
    may_doc = _create_raw_document(
        db_session,
        filename="202405 Telmar UBS SW Brokerage (0402 61K).pdf",
        bank_code="ubs",
    )
    jun_doc = _create_raw_document(
        db_session,
        filename="202406 Telmar UBS SW Brokerage (0402 61K).pdf",
        bank_code="ubs",
    )
    pv = _create_parser_version(db_session, parser)
    loader = DataLoadingService(db_session)

    june_result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name=parser.get_parser_name(),
        parser_version=parser.VERSION,
        source_file_hash=_mk_hash("ubs-0402-202406"),
        bank_code="ubs",
        account_number=acct.account_number,
        statement_date=date(2024, 6, 30),
        period_start=date(2024, 6, 1),
        period_end=date(2024, 6, 30),
        closing_balance=Decimal("2956"),
        currency="USD",
        qualitative_data={
            "accounts": [
                {
                    "account_number": acct.account_number,
                    "beginning_value": None,
                    "ending_value": "2956",
                }
            ],
            "account_monthly_activity": [
                {
                    "account_number": acct.account_number,
                    "ending_value_with_accrual": "2956",
                    "ending_value_without_accrual": "2956",
                    "net_contributions": "0",
                    "utilidad": None,
                }
            ],
        },
    )
    stats = loader.load_parse_result(result=june_result, raw_document=jun_doc, parser_version_id=pv.id)
    assert stats["monthly_closings"] == 1
    assert not stats["errors"]

    june_before = (
        db_session.query(MonthlyClosing)
        .filter(MonthlyClosing.account_id == acct.id, MonthlyClosing.year == 2024, MonthlyClosing.month == 6)
        .one()
    )
    assert june_before.income is None

    may_result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name=parser.get_parser_name(),
        parser_version=parser.VERSION,
        source_file_hash=_mk_hash("ubs-0402-202405"),
        bank_code="ubs",
        account_number=acct.account_number,
        statement_date=date(2024, 5, 31),
        period_start=date(2024, 5, 1),
        period_end=date(2024, 5, 31),
        opening_balance=Decimal("-449"),
        closing_balance=Decimal("-462"),
        currency="USD",
        qualitative_data={
            "accounts": [
                {
                    "account_number": acct.account_number,
                    "beginning_value": "-449",
                    "ending_value": "-462",
                }
            ],
            "account_monthly_activity": [
                {
                    "account_number": acct.account_number,
                    "ending_value_with_accrual": "-462",
                    "ending_value_without_accrual": "-462",
                    "net_contributions": "0",
                    "utilidad": "-13",
                }
            ],
        },
    )
    stats = loader.load_parse_result(result=may_result, raw_document=may_doc, parser_version_id=pv.id)
    assert stats["monthly_closings"] == 1
    assert not stats["errors"]

    june_after = (
        db_session.query(MonthlyClosing)
        .filter(MonthlyClosing.account_id == acct.id, MonthlyClosing.year == 2024, MonthlyClosing.month == 6)
        .one()
    )
    assert june_after.change_in_value == Decimal("0")
    assert june_after.income == Decimal("3418")

    june_norm = (
        db_session.query(MonthlyMetricNormalized)
        .filter(
            MonthlyMetricNormalized.account_id == acct.id,
            MonthlyMetricNormalized.year == 2024,
            MonthlyMetricNormalized.month == 6,
        )
        .one()
    )
    assert june_norm.movements_net == Decimal("0")
    assert june_norm.profit_period == Decimal("3418")


def test_loader_ubs_history_does_not_override_direct_monthly_statement(db_session):
    parser = UBSSwitzerlandCustodyParser()
    acct = _create_account(
        db_session,
        account_number="206-579943-01",
        bank_code="ubs",
        account_type="brokerage",
    )
    jan_doc = _create_raw_document(
        db_session,
        filename="202601 MI - UBS Sw (9943).pdf",
        bank_code="ubs",
    )
    feb_doc = _create_raw_document(
        db_session,
        filename="202602 MI - UBS Sw (9943).pdf",
        bank_code="ubs",
    )
    pv = _create_parser_version(db_session, parser)
    loader = DataLoadingService(db_session)

    jan_result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name=parser.get_parser_name(),
        parser_version=parser.VERSION,
        source_file_hash=_mk_hash("ubs-mi-202601"),
        bank_code="ubs",
        account_number=acct.account_number,
        statement_date=date(2026, 1, 31),
        period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31),
        opening_balance=Decimal("96028"),
        closing_balance=Decimal("96418"),
        currency="USD",
        qualitative_data={
            "accounts": [
                {
                    "account_number": acct.account_number,
                    "beginning_value": "96028",
                    "ending_value": "96418",
                }
            ],
            "account_monthly_activity": [
                {
                    "account_number": acct.account_number,
                    "ending_value_with_accrual": "96418",
                    "ending_value_without_accrual": "96202",
                    "accrual_ending": "216",
                    "net_contributions": "0",
                    "utilidad": "390",
                }
            ],
        },
    )
    stats = loader.load_parse_result(result=jan_result, raw_document=jan_doc, parser_version_id=pv.id)
    assert stats["monthly_closings"] == 1
    assert not stats["errors"]

    feb_result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name=parser.get_parser_name(),
        parser_version=parser.VERSION,
        source_file_hash=_mk_hash("ubs-mi-202602"),
        bank_code="ubs",
        account_number=acct.account_number,
        statement_date=date(2026, 2, 28),
        period_start=date(2026, 2, 1),
        period_end=date(2026, 2, 28),
        opening_balance=Decimal("96418"),
        closing_balance=Decimal("96554"),
        currency="USD",
        qualitative_data={
            "accounts": [
                {
                    "account_number": acct.account_number,
                    "beginning_value": "96418",
                    "ending_value": "96554",
                }
            ],
            "account_monthly_activity": [
                {
                    "account_number": acct.account_number,
                    "ending_value_with_accrual": "96554",
                    "ending_value_without_accrual": "96359",
                    "accrual_ending": "195",
                    "net_contributions": "0",
                    "utilidad": "352",
                }
            ],
            "account_monthly_activity_history": [
                {
                    "account_number": acct.account_number,
                    "period_year": 2026,
                    "period_month": 1,
                    "period_end": "2026-01-31",
                    "ending_value_with_accrual": "96202",
                    "net_contributions": "0",
                    "utilidad": "174",
                }
            ],
        },
    )
    stats = loader.load_parse_result(result=feb_result, raw_document=feb_doc, parser_version_id=pv.id)
    assert stats["monthly_closings"] == 1
    assert not stats["errors"]

    jan = (
        db_session.query(MonthlyClosing)
        .filter(MonthlyClosing.account_id == acct.id, MonthlyClosing.year == 2026, MonthlyClosing.month == 1)
        .one()
    )
    assert jan.net_value == Decimal("96418")
    assert jan.change_in_value == Decimal("0")
    assert jan.income == Decimal("390")

    jan_norm = (
        db_session.query(MonthlyMetricNormalized)
        .filter(
            MonthlyMetricNormalized.account_id == acct.id,
            MonthlyMetricNormalized.year == 2026,
            MonthlyMetricNormalized.month == 1,
        )
        .one()
    )
    assert jan_norm.ending_value_with_accrual == Decimal("96418")
    assert jan_norm.ending_value_without_accrual == Decimal("96202")
    assert jan_norm.movements_net == Decimal("0")
    assert jan_norm.profit_period == Decimal("390")


def test_loader_applies_documented_ubs_feb_2025_manual_override_pair(db_session):
    parser = UBSSwitzerlandCustodyParser()
    boatview = _create_account(
        db_session,
        account_number="206-560552-02",
        bank_code="ubs",
        account_type="mandato",
    )
    telmar = _create_account(
        db_session,
        account_number="206-560402-01",
        bank_code="ubs",
        account_type="mandato",
    )
    boatview_doc = _create_raw_document(
        db_session,
        filename="202502 Boatview UBS SW (206-560552-02) 511UBS SW_P2.pdf",
        bank_code="ubs",
    )
    telmar_doc = _create_raw_document(
        db_session,
        filename="202502 Telmar UBS SW Mandato (0402 60P y 61K).pdf",
        bank_code="ubs",
    )
    pv = _create_parser_version(db_session, parser)

    result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name=parser.get_parser_name(),
        parser_version=parser.VERSION,
        source_file_hash=_mk_hash("ubs-boatview-202502"),
        bank_code="ubs",
        account_number=boatview.account_number,
        statement_date=date(2025, 2, 28),
        period_start=date(2025, 2, 1),
        period_end=date(2025, 2, 28),
        opening_balance=None,
        closing_balance=Decimal("82100670"),
        currency="USD",
        qualitative_data={
            "accounts": [
                {
                    "account_number": boatview.account_number,
                    "beginning_value": None,
                    "ending_value": "82100670",
                }
            ],
            "account_monthly_activity": [
                {
                    "account_number": boatview.account_number,
                    "ending_value_with_accrual": "82100670",
                    "ending_value_without_accrual": "82100670",
                    "accrual_ending": "0",
                    "net_contributions": "0",
                }
            ],
        },
    )

    loader = DataLoadingService(db_session)
    stats = loader.load_parse_result(result=result, raw_document=boatview_doc, parser_version_id=pv.id)
    assert stats["monthly_closings"] == 1
    assert not stats["errors"]

    boatview_feb = (
        db_session.query(MonthlyClosing)
        .filter(MonthlyClosing.account_id == boatview.id, MonthlyClosing.year == 2025, MonthlyClosing.month == 2)
        .one()
    )
    assert boatview_feb.net_value == Decimal("82100670")
    assert boatview_feb.change_in_value == Decimal("82089481")
    assert boatview_feb.income == Decimal("11189")
    assert boatview_feb.source_document_id == boatview_doc.id

    telmar_feb = (
        db_session.query(MonthlyClosing)
        .filter(MonthlyClosing.account_id == telmar.id, MonthlyClosing.year == 2025, MonthlyClosing.month == 2)
        .one()
    )
    assert telmar_feb.net_value == Decimal("0")
    assert telmar_feb.change_in_value == Decimal("-82089481")
    assert telmar_feb.income == Decimal("231316")
    assert telmar_feb.source_document_id == telmar_doc.id

    telmar_norm = (
        db_session.query(MonthlyMetricNormalized)
        .filter(
            MonthlyMetricNormalized.account_id == telmar.id,
            MonthlyMetricNormalized.year == 2025,
            MonthlyMetricNormalized.month == 2,
        )
        .one()
    )
    assert telmar_norm.ending_value_with_accrual == Decimal("0")
    assert telmar_norm.ending_value_without_accrual == Decimal("0")
    assert telmar_norm.movements_net == Decimal("-82089481")
    assert telmar_norm.profit_period == Decimal("231316")


def test_loader_ubs_manual_override_survives_later_quarterly_backfill(db_session):
    parser = UBSSwitzerlandCustodyParser()
    boatview = _create_account(
        db_session,
        account_number="206-560552-02",
        bank_code="ubs",
        account_type="mandato",
    )
    telmar = _create_account(
        db_session,
        account_number="206-560402-01",
        bank_code="ubs",
        account_type="mandato",
    )
    boatview_feb_doc = _create_raw_document(
        db_session,
        filename="202502 Boatview UBS SW (206-560552-02) 511UBS SW_P2.pdf",
        bank_code="ubs",
    )
    telmar_feb_doc = _create_raw_document(
        db_session,
        filename="202502 Telmar UBS SW Mandato (0402 60P y 61K).pdf",
        bank_code="ubs",
    )
    march_doc = _create_raw_document(
        db_session,
        filename="202503 Boatview UBS SW (206-560552-02) 511UBS SW_P2.pdf",
        bank_code="ubs",
    )
    pv = _create_parser_version(db_session, parser)
    loader = DataLoadingService(db_session)

    feb_result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name=parser.get_parser_name(),
        parser_version=parser.VERSION,
        source_file_hash=_mk_hash("ubs-manual-202502"),
        bank_code="ubs",
        account_number=boatview.account_number,
        statement_date=date(2025, 2, 28),
        period_start=date(2025, 2, 1),
        period_end=date(2025, 2, 28),
        closing_balance=Decimal("82100670"),
        currency="USD",
        qualitative_data={
            "accounts": [
                {
                    "account_number": boatview.account_number,
                    "beginning_value": None,
                    "ending_value": "82100670",
                }
            ],
            "account_monthly_activity": [
                {
                    "account_number": boatview.account_number,
                    "ending_value_with_accrual": "82100670",
                    "ending_value_without_accrual": "82100670",
                    "net_contributions": "0",
                    "utilidad": "0",
                }
            ],
        },
    )
    stats = loader.load_parse_result(result=feb_result, raw_document=boatview_feb_doc, parser_version_id=pv.id)
    assert stats["monthly_closings"] == 1
    assert not stats["errors"]

    quarter_result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name=parser.get_parser_name(),
        parser_version=parser.VERSION,
        source_file_hash=_mk_hash("ubs-quarter-202503"),
        bank_code="ubs",
        account_number=boatview.account_number,
        statement_date=date(2025, 3, 31),
        period_start=date(2025, 3, 1),
        period_end=date(2025, 3, 31),
        opening_balance=Decimal("82115663"),
        closing_balance=Decimal("81278864"),
        currency="USD",
        qualitative_data={
            "accounts": [
                {
                    "account_number": boatview.account_number,
                    "beginning_value": "82115663",
                    "ending_value": "81278864",
                }
            ],
            "account_monthly_activity": [
                {
                    "account_number": boatview.account_number,
                    "ending_value_with_accrual": "81278864",
                    "ending_value_without_accrual": "81278864",
                    "net_contributions": "-1435",
                    "utilidad": "-834611",
                }
            ],
            "account_monthly_activity_history": [
                {
                    "account_number": boatview.account_number,
                    "period_year": 2025,
                    "period_month": 2,
                    "period_end": "2025-02-28",
                    "ending_value_with_accrual": "82100670",
                    "net_contributions": "-21257",
                    "utilidad": "99999999",
                }
            ],
        },
    )
    stats = loader.load_parse_result(result=quarter_result, raw_document=march_doc, parser_version_id=pv.id)
    assert stats["monthly_closings"] == 1
    assert not stats["errors"]

    boatview_feb = (
        db_session.query(MonthlyClosing)
        .filter(MonthlyClosing.account_id == boatview.id, MonthlyClosing.year == 2025, MonthlyClosing.month == 2)
        .one()
    )
    assert boatview_feb.source_document_id == boatview_feb_doc.id
    assert boatview_feb.net_value == Decimal("82100670")
    assert boatview_feb.change_in_value == Decimal("82089481")
    assert boatview_feb.income == Decimal("11189")

    telmar_feb = (
        db_session.query(MonthlyClosing)
        .filter(MonthlyClosing.account_id == telmar.id, MonthlyClosing.year == 2025, MonthlyClosing.month == 2)
        .one()
    )
    assert telmar_feb.source_document_id == telmar_feb_doc.id
    assert telmar_feb.net_value == Decimal("0")
    assert telmar_feb.change_in_value == Decimal("-82089481")
    assert telmar_feb.income == Decimal("231316")


def test_loader_bbh_prior_adjustment_is_control_only(db_session):
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
    assert jan.change_in_value == Decimal("59.65")
    assert feb.change_in_value == Decimal("0")

    jan_norm = (
        db_session.query(MonthlyMetricNormalized)
        .filter(
            MonthlyMetricNormalized.account_id == acct.id,
            MonthlyMetricNormalized.year == 2025,
            MonthlyMetricNormalized.month == 1,
        )
        .one()
    )
    feb_norm = (
        db_session.query(MonthlyMetricNormalized)
        .filter(
            MonthlyMetricNormalized.account_id == acct.id,
            MonthlyMetricNormalized.year == 2025,
            MonthlyMetricNormalized.month == 2,
        )
        .one()
    )
    assert jan_norm.movements_net == jan.change_in_value
    assert feb_norm.movements_net == feb.change_in_value

    logs = (
        db_session.query(ValidationLog)
        .filter(ValidationLog.account_id == acct.id)
        .all()
    )
    assert any("YTD caja inconsistente" in (log.message or "") for log in logs)


def test_loader_ytd_alignment_is_control_only_non_bbh(db_session):
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
    assert feb.change_in_value == Decimal("20")
    assert feb.income == Decimal("40")

    feb_norm = (
        db_session.query(MonthlyMetricNormalized)
        .filter(
            MonthlyMetricNormalized.account_id == acct.id,
            MonthlyMetricNormalized.year == 2025,
            MonthlyMetricNormalized.month == 2,
        )
        .one()
    )
    assert feb_norm.movements_net == feb.change_in_value
    assert feb_norm.profit_period == feb.income

    logs = (
        db_session.query(ValidationLog)
        .filter(ValidationLog.account_id == acct.id)
        .all()
    )
    assert any("YTD caja inconsistente" in (log.message or "") for log in logs)
    assert any("YTD utilidad inconsistente" in (log.message or "") for log in logs)


def test_loader_jpmorgan_bonds_uses_portfolio_activity_when_monthly_block_missing(db_session):
    parser = JPMorganBondsParser()
    acct = _create_account(
        db_session,
        account_number="1531100",
        bank_code="jpmorgan",
        account_type="bonds",
    )
    doc = _create_raw_document(
        db_session,
        filename="202504 Ect Intl JPM NY BO (1100).pdf",
        bank_code="jpmorgan",
    )
    pv = _create_parser_version(db_session, parser)

    result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name=parser.get_parser_name(),
        parser_version=parser.VERSION,
        source_file_hash=_mk_hash("jpm-bonds-1100"),
        bank_code="jpmorgan",
        account_number=acct.account_number,
        statement_date=date(2025, 4, 30),
        period_start=date(2025, 4, 1),
        period_end=date(2025, 4, 30),
        opening_balance=Decimal("30796534.06"),
        closing_balance=Decimal("13445790.01"),
        currency="USD",
        qualitative_data={
            "asset_allocation": {
                "Cash, Deposits & Short Term": {
                    "beginning": "3492594.23",
                    "ending": "816748.33",
                    "change": "-2675845.90",
                },
                "Fixed Income": {
                    "beginning": "27303939.83",
                    "ending": "12629041.68",
                    "change": "-14674898.15",
                },
            },
            "portfolio_activity": {
                "beginning_market_value": {
                    "current_period": "30796534.06",
                    "ytd": "30143562.64",
                },
                "net_cash_contributions": {
                    "current_period": "-17260955.84",
                    "ytd": "-17260955.84",
                },
                "income_distributions": {
                    "current_period": "81538.61",
                    "ytd": "432542.54",
                },
                "change_investment": {
                    "current_period": "-171326.82",
                    "ytd": "130640.66",
                },
                "ending_market_value": {
                    "current_period": "13445790.01",
                    "ytd": "13445790.00",
                },
            },
        },
    )

    loader = DataLoadingService(db_session)
    stats = loader.load_parse_result(result=result, raw_document=doc, parser_version_id=pv.id)
    assert stats["monthly_closings"] == 1
    assert not stats["errors"]

    mc = (
        db_session.query(MonthlyClosing)
        .filter(MonthlyClosing.account_id == acct.id, MonthlyClosing.year == 2025, MonthlyClosing.month == 4)
        .one()
    )
    assert mc.change_in_value == Decimal("-17260955.84")
    assert mc.income == Decimal("-89788.21")
    norm = (
        db_session.query(MonthlyMetricNormalized)
        .filter(
            MonthlyMetricNormalized.account_id == acct.id,
            MonthlyMetricNormalized.year == 2025,
            MonthlyMetricNormalized.month == 4,
        )
        .one()
    )
    assert norm.movements_net == mc.change_in_value
    assert norm.profit_period == mc.income


def test_jpmorgan_bonds_cash_holdings_override_applies_only_to_1531100():
    parser = JPMorganBondsParser()
    pages = [
        "\n".join(
            [
                "Statement of Account",
                "Account Summary",
                "Asset Allocation",
                "Cash, Deposits & Short Term 1,314,748.12 1,217,852.19 -96,895.93",
                "Fixed Income 27,303,939.83 12,629,041.68 -14,674,898.15",
                "Portfolio Activity",
                "Beginning Market Value 30,796,534.06 30,143,562.64",
                "Ending Market Value 13,445,790.01 13,445,790.00",
            ]
        ),
        "\n".join(
            [
                "Cash, Deposits & Short Term",
                "Cash Holdings 615,750.66 4.54%",
                "Short Term Investments 639,286.42 4.72%",
                "Total Cash Holdings 615,750.66 4.54%",
            ]
        ),
    ]

    result_target = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name=parser.get_parser_name(),
        parser_version=parser.VERSION,
        source_file_hash=_mk_hash("cash-holdings-1531100"),
        bank_code="jpmorgan",
        account_number="1531100",
    )
    parser._extract_account_summary(pages, result_target)
    alloc_target = result_target.qualitative_data.get("asset_allocation", {})
    assert alloc_target.get("Cash, Deposits & Short Term", {}).get("ending") == "615750.66"
    assert alloc_target.get("Cash, Deposits & Short Term", {}).get("change") == "-698997.46"

    result_other = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name=parser.get_parser_name(),
        parser_version=parser.VERSION,
        source_file_hash=_mk_hash("cash-holdings-other"),
        bank_code="jpmorgan",
        account_number="1530900",
    )
    parser._extract_account_summary(pages, result_other)
    alloc_other = result_other.qualitative_data.get("asset_allocation", {})
    assert alloc_other.get("Cash, Deposits & Short Term", {}).get("ending") == "1217852.19"


def test_loader_loads_alternatives_into_normalized_with_synthetic_bank(db_session):
    raw_doc = RawDocument(
        filename="Alternativos.xlsx",
        filepath="data/raw/alternativos/excel_alternatives/Alternativos.xlsx",
        file_type="excel_alternatives",
        sha256_hash=_mk_hash("alternatives-loader"),
        file_size_bytes=1,
        bank_code="alternativos",
        status="parsed",
    )
    db_session.add(raw_doc)
    db_session.flush()

    result = ParseResult(
        status=ParserStatus.SUCCESS,
        parser_name="parsers.excel.alternatives",
        parser_version="1.0.1",
        source_file_hash=_mk_hash("alternatives-loader-source"),
        bank_code="alternativos",
        rows=[
            ParsedRow(
                row_number=2,
                data={
                    "entity_name": "Telmar",
                    "asset_class": "PE",
                    "strategy": "Buyout",
                    "currency": "USD",
                    "nemo_reference": "TRFV9",
                    "year": 2024,
                    "month": 12,
                    "closing_date": "2024-12-31",
                    "ending_value": 100.0,
                    "movements_net": 90.0,
                    "profit_period": 10.0,
                    "movements_ytd": 90.0,
                    "profit_ytd": 10.0,
                },
            ),
            ParsedRow(
                row_number=3,
                data={
                    "entity_name": "Telmar",
                    "asset_class": "PE",
                    "strategy": "Buyout",
                    "currency": "USD",
                    "nemo_reference": "TRFV9",
                    "year": 2025,
                    "month": 1,
                    "closing_date": "2025-01-31",
                    "ending_value": 130.0,
                    "movements_net": 5.0,
                    "profit_period": 25.0,
                    "movements_ytd": 5.0,
                    "profit_ytd": 25.0,
                },
            ),
        ],
    )

    loader = DataLoadingService(db_session)
    stats = loader.load_alternatives_result(result=result, raw_document=raw_doc)

    assert stats["normalized_rows"] == 2
    assert stats["accounts_created"] == 1
    assert stats["accounts_updated"] == 0
    assert stats["accounts_deleted"] == 0
    assert not stats["errors"]

    account = (
        db_session.query(Account)
        .filter(Account.bank_code == "alternativos", Account.entity_name == "Telmar")
        .one()
    )
    assert account.account_type == "investment"
    assert account.bank_name == "Alternativos"
    assert account.identification_number == "TRFV9"

    metadata = json.loads(account.metadata_json or "{}")
    assert metadata["source"] == "alternatives_excel"
    assert metadata["asset_class"] == "PE"
    assert metadata["strategy"] == "Buyout"
    assert metadata["nemo_reference"] == "TRFV9"
    assert metadata["account_group_label"] == "Telmar-ALT-PE"
    assert metadata["detail_label"] == "Telmar | PE | Buyout | USD"

    normalized_rows = (
        db_session.query(MonthlyMetricNormalized)
        .filter(MonthlyMetricNormalized.account_id == account.id)
        .order_by(MonthlyMetricNormalized.year, MonthlyMetricNormalized.month)
        .all()
    )
    assert len(normalized_rows) == 2
    assert normalized_rows[0].ending_value_with_accrual == Decimal("100.0")
    assert normalized_rows[0].movements_net == Decimal("90.0")
    assert normalized_rows[1].ending_value_with_accrual == Decimal("130.0")
    assert normalized_rows[1].profit_period == Decimal("25.0")

    closings = db_session.query(MonthlyClosing).filter(MonthlyClosing.account_id == account.id).all()
    assert closings == []


# ── Parser selection by account_id (regression) ─────────────────────


def test_process_document_prefers_account_type_bonds_over_autodetect(db_session, tmp_dir):
    """JPMorgan doc with account_id pointing to bonds account must select
    parsers.jpmorgan.bonds even when the filename has no bonds hint."""
    from backend.services.document_service import DocumentService

    acct = _create_account(
        db_session,
        account_number="1531100-test-bonds",
        bank_code="jpmorgan",
        account_type="bonds",
    )

    dummy_pdf = tmp_dir / "20250430-jpmorgan-cartola.pdf"
    dummy_pdf.write_bytes(b"%PDF-1.4 dummy content for test")

    doc = RawDocument(
        filename="20250430-jpmorgan-cartola.pdf",
        filepath=str(dummy_pdf),
        file_type="pdf_cartola",
        sha256_hash=_mk_hash("test-bonds-account-priority"),
        file_size_bytes=dummy_pdf.stat().st_size,
        bank_code="jpmorgan",
        account_id=acct.id,
        status="uploaded",
    )
    db_session.add(doc)
    db_session.flush()

    service = DocumentService(db_session)
    service.process_document(doc.id)

    db_session.refresh(doc)
    assert doc.parser_version_id is not None
    pv = (
        db_session.query(ParserVersion)
        .filter(ParserVersion.id == doc.parser_version_id)
        .one()
    )
    assert pv.parser_name == "parsers.jpmorgan.bonds"


def test_process_document_prefers_account_type_brokerage_over_autodetect(db_session, tmp_dir):
    """JPMorgan doc with account_id pointing to brokerage account must select
    parsers.jpmorgan.brokerage even when the filename has no brokerage hint."""
    from backend.services.document_service import DocumentService

    acct = _create_account(
        db_session,
        account_number="E92755009-test-brok",
        bank_code="jpmorgan",
        account_type="brokerage",
    )

    dummy_pdf = tmp_dir / "20250531-jpmorgan-cartola.pdf"
    dummy_pdf.write_bytes(b"%PDF-1.4 dummy content for test")

    doc = RawDocument(
        filename="20250531-jpmorgan-cartola.pdf",
        filepath=str(dummy_pdf),
        file_type="pdf_cartola",
        sha256_hash=_mk_hash("test-brokerage-account-priority"),
        file_size_bytes=dummy_pdf.stat().st_size,
        bank_code="jpmorgan",
        account_id=acct.id,
        status="uploaded",
    )
    db_session.add(doc)
    db_session.flush()

    service = DocumentService(db_session)
    service.process_document(doc.id)

    db_session.refresh(doc)
    assert doc.parser_version_id is not None
    pv = (
        db_session.query(ParserVersion)
        .filter(ParserVersion.id == doc.parser_version_id)
        .one()
    )
    assert pv.parser_name == "parsers.jpmorgan.brokerage"
