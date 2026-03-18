from __future__ import annotations

from datetime import date
from decimal import Decimal

from backend.db.models import Account, MonthlyClosing, RawDocument
from backend.routers.documents import (
    _document_account_display_fields,
    _manual_batch_context_rows,
    _preview_batch_recognition_rows,
    _resolve_upload_account,
    get_cartola_coverage,
)


def _mk_account(
    db_session,
    *,
    account_number: str,
    identification_number: str,
    bank_code: str,
    account_type: str,
    entity_name: str,
) -> Account:
    account = Account(
        account_number=account_number,
        identification_number=identification_number,
        bank_code=bank_code,
        bank_name=bank_code.replace("_", " ").title(),
        account_type=account_type,
        entity_name=entity_name,
        entity_type="sociedad",
        currency="USD",
        country="US",
        is_active=True,
    )
    db_session.add(account)
    db_session.flush()
    return account


def test_resolve_upload_account_prefers_exact_account_number(db_session):
    account = _mk_account(
        db_session,
        account_number="JPM-2026-3400",
        identification_number="3400",
        bank_code="jpmorgan",
        account_type="mandato",
        entity_name="Boatview",
    )
    db_session.commit()

    resolved = _resolve_upload_account(
        db=db_session,
        account_number="JPM-2026-3400",
        bank_code="jpmorgan",
        entity_name="Boatview",
        account_type="mandato",
    )

    assert resolved is not None
    assert resolved.id == account.id


def test_cartola_coverage_groups_loaded_months_by_society_and_account_type(db_session):
    boatview_brokerage = _mk_account(
        db_session,
        account_number="BV-BR-001",
        identification_number="5001",
        bank_code="jpmorgan",
        account_type="brokerage",
        entity_name="Boatview",
    )
    boatview_mandate = _mk_account(
        db_session,
        account_number="BV-MA-001",
        identification_number="3400",
        bank_code="ubs",
        account_type="mandato",
        entity_name="Boatview",
    )
    telmar_bonds = _mk_account(
        db_session,
        account_number="TEL-BO-001",
        identification_number="0900",
        bank_code="jpmorgan",
        account_type="bonds",
        entity_name="Telmar",
    )

    jan_doc = RawDocument(
        filename="2026-01 Boatview brokerage.pdf",
        filepath="data/raw/jpmorgan/pdf_cartola/bv-2026-01.pdf",
        file_type="pdf_cartola",
        sha256_hash="a" * 64,
        file_size_bytes=100,
        bank_code="jpmorgan",
        account_id=boatview_brokerage.id,
        status="parsed",
    )
    mar_doc = RawDocument(
        filename="2026-03 Boatview mandato.pdf",
        filepath="data/raw/ubs/pdf_cartola/bv-2026-03.pdf",
        file_type="pdf_cartola",
        sha256_hash="b" * 64,
        file_size_bytes=100,
        bank_code="ubs",
        account_id=boatview_mandate.id,
        status="parsed",
    )
    dec_doc = RawDocument(
        filename="2025-12 Telmar bonds.pdf",
        filepath="data/raw/jpmorgan/pdf_cartola/tel-2025-12.pdf",
        file_type="pdf_cartola",
        sha256_hash="c" * 64,
        file_size_bytes=100,
        bank_code="jpmorgan",
        account_id=telmar_bonds.id,
        status="parsed",
    )
    db_session.add_all([jan_doc, mar_doc, dec_doc])
    db_session.flush()

    db_session.add_all(
        [
            MonthlyClosing(
                account_id=boatview_brokerage.id,
                closing_date=date(2026, 1, 31),
                year=2026,
                month=1,
                net_value=Decimal("100.00"),
                income=Decimal("0.00"),
                change_in_value=Decimal("0.00"),
                currency="USD",
                source_document_id=jan_doc.id,
            ),
            MonthlyClosing(
                account_id=boatview_mandate.id,
                closing_date=date(2026, 3, 31),
                year=2026,
                month=3,
                net_value=Decimal("150.00"),
                income=Decimal("0.00"),
                change_in_value=Decimal("0.00"),
                currency="USD",
                source_document_id=mar_doc.id,
            ),
            MonthlyClosing(
                account_id=telmar_bonds.id,
                closing_date=date(2025, 12, 31),
                year=2025,
                month=12,
                net_value=Decimal("200.00"),
                income=Decimal("0.00"),
                change_in_value=Decimal("0.00"),
                currency="USD",
                source_document_id=dec_doc.id,
            ),
        ]
    )
    db_session.commit()

    payload = get_cartola_coverage(year=2026, db=db_session)

    rows = {
        (row["entity_name"], row["account_type"]): row["loaded_months"]
        for row in payload["rows"]
    }
    assert rows[("Boatview", "brokerage")] == [1]
    assert rows[("Boatview", "mandato")] == [3]
    assert rows[("Telmar", "bonds")] == []
    assert 2026 in payload["available_years"]
    assert 2025 in payload["available_years"]


def test_preview_batch_recognition_uses_filename_and_context(db_session):
    mandate = _mk_account(
        db_session,
        account_number="JPM-MAND-3400",
        identification_number="3400",
        bank_code="jpmorgan",
        account_type="mandato",
        entity_name="Boatview",
    )
    brokerage = _mk_account(
        db_session,
        account_number="JPM-BR-1000",
        identification_number="1000",
        bank_code="jpmorgan",
        account_type="brokerage",
        entity_name="Mi Investments",
    )
    db_session.commit()

    rows = _preview_batch_recognition_rows(
        db=db_session,
        filenames=[
            "20261231-statements-3400-Mandato - JPMorgan.pdf",
            "20261231-statements-1000-Brokerage - JPMorgan.pdf",
        ],
        bank_code="jpmorgan",
    )

    first = rows[0]
    second = rows[1]
    assert first["status"] == "reconocido"
    assert first["account_id"] == mandate.id
    assert first["account_type"] == "mandato"
    assert first["entity_name"] == "Boatview"
    assert second["status"] == "reconocido"
    assert second["account_id"] == brokerage.id
    assert second["account_type"] == "brokerage"
    assert second["entity_name"] == "Mi Investments"


def test_manual_batch_context_confirms_unique_account_without_filename_heuristics(db_session):
    comau = _mk_account(
        db_session,
        account_number="JPM-BR-7000-COM",
        identification_number="7000",
        bank_code="jpmorgan",
        account_type="brokerage",
        entity_name="Comau",
    )
    _mk_account(
        db_session,
        account_number="JPM-BR-7000-REN",
        identification_number="7000",
        bank_code="jpmorgan",
        account_type="brokerage",
        entity_name="Rengiroa",
    )
    db_session.commit()

    rows = _manual_batch_context_rows(
        db=db_session,
        filenames=["20260228-statements-7000-.pdf"],
        bank_code="jpmorgan",
        entity_name="Comau",
        account_type="brokerage",
    )

    assert len(rows) == 1
    assert rows[0]["status"] == "reconocido"
    assert rows[0]["confidence"] == "Manual"
    assert rows[0]["account_id"] == comau.id
    assert rows[0]["entity_name"] == "Comau"
    assert rows[0]["recognition_reason"] == "contexto confirmado manualmente"


def test_document_account_display_fields_fallbacks_to_persisted_statement_account(db_session):
    account = _mk_account(
        db_session,
        account_number="UBS-BR-60F",
        identification_number="60F",
        bank_code="ubs",
        account_type="brokerage",
        entity_name="Boatview",
    )
    raw_doc = RawDocument(
        filename="202502 Boatview UBS SW BR.pdf",
        filepath="data/raw/ubs/pdf_cartola/test.pdf",
        file_type="pdf_cartola",
        sha256_hash="d" * 64,
        file_size_bytes=123,
        bank_code="ubs",
        account_id=None,
        status="parsed",
    )
    db_session.add(raw_doc)
    db_session.flush()
    db_session.add(
        MonthlyClosing(
            account_id=account.id,
            closing_date=date(2025, 2, 28),
            year=2025,
            month=2,
            net_value=Decimal("100.00"),
            income=Decimal("0.00"),
            change_in_value=Decimal("0.00"),
            currency="USD",
            source_document_id=raw_doc.id,
        )
    )
    db_session.commit()

    entity_name, account_type = _document_account_display_fields(db=db_session, doc=raw_doc)

    assert entity_name == "Boatview"
    assert account_type == "brokerage"
