"""Backfill de asset allocation normalizado (Mandatos) en monthly_closings."""

from __future__ import annotations

import json
from pathlib import Path

from backend.config import PROJECT_ROOT
from backend.db.models import Account, MonthlyClosing, ParsedStatement, RawDocument
from backend.db.session import get_session_factory
from backend.services.data_loading_service import (
    DataLoadingService,
    _normalize_mandate_asset_allocation,
)
from parsers.registry import get_registry


def _resolve_parser(bank_code: str):
    registry = get_registry()
    if bank_code == "jpmorgan":
        return registry.get_parser("jpmorgan", "custody")
    return registry.get_parser(bank_code, "custody")


def _to_absolute(filepath: str) -> Path:
    p = Path(filepath)
    if p.is_absolute():
        return p
    return PROJECT_ROOT / p


def main() -> None:
    factory = get_session_factory()
    db = factory()
    try:
        loader = DataLoadingService(db)
        rows = (
            db.query(MonthlyClosing, Account, RawDocument)
            .join(Account, Account.id == MonthlyClosing.account_id)
            .join(RawDocument, RawDocument.id == MonthlyClosing.source_document_id)
            .filter(Account.account_type == "mandato")
            .order_by(Account.bank_code, Account.account_number, MonthlyClosing.year, MonthlyClosing.month)
            .all()
        )
        statement_doc_rows = (
            db.query(
                ParsedStatement.account_id,
                ParsedStatement.statement_date,
                ParsedStatement.raw_document_id,
            )
            .all()
        )
        statement_doc_by_account_month: dict[tuple[int, int, int], int] = {}
        for account_id, statement_date, raw_document_id in statement_doc_rows:
            if not account_id or not statement_date or not raw_document_id:
                continue
            statement_doc_by_account_month[
                (int(account_id), int(statement_date.year), int(statement_date.month))
            ] = int(raw_document_id)

        parsed_cache: dict[int, object] = {}
        raw_doc_cache: dict[int, RawDocument] = {}
        updated = 0
        unchanged = 0
        skipped = 0
        errors: list[str] = []

        for closing, account, raw_doc in rows:
            target_doc_id = statement_doc_by_account_month.get(
                (int(closing.account_id), int(closing.year), int(closing.month)),
                raw_doc.id,
            )

            if target_doc_id not in raw_doc_cache:
                raw_doc_cache[target_doc_id] = (
                    db.query(RawDocument).filter(RawDocument.id == target_doc_id).first()
                )
            target_doc = raw_doc_cache.get(target_doc_id)
            if target_doc is None:
                errors.append(
                    f"RawDocument inexistente para cuenta={account.account_number} {closing.year}-{closing.month:02d}"
                )
                skipped += 1
                continue

            if target_doc.id not in parsed_cache:
                parser = _resolve_parser(target_doc.bank_code or account.bank_code)
                if parser is None:
                    errors.append(f"Sin parser para doc {target_doc.id} ({target_doc.filename})")
                    parsed_cache[target_doc.id] = None
                else:
                    abs_path = _to_absolute(target_doc.filepath)
                    if not abs_path.exists():
                        errors.append(f"No existe archivo doc {target_doc.id}: {abs_path}")
                        parsed_cache[target_doc.id] = None
                    else:
                        parsed_cache[target_doc.id] = parser.safe_parse(abs_path)

            result = parsed_cache.get(target_doc.id)
            if result is None or not getattr(result, "is_success", False):
                skipped += 1
                continue

            stmt_date = getattr(result, "statement_date", None)
            if stmt_date is None:
                skipped += 1
                continue

            # Solo asignar cuando la cartola parseada corresponde al mes del closing.
            if closing.year != stmt_date.year or closing.month != stmt_date.month:
                skipped += 1
                continue

            account_values = loader._get_account_specific_values(result=result, account=account)
            raw_alloc = loader._resolve_asset_allocation_for_account(
                result=result,
                account=account,
                account_values=account_values,
            )
            normalized = _normalize_mandate_asset_allocation(raw_alloc)
            if not normalized:
                skipped += 1
                continue

            payload = json.dumps(normalized)
            if closing.asset_allocation_json == payload:
                unchanged += 1
                continue

            closing.asset_allocation_json = payload
            updated += 1

        db.commit()

        print(f"Mandato asset allocation backfill OK. updated={updated} unchanged={unchanged} skipped={skipped}")
        if errors:
            print("Errors:")
            for err in errors:
                print(f"- {err}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
