"""Selección del universo de documentos cartola para auditoría."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from sqlalchemy import and_, extract, func
from sqlalchemy.orm import Session

from backend.db.models import (
    Account,
    FileType,
    MonthlyMetricNormalized,
    ParsedStatement,
    ParserVersion,
    RawDocument,
)


@dataclass
class AuditUniverseRow:
    raw_document: RawDocument
    parsed_statement: ParsedStatement
    account: Account
    normalized: Optional[MonthlyMetricNormalized]
    parser_name: Optional[str]


def _period_exprs():
    year_expr = func.coalesce(
        RawDocument.period_year,
        extract("year", ParsedStatement.statement_date),
    )
    month_expr = func.coalesce(
        RawDocument.period_month,
        extract("month", ParsedStatement.statement_date),
    )
    return year_expr, month_expr


def fetch_universe(
    db: Session,
    *,
    bank_codes: list[str],
    entity_names: list[str],
    account_types: list[str],
    year_start: Optional[int],
    year_end: Optional[int],
    month_start: Optional[int],
    month_end: Optional[int],
) -> list[AuditUniverseRow]:
    year_expr, month_expr = _period_exprs()
    period_ord = year_expr * 12 + month_expr

    q = (
        db.query(RawDocument, ParsedStatement, Account, MonthlyMetricNormalized, ParserVersion)
        .join(ParsedStatement, ParsedStatement.raw_document_id == RawDocument.id)
        .join(Account, Account.id == ParsedStatement.account_id)
        .outerjoin(
            MonthlyMetricNormalized,
            and_(
                MonthlyMetricNormalized.account_id == Account.id,
                MonthlyMetricNormalized.year == year_expr,
                MonthlyMetricNormalized.month == month_expr,
            ),
        )
        .outerjoin(ParserVersion, ParserVersion.id == ParsedStatement.parser_version_id)
        .filter(RawDocument.file_type == FileType.PDF_CARTOLA.value)
        .filter(RawDocument.status.in_(["parsed", "validated"]))
    )

    if bank_codes:
        q = q.filter(Account.bank_code.in_(bank_codes))
    if entity_names:
        q = q.filter(Account.entity_name.in_(entity_names))
    if account_types:
        q = q.filter(Account.account_type.in_(account_types))

    if year_start is not None and month_start is not None:
        q = q.filter(period_ord >= year_start * 12 + month_start)
    elif year_start is not None:
        q = q.filter(year_expr >= year_start)

    if year_end is not None and month_end is not None:
        q = q.filter(period_ord <= year_end * 12 + month_end)
    elif year_end is not None:
        q = q.filter(year_expr <= year_end)

    q = q.order_by(year_expr.desc(), month_expr.desc(), RawDocument.id.desc())

    out: list[AuditUniverseRow] = []
    for raw, stmt, acct, norm, pver in q.all():
        pname = f"{pver.parser_name} {pver.version}".strip() if pver else None
        out.append(
            AuditUniverseRow(
                raw_document=raw,
                parsed_statement=stmt,
                account=acct,
                normalized=norm,
                parser_name=pname,
            )
        )
    return out


def fetch_previous_normalized_ending(
    db: Session,
    *,
    account_id: int,
    year: int,
    month: int,
) -> Optional[MonthlyMetricNormalized]:
    if month > 1:
        py, pm = year, month - 1
    else:
        py, pm = year - 1, 12
    return (
        db.query(MonthlyMetricNormalized)
        .filter(
            MonthlyMetricNormalized.account_id == account_id,
            MonthlyMetricNormalized.year == py,
            MonthlyMetricNormalized.month == pm,
        )
        .first()
    )
