"""initial schema v1

Revision ID: 0001
Revises: None
Create Date: 2026-02-26

Migración inicial: crea las 12 tablas del esquema base.
Esta migración se genera ANTES de que existan datos, por lo que
representa el estado completo del schema v1.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. accounts
    op.create_table(
        "accounts",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("account_number", sa.String(100), nullable=False),
        sa.Column("bank_code", sa.String(50), nullable=False),
        sa.Column("bank_name", sa.String(200), nullable=False),
        sa.Column("account_type", sa.String(50), nullable=False),
        sa.Column("entity_name", sa.String(200), nullable=False),
        sa.Column("entity_type", sa.String(50), nullable=False),
        sa.Column("currency", sa.String(10), nullable=False),
        sa.Column("country", sa.String(100), nullable=False),
        sa.Column("mandate_type", sa.String(100), nullable=True),
        sa.Column("is_active", sa.Boolean(), default=True),
        sa.Column("metadata_json", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.Column("source_file_hash", sa.String(64), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("account_number"),
    )
    op.create_index("ix_accounts_bank_code", "accounts", ["bank_code"])
    op.create_index("ix_accounts_bank_entity", "accounts", ["bank_code", "entity_name"])

    # 2. parser_versions
    op.create_table(
        "parser_versions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("parser_name", sa.String(200), nullable=False),
        sa.Column("version", sa.String(50), nullable=False),
        sa.Column("source_hash", sa.String(64), nullable=False),
        sa.Column("registered_at", sa.DateTime(), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("parser_name", "version", name="uq_parser_name_version"),
    )

    # 3. raw_documents
    op.create_table(
        "raw_documents",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("filename", sa.String(500), nullable=False),
        sa.Column("filepath", sa.String(1000), nullable=False),
        sa.Column("file_type", sa.String(20), nullable=False),
        sa.Column("sha256_hash", sa.String(64), nullable=False),
        sa.Column("file_size_bytes", sa.Integer(), nullable=False),
        sa.Column("bank_code", sa.String(50), nullable=True),
        sa.Column("account_id", sa.Integer(), sa.ForeignKey("accounts.id"), nullable=True),
        sa.Column("period_year", sa.Integer(), nullable=True),
        sa.Column("period_month", sa.Integer(), nullable=True),
        sa.Column("status", sa.String(30), default="uploaded"),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("uploaded_at", sa.DateTime(), nullable=True),
        sa.Column("processed_at", sa.DateTime(), nullable=True),
        sa.Column("parser_version_id", sa.Integer(), sa.ForeignKey("parser_versions.id"), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("sha256_hash"),
    )
    op.create_index("ix_raw_docs_bank_code", "raw_documents", ["bank_code"])
    op.create_index("ix_raw_docs_bank_period", "raw_documents", ["bank_code", "period_year", "period_month"])

    # 4. parsed_statements
    op.create_table(
        "parsed_statements",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("raw_document_id", sa.Integer(), sa.ForeignKey("raw_documents.id"), nullable=False),
        sa.Column("account_id", sa.Integer(), sa.ForeignKey("accounts.id"), nullable=False),
        sa.Column("statement_date", sa.Date(), nullable=False),
        sa.Column("period_start", sa.Date(), nullable=False),
        sa.Column("period_end", sa.Date(), nullable=False),
        sa.Column("opening_balance", sa.Numeric(20, 4), nullable=True),
        sa.Column("closing_balance", sa.Numeric(20, 4), nullable=True),
        sa.Column("total_credits", sa.Numeric(20, 4), nullable=True),
        sa.Column("total_debits", sa.Numeric(20, 4), nullable=True),
        sa.Column("currency", sa.String(10), nullable=False),
        sa.Column("parsed_data_json", sa.Text(), nullable=True),
        sa.Column("parsed_at", sa.DateTime(), nullable=True),
        sa.Column("parser_version_id", sa.Integer(), sa.ForeignKey("parser_versions.id")),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("raw_document_id", "account_id", "statement_date", name="uq_parsed_stmt_doc_acct_date"),
    )
    op.create_index("ix_parsed_stmt_period", "parsed_statements", ["account_id", "period_start", "period_end"])

    # 5. daily_positions
    op.create_table(
        "daily_positions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("account_id", sa.Integer(), sa.ForeignKey("accounts.id"), nullable=False),
        sa.Column("position_date", sa.Date(), nullable=False),
        sa.Column("instrument_code", sa.String(100), nullable=False),
        sa.Column("instrument_name", sa.String(500), nullable=True),
        sa.Column("instrument_type", sa.String(50), nullable=True),
        sa.Column("isin", sa.String(12), nullable=True),
        sa.Column("quantity", sa.Numeric(20, 6), nullable=True),
        sa.Column("market_price", sa.Numeric(20, 6), nullable=True),
        sa.Column("market_value", sa.Numeric(20, 4), nullable=True),
        sa.Column("cost_basis", sa.Numeric(20, 4), nullable=True),
        sa.Column("unrealized_pnl", sa.Numeric(20, 4), nullable=True),
        sa.Column("currency", sa.String(10), nullable=False),
        sa.Column("market_value_usd", sa.Numeric(20, 4), nullable=True),
        sa.Column("accrued_interest", sa.Numeric(20, 4), nullable=True),
        sa.Column("source_file_hash", sa.String(64), nullable=False),
        sa.Column("loaded_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("account_id", "position_date", "instrument_code", name="uq_daily_pos_acct_date_inst"),
    )
    op.create_index("ix_daily_pos_date", "daily_positions", ["position_date"])
    op.create_index("ix_daily_pos_acct_date", "daily_positions", ["account_id", "position_date"])

    # 6. daily_movements
    op.create_table(
        "daily_movements",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("account_id", sa.Integer(), sa.ForeignKey("accounts.id"), nullable=False),
        sa.Column("movement_date", sa.Date(), nullable=False),
        sa.Column("settlement_date", sa.Date(), nullable=True),
        sa.Column("movement_type", sa.String(50), nullable=False),
        sa.Column("instrument_code", sa.String(100), nullable=True),
        sa.Column("instrument_name", sa.String(500), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("quantity", sa.Numeric(20, 6), nullable=True),
        sa.Column("price", sa.Numeric(20, 6), nullable=True),
        sa.Column("gross_amount", sa.Numeric(20, 4), nullable=True),
        sa.Column("net_amount", sa.Numeric(20, 4), nullable=True),
        sa.Column("fees", sa.Numeric(20, 4), nullable=True),
        sa.Column("tax", sa.Numeric(20, 4), nullable=True),
        sa.Column("currency", sa.String(10), nullable=False),
        sa.Column("amount_usd", sa.Numeric(20, 4), nullable=True),
        sa.Column("source_file_hash", sa.String(64), nullable=False),
        sa.Column("loaded_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_daily_mov_date", "daily_movements", ["movement_date"])
    op.create_index("ix_daily_mov_acct_date", "daily_movements", ["account_id", "movement_date"])

    # 7. daily_prices
    op.create_table(
        "daily_prices",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("price_date", sa.Date(), nullable=False),
        sa.Column("instrument_code", sa.String(100), nullable=False),
        sa.Column("instrument_type", sa.String(50), nullable=False),
        sa.Column("price", sa.Numeric(20, 8), nullable=False),
        sa.Column("currency", sa.String(10), nullable=False),
        sa.Column("source", sa.String(100), nullable=True),
        sa.Column("source_file_hash", sa.String(64), nullable=False),
        sa.Column("loaded_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("price_date", "instrument_code", name="uq_daily_price_date_inst"),
    )
    op.create_index("ix_daily_price_date", "daily_prices", ["price_date"])

    # 8. monthly_closings
    op.create_table(
        "monthly_closings",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("account_id", sa.Integer(), sa.ForeignKey("accounts.id"), nullable=False),
        sa.Column("closing_date", sa.Date(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("month", sa.Integer(), nullable=False),
        sa.Column("total_assets", sa.Numeric(20, 4), nullable=True),
        sa.Column("total_liabilities", sa.Numeric(20, 4), nullable=True),
        sa.Column("net_value", sa.Numeric(20, 4), nullable=True),
        sa.Column("currency", sa.String(10), nullable=False),
        sa.Column("net_value_usd", sa.Numeric(20, 4), nullable=True),
        sa.Column("income", sa.Numeric(20, 4), nullable=True),
        sa.Column("change_in_value", sa.Numeric(20, 4), nullable=True),
        sa.Column("total_return", sa.Numeric(20, 4), nullable=True),
        sa.Column("accrual", sa.Numeric(20, 4), nullable=True),
        sa.Column("asset_allocation_json", sa.Text(), nullable=True),
        sa.Column("geography_json", sa.Text(), nullable=True),
        sa.Column("currency_allocation_json", sa.Text(), nullable=True),
        sa.Column("source_document_id", sa.Integer(), sa.ForeignKey("raw_documents.id"), nullable=True),
        sa.Column("loaded_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("account_id", "year", "month", name="uq_monthly_closing_acct_period"),
    )
    op.create_index("ix_monthly_closing_period", "monthly_closings", ["year", "month"])

    # 9. reconciliations
    op.create_table(
        "reconciliations",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("monthly_closing_id", sa.Integer(), sa.ForeignKey("monthly_closings.id"), nullable=False),
        sa.Column("account_id", sa.Integer(), sa.ForeignKey("accounts.id"), nullable=False),
        sa.Column("reconciliation_date", sa.Date(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("month", sa.Integer(), nullable=False),
        sa.Column("daily_total", sa.Numeric(20, 4), nullable=True),
        sa.Column("monthly_total", sa.Numeric(20, 4), nullable=True),
        sa.Column("difference", sa.Numeric(20, 4), nullable=True),
        sa.Column("difference_pct", sa.Numeric(10, 6), nullable=True),
        sa.Column("status", sa.String(30), nullable=False),
        sa.Column("threshold_used", sa.Numeric(10, 6), nullable=True),
        sa.Column("currency", sa.String(10), nullable=False),
        sa.Column("details_json", sa.Text(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("resolved", sa.Boolean(), default=False),
        sa.Column("resolved_at", sa.DateTime(), nullable=True),
        sa.Column("resolved_by", sa.String(100), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("account_id", "year", "month", name="uq_reconciliation_acct_period"),
    )

    # 10. validation_logs
    op.create_table(
        "validation_logs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("raw_document_id", sa.Integer(), sa.ForeignKey("raw_documents.id"), nullable=True),
        sa.Column("account_id", sa.Integer(), sa.ForeignKey("accounts.id"), nullable=True),
        sa.Column("validation_type", sa.String(50), nullable=False),
        sa.Column("severity", sa.String(20), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("details_json", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("source_module", sa.String(200), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_validation_log_type_sev", "validation_logs", ["validation_type", "severity"])
    op.create_index("ix_validation_log_date", "validation_logs", ["created_at"])

    # 11. etf_compositions
    op.create_table(
        "etf_compositions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("account_id", sa.Integer(), sa.ForeignKey("accounts.id"), nullable=False),
        sa.Column("bank_code", sa.String(50), nullable=False),
        sa.Column("report_date", sa.Date(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("month", sa.Integer(), nullable=False),
        sa.Column("etf_code", sa.String(50), nullable=False),
        sa.Column("etf_name", sa.String(500), nullable=False),
        sa.Column("isin", sa.String(12), nullable=True),
        sa.Column("quantity", sa.Numeric(20, 6), nullable=True),
        sa.Column("market_value", sa.Numeric(20, 4), nullable=True),
        sa.Column("weight_pct", sa.Numeric(10, 6), nullable=True),
        sa.Column("currency", sa.String(10), nullable=False),
        sa.Column("market_value_usd", sa.Numeric(20, 4), nullable=True),
        sa.Column("source_document_id", sa.Integer(), sa.ForeignKey("raw_documents.id"), nullable=True),
        sa.Column("loaded_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("account_id", "bank_code", "year", "month", "etf_code", name="uq_etf_comp_acct_bank_period_etf"),
    )
    op.create_index("ix_etf_comp_bank_period", "etf_compositions", ["bank_code", "year", "month"])

    # 12. cache_metadata
    op.create_table(
        "cache_metadata",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("cache_key", sa.String(500), nullable=False),
        sa.Column("filepath", sa.String(1000), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("expires_at", sa.DateTime(), nullable=True),
        sa.Column("data_hash", sa.String(64), nullable=False),
        sa.Column("is_valid", sa.Boolean(), default=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("cache_key"),
    )


def downgrade() -> None:
    op.drop_table("cache_metadata")
    op.drop_table("etf_compositions")
    op.drop_table("validation_logs")
    op.drop_table("reconciliations")
    op.drop_table("monthly_closings")
    op.drop_table("daily_prices")
    op.drop_table("daily_movements")
    op.drop_table("daily_positions")
    op.drop_table("parsed_statements")
    op.drop_table("raw_documents")
    op.drop_table("parser_versions")
    op.drop_table("accounts")
