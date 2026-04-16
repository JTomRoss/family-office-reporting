"""add bice_monthly_snapshot table

Revision ID: 0004
Revises: 0003
Create Date: 2026-04-16

SSOT para inversiones nacionales (BICE, Banchile).
Saldos y movimientos en CLP y USD de forma completamente independiente.
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0004"
down_revision: Union[str, None] = "0003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "bice_monthly_snapshot",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("month", sa.Integer(), nullable=False),
        sa.Column("closing_date", sa.Date(), nullable=False),
        # Saldos CLP
        sa.Column("ending_clp", sa.Numeric(20, 4), nullable=True),
        sa.Column("caja_clp", sa.Numeric(20, 4), nullable=True),
        sa.Column("renta_fija_clp", sa.Numeric(20, 4), nullable=True),
        sa.Column("equities_clp", sa.Numeric(20, 4), nullable=True),
        # Movimientos CLP
        sa.Column("aportes_clp", sa.Numeric(20, 4), nullable=True),
        sa.Column("retiros_clp", sa.Numeric(20, 4), nullable=True),
        sa.Column("dividendos_clp", sa.Numeric(20, 4), nullable=True),
        sa.Column("profit_clp", sa.Numeric(20, 4), nullable=True),
        # Saldos USD
        sa.Column("ending_usd", sa.Numeric(20, 4), nullable=True),
        sa.Column("caja_usd", sa.Numeric(20, 4), nullable=True),
        sa.Column("renta_fija_usd", sa.Numeric(20, 4), nullable=True),
        sa.Column("equities_usd", sa.Numeric(20, 4), nullable=True),
        # Movimientos USD
        sa.Column("aportes_usd", sa.Numeric(20, 4), nullable=True),
        sa.Column("retiros_usd", sa.Numeric(20, 4), nullable=True),
        sa.Column("dividendos_usd", sa.Numeric(20, 4), nullable=True),
        sa.Column("profit_usd", sa.Numeric(20, 4), nullable=True),
        # Trazabilidad
        sa.Column("source_document_id", sa.Integer(), nullable=True),
        sa.Column("loaded_at", sa.DateTime(), nullable=True),
        # Constraints
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"]),
        sa.ForeignKeyConstraint(["source_document_id"], ["raw_documents.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("account_id", "year", "month", name="uq_bice_snapshot_acct_period"),
    )
    op.create_index("ix_bice_snapshot_period", "bice_monthly_snapshot", ["year", "month"])


def downgrade() -> None:
    op.drop_index("ix_bice_snapshot_period", table_name="bice_monthly_snapshot")
    op.drop_table("bice_monthly_snapshot")
