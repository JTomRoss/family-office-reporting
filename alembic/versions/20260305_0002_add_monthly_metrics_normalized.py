"""add monthly metrics normalized

Revision ID: 0002
Revises: 0001
Create Date: 2026-03-05
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "monthly_metrics_normalized",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("account_id", sa.Integer(), sa.ForeignKey("accounts.id"), nullable=False),
        sa.Column("closing_date", sa.Date(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("month", sa.Integer(), nullable=False),
        sa.Column("ending_value_with_accrual", sa.Numeric(20, 4), nullable=True),
        sa.Column("ending_value_without_accrual", sa.Numeric(20, 4), nullable=True),
        sa.Column("accrual_ending", sa.Numeric(20, 4), nullable=True),
        sa.Column("cash_value", sa.Numeric(20, 4), nullable=True),
        sa.Column("movements_net", sa.Numeric(20, 4), nullable=True),
        sa.Column("profit_period", sa.Numeric(20, 4), nullable=True),
        sa.Column("currency", sa.String(10), nullable=False),
        sa.Column("source_document_id", sa.Integer(), sa.ForeignKey("raw_documents.id"), nullable=True),
        sa.Column("loaded_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "account_id", "year", "month",
            name="uq_norm_monthly_metric_acct_period",
        ),
    )
    op.create_index(
        "ix_norm_monthly_metric_period",
        "monthly_metrics_normalized",
        ["year", "month"],
    )


def downgrade() -> None:
    op.drop_index("ix_norm_monthly_metric_period", table_name="monthly_metrics_normalized")
    op.drop_table("monthly_metrics_normalized")

