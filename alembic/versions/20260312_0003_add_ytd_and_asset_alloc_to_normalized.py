"""add ytd and asset_allocation columns to monthly_metrics_normalized

Revision ID: 0003
Revises: 0002
Create Date: 2026-03-12
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0003"
down_revision: Union[str, None] = "0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("monthly_metrics_normalized") as batch_op:
        batch_op.add_column(sa.Column("movements_ytd", sa.Numeric(20, 4), nullable=True))
        batch_op.add_column(sa.Column("profit_ytd", sa.Numeric(20, 4), nullable=True))
        batch_op.add_column(sa.Column("asset_allocation_json", sa.Text(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("monthly_metrics_normalized") as batch_op:
        batch_op.drop_column("asset_allocation_json")
        batch_op.drop_column("profit_ytd")
        batch_op.drop_column("movements_ytd")
