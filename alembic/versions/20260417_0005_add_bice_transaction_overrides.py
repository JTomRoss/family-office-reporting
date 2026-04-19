"""add transaction_overrides_json to bice_monthly_snapshot

Revision ID: 0005
Revises: 0004
Create Date: 2026-04-17

Agrega columna nullable TEXT para persistir overrides manuales de categoría
de transacciones detectadas en cartolas BICE Asesorías.
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0005"
down_revision: Union[str, None] = "0004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "bice_monthly_snapshot",
        sa.Column("transaction_overrides_json", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("bice_monthly_snapshot", "transaction_overrides_json")
