"""Phase 1 corporate-actions storage scaffold."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260422_0002"
down_revision = "20250918_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "raw_corporate_actions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("instrument_id", sa.Integer(), nullable=False),
        sa.Column("vendor", sa.String(length=64), nullable=False),
        sa.Column("action_type", sa.String(length=32), nullable=False),
        sa.Column("ex_date", sa.Date(), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name=op.f("fk_raw_corporate_actions_instrument_id_instruments"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_raw_corporate_actions")),
        sa.UniqueConstraint(
            "instrument_id",
            "vendor",
            "action_type",
            "ex_date",
            name=op.f("uq_raw_corporate_actions_instrument_id"),
        ),
    )

    op.create_table(
        "corporate_actions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("instrument_id", sa.Integer(), nullable=False),
        sa.Column("raw_action_id", sa.Integer(), nullable=False),
        sa.Column("action_type", sa.String(length=32), nullable=False),
        sa.Column("ex_date", sa.Date(), nullable=False),
        sa.Column("effective_date", sa.Date(), nullable=True),
        sa.Column("cash_amount", sa.Numeric(precision=18, scale=6), nullable=True),
        sa.Column("currency", sa.String(length=8), nullable=True),
        sa.Column("split_from", sa.Integer(), nullable=True),
        sa.Column("split_to", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name=op.f("fk_corporate_actions_instrument_id_instruments"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["raw_action_id"],
            ["raw_corporate_actions.id"],
            name=op.f("fk_corporate_actions_raw_action_id_raw_corporate_actions"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_corporate_actions")),
        sa.UniqueConstraint(
            "instrument_id",
            "action_type",
            "ex_date",
            name=op.f("uq_corporate_actions_instrument_id"),
        ),
        sa.UniqueConstraint("raw_action_id", name=op.f("uq_corporate_actions_raw_action_id")),
    )


def downgrade() -> None:
    op.drop_table("corporate_actions")
    op.drop_table("raw_corporate_actions")
