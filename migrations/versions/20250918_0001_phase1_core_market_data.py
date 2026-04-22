"""Phase 1 core market-data schema foundations."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20250918_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "instruments",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("category", sa.String(length=64), nullable=False),
        sa.Column("exchange", sa.String(length=32), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_instruments")),
        sa.UniqueConstraint("symbol", name=op.f("uq_instruments_symbol")),
    )
    op.create_index(op.f("ix_instruments_symbol"), "instruments", ["symbol"], unique=False)

    op.create_table(
        "trading_calendar",
        sa.Column("trading_date", sa.Date(), nullable=False),
        sa.Column("market_open_utc", sa.DateTime(timezone=True), nullable=True),
        sa.Column("market_close_utc", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_open", sa.Boolean(), nullable=False),
        sa.Column("is_early_close", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.PrimaryKeyConstraint("trading_date", name=op.f("pk_trading_calendar")),
    )

    op.create_table(
        "raw_bars_daily",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("instrument_id", sa.Integer(), nullable=False),
        sa.Column("vendor", sa.String(length=64), nullable=False),
        sa.Column("bar_date", sa.Date(), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name=op.f("fk_raw_bars_daily_instrument_id_instruments"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_raw_bars_daily")),
        sa.UniqueConstraint(
            "instrument_id",
            "vendor",
            "bar_date",
            name=op.f("uq_raw_bars_daily_instrument_id"),
        ),
    )

    op.create_table(
        "bars_daily",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("instrument_id", sa.Integer(), nullable=False),
        sa.Column("raw_bar_id", sa.Integer(), nullable=False),
        sa.Column("bar_date", sa.Date(), nullable=False),
        sa.Column("open", sa.Numeric(precision=18, scale=6), nullable=False),
        sa.Column("high", sa.Numeric(precision=18, scale=6), nullable=False),
        sa.Column("low", sa.Numeric(precision=18, scale=6), nullable=False),
        sa.Column("close", sa.Numeric(precision=18, scale=6), nullable=False),
        sa.Column("adjusted_close", sa.Numeric(precision=18, scale=6), nullable=False),
        sa.Column("volume", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name=op.f("fk_bars_daily_instrument_id_instruments"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["raw_bar_id"],
            ["raw_bars_daily.id"],
            name=op.f("fk_bars_daily_raw_bar_id_raw_bars_daily"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_bars_daily")),
        sa.UniqueConstraint(
            "instrument_id",
            "bar_date",
            name=op.f("uq_bars_daily_instrument_id"),
        ),
        sa.UniqueConstraint("raw_bar_id", name=op.f("uq_bars_daily_raw_bar_id")),
    )


def downgrade() -> None:
    op.drop_table("bars_daily")
    op.drop_table("raw_bars_daily")
    op.drop_table("trading_calendar")
    op.drop_index(op.f("ix_instruments_symbol"), table_name="instruments")
    op.drop_table("instruments")
