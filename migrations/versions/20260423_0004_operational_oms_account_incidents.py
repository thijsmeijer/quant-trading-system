"""Operational OMS, account, and incident persistence."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260423_0004"
down_revision = "20260423_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "orders",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("strategy_run_id", sa.Integer(), nullable=True),
        sa.Column("instrument_id", sa.Integer(), nullable=True),
        sa.Column("internal_order_id", sa.String(length=64), nullable=False),
        sa.Column("run_mode", sa.String(length=16), nullable=False),
        sa.Column("order_type", sa.String(length=32), nullable=False),
        sa.Column("side", sa.String(length=8), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("requested_quantity", sa.Numeric(precision=18, scale=6), nullable=False),
        sa.Column("requested_notional", sa.Numeric(precision=18, scale=6), nullable=True),
        sa.Column("time_in_force", sa.String(length=16), nullable=True),
        sa.Column("broker_order_id", sa.String(length=128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("canceled_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name=op.f("fk_orders_instrument_id_instruments"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["strategy_run_id"],
            ["strategy_runs.id"],
            name=op.f("fk_orders_strategy_run_id_strategy_runs"),
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_orders")),
        sa.UniqueConstraint("internal_order_id", name=op.f("uq_orders_internal_order_id")),
    )

    op.create_table(
        "order_events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("order_id", sa.Integer(), nullable=False),
        sa.Column("event_type", sa.String(length=32), nullable=False),
        sa.Column("event_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("broker_event_id", sa.String(length=128), nullable=True),
        sa.Column("details", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.ForeignKeyConstraint(
            ["order_id"],
            ["orders.id"],
            name=op.f("fk_order_events_order_id_orders"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_order_events")),
        sa.UniqueConstraint(
            "order_id",
            "event_type",
            "event_at",
            name=op.f("uq_order_events_order_id"),
        ),
    )

    op.create_table(
        "fills",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("order_id", sa.Integer(), nullable=False),
        sa.Column("broker_fill_id", sa.String(length=128), nullable=True),
        sa.Column("fill_quantity", sa.Numeric(precision=18, scale=6), nullable=False),
        sa.Column("fill_price", sa.Numeric(precision=18, scale=6), nullable=False),
        sa.Column("fill_notional", sa.Numeric(precision=18, scale=6), nullable=False),
        sa.Column("fill_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["order_id"],
            ["orders.id"],
            name=op.f("fk_fills_order_id_orders"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_fills")),
        sa.UniqueConstraint(
            "order_id",
            "fill_at",
            "fill_price",
            "fill_quantity",
            name=op.f("uq_fills_order_id"),
        ),
    )

    op.create_table(
        "positions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_mode", sa.String(length=16), nullable=False),
        sa.Column("instrument_id", sa.Integer(), nullable=False),
        sa.Column("quantity", sa.Numeric(precision=18, scale=6), nullable=False),
        sa.Column("average_cost", sa.Numeric(precision=18, scale=6), nullable=True),
        sa.Column("market_value", sa.Numeric(precision=18, scale=6), nullable=False),
        sa.Column("as_of", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name=op.f("fk_positions_instrument_id_instruments"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_positions")),
        sa.UniqueConstraint(
            "run_mode",
            "instrument_id",
            "as_of",
            name=op.f("uq_positions_run_mode"),
        ),
    )

    op.create_table(
        "account_snapshots",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_mode", sa.String(length=16), nullable=False),
        sa.Column("cash", sa.Numeric(precision=18, scale=6), nullable=False),
        sa.Column("equity", sa.Numeric(precision=18, scale=6), nullable=False),
        sa.Column("buying_power", sa.Numeric(precision=18, scale=6), nullable=False),
        sa.Column("as_of", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_account_snapshots")),
        sa.UniqueConstraint("run_mode", "as_of", name=op.f("uq_account_snapshots_run_mode")),
    )

    op.create_table(
        "pnl_snapshots",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_mode", sa.String(length=16), nullable=False),
        sa.Column("realized_pnl", sa.Numeric(precision=18, scale=6), nullable=False),
        sa.Column("unrealized_pnl", sa.Numeric(precision=18, scale=6), nullable=False),
        sa.Column("total_pnl", sa.Numeric(precision=18, scale=6), nullable=False),
        sa.Column("as_of", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_pnl_snapshots")),
        sa.UniqueConstraint("run_mode", "as_of", name=op.f("uq_pnl_snapshots_run_mode")),
    )

    op.create_table(
        "risk_snapshots",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_mode", sa.String(length=16), nullable=False),
        sa.Column("gross_exposure", sa.Numeric(precision=18, scale=6), nullable=False),
        sa.Column("net_exposure", sa.Numeric(precision=18, scale=6), nullable=False),
        sa.Column("drawdown", sa.Numeric(precision=18, scale=6), nullable=True),
        sa.Column("open_order_count", sa.Integer(), nullable=False),
        sa.Column("as_of", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_risk_snapshots")),
        sa.UniqueConstraint("run_mode", "as_of", name=op.f("uq_risk_snapshots_run_mode")),
    )

    op.create_table(
        "incidents",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_mode", sa.String(length=16), nullable=False),
        sa.Column("incident_type", sa.String(length=64), nullable=False),
        sa.Column("severity", sa.String(length=16), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("summary", sa.String(length=255), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("details", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_incidents")),
        sa.UniqueConstraint(
            "run_mode",
            "incident_type",
            "occurred_at",
            name=op.f("uq_incidents_run_mode"),
        ),
    )


def downgrade() -> None:
    op.drop_table("incidents")
    op.drop_table("risk_snapshots")
    op.drop_table("pnl_snapshots")
    op.drop_table("account_snapshots")
    op.drop_table("positions")
    op.drop_table("fills")
    op.drop_table("order_events")
    op.drop_table("orders")
