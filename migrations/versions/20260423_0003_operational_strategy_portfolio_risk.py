"""Operational strategy, portfolio, and risk persistence."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260423_0003"
down_revision = "20260422_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "features_daily",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("instrument_id", sa.Integer(), nullable=False),
        sa.Column("bar_date", sa.Date(), nullable=False),
        sa.Column("feature_name", sa.String(length=128), nullable=False),
        sa.Column("feature_version", sa.String(length=64), nullable=False),
        sa.Column("feature_value", sa.Numeric(precision=18, scale=6), nullable=False),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name=op.f("fk_features_daily_instrument_id_instruments"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_features_daily")),
        sa.UniqueConstraint(
            "instrument_id",
            "bar_date",
            "feature_name",
            "feature_version",
            name=op.f("uq_features_daily_instrument_id"),
        ),
    )

    op.create_table(
        "strategy_runs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_mode", sa.String(length=16), nullable=False),
        sa.Column("strategy_name", sa.String(length=128), nullable=False),
        sa.Column("config_version", sa.String(length=128), nullable=False),
        sa.Column("config_hash", sa.String(length=128), nullable=False),
        sa.Column("signal_date", sa.Date(), nullable=False),
        sa.Column("execution_date", sa.Date(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("metadata_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_strategy_runs")),
    )

    op.create_table(
        "signals",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("strategy_run_id", sa.Integer(), nullable=False),
        sa.Column("instrument_id", sa.Integer(), nullable=False),
        sa.Column("signal_name", sa.String(length=64), nullable=False),
        sa.Column("rank", sa.Integer(), nullable=True),
        sa.Column("score", sa.Numeric(precision=18, scale=6), nullable=True),
        sa.Column("is_selected", sa.Boolean(), nullable=False),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name=op.f("fk_signals_instrument_id_instruments"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["strategy_run_id"],
            ["strategy_runs.id"],
            name=op.f("fk_signals_strategy_run_id_strategy_runs"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_signals")),
        sa.UniqueConstraint(
            "strategy_run_id",
            "instrument_id",
            "signal_name",
            name=op.f("uq_signals_strategy_run_id"),
        ),
    )

    op.create_table(
        "target_weights",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("strategy_run_id", sa.Integer(), nullable=False),
        sa.Column("instrument_id", sa.Integer(), nullable=True),
        sa.Column("allocation_key", sa.String(length=64), nullable=False),
        sa.Column("target_weight", sa.Numeric(precision=18, scale=6), nullable=False),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name=op.f("fk_target_weights_instrument_id_instruments"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["strategy_run_id"],
            ["strategy_runs.id"],
            name=op.f("fk_target_weights_strategy_run_id_strategy_runs"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_target_weights")),
        sa.UniqueConstraint(
            "strategy_run_id",
            "allocation_key",
            name=op.f("uq_target_weights_strategy_run_id"),
        ),
    )

    op.create_table(
        "target_positions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("strategy_run_id", sa.Integer(), nullable=False),
        sa.Column("instrument_id", sa.Integer(), nullable=True),
        sa.Column("allocation_key", sa.String(length=64), nullable=False),
        sa.Column("target_weight", sa.Numeric(precision=18, scale=6), nullable=False),
        sa.Column("target_notional", sa.Numeric(precision=18, scale=6), nullable=False),
        sa.Column("target_quantity", sa.Numeric(precision=18, scale=6), nullable=False),
        sa.Column("reference_price", sa.Numeric(precision=18, scale=6), nullable=True),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name=op.f("fk_target_positions_instrument_id_instruments"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["strategy_run_id"],
            ["strategy_runs.id"],
            name=op.f("fk_target_positions_strategy_run_id_strategy_runs"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_target_positions")),
        sa.UniqueConstraint(
            "strategy_run_id",
            "allocation_key",
            name=op.f("uq_target_positions_strategy_run_id"),
        ),
    )

    op.create_table(
        "risk_checks",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("strategy_run_id", sa.Integer(), nullable=False),
        sa.Column("check_scope", sa.String(length=32), nullable=False),
        sa.Column("check_name", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("reason_code", sa.String(length=64), nullable=True),
        sa.Column("checked_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("details", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.ForeignKeyConstraint(
            ["strategy_run_id"],
            ["strategy_runs.id"],
            name=op.f("fk_risk_checks_strategy_run_id_strategy_runs"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_risk_checks")),
        sa.UniqueConstraint(
            "strategy_run_id",
            "check_scope",
            "check_name",
            name=op.f("uq_risk_checks_strategy_run_id"),
        ),
    )


def downgrade() -> None:
    op.drop_table("risk_checks")
    op.drop_table("target_positions")
    op.drop_table("target_weights")
    op.drop_table("signals")
    op.drop_table("strategy_runs")
    op.drop_table("features_daily")
