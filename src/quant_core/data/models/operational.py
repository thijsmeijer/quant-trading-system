"""Operational schema models for strategy, portfolio, and risk state."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from quant_core.data.models.base import Base


class FeaturesDaily(Base):
    """Persisted derived features keyed by instrument, date, and feature name."""

    __tablename__ = "features_daily"
    __table_args__ = (
        UniqueConstraint("instrument_id", "bar_date", "feature_name", "feature_version"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    instrument_id: Mapped[int] = mapped_column(
        ForeignKey("instruments.id", ondelete="CASCADE"),
        nullable=False,
    )
    bar_date: Mapped[date] = mapped_column(Date, nullable=False)
    feature_name: Mapped[str] = mapped_column(String(128), nullable=False)
    feature_version: Mapped[str] = mapped_column(String(64), nullable=False)
    feature_value: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class StrategyRun(Base):
    """Metadata for one strategy execution in dev, paper, or live mode."""

    __tablename__ = "strategy_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_mode: Mapped[str] = mapped_column(String(16), nullable=False)
    strategy_name: Mapped[str] = mapped_column(String(128), nullable=False)
    config_version: Mapped[str] = mapped_column(String(128), nullable=False)
    config_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    signal_date: Mapped[date] = mapped_column(Date, nullable=False)
    execution_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)


class Signal(Base):
    """Instrument-level strategy outputs before portfolio construction."""

    __tablename__ = "signals"
    __table_args__ = (UniqueConstraint("strategy_run_id", "instrument_id", "signal_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    strategy_run_id: Mapped[int] = mapped_column(
        ForeignKey("strategy_runs.id", ondelete="CASCADE"),
        nullable=False,
    )
    instrument_id: Mapped[int] = mapped_column(
        ForeignKey("instruments.id", ondelete="CASCADE"),
        nullable=False,
    )
    signal_name: Mapped[str] = mapped_column(String(64), nullable=False)
    rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    score: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    is_selected: Mapped[bool] = mapped_column(Boolean, nullable=False)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class TargetWeight(Base):
    """Desired allocation weights emitted after portfolio construction."""

    __tablename__ = "target_weights"
    __table_args__ = (UniqueConstraint("strategy_run_id", "allocation_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    strategy_run_id: Mapped[int] = mapped_column(
        ForeignKey("strategy_runs.id", ondelete="CASCADE"),
        nullable=False,
    )
    instrument_id: Mapped[int | None] = mapped_column(
        ForeignKey("instruments.id", ondelete="CASCADE"),
        nullable=True,
    )
    allocation_key: Mapped[str] = mapped_column(String(64), nullable=False)
    target_weight: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class TargetPosition(Base):
    """Target notionals and quantities derived from desired target weights."""

    __tablename__ = "target_positions"
    __table_args__ = (UniqueConstraint("strategy_run_id", "allocation_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    strategy_run_id: Mapped[int] = mapped_column(
        ForeignKey("strategy_runs.id", ondelete="CASCADE"),
        nullable=False,
    )
    instrument_id: Mapped[int | None] = mapped_column(
        ForeignKey("instruments.id", ondelete="CASCADE"),
        nullable=True,
    )
    allocation_key: Mapped[str] = mapped_column(String(64), nullable=False)
    target_weight: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    target_notional: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    target_quantity: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    reference_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class RiskCheck(Base):
    """Persisted risk approval or rejection decisions with explicit reasons."""

    __tablename__ = "risk_checks"
    __table_args__ = (UniqueConstraint("strategy_run_id", "check_scope", "check_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    strategy_run_id: Mapped[int] = mapped_column(
        ForeignKey("strategy_runs.id", ondelete="CASCADE"),
        nullable=False,
    )
    check_scope: Mapped[str] = mapped_column(String(32), nullable=False)
    check_name: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    reason_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    checked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    details: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)


class Order(Base):
    """Internal order records created before any broker submission."""

    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    strategy_run_id: Mapped[int | None] = mapped_column(
        ForeignKey("strategy_runs.id", ondelete="SET NULL"),
        nullable=True,
    )
    instrument_id: Mapped[int | None] = mapped_column(
        ForeignKey("instruments.id", ondelete="CASCADE"),
        nullable=True,
    )
    internal_order_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    run_mode: Mapped[str] = mapped_column(String(16), nullable=False)
    order_type: Mapped[str] = mapped_column(String(32), nullable=False)
    side: Mapped[str] = mapped_column(String(8), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    requested_quantity: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    requested_notional: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    time_in_force: Mapped[str | None] = mapped_column(String(16), nullable=True)
    broker_order_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    canceled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class OrderEvent(Base):
    """Lifecycle event history for internal orders."""

    __tablename__ = "order_events"
    __table_args__ = (UniqueConstraint("order_id", "event_type", "event_at"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_id: Mapped[int] = mapped_column(
        ForeignKey("orders.id", ondelete="CASCADE"),
        nullable=False,
    )
    event_type: Mapped[str] = mapped_column(String(32), nullable=False)
    event_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    broker_event_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    details: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)


class Fill(Base):
    """Execution fills linked back to internal orders."""

    __tablename__ = "fills"
    __table_args__ = (UniqueConstraint("order_id", "fill_at", "fill_price", "fill_quantity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_id: Mapped[int] = mapped_column(
        ForeignKey("orders.id", ondelete="CASCADE"),
        nullable=False,
    )
    broker_fill_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    fill_quantity: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    fill_price: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    fill_notional: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    fill_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class Position(Base):
    """Internal position snapshots by environment and timestamp."""

    __tablename__ = "positions"
    __table_args__ = (UniqueConstraint("run_mode", "instrument_id", "as_of"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_mode: Mapped[str] = mapped_column(String(16), nullable=False)
    instrument_id: Mapped[int] = mapped_column(
        ForeignKey("instruments.id", ondelete="CASCADE"),
        nullable=False,
    )
    quantity: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    average_cost: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    market_value: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class AccountSnapshot(Base):
    """Account cash and equity state by environment and timestamp."""

    __tablename__ = "account_snapshots"
    __table_args__ = (UniqueConstraint("run_mode", "as_of"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_mode: Mapped[str] = mapped_column(String(16), nullable=False)
    cash: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    equity: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    buying_power: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class PnlSnapshot(Base):
    """Realized and unrealized PnL state by environment and timestamp."""

    __tablename__ = "pnl_snapshots"
    __table_args__ = (UniqueConstraint("run_mode", "as_of"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_mode: Mapped[str] = mapped_column(String(16), nullable=False)
    realized_pnl: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    unrealized_pnl: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    total_pnl: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class RiskSnapshot(Base):
    """Exposure and drawdown state by environment and timestamp."""

    __tablename__ = "risk_snapshots"
    __table_args__ = (UniqueConstraint("run_mode", "as_of"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_mode: Mapped[str] = mapped_column(String(16), nullable=False)
    gross_exposure: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    net_exposure: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    drawdown: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    open_order_count: Mapped[int] = mapped_column(Integer, nullable=False)
    as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class Incident(Base):
    """Operational incidents and alerts with severity and timestamps."""

    __tablename__ = "incidents"
    __table_args__ = (UniqueConstraint("run_mode", "incident_type", "occurred_at"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_mode: Mapped[str] = mapped_column(String(16), nullable=False)
    incident_type: Mapped[str] = mapped_column(String(64), nullable=False)
    severity: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    summary: Mapped[str] = mapped_column(String(255), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    details: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
