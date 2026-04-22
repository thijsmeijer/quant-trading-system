"""Canonical market-data schema models."""

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


class Instrument(Base):
    """Canonical tradable instrument metadata."""

    __tablename__ = "instruments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    category: Mapped[str] = mapped_column(String(64), nullable=False)
    exchange: Mapped[str] = mapped_column(String(32), nullable=False)
    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        server_default="true",
    )


class TradingCalendar(Base):
    """Trading-day schedule information normalized to UTC."""

    __tablename__ = "trading_calendar"

    trading_date: Mapped[date] = mapped_column(Date, primary_key=True)
    market_open_utc: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    market_close_utc: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    is_open: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_early_close: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default="false",
    )


class RawBarsDaily(Base):
    """Raw vendor payloads captured before normalization."""

    __tablename__ = "raw_bars_daily"
    __table_args__ = (UniqueConstraint("instrument_id", "vendor", "bar_date"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    instrument_id: Mapped[int] = mapped_column(
        ForeignKey("instruments.id", ondelete="CASCADE"),
        nullable=False,
    )
    vendor: Mapped[str] = mapped_column(String(64), nullable=False)
    bar_date: Mapped[date] = mapped_column(Date, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class BarsDaily(Base):
    """Normalized daily OHLCV bars derived from raw vendor data."""

    __tablename__ = "bars_daily"
    __table_args__ = (
        UniqueConstraint("instrument_id", "bar_date"),
        UniqueConstraint("raw_bar_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    instrument_id: Mapped[int] = mapped_column(
        ForeignKey("instruments.id", ondelete="CASCADE"),
        nullable=False,
    )
    raw_bar_id: Mapped[int] = mapped_column(
        ForeignKey("raw_bars_daily.id", ondelete="CASCADE"),
        nullable=False,
    )
    bar_date: Mapped[date] = mapped_column(Date, nullable=False)
    open: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    high: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    low: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    close: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    adjusted_close: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    volume: Mapped[int] = mapped_column(Integer, nullable=False)


class RawCorporateAction(Base):
    """Raw vendor payloads for corporate actions before normalization."""

    __tablename__ = "raw_corporate_actions"
    __table_args__ = (UniqueConstraint("instrument_id", "vendor", "action_type", "ex_date"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    instrument_id: Mapped[int] = mapped_column(
        ForeignKey("instruments.id", ondelete="CASCADE"),
        nullable=False,
    )
    vendor: Mapped[str] = mapped_column(String(64), nullable=False)
    action_type: Mapped[str] = mapped_column(String(32), nullable=False)
    ex_date: Mapped[date] = mapped_column(Date, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class CorporateAction(Base):
    """Normalized dividend and split metadata for future adjusted-data work."""

    __tablename__ = "corporate_actions"
    __table_args__ = (
        UniqueConstraint("instrument_id", "action_type", "ex_date"),
        UniqueConstraint("raw_action_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    instrument_id: Mapped[int] = mapped_column(
        ForeignKey("instruments.id", ondelete="CASCADE"),
        nullable=False,
    )
    raw_action_id: Mapped[int] = mapped_column(
        ForeignKey("raw_corporate_actions.id", ondelete="CASCADE"),
        nullable=False,
    )
    action_type: Mapped[str] = mapped_column(String(32), nullable=False)
    ex_date: Mapped[date] = mapped_column(Date, nullable=False)
    effective_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    cash_amount: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    currency: Mapped[str | None] = mapped_column(String(8), nullable=True)
    split_from: Mapped[int | None] = mapped_column(Integer, nullable=True)
    split_to: Mapped[int | None] = mapped_column(Integer, nullable=True)
