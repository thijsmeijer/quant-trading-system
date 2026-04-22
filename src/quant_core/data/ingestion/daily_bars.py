"""Ingestion flow for one vendor-shaped daily bar input."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from quant_core.data.models.market_data import BarsDaily, Instrument, RawBarsDaily


@dataclass(frozen=True, slots=True)
class VendorDailyBar:
    """One vendor-shaped daily bar input for a single ETF symbol."""

    symbol: str
    vendor: str
    bar_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    adjusted_close: Decimal
    volume: int
    fetched_at: datetime
    source_payload: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class DailyBarIngestionResult:
    """Minimal summary for one ingestion run."""

    processed_bars: int


class UnknownInstrumentError(ValueError):
    """Raised when ingestion receives symbols that are not loaded yet."""


def build_canonical_payload(bar: VendorDailyBar) -> dict[str, Any]:
    """Build a stable raw payload wrapper around the original vendor response."""

    return {
        "symbol": bar.symbol,
        "vendor": bar.vendor,
        "bar_date": bar.bar_date.isoformat(),
        "fetched_at": bar.fetched_at.isoformat(),
        "prices": {
            "open": str(bar.open),
            "high": str(bar.high),
            "low": str(bar.low),
            "close": str(bar.close),
            "adjusted_close": str(bar.adjusted_close),
        },
        "volume": bar.volume,
        "source_payload": dict(bar.source_payload),
    }


class DailyBarIngestionService:
    """Persist raw vendor payloads and normalized daily bars idempotently."""

    def ingest(
        self,
        session: Session,
        bars: Sequence[VendorDailyBar],
    ) -> DailyBarIngestionResult:
        if not bars:
            return DailyBarIngestionResult(processed_bars=0)

        instrument_ids = self._instrument_ids_by_symbol(session, bars)

        for bar in bars:
            instrument_id = instrument_ids[bar.symbol]
            raw_bar_id = self._upsert_raw_bar(session, instrument_id, bar)
            self._upsert_normalized_bar(session, instrument_id, raw_bar_id, bar)

        return DailyBarIngestionResult(processed_bars=len(bars))

    def _instrument_ids_by_symbol(
        self,
        session: Session,
        bars: Sequence[VendorDailyBar],
    ) -> dict[str, int]:
        symbols = sorted({bar.symbol for bar in bars})
        rows = session.execute(
            select(Instrument.symbol, Instrument.id).where(Instrument.symbol.in_(symbols))
        ).all()
        symbol_to_id = {symbol: instrument_id for symbol, instrument_id in rows}

        missing_symbols = sorted(set(symbols) - set(symbol_to_id))
        if missing_symbols:
            joined = ", ".join(missing_symbols)
            raise UnknownInstrumentError(f"Missing instruments for symbols: {joined}")

        return symbol_to_id

    def _upsert_raw_bar(
        self,
        session: Session,
        instrument_id: int,
        bar: VendorDailyBar,
    ) -> int:
        statement = (
            insert(RawBarsDaily)
            .values(
                instrument_id=instrument_id,
                vendor=bar.vendor,
                bar_date=bar.bar_date,
                payload=build_canonical_payload(bar),
                fetched_at=bar.fetched_at,
            )
            .on_conflict_do_update(
                index_elements=[
                    RawBarsDaily.instrument_id,
                    RawBarsDaily.vendor,
                    RawBarsDaily.bar_date,
                ],
                set_={
                    "payload": build_canonical_payload(bar),
                    "fetched_at": bar.fetched_at,
                },
            )
            .returning(RawBarsDaily.id)
        )

        return session.execute(statement).scalar_one()

    def _upsert_normalized_bar(
        self,
        session: Session,
        instrument_id: int,
        raw_bar_id: int,
        bar: VendorDailyBar,
    ) -> None:
        statement = insert(BarsDaily).values(
            instrument_id=instrument_id,
            raw_bar_id=raw_bar_id,
            bar_date=bar.bar_date,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            adjusted_close=bar.adjusted_close,
            volume=bar.volume,
        )

        statement = statement.on_conflict_do_update(
            index_elements=[BarsDaily.instrument_id, BarsDaily.bar_date],
            set_={
                "raw_bar_id": raw_bar_id,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "adjusted_close": bar.adjusted_close,
                "volume": bar.volume,
            },
        )

        session.execute(statement)
