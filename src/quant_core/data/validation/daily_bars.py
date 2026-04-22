"""Validation checks for stored daily-bar market data."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import TypeVar

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from quant_core.data.models.market_data import BarsDaily, Instrument, RawBarsDaily, TradingCalendar

KeyT = TypeVar("KeyT")


@dataclass(frozen=True, slots=True)
class DuplicateKey[KeyT]:
    """One duplicate key group plus its observed occurrence count."""

    key: KeyT
    occurrences: int


@dataclass(frozen=True, slots=True)
class MissingBarIssue:
    """A missing normalized bar for a symbol and open trading date."""

    symbol: str
    bar_date: date


@dataclass(frozen=True, slots=True)
class StaleSymbolIssue:
    """A symbol whose latest normalized bar is older than expected."""

    symbol: str
    latest_bar_date: date | None
    expected_bar_date: date


@dataclass(frozen=True, slots=True)
class PriceSanityIssue:
    """A normalized bar whose OHLC envelope is impossible."""

    symbol: str
    bar_date: date
    detail: str


@dataclass(frozen=True, slots=True)
class DailyBarValidationReport:
    """Structured validation report for stored daily-bar quality checks."""

    checked_as_of: date
    raw_duplicates: list[DuplicateKey[tuple[str, str, date]]]
    normalized_duplicates: list[DuplicateKey[tuple[str, date]]]
    missing_bars: list[MissingBarIssue]
    stale_symbols: list[StaleSymbolIssue]
    price_sanity_issues: list[PriceSanityIssue]


class DailyBarValidationService:
    """Validate stored daily-bar data before downstream strategy usage."""

    @staticmethod
    def find_duplicate_keys(keys: list[KeyT]) -> list[DuplicateKey[KeyT]]:
        """Return only the duplicate groups from a list of composite keys."""

        counts = Counter(keys)
        duplicates = [
            DuplicateKey(key=key, occurrences=occurrences)
            for key, occurrences in counts.items()
            if occurrences > 1
        ]
        return sorted(duplicates, key=lambda item: repr(item.key))

    def validate(
        self,
        session: Session,
        *,
        as_of: date,
    ) -> DailyBarValidationReport:
        active_symbols = self._active_symbols(session)
        open_dates = self._open_dates(session, as_of)
        expected_latest = open_dates[-1] if open_dates else as_of

        return DailyBarValidationReport(
            checked_as_of=as_of,
            raw_duplicates=self._raw_duplicates(session),
            normalized_duplicates=self._normalized_duplicates(session),
            missing_bars=self._missing_bars(session, active_symbols, open_dates),
            stale_symbols=self._stale_symbols(session, active_symbols, expected_latest),
            price_sanity_issues=self._price_sanity_issues(session),
        )

    def _active_symbols(self, session: Session) -> list[tuple[int, str]]:
        rows = session.execute(
            select(Instrument.id, Instrument.symbol)
            .where(Instrument.is_active.is_(True))
            .order_by(Instrument.symbol)
        ).all()
        return [(instrument_id, symbol) for instrument_id, symbol in rows]

    def _open_dates(self, session: Session, as_of: date) -> list[date]:
        rows = session.execute(
            select(TradingCalendar.trading_date)
            .where(
                TradingCalendar.is_open.is_(True),
                TradingCalendar.trading_date <= as_of,
            )
            .order_by(TradingCalendar.trading_date)
        ).all()
        return [trading_date for (trading_date,) in rows]

    def _raw_duplicates(self, session: Session) -> list[DuplicateKey[tuple[str, str, date]]]:
        rows = session.execute(
            select(
                Instrument.symbol,
                RawBarsDaily.vendor,
                RawBarsDaily.bar_date,
                func.count().label("occurrences"),
            )
            .join(Instrument, Instrument.id == RawBarsDaily.instrument_id)
            .group_by(Instrument.symbol, RawBarsDaily.vendor, RawBarsDaily.bar_date)
            .having(func.count() > 1)
            .order_by(Instrument.symbol, RawBarsDaily.vendor, RawBarsDaily.bar_date)
        ).all()
        return [
            DuplicateKey(
                key=(symbol, vendor, bar_date),
                occurrences=occurrences,
            )
            for symbol, vendor, bar_date, occurrences in rows
        ]

    def _normalized_duplicates(self, session: Session) -> list[DuplicateKey[tuple[str, date]]]:
        rows = session.execute(
            select(
                Instrument.symbol,
                BarsDaily.bar_date,
                func.count().label("occurrences"),
            )
            .join(Instrument, Instrument.id == BarsDaily.instrument_id)
            .group_by(Instrument.symbol, BarsDaily.bar_date)
            .having(func.count() > 1)
            .order_by(Instrument.symbol, BarsDaily.bar_date)
        ).all()
        return [
            DuplicateKey(key=(symbol, bar_date), occurrences=occurrences)
            for symbol, bar_date, occurrences in rows
        ]

    def _missing_bars(
        self,
        session: Session,
        active_symbols: list[tuple[int, str]],
        open_dates: list[date],
    ) -> list[MissingBarIssue]:
        existing_rows = session.execute(select(BarsDaily.instrument_id, BarsDaily.bar_date)).all()
        existing = {(instrument_id, bar_date) for instrument_id, bar_date in existing_rows}

        issues: list[MissingBarIssue] = []
        for instrument_id, symbol in active_symbols:
            for bar_date in open_dates:
                if (instrument_id, bar_date) not in existing:
                    issues.append(MissingBarIssue(symbol=symbol, bar_date=bar_date))

        return issues

    def _stale_symbols(
        self,
        session: Session,
        active_symbols: list[tuple[int, str]],
        expected_latest: date,
    ) -> list[StaleSymbolIssue]:
        rows = session.execute(
            select(BarsDaily.instrument_id, func.max(BarsDaily.bar_date)).group_by(
                BarsDaily.instrument_id
            )
        ).all()
        latest_by_instrument = {
            instrument_id: latest_bar_date for instrument_id, latest_bar_date in rows
        }

        issues: list[StaleSymbolIssue] = []
        for instrument_id, symbol in active_symbols:
            latest_bar_date = latest_by_instrument.get(instrument_id)
            if latest_bar_date != expected_latest:
                issues.append(
                    StaleSymbolIssue(
                        symbol=symbol,
                        latest_bar_date=latest_bar_date,
                        expected_bar_date=expected_latest,
                    )
                )

        return issues

    def _price_sanity_issues(self, session: Session) -> list[PriceSanityIssue]:
        rows = session.execute(
            select(
                Instrument.symbol,
                BarsDaily.bar_date,
                BarsDaily.open,
                BarsDaily.high,
                BarsDaily.low,
                BarsDaily.close,
            )
            .join(Instrument, Instrument.id == BarsDaily.instrument_id)
            .order_by(Instrument.symbol, BarsDaily.bar_date)
        ).all()

        issues: list[PriceSanityIssue] = []
        for symbol, bar_date, open_price, high_price, low_price, close_price in rows:
            if not self._ohlc_is_sane(open_price, high_price, low_price, close_price):
                issues.append(
                    PriceSanityIssue(
                        symbol=symbol,
                        bar_date=bar_date,
                        detail="high/low envelope violated for OHLC prices",
                    )
                )

        return issues

    @staticmethod
    def _ohlc_is_sane(
        open_price: Decimal,
        high_price: Decimal,
        low_price: Decimal,
        close_price: Decimal,
    ) -> bool:
        upper_bound = max(open_price, low_price, close_price)
        lower_bound = min(open_price, high_price, close_price)
        return high_price >= upper_bound and low_price <= lower_bound
