"""Typed research dataset seams over canonical daily bars."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, field_validator


class DuplicateResearchBarError(ValueError):
    """Raised when a research dataset includes duplicate symbol/date bars."""


class ResearchDailyBar(BaseModel):
    """Immutable canonical daily bar input for research and backtests."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    symbol: str
    bar_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    adjusted_close: Decimal
    volume: int

    @field_validator("symbol")
    @classmethod
    def uppercase_symbol(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("symbol must not be empty")
        return normalized


@dataclass(frozen=True, slots=True)
class ResearchDataset:
    """Sorted, duplicate-safe daily-bar history for research use."""

    _bars_by_symbol: dict[str, tuple[ResearchDailyBar, ...]]

    @classmethod
    def from_bars(cls, bars: Iterable[ResearchDailyBar]) -> ResearchDataset:
        grouped: dict[str, list[ResearchDailyBar]] = defaultdict(list)
        seen_keys: set[tuple[str, date]] = set()

        for bar in bars:
            key = (bar.symbol, bar.bar_date)
            if key in seen_keys:
                detail = f"{bar.symbol} {bar.bar_date.isoformat()}"
                raise DuplicateResearchBarError(f"Duplicate research bar for symbol/date: {detail}")
            seen_keys.add(key)
            grouped[bar.symbol].append(bar)

        normalized = {
            symbol: tuple(sorted(symbol_bars, key=lambda item: item.bar_date))
            for symbol, symbol_bars in grouped.items()
        }
        return cls(_bars_by_symbol=dict(sorted(normalized.items())))

    @property
    def symbols(self) -> tuple[str, ...]:
        return tuple(self._bars_by_symbol.keys())

    def bars_for_symbol(self, symbol: str) -> tuple[ResearchDailyBar, ...]:
        return self._bars_by_symbol.get(symbol.upper(), ())

    def available_dates(self) -> tuple[date, ...]:
        return tuple(
            sorted({bar.bar_date for bars in self._bars_by_symbol.values() for bar in bars})
        )

    def next_available_date(self, after: date) -> date | None:
        for available_date in self.available_dates():
            if available_date > after:
                return available_date
        return None

    def history_up_to(self, as_of: date) -> ResearchDataset:
        filtered = {
            symbol: tuple(bar for bar in bars if bar.bar_date <= as_of)
            for symbol, bars in self._bars_by_symbol.items()
        }
        filtered = {symbol: bars for symbol, bars in filtered.items() if bars}
        return ResearchDataset(_bars_by_symbol=filtered)

    def latest_adjusted_closes(self) -> dict[str, Decimal]:
        latest: dict[str, Decimal] = {}
        for symbol, bars in self._bars_by_symbol.items():
            if bars:
                latest[symbol] = bars[-1].adjusted_close
        return latest
