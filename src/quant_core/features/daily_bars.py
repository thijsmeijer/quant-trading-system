"""Feature engineering over canonical research daily bars."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal

from quant_core.research.daily_bars import ResearchDataset

MOMENTUM_PRECISION = Decimal("0.000001")


@dataclass(frozen=True, slots=True)
class MomentumSnapshot:
    """Trailing adjusted-close return features at one signal date."""

    signal_date: date
    lookback_bars: int
    values: dict[str, Decimal]


def build_momentum_snapshot(
    *,
    dataset: ResearchDataset,
    signal_date: date,
    lookback_bars: int,
) -> MomentumSnapshot:
    """Build trailing-return features using only bars known on the signal date."""

    if lookback_bars <= 0:
        raise ValueError("lookback_bars must be positive")

    history = dataset.history_up_to(signal_date)
    values: dict[str, Decimal] = {}

    for symbol in history.symbols:
        bars = history.bars_for_symbol(symbol)
        if len(bars) <= lookback_bars:
            continue

        current = bars[-1].adjusted_close
        prior = bars[-(lookback_bars + 1)].adjusted_close
        values[symbol] = ((current / prior) - Decimal("1")).quantize(
            MOMENTUM_PRECISION,
            rounding=ROUND_HALF_UP,
        )

    return MomentumSnapshot(
        signal_date=signal_date,
        lookback_bars=lookback_bars,
        values=values,
    )
