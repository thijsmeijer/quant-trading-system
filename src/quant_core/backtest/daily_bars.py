"""Minimal backtest input seams over research daily-bar datasets."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from quant_core.research.daily_bars import ResearchDataset


class InvalidBacktestWindowError(ValueError):
    """Raised when a backtest decision window violates timing rules."""


@dataclass(frozen=True, slots=True)
class RebalanceInput:
    """Signal-time market snapshot for a later execution decision."""

    signal_date: date
    execution_date: date
    latest_adjusted_closes: dict[str, Decimal]


def build_rebalance_input(
    *,
    dataset: ResearchDataset,
    signal_date: date,
    execution_date: date,
) -> RebalanceInput:
    """Freeze the market state known on the signal date for later execution."""

    available_dates = dataset.available_dates()
    if signal_date not in available_dates:
        raise InvalidBacktestWindowError("signal_date must be a known market date")

    if execution_date <= signal_date:
        raise InvalidBacktestWindowError("execution_date must be after signal_date")
    if execution_date not in available_dates:
        raise InvalidBacktestWindowError("execution_date must be an available trading date")

    signal_history = dataset.history_up_to(signal_date)
    return RebalanceInput(
        signal_date=signal_date,
        execution_date=execution_date,
        latest_adjusted_closes=signal_history.latest_adjusted_closes(),
    )
