"""Benchmark-aware summary helpers for backtest return series."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from typing import Final

RETURN_PRECISION: Final = Decimal("0.000001")
ONE: Final = Decimal("1")


class DuplicateReturnObservationError(ValueError):
    """Raised when a return series repeats the same observation date."""


class EmptyAlignedReturnWindowError(ValueError):
    """Raised when strategy and benchmark returns do not overlap on dates."""


@dataclass(frozen=True, slots=True)
class ReturnObservation:
    """Single-period return observed on one aligned backtest date."""

    as_of: date
    period_return: Decimal


@dataclass(frozen=True, slots=True)
class BacktestResultSummary:
    """Compounded strategy-versus-benchmark summary on common dates only."""

    benchmark_name: str
    start_date: date
    end_date: date
    observation_count: int
    aligned_dates: tuple[date, ...]
    strategy_cumulative_return: Decimal
    benchmark_cumulative_return: Decimal
    excess_return: Decimal


def build_backtest_result_summary(
    *,
    strategy_returns: list[ReturnObservation],
    benchmark_name: str,
    benchmark_returns: list[ReturnObservation],
) -> BacktestResultSummary:
    """Compound strategy and benchmark returns on their shared dates."""

    normalized_benchmark_name = benchmark_name.strip().upper()
    if not normalized_benchmark_name:
        raise ValueError("benchmark_name must not be empty")

    strategy_by_date = _normalize_observations(strategy_returns)
    benchmark_by_date = _normalize_observations(benchmark_returns)

    aligned_dates = tuple(sorted(strategy_by_date.keys() & benchmark_by_date.keys()))
    if not aligned_dates:
        raise EmptyAlignedReturnWindowError(
            "strategy and benchmark must share aligned return dates"
        )

    strategy_cumulative_return = _compound_returns(
        returns_by_date=strategy_by_date,
        aligned_dates=aligned_dates,
    )
    benchmark_cumulative_return = _compound_returns(
        returns_by_date=benchmark_by_date,
        aligned_dates=aligned_dates,
    )

    return BacktestResultSummary(
        benchmark_name=normalized_benchmark_name,
        start_date=aligned_dates[0],
        end_date=aligned_dates[-1],
        observation_count=len(aligned_dates),
        aligned_dates=aligned_dates,
        strategy_cumulative_return=strategy_cumulative_return,
        benchmark_cumulative_return=benchmark_cumulative_return,
        excess_return=(strategy_cumulative_return - benchmark_cumulative_return).quantize(
            RETURN_PRECISION,
            rounding=ROUND_HALF_UP,
        ),
    )


def _normalize_observations(
    observations: list[ReturnObservation],
) -> dict[date, Decimal]:
    values: dict[date, Decimal] = {}

    for observation in observations:
        if observation.as_of in values:
            detail = observation.as_of.isoformat()
            raise DuplicateReturnObservationError(
                f"duplicate return observation for date: {detail}"
            )
        values[observation.as_of] = observation.period_return

    return values


def _compound_returns(
    *,
    returns_by_date: dict[date, Decimal],
    aligned_dates: tuple[date, ...],
) -> Decimal:
    cumulative = ONE

    for as_of in aligned_dates:
        cumulative *= ONE + returns_by_date[as_of]

    return (cumulative - ONE).quantize(RETURN_PRECISION, rounding=ROUND_HALF_UP)
