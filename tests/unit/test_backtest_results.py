from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from quant_core.backtest.results import (
    DuplicateReturnObservationError,
    EmptyAlignedReturnWindowError,
    ReturnObservation,
    build_backtest_result_summary,
)


def test_build_backtest_result_summary_aligns_dates_and_compounds_returns() -> None:
    summary = build_backtest_result_summary(
        strategy_returns=[
            ReturnObservation(as_of=date(2026, 4, 21), period_return=Decimal("0.010000")),
            ReturnObservation(as_of=date(2026, 4, 22), period_return=Decimal("0.020000")),
            ReturnObservation(as_of=date(2026, 4, 23), period_return=Decimal("-0.005000")),
        ],
        benchmark_name="SPY",
        benchmark_returns=[
            ReturnObservation(as_of=date(2026, 4, 20), period_return=Decimal("0.010000")),
            ReturnObservation(as_of=date(2026, 4, 21), period_return=Decimal("0.005000")),
            ReturnObservation(as_of=date(2026, 4, 22), period_return=Decimal("0.015000")),
            ReturnObservation(as_of=date(2026, 4, 23), period_return=Decimal("-0.002500")),
        ],
    )

    assert summary.benchmark_name == "SPY"
    assert summary.start_date == date(2026, 4, 21)
    assert summary.end_date == date(2026, 4, 23)
    assert summary.observation_count == 3
    assert summary.aligned_dates == (
        date(2026, 4, 21),
        date(2026, 4, 22),
        date(2026, 4, 23),
    )
    assert summary.strategy_cumulative_return == Decimal("0.025049")
    assert summary.benchmark_cumulative_return == Decimal("0.017525")
    assert summary.excess_return == Decimal("0.007524")


def test_build_backtest_result_summary_rejects_duplicate_observation_dates() -> None:
    with pytest.raises(DuplicateReturnObservationError, match="2026-04-21"):
        build_backtest_result_summary(
            strategy_returns=[
                ReturnObservation(as_of=date(2026, 4, 21), period_return=Decimal("0.010000")),
                ReturnObservation(as_of=date(2026, 4, 21), period_return=Decimal("0.020000")),
            ],
            benchmark_name="SPY",
            benchmark_returns=[
                ReturnObservation(as_of=date(2026, 4, 21), period_return=Decimal("0.005000")),
            ],
        )


def test_build_backtest_result_summary_rejects_missing_aligned_dates() -> None:
    with pytest.raises(EmptyAlignedReturnWindowError, match="aligned return dates"):
        build_backtest_result_summary(
            strategy_returns=[
                ReturnObservation(as_of=date(2026, 4, 21), period_return=Decimal("0.010000")),
            ],
            benchmark_name="SPY",
            benchmark_returns=[
                ReturnObservation(as_of=date(2026, 4, 22), period_return=Decimal("0.005000")),
            ],
        )
