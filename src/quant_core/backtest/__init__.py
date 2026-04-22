"""Backtest engine package."""

from quant_core.backtest.daily_bars import (
    InvalidBacktestWindowError,
    RebalanceInput,
    build_rebalance_input,
)
from quant_core.backtest.friction import BacktestFrictionConfig
from quant_core.backtest.results import (
    BacktestResultSummary,
    DuplicateReturnObservationError,
    EmptyAlignedReturnWindowError,
    ReturnObservation,
    build_backtest_result_summary,
)

__all__ = [
    "BacktestResultSummary",
    "BacktestFrictionConfig",
    "DuplicateReturnObservationError",
    "EmptyAlignedReturnWindowError",
    "InvalidBacktestWindowError",
    "RebalanceInput",
    "ReturnObservation",
    "build_backtest_result_summary",
    "build_rebalance_input",
]
