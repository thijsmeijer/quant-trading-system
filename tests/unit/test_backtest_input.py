from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from quant_core.backtest.daily_bars import (
    InvalidBacktestWindowError,
    build_rebalance_input,
)
from quant_core.backtest.friction import BacktestFrictionConfig
from quant_core.research.daily_bars import ResearchDailyBar, ResearchDataset

FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "canonical_daily_bars.json"


def test_build_rebalance_input_uses_only_signal_date_information() -> None:
    dataset = ResearchDataset.from_bars(_fixture_bars())
    friction_config = BacktestFrictionConfig.baseline()

    rebalance_input = build_rebalance_input(
        dataset=dataset,
        signal_date=date(2026, 4, 21),
        execution_date=date(2026, 4, 22),
        friction_config=friction_config,
    )

    assert rebalance_input.signal_date == date(2026, 4, 21)
    assert rebalance_input.execution_date == date(2026, 4, 22)
    assert rebalance_input.friction_config == friction_config
    assert rebalance_input.latest_adjusted_closes == {
        "BND": Decimal("72.300000"),
        "SPY": Decimal("506.000000"),
    }


def test_build_rebalance_input_requires_execution_after_signal() -> None:
    dataset = ResearchDataset.from_bars(_fixture_bars())
    friction_config = BacktestFrictionConfig.baseline()

    with pytest.raises(InvalidBacktestWindowError, match="after signal_date"):
        build_rebalance_input(
            dataset=dataset,
            signal_date=date(2026, 4, 22),
            execution_date=date(2026, 4, 22),
            friction_config=friction_config,
        )


def test_build_rebalance_input_requires_signal_date_with_known_market_state() -> None:
    dataset = ResearchDataset.from_bars(_fixture_bars())
    friction_config = BacktestFrictionConfig.baseline()

    with pytest.raises(InvalidBacktestWindowError, match="signal_date"):
        build_rebalance_input(
            dataset=dataset,
            signal_date=date(2026, 4, 19),
            execution_date=date(2026, 4, 20),
            friction_config=friction_config,
        )


def test_build_rebalance_input_requires_execution_date_on_available_future_bar() -> None:
    dataset = ResearchDataset.from_bars(_fixture_bars())
    friction_config = BacktestFrictionConfig.baseline()

    with pytest.raises(InvalidBacktestWindowError, match="available trading date"):
        build_rebalance_input(
            dataset=dataset,
            signal_date=date(2026, 4, 21),
            execution_date=date(2026, 4, 24),
            friction_config=friction_config,
        )


def _fixture_bars() -> list[ResearchDailyBar]:
    payload = json.loads(FIXTURE_PATH.read_text())
    return [ResearchDailyBar.model_validate(item) for item in payload]
