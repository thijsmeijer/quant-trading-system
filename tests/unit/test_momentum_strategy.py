from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from quant_core.research.daily_bars import ResearchDailyBar, ResearchDataset
from quant_core.strategy import (
    MomentumRotationStrategy,
    MomentumStrategyConfig,
    StrategyTargetWeight,
    UnavailableSignalDateError,
)

FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "canonical_daily_bars.json"


def test_build_decision_selects_top_momentum_symbol_without_future_lookahead() -> None:
    dataset = ResearchDataset.from_bars(
        [
            *_fixture_bars(),
            _future_bar("SPY", adjusted_close="1000.000000"),
            _future_bar("BND", adjusted_close="10.000000"),
        ]
    )
    strategy = MomentumRotationStrategy()
    config = MomentumStrategyConfig(
        version="v1",
        lookback_bars=2,
        trend_lookback_bars=3,
        top_n=1,
    )

    decision = strategy.build_decision(
        dataset=dataset,
        signal_date=date(2026, 4, 22),
        config=config,
    )

    assert decision.execution_date == date(2026, 4, 23)
    assert decision.signals[0].symbol == "SPY"
    assert decision.signals[0].score == Decimal("0.007937")
    assert decision.signals[0].passes_trend_filter is True
    assert decision.signals[0].is_selected is True
    assert decision.signals[1].symbol == "BND"
    assert decision.signals[1].score == Decimal("0.004161")
    assert decision.signals[1].is_selected is False
    assert decision.target_weights == (
        StrategyTargetWeight(
            allocation_key="SPY",
            target_weight=Decimal("1.000000"),
            symbol="SPY",
        ),
    )


def test_build_decision_uses_cash_fallback_when_no_symbol_meets_minimum_momentum() -> None:
    dataset = ResearchDataset.from_bars([*_fixture_bars(), _future_bar("SPY", "509.000000")])
    strategy = MomentumRotationStrategy()
    config = MomentumStrategyConfig(
        version="v1",
        lookback_bars=2,
        trend_lookback_bars=3,
        top_n=1,
        minimum_momentum=Decimal("0.010000"),
    )

    decision = strategy.build_decision(
        dataset=dataset,
        signal_date=date(2026, 4, 22),
        config=config,
        execution_date=date(2026, 4, 23),
    )

    assert all(signal.is_selected is False for signal in decision.signals)
    assert decision.target_weights == (
        StrategyTargetWeight(
            allocation_key="cash",
            target_weight=Decimal("1.000000"),
            symbol=None,
        ),
    )


def test_build_decision_rejects_signal_dates_not_present_in_dataset() -> None:
    strategy = MomentumRotationStrategy()
    dataset = ResearchDataset.from_bars(_fixture_bars())
    config = MomentumStrategyConfig(
        version="v1",
        lookback_bars=2,
        trend_lookback_bars=3,
        top_n=1,
    )

    with pytest.raises(UnavailableSignalDateError, match="signal_date"):
        strategy.build_decision(
            dataset=dataset,
            signal_date=date(2026, 4, 19),
            config=config,
            execution_date=date(2026, 4, 20),
        )


def _fixture_bars() -> list[ResearchDailyBar]:
    payload = json.loads(FIXTURE_PATH.read_text())
    return [ResearchDailyBar.model_validate(item) for item in payload]


def _future_bar(symbol: str, adjusted_close: str) -> ResearchDailyBar:
    return ResearchDailyBar.model_validate(
        {
            "symbol": symbol,
            "bar_date": "2026-04-23",
            "open": adjusted_close,
            "high": adjusted_close,
            "low": adjusted_close,
            "close": adjusted_close,
            "adjusted_close": adjusted_close,
            "volume": 999999,
        }
    )
