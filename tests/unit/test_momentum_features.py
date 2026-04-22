from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

from quant_core.features.daily_bars import build_momentum_snapshot
from quant_core.research.daily_bars import ResearchDailyBar, ResearchDataset

FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "canonical_daily_bars.json"


def test_build_momentum_snapshot_computes_trailing_adjusted_close_returns() -> None:
    dataset = ResearchDataset.from_bars(_fixture_bars())

    snapshot = build_momentum_snapshot(
        dataset=dataset,
        signal_date=date(2026, 4, 22),
        lookback_bars=2,
    )

    assert snapshot.signal_date == date(2026, 4, 22)
    assert snapshot.lookback_bars == 2
    assert snapshot.values == {
        "BND": Decimal("0.004161"),
        "SPY": Decimal("0.007937"),
    }


def test_build_momentum_snapshot_ignores_future_bars_beyond_signal_date() -> None:
    future_bar = ResearchDailyBar.model_validate(
        {
            "symbol": "SPY",
            "bar_date": "2026-04-23",
            "open": "508.000000",
            "high": "1000.000000",
            "low": "508.000000",
            "close": "1000.000000",
            "adjusted_close": "1000.000000",
            "volume": 9999999,
        }
    )
    dataset = ResearchDataset.from_bars([*_fixture_bars(), future_bar])

    snapshot = build_momentum_snapshot(
        dataset=dataset,
        signal_date=date(2026, 4, 22),
        lookback_bars=2,
    )

    assert snapshot.values["SPY"] == Decimal("0.007937")


def test_build_momentum_snapshot_skips_symbols_without_enough_history() -> None:
    dataset = ResearchDataset.from_bars(_fixture_bars())

    snapshot = build_momentum_snapshot(
        dataset=dataset,
        signal_date=date(2026, 4, 22),
        lookback_bars=3,
    )

    assert snapshot.values == {}


def _fixture_bars() -> list[ResearchDailyBar]:
    payload = json.loads(FIXTURE_PATH.read_text())
    return [ResearchDailyBar.model_validate(item) for item in payload]
