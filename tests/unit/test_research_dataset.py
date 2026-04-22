from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from quant_core.research.daily_bars import (
    DuplicateResearchBarError,
    ResearchDailyBar,
    ResearchDataset,
)

FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "canonical_daily_bars.json"


def test_research_dataset_filters_history_up_to_signal_date() -> None:
    dataset = ResearchDataset.from_bars(_fixture_bars())

    history = dataset.history_up_to(date(2026, 4, 21))

    assert history.symbols == ("BND", "SPY")
    assert history.latest_adjusted_closes() == {
        "BND": Decimal("72.300000"),
        "SPY": Decimal("506.000000"),
    }
    assert all(bar.bar_date <= date(2026, 4, 21) for bar in history.bars_for_symbol("SPY"))


def test_research_dataset_exposes_sorted_available_dates_and_next_date() -> None:
    dataset = ResearchDataset.from_bars(_fixture_bars())

    assert dataset.available_dates() == (
        date(2026, 4, 20),
        date(2026, 4, 21),
        date(2026, 4, 22),
    )
    assert dataset.next_available_date(date(2026, 4, 20)) == date(2026, 4, 21)
    assert dataset.next_available_date(date(2026, 4, 22)) is None


def test_research_dataset_rejects_duplicate_symbol_dates() -> None:
    duplicate_bars = [
        *_fixture_bars(),
        ResearchDailyBar(
            symbol="SPY",
            bar_date=date(2026, 4, 21),
            open=Decimal("1"),
            high=Decimal("1"),
            low=Decimal("1"),
            close=Decimal("1"),
            adjusted_close=Decimal("1"),
            volume=1,
        ),
    ]

    with pytest.raises(DuplicateResearchBarError, match="SPY"):
        ResearchDataset.from_bars(duplicate_bars)


def _fixture_bars() -> list[ResearchDailyBar]:
    payload = json.loads(FIXTURE_PATH.read_text())
    return [ResearchDailyBar.model_validate(item) for item in payload]
