from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from quant_core.execution.cli import _load_dataset, _parse_fill_prices, _summary_payload
from quant_core.execution.paper_run import PaperRunSummary


def test_parse_fill_prices_builds_decimal_map() -> None:
    assert _parse_fill_prices(["SPY=508.000000", "BND=72.400000"]) == {
        "BND": Decimal("72.400000"),
        "SPY": Decimal("508.000000"),
    }


def test_parse_fill_prices_rejects_invalid_items() -> None:
    with pytest.raises(ValueError, match="SYMBOL=PRICE"):
        _parse_fill_prices(["SPY"])


def test_load_dataset_reads_json_bars_fixture(tmp_path: Path) -> None:
    path = tmp_path / "bars.json"
    path.write_text(
        """[
  {
    "symbol": "SPY",
    "bar_date": "2026-04-22",
    "open": "506.000000",
    "high": "509.000000",
    "low": "505.000000",
    "close": "508.000000",
    "adjusted_close": "508.000000",
    "volume": 1200000
  }
]"""
    )

    dataset = _load_dataset(path)

    assert dataset.available_dates() == (date(2026, 4, 22),)
    assert dataset.bars_for_symbol("SPY")[0].adjusted_close == Decimal("508.000000")


def test_summary_payload_is_json_safe() -> None:
    payload = _summary_payload(
        PaperRunSummary(
            run_id=42,
            signal_date=date(2026, 4, 22),
            approved=True,
            order_count=1,
            fill_count=1,
            reconciliation_critical_rows=0,
            incident_count=0,
        )
    )

    assert payload == {
        "approved": True,
        "fill_count": 1,
        "incident_count": 0,
        "order_count": 1,
        "reconciliation_critical_rows": 0,
        "run_id": 42,
        "signal_date": "2026-04-22",
    }
