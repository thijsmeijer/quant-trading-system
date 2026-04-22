from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from quant_core.data.ingestion.universe import UniverseValidationError, load_universe_definition


def test_load_universe_definition_reads_versioned_yaml(tmp_path: Path) -> None:
    universe_path = tmp_path / "universe.yaml"
    universe_path.write_text(
        """
version: 1
as_of: 2026-04-20
universe:
  name: core_us_etfs
  venue: us_equities
  bar_frequency: daily
  regular_hours_only: true
eligibility:
  min_price: 20
  min_average_daily_volume: 1000000
  min_history_days: 252
  excluded_flags:
    - leveraged
    - inverse
instruments:
  - symbol: SPY
    name: SPDR S&P 500 ETF Trust
    category: broad_us_equity
    exchange: ARCA
    is_active: true
    flags: []
""".strip()
    )

    definition = load_universe_definition(universe_path)

    assert definition.version == 1
    assert definition.universe.name == "core_us_etfs"
    assert definition.eligibility.min_price == Decimal("20")
    assert definition.eligibility.min_average_daily_volume == 1_000_000
    assert set(definition.eligibility.excluded_flags) == {"leveraged", "inverse"}
    assert definition.instruments[0].symbol == "SPY"
    assert definition.instruments[0].flags == ()


def test_load_universe_definition_rejects_excluded_flags(tmp_path: Path) -> None:
    universe_path = tmp_path / "universe.yaml"
    universe_path.write_text(
        """
version: 1
as_of: 2026-04-20
universe:
  name: core_us_etfs
  venue: us_equities
  bar_frequency: daily
  regular_hours_only: true
eligibility:
  min_price: 20
  min_average_daily_volume: 1000000
  min_history_days: 252
  excluded_flags:
    - leveraged
    - inverse
instruments:
  - symbol: SSO
    name: ProShares Ultra S&P500
    category: broad_us_equity
    exchange: ARCA
    is_active: true
    flags:
      - leveraged
""".strip()
    )

    with pytest.raises(UniverseValidationError, match="SSO"):
        load_universe_definition(universe_path)
