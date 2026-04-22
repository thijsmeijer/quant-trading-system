from __future__ import annotations

from decimal import Decimal

import pytest

from quant_core.backtest.friction import BacktestFrictionConfig


def test_baseline_friction_config_is_serializable_and_sums_one_way_cost() -> None:
    config = BacktestFrictionConfig.baseline()

    assert config.one_way_cost_bps == Decimal("1.0000")
    assert config.model_dump(mode="json") == {
        "commission_bps": "0.0000",
        "slippage_bps": "1.0000",
        "management_fee_bps": "0.0000",
    }


def test_friction_config_rejects_negative_values() -> None:
    with pytest.raises(ValueError, match="greater than or equal"):
        BacktestFrictionConfig(
            commission_bps=Decimal("-0.1000"),
            slippage_bps=Decimal("1.0000"),
            management_fee_bps=Decimal("0.0000"),
        )
