"""Trading-friction configuration for research and backtest runs."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field

FOUR_DP = Decimal("0.0001")


class BacktestFrictionConfig(BaseModel):
    """Serializable one-way friction assumptions for research runs."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    commission_bps: Decimal = Field(ge=0)
    slippage_bps: Decimal = Field(ge=0)
    management_fee_bps: Decimal = Field(ge=0)

    @property
    def one_way_cost_bps(self) -> Decimal:
        return (self.commission_bps + self.slippage_bps).quantize(FOUR_DP)

    @classmethod
    def baseline(cls) -> BacktestFrictionConfig:
        return cls(
            commission_bps=Decimal("0.0000"),
            slippage_bps=Decimal("1.0000"),
            management_fee_bps=Decimal("0.0000"),
        )
