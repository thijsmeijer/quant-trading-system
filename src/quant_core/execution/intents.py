"""Internal broker-agnostic trade intents."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from typing import Final, Literal

from quant_core.risk import RiskGateDecision

WEIGHT_PRECISION: Final = Decimal("0.000001")


class RejectedTargetForExecutionError(ValueError):
    """Raised when execution receives a target that failed risk approval."""


@dataclass(frozen=True, slots=True)
class TradeIntent:
    """Internal trading intent derived from target-versus-current state."""

    as_of: date
    symbol: str
    side: Literal["BUY", "SELL"]
    current_weight: Decimal
    target_weight: Decimal
    delta_weight: Decimal


def build_order_intents(
    *,
    decision: RiskGateDecision,
    current_weights: Mapping[str, Decimal],
) -> tuple[TradeIntent, ...]:
    """Convert an approved target into internal buy and sell intents."""

    if not decision.approved:
        raise RejectedTargetForExecutionError("order intents require an approved target")

    target_weights = decision.target.weights_by_symbol()
    current_by_symbol = {
        symbol.strip().upper(): weight.quantize(WEIGHT_PRECISION, rounding=ROUND_HALF_UP)
        for symbol, weight in current_weights.items()
    }

    intents: list[TradeIntent] = []
    for symbol in sorted(set(target_weights) | set(current_by_symbol)):
        current_weight = current_by_symbol.get(symbol, Decimal("0.000000"))
        target_weight = target_weights.get(symbol, Decimal("0.000000"))
        delta_weight = (target_weight - current_weight).quantize(
            WEIGHT_PRECISION,
            rounding=ROUND_HALF_UP,
        )
        if delta_weight == Decimal("0.000000"):
            continue

        intents.append(
            TradeIntent(
                as_of=decision.target.as_of,
                symbol=symbol,
                side="BUY" if delta_weight > Decimal("0") else "SELL",
                current_weight=current_weight,
                target_weight=target_weight,
                delta_weight=delta_weight,
            )
        )

    return tuple(intents)
