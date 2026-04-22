"""Basic portfolio target limit validation."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from quant_core.portfolio import PortfolioTarget
from quant_core.risk.gates import (
    RiskGateDecision,
    approve_portfolio_target,
    reject_portfolio_target,
)


@dataclass(frozen=True, slots=True)
class PortfolioRiskLimits:
    """Simple broker-agnostic limits for a proposed portfolio target."""

    max_gross_exposure: Decimal
    max_single_weight: Decimal


def validate_portfolio_target_limits(
    *,
    target: PortfolioTarget,
    limits: PortfolioRiskLimits,
) -> RiskGateDecision:
    """Approve or reject a target against the first explicit portfolio limits."""

    reasons: list[str] = []

    if target.gross_exposure > limits.max_gross_exposure:
        reasons.append(f"gross exposure exceeds {limits.max_gross_exposure:.6f}")

    for symbol, weight in target.weights:
        if weight > limits.max_single_weight:
            reasons.append(f"{symbol} weight exceeds {limits.max_single_weight:.6f}")

    if not reasons:
        return approve_portfolio_target(target=target)

    return reject_portfolio_target(target=target, reasons=tuple(reasons))
