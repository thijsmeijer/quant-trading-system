"""Pre-trade portfolio target gate decisions."""

from __future__ import annotations

from dataclasses import dataclass

from quant_core.portfolio import PortfolioTarget


@dataclass(frozen=True, slots=True)
class RiskGateDecision:
    """Result of evaluating whether a target may proceed downstream."""

    approved: bool
    reasons: tuple[str, ...]
    target: PortfolioTarget


def approve_portfolio_target(*, target: PortfolioTarget) -> RiskGateDecision:
    """Create an explicit approval result for a proposed portfolio target."""

    return RiskGateDecision(
        approved=True,
        reasons=(),
        target=target,
    )


def reject_portfolio_target(
    *,
    target: PortfolioTarget,
    reasons: tuple[str, ...],
) -> RiskGateDecision:
    """Create an explicit rejection result for a proposed portfolio target."""

    normalized_reasons = tuple(reason.strip() for reason in reasons if reason.strip())
    if not normalized_reasons:
        raise ValueError("rejected targets must include at least one reason")

    return RiskGateDecision(
        approved=False,
        reasons=normalized_reasons,
        target=target,
    )
