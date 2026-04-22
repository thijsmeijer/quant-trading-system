from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from quant_core.portfolio import build_portfolio_target
from quant_core.risk import (
    RiskGateDecision,
    approve_portfolio_target,
    reject_portfolio_target,
)


def test_approve_portfolio_target_returns_decision_without_rejection_reasons() -> None:
    target = build_portfolio_target(
        as_of=date(2026, 4, 22),
        weights={"SPY": Decimal("1.000000")},
    )

    decision = approve_portfolio_target(target=target)

    assert decision == RiskGateDecision(
        approved=True,
        reasons=(),
        target=target,
    )


def test_reject_portfolio_target_requires_reasons() -> None:
    target = build_portfolio_target(
        as_of=date(2026, 4, 22),
        weights={"SPY": Decimal("1.000000")},
    )

    with pytest.raises(ValueError, match="at least one"):
        reject_portfolio_target(target=target, reasons=())


def test_reject_portfolio_target_normalizes_reasons() -> None:
    target = build_portfolio_target(
        as_of=date(2026, 4, 22),
        weights={"SPY": Decimal("1.000000")},
    )

    decision = reject_portfolio_target(
        target=target,
        reasons=(" stale data ", "max exposure exceeded"),
    )

    assert decision == RiskGateDecision(
        approved=False,
        reasons=("stale data", "max exposure exceeded"),
        target=target,
    )
