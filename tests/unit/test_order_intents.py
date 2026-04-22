from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from quant_core.execution import (
    RejectedTargetForExecutionError,
    TradeIntent,
    build_order_intents,
)
from quant_core.portfolio import build_portfolio_target, normalize_portfolio_target
from quant_core.risk import approve_portfolio_target, reject_portfolio_target


def test_build_order_intents_derives_buy_sell_and_close_from_approved_target() -> None:
    target = normalize_portfolio_target(
        build_portfolio_target(
            as_of=date(2026, 4, 22),
            weights={
                "SPY": Decimal("3.000000"),
                "BND": Decimal("1.000000"),
            },
        )
    )
    decision = approve_portfolio_target(target=target)

    intents = build_order_intents(
        decision=decision,
        current_weights={
            "SPY": Decimal("0.500000"),
            "GLD": Decimal("0.100000"),
        },
    )

    assert intents == (
        TradeIntent(
            as_of=date(2026, 4, 22),
            symbol="BND",
            side="BUY",
            current_weight=Decimal("0.000000"),
            target_weight=Decimal("0.250000"),
            delta_weight=Decimal("0.250000"),
        ),
        TradeIntent(
            as_of=date(2026, 4, 22),
            symbol="GLD",
            side="SELL",
            current_weight=Decimal("0.100000"),
            target_weight=Decimal("0.000000"),
            delta_weight=Decimal("-0.100000"),
        ),
        TradeIntent(
            as_of=date(2026, 4, 22),
            symbol="SPY",
            side="BUY",
            current_weight=Decimal("0.500000"),
            target_weight=Decimal("0.750000"),
            delta_weight=Decimal("0.250000"),
        ),
    )


def test_build_order_intents_skips_zero_delta_rows() -> None:
    target = normalize_portfolio_target(
        build_portfolio_target(
            as_of=date(2026, 4, 22),
            weights={"SPY": Decimal("1.000000")},
        )
    )
    decision = approve_portfolio_target(target=target)

    intents = build_order_intents(
        decision=decision,
        current_weights={"SPY": Decimal("1.000000")},
    )

    assert intents == ()


def test_build_order_intents_rejects_unapproved_target() -> None:
    target = normalize_portfolio_target(
        build_portfolio_target(
            as_of=date(2026, 4, 22),
            weights={"SPY": Decimal("1.000000")},
        )
    )
    decision = reject_portfolio_target(
        target=target,
        reasons=("stale data",),
    )

    with pytest.raises(RejectedTargetForExecutionError, match="approved"):
        build_order_intents(
            decision=decision,
            current_weights={},
        )
