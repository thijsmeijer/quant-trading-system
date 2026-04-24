from __future__ import annotations

from decimal import Decimal

from quant_core.execution import build_execution_order_candidates
from quant_core.portfolio import PortfolioTargetPosition


def test_build_execution_order_candidates_uses_target_minus_current_quantities() -> None:
    candidates = build_execution_order_candidates(
        strategy_run_id=42,
        target_positions=(
            PortfolioTargetPosition(
                allocation_key="SPY",
                target_weight=Decimal("0.600000"),
                target_notional=Decimal("60000.000000"),
                target_quantity=Decimal("30.000000"),
                reference_price=Decimal("500.000000"),
                symbol="SPY",
            ),
            PortfolioTargetPosition(
                allocation_key="BND",
                target_weight=Decimal("0.200000"),
                target_notional=Decimal("20000.000000"),
                target_quantity=Decimal("10.000000"),
                reference_price=Decimal("100.000000"),
                symbol="BND",
            ),
        ),
        current_positions=(
            ("SPY", Decimal("20.000000")),
            ("GLD", Decimal("5.000000")),
        ),
    )

    assert [(candidate.symbol, candidate.side, candidate.quantity) for candidate in candidates] == [
        ("BND", "BUY", Decimal("10.000000")),
        ("GLD", "SELL", Decimal("5.000000")),
        ("SPY", "BUY", Decimal("10.000000")),
    ]


def test_build_execution_order_candidates_prices_full_exits_from_current_market_value() -> None:
    candidates = build_execution_order_candidates(
        strategy_run_id=42,
        target_positions=(),
        current_positions=(("SHY", Decimal("404.137973")),),
        current_market_values={"SHY": Decimal("33333.300000")},
    )

    assert len(candidates) == 1
    assert candidates[0].symbol == "SHY"
    assert candidates[0].side == "SELL"
    assert candidates[0].quantity == Decimal("404.137973")
    assert candidates[0].reference_price == Decimal("82.480000")
    assert candidates[0].notional == Decimal("33333.300013")
