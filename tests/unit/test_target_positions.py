from __future__ import annotations

from decimal import Decimal

import pytest

from quant_core.portfolio import (
    MissingPortfolioPriceError,
    PortfolioTargetAllocation,
    build_target_positions,
)


def test_build_target_positions_sizes_symbol_and_cash_allocations() -> None:
    positions = build_target_positions(
        allocations=[
            PortfolioTargetAllocation(
                allocation_key="SPY",
                target_weight=Decimal("0.600000"),
                symbol="SPY",
            ),
            PortfolioTargetAllocation(
                allocation_key="cash",
                target_weight=Decimal("0.400000"),
                symbol=None,
            ),
        ],
        account_equity=Decimal("100000.000000"),
        price_by_symbol={"SPY": Decimal("500.000000")},
    )

    assert positions[0].allocation_key == "SPY"
    assert positions[0].target_notional == Decimal("60000.000000")
    assert positions[0].target_quantity == Decimal("120.000000")
    assert positions[0].reference_price == Decimal("500.000000")
    assert positions[1].allocation_key == "cash"
    assert positions[1].target_notional == Decimal("40000.000000")
    assert positions[1].target_quantity == Decimal("0.000000")
    assert positions[1].reference_price is None


def test_build_target_positions_requires_prices_for_symbol_allocations() -> None:
    with pytest.raises(MissingPortfolioPriceError, match="Missing canonical reference price"):
        build_target_positions(
            allocations=[
                PortfolioTargetAllocation(
                    allocation_key="SPY",
                    target_weight=Decimal("1.000000"),
                    symbol="SPY",
                )
            ],
            account_equity=Decimal("100000.000000"),
            price_by_symbol={},
        )
