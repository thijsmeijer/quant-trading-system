from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from quant_core.portfolio import (
    DuplicateTargetWeightError,
    PortfolioTarget,
    build_portfolio_target,
)


def test_build_portfolio_target_normalizes_symbols_and_exposes_weights() -> None:
    target = build_portfolio_target(
        as_of=date(2026, 4, 22),
        weights={
            "spy": Decimal("0.600000"),
            "BND": Decimal("0.400000"),
        },
    )

    assert target == PortfolioTarget(
        as_of=date(2026, 4, 22),
        weights=(
            ("BND", Decimal("0.400000")),
            ("SPY", Decimal("0.600000")),
        ),
    )
    assert target.weights_by_symbol() == {
        "BND": Decimal("0.400000"),
        "SPY": Decimal("0.600000"),
    }
    assert target.gross_exposure == Decimal("1.000000")


def test_build_portfolio_target_rejects_negative_weight() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        build_portfolio_target(
            as_of=date(2026, 4, 22),
            weights={"SPY": Decimal("-0.100000")},
        )


def test_build_portfolio_target_rejects_duplicate_symbols_after_normalization() -> None:
    with pytest.raises(DuplicateTargetWeightError, match="SPY"):
        build_portfolio_target(
            as_of=date(2026, 4, 22),
            weights=[
                ("SPY", Decimal("0.500000")),
                ("spy", Decimal("0.500000")),
            ],
        )
