from __future__ import annotations

from datetime import date
from decimal import Decimal

from quant_core.portfolio import build_portfolio_target, normalize_portfolio_target
from quant_core.risk import PortfolioRiskLimits, validate_portfolio_target_limits


def test_validate_portfolio_target_limits_approves_target_inside_limits() -> None:
    target = normalize_portfolio_target(
        build_portfolio_target(
            as_of=date(2026, 4, 22),
            weights={
                "SPY": Decimal("3.000000"),
                "BND": Decimal("2.000000"),
            },
        )
    )

    decision = validate_portfolio_target_limits(
        target=target,
        limits=PortfolioRiskLimits(
            max_gross_exposure=Decimal("1.000000"),
            max_single_weight=Decimal("0.700000"),
        ),
    )

    assert decision.approved is True
    assert decision.reasons == ()


def test_validate_portfolio_target_limits_rejects_over_exposed_target() -> None:
    target = build_portfolio_target(
        as_of=date(2026, 4, 22),
        weights={
            "SPY": Decimal("0.800000"),
            "BND": Decimal("0.400000"),
        },
    )

    decision = validate_portfolio_target_limits(
        target=target,
        limits=PortfolioRiskLimits(
            max_gross_exposure=Decimal("1.000000"),
            max_single_weight=Decimal("0.900000"),
        ),
    )

    assert decision.approved is False
    assert decision.reasons == ("gross exposure exceeds 1.000000",)


def test_validate_portfolio_target_limits_rejects_over_concentrated_target() -> None:
    target = normalize_portfolio_target(
        build_portfolio_target(
            as_of=date(2026, 4, 22),
            weights={
                "SPY": Decimal("4.000000"),
                "BND": Decimal("1.000000"),
            },
        )
    )

    decision = validate_portfolio_target_limits(
        target=target,
        limits=PortfolioRiskLimits(
            max_gross_exposure=Decimal("1.000000"),
            max_single_weight=Decimal("0.700000"),
        ),
    )

    assert decision.approved is False
    assert decision.reasons == ("SPY weight exceeds 0.700000",)


def test_validate_portfolio_target_limits_accumulates_multiple_reasons() -> None:
    target = build_portfolio_target(
        as_of=date(2026, 4, 22),
        weights={
            "SPY": Decimal("0.900000"),
            "BND": Decimal("0.400000"),
        },
    )

    decision = validate_portfolio_target_limits(
        target=target,
        limits=PortfolioRiskLimits(
            max_gross_exposure=Decimal("1.000000"),
            max_single_weight=Decimal("0.700000"),
        ),
    )

    assert decision.approved is False
    assert decision.reasons == (
        "gross exposure exceeds 1.000000",
        "SPY weight exceeds 0.700000",
    )
