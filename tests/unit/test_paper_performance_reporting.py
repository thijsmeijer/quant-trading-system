from __future__ import annotations

from datetime import date
from decimal import Decimal

from quant_core.reporting.paper_performance import (
    BenchmarkPrice,
    calculate_period_return,
    calculate_turnover_ratio,
    calculate_weight,
    calculate_weighted_benchmark_return,
)


def test_calculate_period_return_uses_starting_value() -> None:
    assert calculate_period_return(Decimal("100.000000"), Decimal("112.500000")) == Decimal(
        "0.125000"
    )


def test_calculate_period_return_returns_none_for_invalid_start() -> None:
    assert calculate_period_return(Decimal("0.000000"), Decimal("112.500000")) is None


def test_calculate_turnover_ratio_uses_latest_equity() -> None:
    assert calculate_turnover_ratio(
        turnover_notional=Decimal("25000.000000"),
        equity=Decimal("100000.000000"),
    ) == Decimal("0.250000")


def test_calculate_weight_uses_latest_equity() -> None:
    assert calculate_weight(
        market_value=Decimal("33333.300000"),
        equity=Decimal("100000.000000"),
    ) == Decimal("0.333333")


def test_calculate_weighted_benchmark_return_combines_symbol_returns() -> None:
    benchmark_return = calculate_weighted_benchmark_return(
        name="sixty_forty_spy_bnd",
        start_date=date(2026, 4, 20),
        end_date=date(2026, 4, 23),
        prices={
            "SPY": BenchmarkPrice(
                symbol="SPY",
                start_date=date(2026, 4, 20),
                start_price=Decimal("500.000000"),
                end_date=date(2026, 4, 23),
                end_price=Decimal("525.000000"),
            ),
            "BND": BenchmarkPrice(
                symbol="BND",
                start_date=date(2026, 4, 20),
                start_price=Decimal("75.000000"),
                end_date=date(2026, 4, 23),
                end_price=Decimal("74.250000"),
            ),
        },
        weights={"SPY": Decimal("0.600000"), "BND": Decimal("0.400000")},
    )

    assert benchmark_return.name == "sixty_forty_spy_bnd"
    assert benchmark_return.total_return == Decimal("0.026000")
    assert benchmark_return.missing_symbols == ()


def test_calculate_weighted_benchmark_return_reports_missing_prices() -> None:
    benchmark_return = calculate_weighted_benchmark_return(
        name="spy_buy_and_hold",
        start_date=date(2026, 4, 20),
        end_date=date(2026, 4, 23),
        prices={},
        weights={"SPY": Decimal("1.000000")},
    )

    assert benchmark_return.total_return is None
    assert benchmark_return.missing_symbols == ("SPY",)
