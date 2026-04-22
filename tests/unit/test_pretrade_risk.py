from __future__ import annotations

from datetime import date
from decimal import Decimal

from quant_core.portfolio import PortfolioTargetPosition
from quant_core.risk import PreTradeRiskConfig, evaluate_pretrade_risk


def test_evaluate_pretrade_risk_approves_valid_target_positions() -> None:
    decision = evaluate_pretrade_risk(
        target_positions=[
            PortfolioTargetPosition(
                allocation_key="SPY",
                target_weight=Decimal("0.600000"),
                target_notional=Decimal("60000.000000"),
                target_quantity=Decimal("120.000000"),
                reference_price=Decimal("500.000000"),
                symbol="SPY",
            ),
            PortfolioTargetPosition(
                allocation_key="cash",
                target_weight=Decimal("0.400000"),
                target_notional=Decimal("40000.000000"),
                target_quantity=Decimal("0.000000"),
                reference_price=None,
                symbol=None,
            ),
        ],
        active_symbols={"SPY"},
        latest_price_date_by_symbol={"SPY": date(2026, 4, 22)},
        expected_price_date=date(2026, 4, 22),
        execution_session_is_open=True,
        kill_switch_active=False,
        open_order_count=0,
        config=PreTradeRiskConfig(
            max_gross_exposure=Decimal("1.000000"),
            max_position_notional=Decimal("100000.000000"),
            max_open_orders=2,
        ),
    )

    assert decision.approved is True
    assert decision.failed_reason_codes == ()
    assert all(check.status == "pass" for check in decision.checks)


def test_evaluate_pretrade_risk_collects_failures_across_controls() -> None:
    decision = evaluate_pretrade_risk(
        target_positions=[
            PortfolioTargetPosition(
                allocation_key="SPY",
                target_weight=Decimal("1.100000"),
                target_notional=Decimal("110000.000000"),
                target_quantity=Decimal("220.000000"),
                reference_price=Decimal("500.000000"),
                symbol="SPY",
            ),
            PortfolioTargetPosition(
                allocation_key="QQQ",
                target_weight=Decimal("0.200000"),
                target_notional=Decimal("20000.000000"),
                target_quantity=Decimal("50.000000"),
                reference_price=Decimal("400.000000"),
                symbol="QQQ",
            ),
        ],
        active_symbols={"SPY"},
        latest_price_date_by_symbol={
            "SPY": date(2026, 4, 21),
            "QQQ": date(2026, 4, 20),
        },
        expected_price_date=date(2026, 4, 22),
        execution_session_is_open=False,
        kill_switch_active=True,
        open_order_count=5,
        config=PreTradeRiskConfig(
            max_gross_exposure=Decimal("1.000000"),
            max_position_notional=Decimal("100000.000000"),
            max_open_orders=2,
        ),
    )

    assert decision.approved is False
    assert decision.failed_reason_codes == (
        "inactive_symbol",
        "gross_exposure_limit",
        "position_notional_limit",
        "stale_data",
        "invalid_session",
        "kill_switch_active",
        "open_order_limit",
    )
