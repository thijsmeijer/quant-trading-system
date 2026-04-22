from __future__ import annotations

from datetime import date
from decimal import Decimal

from quant_core.execution import (
    PaperExecutionStatus,
    create_paper_execution_order,
    identify_trade_intents,
    transition_paper_execution_order,
)
from quant_core.execution.intents import TradeIntent
from quant_core.reporting import ExecutionReportSummary, build_execution_report_summary


def test_build_execution_report_summary_counts_orders_by_status() -> None:
    pending = create_paper_execution_order(intent=_identified_intent("SPY", "BUY"))
    submitted = transition_paper_execution_order(
        order=create_paper_execution_order(intent=_identified_intent("BND", "BUY")),
        new_status=PaperExecutionStatus.SUBMITTED,
    )
    filled = transition_paper_execution_order(
        order=submitted,
        new_status=PaperExecutionStatus.FILLED,
    )

    summary = build_execution_report_summary(
        orders=(pending, submitted, filled),
    )

    assert summary == ExecutionReportSummary(
        total_orders=3,
        pending_orders=1,
        submitted_orders=1,
        filled_orders=1,
        canceled_orders=0,
        rejected_orders=0,
    )


def test_build_execution_report_summary_handles_empty_input() -> None:
    summary = build_execution_report_summary(orders=())

    assert summary.total_orders == 0
    assert summary.pending_orders == 0
    assert summary.submitted_orders == 0
    assert summary.filled_orders == 0
    assert summary.canceled_orders == 0
    assert summary.rejected_orders == 0


def _identified_intent(symbol: str, side: str):
    target_weight = Decimal("0.750000") if side == "BUY" else Decimal("0.000000")
    delta_weight = Decimal("0.250000") if side == "BUY" else Decimal("-0.250000")

    return identify_trade_intents(
        (
            TradeIntent(
                as_of=date(2026, 4, 22),
                symbol=symbol,
                side=side,
                current_weight=Decimal("0.500000"),
                target_weight=target_weight,
                delta_weight=delta_weight,
            ),
        )
    )[0]
