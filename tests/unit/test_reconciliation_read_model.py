from __future__ import annotations

from datetime import date
from decimal import Decimal

from quant_core.broker import PaperBrokerOrderRequest, build_paper_broker_order_request
from quant_core.execution import (
    create_paper_execution_order,
    identify_trade_intents,
)
from quant_core.execution.intents import TradeIntent
from quant_core.reconciliation import (
    ReconciliationReadModelSummary,
    ReconciliationRow,
    ReconciliationRowStatus,
    build_reconciliation_read_model,
)


def test_build_reconciliation_read_model_surfaces_matched_missing_and_orphaned_rows() -> None:
    matched_order = create_paper_execution_order(intent=_identified_intent("SPY"))
    missing_order = create_paper_execution_order(intent=_identified_intent("BND"))
    orphan_request = PaperBrokerOrderRequest(
        internal_order_id="intent_orphan",
        idempotency_key="intent:orphan",
        symbol="GLD",
        side="SELL",
        delta_weight=Decimal("-0.100000"),
    )

    model = build_reconciliation_read_model(
        orders=(matched_order, missing_order),
        broker_requests=(
            build_paper_broker_order_request(order=matched_order),
            orphan_request,
        ),
    )

    assert model.rows == (
        ReconciliationRow(
            internal_order_id="intent_orphan",
            symbol="GLD",
            row_status=ReconciliationRowStatus.ORPHANED_BROKER_REQUEST,
        ),
        ReconciliationRow(
            internal_order_id=missing_order.intent.internal_order_id,
            symbol="BND",
            row_status=ReconciliationRowStatus.MISSING_BROKER_REQUEST,
        ),
        ReconciliationRow(
            internal_order_id=matched_order.intent.internal_order_id,
            symbol="SPY",
            row_status=ReconciliationRowStatus.MATCHED,
        ),
    )
    assert model.summary == ReconciliationReadModelSummary(
        total_rows=3,
        matched_rows=1,
        missing_broker_rows=1,
        orphaned_broker_rows=1,
    )


def test_build_reconciliation_read_model_handles_empty_inputs() -> None:
    model = build_reconciliation_read_model(orders=(), broker_requests=())

    assert model.rows == ()
    assert model.summary.total_rows == 0
    assert model.summary.matched_rows == 0
    assert model.summary.missing_broker_rows == 0
    assert model.summary.orphaned_broker_rows == 0


def _identified_intent(symbol: str):
    return identify_trade_intents(
        (
            TradeIntent(
                as_of=date(2026, 4, 22),
                symbol=symbol,
                side="BUY",
                current_weight=Decimal("0.000000"),
                target_weight=Decimal("0.250000"),
                delta_weight=Decimal("0.250000"),
            ),
        )
    )[0]
