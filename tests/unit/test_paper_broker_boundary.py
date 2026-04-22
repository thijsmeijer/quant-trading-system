from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from quant_core.broker import (
    InvalidPaperBrokerRequestError,
    PaperBrokerOrderRequest,
    build_paper_broker_order_request,
)
from quant_core.execution import (
    PaperExecutionStatus,
    create_paper_execution_order,
    identify_trade_intents,
)
from quant_core.execution.intents import TradeIntent


def test_build_paper_broker_order_request_translates_pending_order() -> None:
    order = create_paper_execution_order(intent=_identified_intent())

    request = build_paper_broker_order_request(order=order)

    assert request == PaperBrokerOrderRequest(
        internal_order_id=order.intent.internal_order_id,
        idempotency_key=order.intent.idempotency_key,
        symbol="SPY",
        side="BUY",
        delta_weight=Decimal("0.250000"),
    )


def test_build_paper_broker_order_request_rejects_non_pending_order() -> None:
    order = create_paper_execution_order(intent=_identified_intent())
    submitted = order.__class__(intent=order.intent, status=PaperExecutionStatus.SUBMITTED)

    with pytest.raises(InvalidPaperBrokerRequestError, match="PENDING"):
        build_paper_broker_order_request(order=submitted)


def _identified_intent():
    return identify_trade_intents(
        (
            TradeIntent(
                as_of=date(2026, 4, 22),
                symbol="SPY",
                side="BUY",
                current_weight=Decimal("0.500000"),
                target_weight=Decimal("0.750000"),
                delta_weight=Decimal("0.250000"),
            ),
        )
    )[0]
