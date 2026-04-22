from __future__ import annotations

from decimal import Decimal

from quant_core.broker import (
    BrokerOrderRequest,
    BrokerOrderStatus,
    FakeBrokerGateway,
)


def test_fake_broker_submit_is_idempotent_and_can_auto_fill() -> None:
    broker = FakeBrokerGateway(
        auto_fill=True,
        fill_price_by_symbol={"SPY": Decimal("500.000000")},
    )
    request = BrokerOrderRequest(
        internal_order_id="order_1",
        idempotency_key="order:key-1",
        symbol="SPY",
        side="BUY",
        quantity=Decimal("10.000000"),
        notional=Decimal("5000.000000"),
        order_type="market",
        time_in_force="day",
        reference_price=Decimal("500.000000"),
    )

    first = broker.submit_order(request)
    second = broker.submit_order(request)

    assert first.order.broker_order_id == second.order.broker_order_id
    assert first.order.status is BrokerOrderStatus.FILLED
    assert len(broker.list_orders()) == 1
    assert len(broker.list_fills()) == 1
    assert broker.get_account().cash == Decimal("95000.000000")


def test_fake_broker_can_cancel_submitted_orders() -> None:
    broker = FakeBrokerGateway(auto_fill=False)
    request = BrokerOrderRequest(
        internal_order_id="order_2",
        idempotency_key="order:key-2",
        symbol="SPY",
        side="BUY",
        quantity=Decimal("1.000000"),
        notional=Decimal("500.000000"),
        order_type="market",
        time_in_force="day",
        reference_price=Decimal("500.000000"),
    )

    submission = broker.submit_order(request)
    canceled = broker.cancel_order(submission.order.broker_order_id)

    assert submission.order.status is BrokerOrderStatus.SUBMITTED
    assert canceled.status is BrokerOrderStatus.CANCELED
