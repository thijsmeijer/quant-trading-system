"""Deterministic fake broker for OMS and broker-sync tests."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal

from quant_core.broker.base import (
    BrokerAccount,
    BrokerFill,
    BrokerGateway,
    BrokerOrder,
    BrokerOrderRequest,
    BrokerOrderStatus,
    BrokerPosition,
    BrokerSubmission,
)


@dataclass(slots=True)
class FakeBrokerGateway(BrokerGateway):
    """In-memory broker with idempotent submit semantics."""

    auto_fill: bool = False
    rejection_symbols: set[str] = field(default_factory=set)
    fill_price_by_symbol: dict[str, Decimal] = field(default_factory=dict)
    starting_cash: Decimal = Decimal("100000.000000")

    def __post_init__(self) -> None:
        self._order_count = 0
        self._fill_count = 0
        self._orders_by_idempotency: dict[str, BrokerSubmission] = {}
        self._orders_by_broker_id: dict[str, BrokerOrder] = {}
        self._fills_by_id: dict[str, BrokerFill] = {}
        self._positions: dict[str, Decimal] = {}
        self._cash = self.starting_cash
        self._updated_at = datetime.now(tz=UTC)

    def submit_order(self, request: BrokerOrderRequest) -> BrokerSubmission:
        if request.idempotency_key in self._orders_by_idempotency:
            return self._orders_by_idempotency[request.idempotency_key]

        self._order_count += 1
        broker_order_id = f"fake_order_{self._order_count:04d}"
        submitted_at = datetime.now(tz=UTC)

        if request.symbol in self.rejection_symbols:
            order = BrokerOrder(
                broker_order_id=broker_order_id,
                internal_order_id=request.internal_order_id,
                status=BrokerOrderStatus.REJECTED,
                symbol=request.symbol,
                side=request.side,
                quantity=request.quantity,
                notional=request.notional,
                submitted_at=submitted_at,
            )
            submission = BrokerSubmission(order=order)
        else:
            status = BrokerOrderStatus.FILLED if self.auto_fill else BrokerOrderStatus.SUBMITTED
            order = BrokerOrder(
                broker_order_id=broker_order_id,
                internal_order_id=request.internal_order_id,
                status=status,
                symbol=request.symbol,
                side=request.side,
                quantity=request.quantity,
                notional=request.notional,
                submitted_at=submitted_at,
            )
            fills = self._fills_for_order(request, order) if self.auto_fill else ()
            submission = BrokerSubmission(order=order, fills=fills)

        self._orders_by_idempotency[request.idempotency_key] = submission
        self._orders_by_broker_id[submission.order.broker_order_id] = submission.order
        self._updated_at = submitted_at
        return submission

    def cancel_order(self, broker_order_id: str) -> BrokerOrder:
        order = self._orders_by_broker_id[broker_order_id]
        if order.status in {BrokerOrderStatus.CANCELED, BrokerOrderStatus.FILLED}:
            return order

        canceled_order = BrokerOrder(
            broker_order_id=order.broker_order_id,
            internal_order_id=order.internal_order_id,
            status=BrokerOrderStatus.CANCELED,
            symbol=order.symbol,
            side=order.side,
            quantity=order.quantity,
            notional=order.notional,
            submitted_at=order.submitted_at,
            canceled_at=datetime.now(tz=UTC),
        )
        self._orders_by_broker_id[broker_order_id] = canceled_order
        self._updated_at = canceled_order.canceled_at or self._updated_at
        return canceled_order

    def list_orders(self) -> tuple[BrokerOrder, ...]:
        return tuple(
            sorted(self._orders_by_broker_id.values(), key=lambda item: item.broker_order_id)
        )

    def list_fills(self) -> tuple[BrokerFill, ...]:
        return tuple(sorted(self._fills_by_id.values(), key=lambda item: item.broker_fill_id))

    def list_positions(self) -> tuple[BrokerPosition, ...]:
        return tuple(
            BrokerPosition(
                symbol=symbol,
                quantity=quantity,
                market_value=quantity * self.fill_price_by_symbol.get(symbol, Decimal("0")),
            )
            for symbol, quantity in sorted(self._positions.items())
            if quantity != Decimal("0")
        )

    def get_account(self) -> BrokerAccount:
        market_value = sum(
            (position.market_value for position in self.list_positions()),
            start=Decimal("0"),
        )
        equity = (self._cash + market_value).quantize(Decimal("0.000001"))
        return BrokerAccount(
            cash=self._cash.quantize(Decimal("0.000001")),
            equity=equity,
            buying_power=self._cash.quantize(Decimal("0.000001")),
            as_of=self._updated_at,
        )

    def _fills_for_order(
        self,
        request: BrokerOrderRequest,
        order: BrokerOrder,
    ) -> tuple[BrokerFill, ...]:
        self._fill_count += 1
        fill_price = request.reference_price or self.fill_price_by_symbol.get(
            request.symbol,
            Decimal("1.000000"),
        )
        fill = BrokerFill(
            broker_fill_id=f"fake_fill_{self._fill_count:04d}",
            broker_order_id=order.broker_order_id,
            internal_order_id=order.internal_order_id,
            symbol=request.symbol,
            quantity=request.quantity,
            price=fill_price,
            notional=(request.quantity * fill_price).quantize(Decimal("0.000001")),
            filled_at=order.submitted_at,
        )
        self._fills_by_id[fill.broker_fill_id] = fill

        signed_quantity = request.quantity if request.side == "BUY" else -request.quantity
        self._positions[request.symbol] = (
            self._positions.get(request.symbol, Decimal("0")) + signed_quantity
        ).quantize(Decimal("0.000001"))
        cash_delta = fill.notional if request.side == "SELL" else -fill.notional
        self._cash = (self._cash + cash_delta).quantize(Decimal("0.000001"))
        return (fill,)
