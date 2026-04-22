"""Paper-only broker adapter boundary."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

from quant_core.broker.base import (
    BrokerAccount,
    BrokerFill,
    BrokerGateway,
    BrokerOrder,
    BrokerOrderRequest,
    BrokerPosition,
    BrokerSubmission,
)
from quant_core.broker.fake import FakeBrokerGateway

if TYPE_CHECKING:
    from quant_core.execution.paper import PaperExecutionOrder


class InvalidPaperBrokerRequestError(ValueError):
    """Raised when a paper execution order cannot be submitted to the adapter boundary."""


@dataclass(frozen=True, slots=True)
class PaperBrokerOrderRequest:
    """Paper-only adapter request derived from an internal execution order."""

    internal_order_id: str
    idempotency_key: str
    symbol: str
    side: str
    delta_weight: Decimal


@dataclass(slots=True)
class PaperBrokerAdapter(BrokerGateway):
    """Paper-mode broker adapter backed by the deterministic fake broker."""

    gateway: FakeBrokerGateway

    def submit_order(self, request: BrokerOrderRequest) -> BrokerSubmission:
        return self.gateway.submit_order(request)

    def cancel_order(self, broker_order_id: str) -> BrokerOrder:
        return self.gateway.cancel_order(broker_order_id)

    def list_orders(self) -> tuple[BrokerOrder, ...]:
        return self.gateway.list_orders()

    def list_fills(self) -> tuple[BrokerFill, ...]:
        return self.gateway.list_fills()

    def list_positions(self) -> tuple[BrokerPosition, ...]:
        return self.gateway.list_positions()

    def get_account(self) -> BrokerAccount:
        return self.gateway.get_account()


def build_paper_broker_order_request(
    *,
    order: PaperExecutionOrder,
) -> PaperBrokerOrderRequest:
    """Translate a pending paper execution order into the paper broker boundary."""

    if order.status.value != "pending":
        raise InvalidPaperBrokerRequestError(
            "paper broker requests require a PENDING execution order"
        )

    intent = order.intent.intent
    return PaperBrokerOrderRequest(
        internal_order_id=order.intent.internal_order_id,
        idempotency_key=order.intent.idempotency_key,
        symbol=intent.symbol,
        side=intent.side,
        delta_weight=intent.delta_weight,
    )
