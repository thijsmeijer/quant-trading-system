"""Paper-only broker adapter boundary."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from quant_core.execution import PaperExecutionOrder, PaperExecutionStatus


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


def build_paper_broker_order_request(
    *,
    order: PaperExecutionOrder,
) -> PaperBrokerOrderRequest:
    """Translate a pending paper execution order into the paper broker boundary."""

    if order.status is not PaperExecutionStatus.PENDING:
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
