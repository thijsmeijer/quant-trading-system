"""Broker protocol and typed broker-facing records."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Protocol


class BrokerOrderStatus(StrEnum):
    """Normalized broker-order lifecycle states."""

    SUBMITTED = "submitted"
    CANCELED = "canceled"
    REJECTED = "rejected"
    FILLED = "filled"


@dataclass(frozen=True, slots=True)
class BrokerOrderRequest:
    """Canonical broker submit payload owned by execution."""

    internal_order_id: str
    idempotency_key: str
    symbol: str
    side: str
    quantity: Decimal
    notional: Decimal | None
    order_type: str
    time_in_force: str | None
    reference_price: Decimal | None = None


@dataclass(frozen=True, slots=True)
class BrokerOrder:
    """Canonical broker order state."""

    broker_order_id: str
    internal_order_id: str
    status: BrokerOrderStatus
    symbol: str
    side: str
    quantity: Decimal
    notional: Decimal | None
    submitted_at: datetime
    canceled_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class BrokerFill:
    """Canonical broker fill state."""

    broker_fill_id: str
    broker_order_id: str
    internal_order_id: str
    symbol: str
    quantity: Decimal
    price: Decimal
    notional: Decimal
    filled_at: datetime


@dataclass(frozen=True, slots=True)
class BrokerPosition:
    """Canonical broker position snapshot."""

    symbol: str
    quantity: Decimal
    market_value: Decimal


@dataclass(frozen=True, slots=True)
class BrokerAccount:
    """Canonical broker account snapshot."""

    cash: Decimal
    equity: Decimal
    buying_power: Decimal
    as_of: datetime


@dataclass(frozen=True, slots=True)
class BrokerSubmission:
    """Submit result including any immediate fill information."""

    order: BrokerOrder
    fills: tuple[BrokerFill, ...] = ()


class BrokerGateway(Protocol):
    """Protocol for broker adapters used by the OMS layer."""

    def submit_order(self, request: BrokerOrderRequest) -> BrokerSubmission:
        """Submit or idempotently replay one order request."""

    def cancel_order(self, broker_order_id: str) -> BrokerOrder:
        """Cancel one submitted broker order."""

    def list_orders(self) -> tuple[BrokerOrder, ...]:
        """List broker orders known to the adapter."""

    def list_fills(self) -> tuple[BrokerFill, ...]:
        """List broker fills known to the adapter."""

    def list_positions(self) -> tuple[BrokerPosition, ...]:
        """List broker positions known to the adapter."""

    def get_account(self) -> BrokerAccount:
        """Return the latest broker account snapshot."""
