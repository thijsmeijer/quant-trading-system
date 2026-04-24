"""OMS and broker-sync seam over persisted target positions."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from hashlib import sha256
from typing import Final, Literal

from sqlalchemy.orm import Session

from quant_core.broker import (
    BrokerGateway,
    BrokerOrderRequest,
    BrokerOrderStatus,
)
from quant_core.data import (
    FillWrite,
    OperationalRunMode,
    OrderCreate,
    OrderEventWrite,
    OrderRepository,
    SnapshotRepository,
    StoredOrder,
    StrategyRunRepository,
)
from quant_core.portfolio import PortfolioTargetPosition

ORDER_PRECISION: Final = Decimal("0.000001")


class RiskChecksFailedForExecutionError(ValueError):
    """Raised when OMS work is requested without a clean persisted risk decision."""


@dataclass(frozen=True, slots=True)
class ExecutionOrderCandidate:
    """One order candidate derived from target-versus-current quantity state."""

    internal_order_id: str
    idempotency_key: str
    symbol: str
    side: Literal["BUY", "SELL"]
    quantity: Decimal
    notional: Decimal | None
    reference_price: Decimal | None


def build_execution_order_candidates(
    *,
    strategy_run_id: int,
    target_positions: tuple[PortfolioTargetPosition, ...],
    current_positions: tuple[tuple[str, Decimal], ...],
    current_market_values: Mapping[str, Decimal] | None = None,
) -> tuple[ExecutionOrderCandidate, ...]:
    """Build deterministic OMS candidates from target and current quantities."""

    target_by_symbol = {
        position.symbol: position for position in target_positions if position.symbol is not None
    }
    current_by_symbol = {
        symbol: quantity.quantize(ORDER_PRECISION) for symbol, quantity in current_positions
    }
    current_market_values = current_market_values or {}

    candidates: list[ExecutionOrderCandidate] = []
    for symbol in sorted(set(target_by_symbol) | set(current_by_symbol)):
        target_position = target_by_symbol.get(symbol)
        target_quantity = (
            target_position.target_quantity if target_position is not None else Decimal("0")
        )
        current_quantity = current_by_symbol.get(symbol, Decimal("0"))
        delta_quantity = (target_quantity - current_quantity).quantize(ORDER_PRECISION)
        if delta_quantity == Decimal("0"):
            continue

        side: Literal["BUY", "SELL"] = "BUY" if delta_quantity > Decimal("0") else "SELL"
        quantity = abs(delta_quantity)
        reference_price = target_position.reference_price if target_position is not None else None
        if reference_price is None and current_quantity != Decimal("0"):
            current_market_value = current_market_values.get(symbol)
            if current_market_value is not None:
                reference_price = (abs(current_market_value) / abs(current_quantity)).quantize(
                    ORDER_PRECISION
                )
        notional = None
        if reference_price is not None:
            notional = (
                target_position.target_notional
                if target_position is not None and current_quantity == Decimal("0")
                else (quantity * reference_price).quantize(ORDER_PRECISION)
            )
        digest = sha256(
            "|".join(
                [
                    str(strategy_run_id),
                    symbol,
                    side,
                    f"{quantity:.6f}",
                    f"{reference_price:.6f}" if reference_price is not None else "none",
                ]
            ).encode("ascii")
        ).hexdigest()
        candidates.append(
            ExecutionOrderCandidate(
                internal_order_id=f"order_{digest[:16]}",
                idempotency_key=f"order:{digest}",
                symbol=symbol,
                side=side,
                quantity=quantity,
                notional=notional,
                reference_price=reference_price,
            )
        )

    return tuple(candidates)


class OrderManagementService:
    """Persist internal orders, submit them to a broker, and sync broker state."""

    def __init__(
        self,
        *,
        broker: BrokerGateway,
        strategy_repository: StrategyRunRepository | None = None,
        snapshot_repository: SnapshotRepository | None = None,
        order_repository: OrderRepository | None = None,
    ) -> None:
        self._broker = broker
        self._strategy_repository = strategy_repository or StrategyRunRepository()
        self._snapshot_repository = snapshot_repository or SnapshotRepository()
        self._order_repository = order_repository or OrderRepository()

    def create_orders_for_strategy_run(
        self,
        session: Session,
        *,
        strategy_run_id: int,
        run_mode: OperationalRunMode,
        created_at: datetime,
        current_market_values: Mapping[str, Decimal] | None = None,
    ) -> tuple[StoredOrder, ...]:
        """Create internal orders from persisted target positions after risk approval."""

        self._require_approved_risk_checks(session, strategy_run_id=strategy_run_id)
        stored_positions = self._strategy_repository.list_target_positions(
            session,
            strategy_run_id=strategy_run_id,
        )
        current_positions = tuple(
            (position.symbol, position.quantity)
            for position in self._snapshot_repository.latest_positions(session, run_mode=run_mode)
        )
        if current_market_values is None:
            current_market_values = {
                position.symbol: position.market_value
                for position in self._snapshot_repository.latest_positions(
                    session,
                    run_mode=run_mode,
                )
            }
        candidates = build_execution_order_candidates(
            strategy_run_id=strategy_run_id,
            target_positions=tuple(
                PortfolioTargetPosition(
                    allocation_key=position.allocation_key,
                    target_weight=position.target_weight,
                    target_notional=position.target_notional,
                    target_quantity=position.target_quantity,
                    reference_price=position.reference_price,
                    symbol=position.symbol,
                )
                for position in stored_positions
            ),
            current_positions=current_positions,
            current_market_values=current_market_values,
        )

        created_orders = tuple(
            self._order_repository.create_order(
                session,
                OrderCreate(
                    internal_order_id=candidate.internal_order_id,
                    strategy_run_id=strategy_run_id,
                    run_mode=run_mode,
                    symbol=candidate.symbol,
                    order_type="market",
                    side=candidate.side,
                    status="pending",
                    requested_quantity=candidate.quantity,
                    requested_notional=candidate.notional,
                    time_in_force="day",
                    created_at=created_at,
                ),
            )
            for candidate in candidates
        )
        self._order_repository.record_events(
            session,
            run_mode=run_mode,
            events=[
                OrderEventWrite(
                    internal_order_id=order.internal_order_id,
                    event_type="created",
                    event_at=created_at,
                    details={"status": "pending"},
                )
                for order in created_orders
            ],
        )
        return created_orders

    def submit_orders(
        self,
        session: Session,
        *,
        run_mode: OperationalRunMode,
        internal_order_ids: tuple[str, ...],
        submitted_at: datetime,
    ) -> tuple[StoredOrder, ...]:
        """Submit internal orders to the configured broker and persist lifecycle updates."""

        submitted_orders = []
        for internal_order_id in internal_order_ids:
            order = self._order_repository.get_order(
                session,
                run_mode=run_mode,
                internal_order_id=internal_order_id,
            )
            request = BrokerOrderRequest(
                internal_order_id=order.internal_order_id,
                idempotency_key=_idempotency_key(order),
                symbol=order.symbol or "",
                side=order.side,
                quantity=order.requested_quantity,
                notional=order.requested_notional,
                order_type=order.order_type,
                time_in_force=order.time_in_force,
                reference_price=_reference_price(order),
            )
            submission = self._broker.submit_order(request)
            updated_order = self._order_repository.update_order_status(
                session,
                run_mode=run_mode,
                internal_order_id=internal_order_id,
                status=submission.order.status.value,
                broker_order_id=submission.order.broker_order_id,
                submitted_at=submission.order.submitted_at,
            )
            event_types = ["submitted"]
            if submission.order.status is BrokerOrderStatus.REJECTED:
                event_types = ["rejected"]
            elif submission.order.status is BrokerOrderStatus.FILLED:
                event_types = ["submitted", "filled"]
            event_at = submission.order.canceled_at or submission.order.submitted_at

            self._order_repository.record_events(
                session,
                run_mode=run_mode,
                events=[
                    OrderEventWrite(
                        internal_order_id=internal_order_id,
                        event_type=event_type,
                        event_at=event_at,
                        details={"status": submission.order.status.value},
                    )
                    for event_type in event_types
                ],
            )
            if submission.fills:
                self._order_repository.record_fills(
                    session,
                    run_mode=run_mode,
                    fills=[
                        FillWrite(
                            internal_order_id=fill.internal_order_id,
                            broker_fill_id=fill.broker_fill_id,
                            fill_quantity=fill.quantity,
                            fill_price=fill.price,
                            fill_notional=fill.notional,
                            fill_at=fill.filled_at,
                        )
                        for fill in submission.fills
                    ],
                )
            submitted_orders.append(updated_order)

        return tuple(submitted_orders)

    def cancel_orders(
        self,
        session: Session,
        *,
        run_mode: OperationalRunMode,
        internal_order_ids: tuple[str, ...],
        canceled_at: datetime,
    ) -> tuple[StoredOrder, ...]:
        """Cancel broker orders and persist the canceled lifecycle state."""

        canceled_orders = []
        for internal_order_id in internal_order_ids:
            order = self._order_repository.get_order(
                session,
                run_mode=run_mode,
                internal_order_id=internal_order_id,
            )
            if order.broker_order_id is None:
                raise ValueError("cannot cancel an order without a broker_order_id")

            broker_order = self._broker.cancel_order(order.broker_order_id)
            canceled_order = self._order_repository.update_order_status(
                session,
                run_mode=run_mode,
                internal_order_id=internal_order_id,
                status=broker_order.status.value,
                canceled_at=canceled_at,
            )
            self._order_repository.record_events(
                session,
                run_mode=run_mode,
                events=[
                    OrderEventWrite(
                        internal_order_id=internal_order_id,
                        event_type="canceled",
                        event_at=canceled_at,
                        details={"status": broker_order.status.value},
                    )
                ],
            )
            canceled_orders.append(canceled_order)

        return tuple(canceled_orders)

    def sync_broker_state(
        self,
        session: Session,
        *,
        run_mode: OperationalRunMode,
    ) -> None:
        """Persist broker order and fill state idempotently."""

        for broker_order in self._broker.list_orders():
            try:
                self._order_repository.get_order(
                    session,
                    run_mode=run_mode,
                    internal_order_id=broker_order.internal_order_id,
                )
            except Exception:
                continue

            self._order_repository.update_order_status(
                session,
                run_mode=run_mode,
                internal_order_id=broker_order.internal_order_id,
                status=broker_order.status.value,
                broker_order_id=broker_order.broker_order_id,
                submitted_at=broker_order.submitted_at,
                canceled_at=broker_order.canceled_at,
            )
            event_type = broker_order.status.value
            self._order_repository.record_events(
                session,
                run_mode=run_mode,
                events=[
                    OrderEventWrite(
                        internal_order_id=broker_order.internal_order_id,
                        event_type=event_type,
                        event_at=broker_order.canceled_at or broker_order.submitted_at,
                        details={"status": broker_order.status.value},
                    )
                ],
            )

        fills = self._broker.list_fills()
        if fills:
            self._order_repository.record_fills(
                session,
                run_mode=run_mode,
                fills=[
                    FillWrite(
                        internal_order_id=fill.internal_order_id,
                        broker_fill_id=fill.broker_fill_id,
                        fill_quantity=fill.quantity,
                        fill_price=fill.price,
                        fill_notional=fill.notional,
                        fill_at=fill.filled_at,
                    )
                    for fill in fills
                ],
            )

    def _require_approved_risk_checks(self, session: Session, *, strategy_run_id: int) -> None:
        checks = self._strategy_repository.list_risk_checks(
            session,
            strategy_run_id=strategy_run_id,
        )
        if not checks:
            raise RiskChecksFailedForExecutionError("execution requires persisted risk checks")
        if any(check.status != "pass" for check in checks):
            raise RiskChecksFailedForExecutionError(
                "execution requires all persisted risk checks to pass"
            )


def _reference_price(order: StoredOrder) -> Decimal | None:
    if order.requested_notional is None or order.requested_quantity == Decimal("0"):
        return None
    return (order.requested_notional / order.requested_quantity).quantize(ORDER_PRECISION)


def _idempotency_key(order: StoredOrder) -> str:
    digest = sha256(
        "|".join(
            [
                order.internal_order_id,
                order.symbol or "",
                order.side,
                f"{order.requested_quantity:.6f}",
                f"{order.requested_notional:.6f}"
                if order.requested_notional is not None
                else "none",
            ]
        ).encode("ascii")
    ).hexdigest()
    return f"order:{digest}"
