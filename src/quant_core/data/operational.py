"""Persistence repositories for operational platform state."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal, cast

from sqlalchemy import delete, func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from quant_core.data.models import (
    AccountSnapshot,
    Fill,
    Incident,
    Instrument,
    Order,
    OrderEvent,
    PnlSnapshot,
    Position,
    RiskCheck,
    RiskSnapshot,
    Signal,
    StrategyRun,
    TargetPosition,
    TargetWeight,
)

OperationalRunMode = Literal["dev", "paper", "live"]
RunMode = OperationalRunMode


class UnknownOperationalInstrumentError(ValueError):
    """Raised when operational writes reference unloaded ETF symbols."""


@dataclass(frozen=True, slots=True)
class StrategyRunCreate:
    """Input payload for a persisted strategy run."""

    run_mode: RunMode
    strategy_name: str
    config_version: str
    config_hash: str
    signal_date: date
    execution_date: date | None
    status: str
    started_at: datetime
    metadata_json: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class StoredStrategyRun:
    """Stored strategy-run metadata."""

    id: int
    run_mode: RunMode
    strategy_name: str
    config_version: str
    config_hash: str
    signal_date: date
    execution_date: date | None
    status: str
    started_at: datetime
    completed_at: datetime | None
    metadata_json: dict[str, Any] | None


@dataclass(frozen=True, slots=True)
class SignalWrite:
    """One signal row keyed by ETF symbol."""

    symbol: str
    signal_name: str
    rank: int | None
    score: Decimal | None
    is_selected: bool
    generated_at: datetime


@dataclass(frozen=True, slots=True)
class StoredSignal:
    """Persisted signal row with resolved instrument identity."""

    symbol: str
    signal_name: str
    rank: int | None
    score: Decimal | None
    is_selected: bool
    generated_at: datetime


@dataclass(frozen=True, slots=True)
class TargetWeightWrite:
    """One persisted target-weight row."""

    allocation_key: str
    target_weight: Decimal
    generated_at: datetime
    symbol: str | None = None


@dataclass(frozen=True, slots=True)
class StoredTargetWeight:
    """Persisted target-weight row with optional ETF symbol."""

    allocation_key: str
    target_weight: Decimal
    generated_at: datetime
    symbol: str | None


@dataclass(frozen=True, slots=True)
class TargetPositionWrite:
    """One persisted target-position row."""

    allocation_key: str
    target_weight: Decimal
    target_notional: Decimal
    target_quantity: Decimal
    generated_at: datetime
    symbol: str | None = None
    reference_price: Decimal | None = None


@dataclass(frozen=True, slots=True)
class StoredTargetPosition:
    """Persisted target-position row with optional ETF symbol."""

    allocation_key: str
    target_weight: Decimal
    target_notional: Decimal
    target_quantity: Decimal
    reference_price: Decimal | None
    generated_at: datetime
    symbol: str | None


@dataclass(frozen=True, slots=True)
class RiskCheckWrite:
    """One persisted risk-check decision."""

    check_scope: str
    check_name: str
    status: str
    checked_at: datetime
    reason_code: str | None = None
    details: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class StoredRiskCheck:
    """Persisted risk-check decision."""

    check_scope: str
    check_name: str
    status: str
    reason_code: str | None
    checked_at: datetime
    details: dict[str, Any] | None


@dataclass(frozen=True, slots=True)
class OrderCreate:
    """Input payload for one persisted internal order."""

    internal_order_id: str
    run_mode: RunMode
    order_type: str
    side: str
    status: str
    requested_quantity: Decimal
    created_at: datetime
    strategy_run_id: int | None = None
    symbol: str | None = None
    requested_notional: Decimal | None = None
    time_in_force: str | None = None


@dataclass(frozen=True, slots=True)
class StoredOrder:
    """Stored internal order row."""

    id: int
    internal_order_id: str
    run_mode: RunMode
    order_type: str
    side: str
    status: str
    requested_quantity: Decimal
    requested_notional: Decimal | None
    time_in_force: str | None
    broker_order_id: str | None
    created_at: datetime
    submitted_at: datetime | None
    canceled_at: datetime | None
    strategy_run_id: int | None
    symbol: str | None


@dataclass(frozen=True, slots=True)
class OrderEventWrite:
    """One persisted order event."""

    internal_order_id: str
    event_type: str
    event_at: datetime
    broker_event_id: str | None = None
    details: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class StoredOrderEvent:
    """Stored order-event row."""

    internal_order_id: str
    event_type: str
    event_at: datetime
    broker_event_id: str | None
    details: dict[str, Any] | None


@dataclass(frozen=True, slots=True)
class FillWrite:
    """One persisted order fill."""

    internal_order_id: str
    fill_quantity: Decimal
    fill_price: Decimal
    fill_notional: Decimal
    fill_at: datetime
    broker_fill_id: str | None = None


@dataclass(frozen=True, slots=True)
class StoredFill:
    """Stored fill row."""

    internal_order_id: str
    fill_quantity: Decimal
    fill_price: Decimal
    fill_notional: Decimal
    fill_at: datetime
    broker_fill_id: str | None


@dataclass(frozen=True, slots=True)
class PositionSnapshotWrite:
    """One persisted position snapshot row."""

    symbol: str
    quantity: Decimal
    market_value: Decimal
    as_of: datetime
    average_cost: Decimal | None = None


@dataclass(frozen=True, slots=True)
class StoredPositionSnapshot:
    """Stored position snapshot row with resolved ETF symbol."""

    symbol: str
    quantity: Decimal
    market_value: Decimal
    as_of: datetime
    average_cost: Decimal | None


@dataclass(frozen=True, slots=True)
class AccountSnapshotWrite:
    """One persisted account-state snapshot."""

    run_mode: RunMode
    cash: Decimal
    equity: Decimal
    buying_power: Decimal
    as_of: datetime


@dataclass(frozen=True, slots=True)
class StoredAccountSnapshot:
    """Stored account-state snapshot."""

    run_mode: RunMode
    cash: Decimal
    equity: Decimal
    buying_power: Decimal
    as_of: datetime


@dataclass(frozen=True, slots=True)
class PnlSnapshotWrite:
    """One persisted PnL snapshot."""

    run_mode: RunMode
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    total_pnl: Decimal
    as_of: datetime


@dataclass(frozen=True, slots=True)
class StoredPnlSnapshot:
    """Stored PnL snapshot."""

    run_mode: RunMode
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    total_pnl: Decimal
    as_of: datetime


@dataclass(frozen=True, slots=True)
class RiskSnapshotWrite:
    """One persisted risk snapshot."""

    run_mode: RunMode
    gross_exposure: Decimal
    net_exposure: Decimal
    open_order_count: int
    as_of: datetime
    drawdown: Decimal | None = None


@dataclass(frozen=True, slots=True)
class StoredRiskSnapshot:
    """Stored risk snapshot."""

    run_mode: RunMode
    gross_exposure: Decimal
    net_exposure: Decimal
    open_order_count: int
    as_of: datetime
    drawdown: Decimal | None


@dataclass(frozen=True, slots=True)
class IncidentWrite:
    """One persisted operational incident."""

    run_mode: RunMode
    incident_type: str
    severity: str
    status: str
    summary: str
    occurred_at: datetime
    resolved_at: datetime | None = None
    details: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class StoredIncident:
    """Stored operational incident."""

    run_mode: RunMode
    incident_type: str
    severity: str
    status: str
    summary: str
    occurred_at: datetime
    resolved_at: datetime | None
    details: dict[str, Any] | None


class InstrumentRepository:
    """Resolve ETF symbols for operational state repositories."""

    def resolve_symbols(self, session: Session, symbols: Sequence[str]) -> dict[str, int]:
        unique_symbols = sorted(set(symbols))
        if not unique_symbols:
            return {}

        rows = session.execute(
            select(Instrument.symbol, Instrument.id).where(Instrument.symbol.in_(unique_symbols))
        ).all()
        symbol_to_id = {symbol: instrument_id for symbol, instrument_id in rows}

        missing_symbols = sorted(set(unique_symbols) - set(symbol_to_id))
        if missing_symbols:
            joined = ", ".join(missing_symbols)
            raise UnknownOperationalInstrumentError(f"Missing instruments for symbols: {joined}")

        return symbol_to_id


class StrategyRunRepository:
    """Persist and read strategy runs plus their derived rows."""

    def __init__(self, instrument_repository: InstrumentRepository | None = None) -> None:
        self._instrument_repository = instrument_repository or InstrumentRepository()

    def create_run(self, session: Session, run: StrategyRunCreate) -> StoredStrategyRun:
        model = StrategyRun(
            run_mode=run.run_mode,
            strategy_name=run.strategy_name,
            config_version=run.config_version,
            config_hash=run.config_hash,
            signal_date=run.signal_date,
            execution_date=run.execution_date,
            status=run.status,
            started_at=run.started_at,
            metadata_json=dict(run.metadata_json) if run.metadata_json is not None else None,
        )
        session.add(model)
        session.flush()
        return _strategy_run_from_model(model)

    def get_run(self, session: Session, *, strategy_run_id: int) -> StoredStrategyRun | None:
        model = session.get(StrategyRun, strategy_run_id)
        if model is None:
            return None
        return _strategy_run_from_model(model)

    def latest_run(
        self,
        session: Session,
        *,
        run_mode: RunMode,
    ) -> StoredStrategyRun | None:
        """Return the most recent strategy run for one environment."""

        model = session.execute(
            select(StrategyRun)
            .where(StrategyRun.run_mode == run_mode)
            .order_by(StrategyRun.started_at.desc(), StrategyRun.id.desc())
            .limit(1)
        ).scalar_one_or_none()
        if model is None:
            return None
        return _strategy_run_from_model(model)

    def count_runs(
        self,
        session: Session,
        *,
        run_mode: RunMode,
        statuses: Sequence[str] | None = None,
    ) -> int:
        """Count persisted runs for one environment, optionally filtered by status."""

        query = (
            select(func.count()).select_from(StrategyRun).where(StrategyRun.run_mode == run_mode)
        )
        if statuses is not None:
            query = query.where(StrategyRun.status.in_(list(statuses)))
        result = session.scalar(query)
        return int(result or 0)

    def find_run_by_identity(
        self,
        session: Session,
        *,
        run_mode: RunMode,
        strategy_name: str,
        config_hash: str,
        signal_date: date,
        execution_date: date | None,
    ) -> StoredStrategyRun | None:
        """Find an existing strategy run for deterministic reruns."""

        model = session.execute(
            select(StrategyRun)
            .where(
                StrategyRun.run_mode == run_mode,
                StrategyRun.strategy_name == strategy_name,
                StrategyRun.config_hash == config_hash,
                StrategyRun.signal_date == signal_date,
                StrategyRun.execution_date == execution_date,
            )
            .order_by(StrategyRun.id.desc())
            .limit(1)
        ).scalar_one_or_none()
        if model is None:
            return None
        return _strategy_run_from_model(model)

    def update_run_status(
        self,
        session: Session,
        *,
        strategy_run_id: int,
        status: str,
        completed_at: datetime | None = None,
        metadata_json: Mapping[str, Any] | None = None,
    ) -> StoredStrategyRun:
        values: dict[str, Any] = {"status": status}
        if completed_at is not None:
            values["completed_at"] = completed_at
        if metadata_json is not None:
            existing = self.get_run(session, strategy_run_id=strategy_run_id)
            merged_metadata: dict[str, Any] = {}
            if existing is not None and existing.metadata_json is not None:
                merged_metadata.update(existing.metadata_json)
            merged_metadata.update(dict(metadata_json))
            values["metadata_json"] = merged_metadata

        session.execute(
            update(StrategyRun).where(StrategyRun.id == strategy_run_id).values(**values)
        )
        return self.get_run(session, strategy_run_id=strategy_run_id)  # type: ignore[return-value]

    def replace_signals(
        self,
        session: Session,
        *,
        strategy_run_id: int,
        signals: Sequence[SignalWrite],
    ) -> tuple[StoredSignal, ...]:
        session.execute(delete(Signal).where(Signal.strategy_run_id == strategy_run_id))
        if not signals:
            return ()

        symbol_to_id = self._instrument_repository.resolve_symbols(
            session,
            [signal.symbol for signal in signals],
        )
        session.add_all(
            Signal(
                strategy_run_id=strategy_run_id,
                instrument_id=symbol_to_id[signal.symbol],
                signal_name=signal.signal_name,
                rank=signal.rank,
                score=signal.score,
                is_selected=signal.is_selected,
                generated_at=signal.generated_at,
            )
            for signal in signals
        )
        session.flush()
        return self.list_signals(session, strategy_run_id=strategy_run_id)

    def list_signals(self, session: Session, *, strategy_run_id: int) -> tuple[StoredSignal, ...]:
        rows = session.execute(
            select(
                Instrument.symbol,
                Signal.signal_name,
                Signal.rank,
                Signal.score,
                Signal.is_selected,
                Signal.generated_at,
            )
            .join(Instrument, Instrument.id == Signal.instrument_id)
            .where(Signal.strategy_run_id == strategy_run_id)
            .order_by(Signal.rank.nullslast(), Instrument.symbol, Signal.signal_name)
        ).all()
        return tuple(
            StoredSignal(
                symbol=symbol,
                signal_name=signal_name,
                rank=rank,
                score=score,
                is_selected=is_selected,
                generated_at=generated_at,
            )
            for symbol, signal_name, rank, score, is_selected, generated_at in rows
        )

    def replace_target_weights(
        self,
        session: Session,
        *,
        strategy_run_id: int,
        target_weights: Sequence[TargetWeightWrite],
    ) -> tuple[StoredTargetWeight, ...]:
        session.execute(delete(TargetWeight).where(TargetWeight.strategy_run_id == strategy_run_id))
        if not target_weights:
            return ()

        symbol_to_id = self._instrument_repository.resolve_symbols(
            session,
            [row.symbol for row in target_weights if row.symbol is not None],
        )
        session.add_all(
            TargetWeight(
                strategy_run_id=strategy_run_id,
                instrument_id=symbol_to_id[row.symbol] if row.symbol is not None else None,
                allocation_key=row.allocation_key,
                target_weight=row.target_weight,
                generated_at=row.generated_at,
            )
            for row in target_weights
        )
        session.flush()
        return self.list_target_weights(session, strategy_run_id=strategy_run_id)

    def list_target_weights(
        self,
        session: Session,
        *,
        strategy_run_id: int,
    ) -> tuple[StoredTargetWeight, ...]:
        rows = session.execute(
            select(
                TargetWeight.allocation_key,
                TargetWeight.target_weight,
                TargetWeight.generated_at,
                Instrument.symbol,
            )
            .outerjoin(Instrument, Instrument.id == TargetWeight.instrument_id)
            .where(TargetWeight.strategy_run_id == strategy_run_id)
            .order_by(TargetWeight.allocation_key)
        ).all()
        return tuple(
            StoredTargetWeight(
                allocation_key=allocation_key,
                target_weight=target_weight,
                generated_at=generated_at,
                symbol=symbol,
            )
            for allocation_key, target_weight, generated_at, symbol in rows
        )

    def replace_target_positions(
        self,
        session: Session,
        *,
        strategy_run_id: int,
        target_positions: Sequence[TargetPositionWrite],
    ) -> tuple[StoredTargetPosition, ...]:
        session.execute(
            delete(TargetPosition).where(TargetPosition.strategy_run_id == strategy_run_id)
        )
        if not target_positions:
            return ()

        symbol_to_id = self._instrument_repository.resolve_symbols(
            session,
            [row.symbol for row in target_positions if row.symbol is not None],
        )
        session.add_all(
            TargetPosition(
                strategy_run_id=strategy_run_id,
                instrument_id=symbol_to_id[row.symbol] if row.symbol is not None else None,
                allocation_key=row.allocation_key,
                target_weight=row.target_weight,
                target_notional=row.target_notional,
                target_quantity=row.target_quantity,
                reference_price=row.reference_price,
                generated_at=row.generated_at,
            )
            for row in target_positions
        )
        session.flush()
        return self.list_target_positions(session, strategy_run_id=strategy_run_id)

    def list_target_positions(
        self,
        session: Session,
        *,
        strategy_run_id: int,
    ) -> tuple[StoredTargetPosition, ...]:
        rows = session.execute(
            select(
                TargetPosition.allocation_key,
                TargetPosition.target_weight,
                TargetPosition.target_notional,
                TargetPosition.target_quantity,
                TargetPosition.reference_price,
                TargetPosition.generated_at,
                Instrument.symbol,
            )
            .outerjoin(Instrument, Instrument.id == TargetPosition.instrument_id)
            .where(TargetPosition.strategy_run_id == strategy_run_id)
            .order_by(TargetPosition.allocation_key)
        ).all()
        return tuple(
            StoredTargetPosition(
                allocation_key=allocation_key,
                target_weight=target_weight,
                target_notional=target_notional,
                target_quantity=target_quantity,
                reference_price=reference_price,
                generated_at=generated_at,
                symbol=symbol,
            )
            for (
                allocation_key,
                target_weight,
                target_notional,
                target_quantity,
                reference_price,
                generated_at,
                symbol,
            ) in rows
        )

    def replace_risk_checks(
        self,
        session: Session,
        *,
        strategy_run_id: int,
        risk_checks: Sequence[RiskCheckWrite],
    ) -> tuple[StoredRiskCheck, ...]:
        session.execute(delete(RiskCheck).where(RiskCheck.strategy_run_id == strategy_run_id))
        if not risk_checks:
            return ()

        session.add_all(
            RiskCheck(
                strategy_run_id=strategy_run_id,
                check_scope=row.check_scope,
                check_name=row.check_name,
                status=row.status,
                reason_code=row.reason_code,
                checked_at=row.checked_at,
                details=dict(row.details) if row.details is not None else None,
            )
            for row in risk_checks
        )
        session.flush()
        return self.list_risk_checks(session, strategy_run_id=strategy_run_id)

    def list_risk_checks(
        self,
        session: Session,
        *,
        strategy_run_id: int,
    ) -> tuple[StoredRiskCheck, ...]:
        rows = session.execute(
            select(
                RiskCheck.check_scope,
                RiskCheck.check_name,
                RiskCheck.status,
                RiskCheck.reason_code,
                RiskCheck.checked_at,
                RiskCheck.details,
            )
            .where(RiskCheck.strategy_run_id == strategy_run_id)
            .order_by(RiskCheck.check_scope, RiskCheck.check_name)
        ).all()
        return tuple(
            StoredRiskCheck(
                check_scope=check_scope,
                check_name=check_name,
                status=status,
                reason_code=reason_code,
                checked_at=checked_at,
                details=details,
            )
            for check_scope, check_name, status, reason_code, checked_at, details in rows
        )


class OrderRepository:
    """Persist internal orders, events, and fills."""

    def __init__(self, instrument_repository: InstrumentRepository | None = None) -> None:
        self._instrument_repository = instrument_repository or InstrumentRepository()

    def create_order(self, session: Session, order: OrderCreate) -> StoredOrder:
        symbol_to_id = self._instrument_repository.resolve_symbols(
            session,
            [order.symbol] if order.symbol is not None else [],
        )
        statement = (
            insert(Order)
            .values(
                strategy_run_id=order.strategy_run_id,
                instrument_id=symbol_to_id[order.symbol] if order.symbol is not None else None,
                internal_order_id=order.internal_order_id,
                run_mode=order.run_mode,
                order_type=order.order_type,
                side=order.side,
                status=order.status,
                requested_quantity=order.requested_quantity,
                requested_notional=order.requested_notional,
                time_in_force=order.time_in_force,
                created_at=order.created_at,
            )
            .on_conflict_do_nothing(index_elements=[Order.internal_order_id])
        )
        session.execute(statement)
        return self.get_order(
            session,
            run_mode=order.run_mode,
            internal_order_id=order.internal_order_id,
        )

    def get_order(
        self,
        session: Session,
        *,
        run_mode: RunMode,
        internal_order_id: str,
    ) -> StoredOrder:
        row = session.execute(
            select(
                Order.id,
                Order.internal_order_id,
                Order.run_mode,
                Order.order_type,
                Order.side,
                Order.status,
                Order.requested_quantity,
                Order.requested_notional,
                Order.time_in_force,
                Order.broker_order_id,
                Order.created_at,
                Order.submitted_at,
                Order.canceled_at,
                Order.strategy_run_id,
                Instrument.symbol,
            )
            .outerjoin(Instrument, Instrument.id == Order.instrument_id)
            .where(Order.run_mode == run_mode, Order.internal_order_id == internal_order_id)
        ).one()
        return StoredOrder(
            id=row.id,
            internal_order_id=row.internal_order_id,
            run_mode=row.run_mode,
            order_type=row.order_type,
            side=row.side,
            status=row.status,
            requested_quantity=row.requested_quantity,
            requested_notional=row.requested_notional,
            time_in_force=row.time_in_force,
            broker_order_id=row.broker_order_id,
            created_at=row.created_at,
            submitted_at=row.submitted_at,
            canceled_at=row.canceled_at,
            strategy_run_id=row.strategy_run_id,
            symbol=row.symbol,
        )

    def update_order_status(
        self,
        session: Session,
        *,
        run_mode: RunMode,
        internal_order_id: str,
        status: str,
        broker_order_id: str | None = None,
        submitted_at: datetime | None = None,
        canceled_at: datetime | None = None,
    ) -> StoredOrder:
        values: dict[str, Any] = {"status": status}
        if broker_order_id is not None:
            values["broker_order_id"] = broker_order_id
        if submitted_at is not None:
            values["submitted_at"] = submitted_at
        if canceled_at is not None:
            values["canceled_at"] = canceled_at
        session.execute(
            update(Order)
            .where(Order.run_mode == run_mode, Order.internal_order_id == internal_order_id)
            .values(**values)
        )
        return self.get_order(session, run_mode=run_mode, internal_order_id=internal_order_id)

    def record_events(
        self,
        session: Session,
        *,
        run_mode: RunMode,
        events: Sequence[OrderEventWrite],
    ) -> tuple[StoredOrderEvent, ...]:
        if not events:
            return ()

        order_ids = self._order_ids_by_internal_id(
            session,
            run_mode=run_mode,
            internal_order_ids=[event.internal_order_id for event in events],
        )
        session.execute(
            insert(OrderEvent)
            .values(
                [
                    {
                        "order_id": order_ids[event.internal_order_id],
                        "event_type": event.event_type,
                        "event_at": event.event_at,
                        "broker_event_id": event.broker_event_id,
                        "details": dict(event.details) if event.details is not None else None,
                    }
                    for event in events
                ]
            )
            .on_conflict_do_nothing(
                index_elements=[OrderEvent.order_id, OrderEvent.event_type, OrderEvent.event_at]
            )
        )
        return self.list_events(session, run_mode=run_mode)

    def list_events(self, session: Session, *, run_mode: RunMode) -> tuple[StoredOrderEvent, ...]:
        rows = session.execute(
            select(
                Order.internal_order_id,
                OrderEvent.event_type,
                OrderEvent.event_at,
                OrderEvent.broker_event_id,
                OrderEvent.details,
            )
            .join(Order, Order.id == OrderEvent.order_id)
            .where(Order.run_mode == run_mode)
            .order_by(OrderEvent.event_at, Order.internal_order_id, OrderEvent.event_type)
        ).all()
        return tuple(
            StoredOrderEvent(
                internal_order_id=internal_order_id,
                event_type=event_type,
                event_at=event_at,
                broker_event_id=broker_event_id,
                details=details,
            )
            for internal_order_id, event_type, event_at, broker_event_id, details in rows
        )

    def record_fills(
        self,
        session: Session,
        *,
        run_mode: RunMode,
        fills: Sequence[FillWrite],
    ) -> tuple[StoredFill, ...]:
        if not fills:
            return ()

        order_ids = self._order_ids_by_internal_id(
            session,
            run_mode=run_mode,
            internal_order_ids=[fill.internal_order_id for fill in fills],
        )
        session.execute(
            insert(Fill)
            .values(
                [
                    {
                        "order_id": order_ids[fill.internal_order_id],
                        "broker_fill_id": fill.broker_fill_id,
                        "fill_quantity": fill.fill_quantity,
                        "fill_price": fill.fill_price,
                        "fill_notional": fill.fill_notional,
                        "fill_at": fill.fill_at,
                    }
                    for fill in fills
                ]
            )
            .on_conflict_do_nothing(
                index_elements=[Fill.order_id, Fill.fill_at, Fill.fill_price, Fill.fill_quantity]
            )
        )
        return self.list_fills(session, run_mode=run_mode)

    def list_fills(
        self,
        session: Session,
        *,
        run_mode: RunMode,
        strategy_run_id: int | None = None,
    ) -> tuple[StoredFill, ...]:
        query = (
            select(
                Order.internal_order_id,
                Fill.fill_quantity,
                Fill.fill_price,
                Fill.fill_notional,
                Fill.fill_at,
                Fill.broker_fill_id,
            )
            .join(Order, Order.id == Fill.order_id)
            .where(Order.run_mode == run_mode)
            .order_by(Fill.fill_at, Order.internal_order_id)
        )
        if strategy_run_id is not None:
            query = query.where(Order.strategy_run_id == strategy_run_id)

        rows = session.execute(query).all()
        return tuple(
            StoredFill(
                internal_order_id=internal_order_id,
                fill_quantity=fill_quantity,
                fill_price=fill_price,
                fill_notional=fill_notional,
                fill_at=fill_at,
                broker_fill_id=broker_fill_id,
            )
            for (
                internal_order_id,
                fill_quantity,
                fill_price,
                fill_notional,
                fill_at,
                broker_fill_id,
            ) in rows
        )

    def list_orders(
        self,
        session: Session,
        *,
        run_mode: RunMode,
        strategy_run_id: int | None = None,
        statuses: Sequence[str] | None = None,
    ) -> tuple[StoredOrder, ...]:
        query = (
            select(
                Order.id,
                Order.internal_order_id,
                Order.run_mode,
                Order.order_type,
                Order.side,
                Order.status,
                Order.requested_quantity,
                Order.requested_notional,
                Order.time_in_force,
                Order.broker_order_id,
                Order.created_at,
                Order.submitted_at,
                Order.canceled_at,
                Order.strategy_run_id,
                Instrument.symbol,
            )
            .outerjoin(Instrument, Instrument.id == Order.instrument_id)
            .where(Order.run_mode == run_mode)
            .order_by(Order.created_at, Order.internal_order_id)
        )
        if strategy_run_id is not None:
            query = query.where(Order.strategy_run_id == strategy_run_id)
        if statuses is not None:
            query = query.where(Order.status.in_(list(statuses)))

        rows = session.execute(query).all()
        return tuple(
            StoredOrder(
                id=row.id,
                internal_order_id=row.internal_order_id,
                run_mode=row.run_mode,
                order_type=row.order_type,
                side=row.side,
                status=row.status,
                requested_quantity=row.requested_quantity,
                requested_notional=row.requested_notional,
                time_in_force=row.time_in_force,
                broker_order_id=row.broker_order_id,
                created_at=row.created_at,
                submitted_at=row.submitted_at,
                canceled_at=row.canceled_at,
                strategy_run_id=row.strategy_run_id,
                symbol=row.symbol,
            )
            for row in rows
        )

    def _order_ids_by_internal_id(
        self,
        session: Session,
        *,
        run_mode: RunMode,
        internal_order_ids: Sequence[str],
    ) -> dict[str, int]:
        unique_ids = sorted(set(internal_order_ids))
        rows = session.execute(
            select(Order.internal_order_id, Order.id).where(
                Order.run_mode == run_mode,
                Order.internal_order_id.in_(unique_ids),
            )
        ).all()
        order_ids = {internal_order_id: order_id for internal_order_id, order_id in rows}

        missing_ids = sorted(set(unique_ids) - set(order_ids))
        if missing_ids:
            joined = ", ".join(missing_ids)
            raise ValueError(f"Missing orders for internal IDs: {joined}")

        return order_ids


class SnapshotRepository:
    """Persist latest account, position, PnL, and risk snapshots."""

    def __init__(self, instrument_repository: InstrumentRepository | None = None) -> None:
        self._instrument_repository = instrument_repository or InstrumentRepository()

    def replace_positions(
        self,
        session: Session,
        *,
        run_mode: RunMode,
        as_of: datetime,
        positions: Sequence[PositionSnapshotWrite],
    ) -> tuple[StoredPositionSnapshot, ...]:
        session.execute(
            delete(Position).where(Position.run_mode == run_mode, Position.as_of == as_of)
        )
        if not positions:
            return ()

        symbol_to_id = self._instrument_repository.resolve_symbols(
            session,
            [position.symbol for position in positions],
        )
        session.add_all(
            Position(
                run_mode=run_mode,
                instrument_id=symbol_to_id[position.symbol],
                quantity=position.quantity,
                average_cost=position.average_cost,
                market_value=position.market_value,
                as_of=as_of,
            )
            for position in positions
        )
        session.flush()
        return self.latest_positions(session, run_mode=run_mode)

    def latest_positions(
        self,
        session: Session,
        *,
        run_mode: RunMode,
    ) -> tuple[StoredPositionSnapshot, ...]:
        latest_as_of = session.scalar(
            select(func.max(Position.as_of)).where(Position.run_mode == run_mode)
        )
        if latest_as_of is None:
            return ()

        rows = session.execute(
            select(
                Instrument.symbol,
                Position.quantity,
                Position.market_value,
                Position.as_of,
                Position.average_cost,
            )
            .join(Instrument, Instrument.id == Position.instrument_id)
            .where(Position.run_mode == run_mode, Position.as_of == latest_as_of)
            .order_by(Instrument.symbol)
        ).all()
        return tuple(
            StoredPositionSnapshot(
                symbol=symbol,
                quantity=quantity,
                market_value=market_value,
                as_of=as_of,
                average_cost=average_cost,
            )
            for symbol, quantity, market_value, as_of, average_cost in rows
        )

    def store_account_snapshot(
        self,
        session: Session,
        snapshot: AccountSnapshotWrite,
    ) -> StoredAccountSnapshot:
        session.execute(
            insert(AccountSnapshot)
            .values(
                run_mode=snapshot.run_mode,
                cash=snapshot.cash,
                equity=snapshot.equity,
                buying_power=snapshot.buying_power,
                as_of=snapshot.as_of,
            )
            .on_conflict_do_update(
                index_elements=[AccountSnapshot.run_mode, AccountSnapshot.as_of],
                set_={
                    "cash": snapshot.cash,
                    "equity": snapshot.equity,
                    "buying_power": snapshot.buying_power,
                },
            )
        )
        return self.latest_account_snapshot(session, run_mode=snapshot.run_mode)  # type: ignore[return-value]

    def latest_account_snapshot(
        self,
        session: Session,
        *,
        run_mode: RunMode,
    ) -> StoredAccountSnapshot | None:
        row = session.execute(
            select(
                AccountSnapshot.run_mode,
                AccountSnapshot.cash,
                AccountSnapshot.equity,
                AccountSnapshot.buying_power,
                AccountSnapshot.as_of,
            )
            .where(AccountSnapshot.run_mode == run_mode)
            .order_by(AccountSnapshot.as_of.desc())
            .limit(1)
        ).one_or_none()
        if row is None:
            return None
        return StoredAccountSnapshot(
            run_mode=row.run_mode,
            cash=row.cash,
            equity=row.equity,
            buying_power=row.buying_power,
            as_of=row.as_of,
        )

    def store_pnl_snapshot(self, session: Session, snapshot: PnlSnapshotWrite) -> StoredPnlSnapshot:
        session.execute(
            insert(PnlSnapshot)
            .values(
                run_mode=snapshot.run_mode,
                realized_pnl=snapshot.realized_pnl,
                unrealized_pnl=snapshot.unrealized_pnl,
                total_pnl=snapshot.total_pnl,
                as_of=snapshot.as_of,
            )
            .on_conflict_do_update(
                index_elements=[PnlSnapshot.run_mode, PnlSnapshot.as_of],
                set_={
                    "realized_pnl": snapshot.realized_pnl,
                    "unrealized_pnl": snapshot.unrealized_pnl,
                    "total_pnl": snapshot.total_pnl,
                },
            )
        )
        return self.latest_pnl_snapshot(session, run_mode=snapshot.run_mode)  # type: ignore[return-value]

    def latest_pnl_snapshot(
        self,
        session: Session,
        *,
        run_mode: RunMode,
    ) -> StoredPnlSnapshot | None:
        row = session.execute(
            select(
                PnlSnapshot.run_mode,
                PnlSnapshot.realized_pnl,
                PnlSnapshot.unrealized_pnl,
                PnlSnapshot.total_pnl,
                PnlSnapshot.as_of,
            )
            .where(PnlSnapshot.run_mode == run_mode)
            .order_by(PnlSnapshot.as_of.desc())
            .limit(1)
        ).one_or_none()
        if row is None:
            return None
        return StoredPnlSnapshot(
            run_mode=row.run_mode,
            realized_pnl=row.realized_pnl,
            unrealized_pnl=row.unrealized_pnl,
            total_pnl=row.total_pnl,
            as_of=row.as_of,
        )

    def store_risk_snapshot(
        self,
        session: Session,
        snapshot: RiskSnapshotWrite,
    ) -> StoredRiskSnapshot:
        session.execute(
            insert(RiskSnapshot)
            .values(
                run_mode=snapshot.run_mode,
                gross_exposure=snapshot.gross_exposure,
                net_exposure=snapshot.net_exposure,
                drawdown=snapshot.drawdown,
                open_order_count=snapshot.open_order_count,
                as_of=snapshot.as_of,
            )
            .on_conflict_do_update(
                index_elements=[RiskSnapshot.run_mode, RiskSnapshot.as_of],
                set_={
                    "gross_exposure": snapshot.gross_exposure,
                    "net_exposure": snapshot.net_exposure,
                    "drawdown": snapshot.drawdown,
                    "open_order_count": snapshot.open_order_count,
                },
            )
        )
        return self.latest_risk_snapshot(session, run_mode=snapshot.run_mode)  # type: ignore[return-value]

    def latest_risk_snapshot(
        self,
        session: Session,
        *,
        run_mode: RunMode,
    ) -> StoredRiskSnapshot | None:
        row = session.execute(
            select(
                RiskSnapshot.run_mode,
                RiskSnapshot.gross_exposure,
                RiskSnapshot.net_exposure,
                RiskSnapshot.drawdown,
                RiskSnapshot.open_order_count,
                RiskSnapshot.as_of,
            )
            .where(RiskSnapshot.run_mode == run_mode)
            .order_by(RiskSnapshot.as_of.desc())
            .limit(1)
        ).one_or_none()
        if row is None:
            return None
        return StoredRiskSnapshot(
            run_mode=row.run_mode,
            gross_exposure=row.gross_exposure,
            net_exposure=row.net_exposure,
            drawdown=row.drawdown,
            open_order_count=row.open_order_count,
            as_of=row.as_of,
        )


class IncidentRepository:
    """Persist operational incidents and read unresolved incidents by environment."""

    def create_incident(self, session: Session, incident: IncidentWrite) -> StoredIncident:
        session.execute(
            insert(Incident)
            .values(
                run_mode=incident.run_mode,
                incident_type=incident.incident_type,
                severity=incident.severity,
                status=incident.status,
                summary=incident.summary,
                occurred_at=incident.occurred_at,
                resolved_at=incident.resolved_at,
                details=dict(incident.details) if incident.details is not None else None,
            )
            .on_conflict_do_update(
                index_elements=[Incident.run_mode, Incident.incident_type, Incident.occurred_at],
                set_={
                    "severity": incident.severity,
                    "status": incident.status,
                    "summary": incident.summary,
                    "resolved_at": incident.resolved_at,
                    "details": dict(incident.details) if incident.details is not None else None,
                },
            )
        )
        return self.list_open_incidents(session, run_mode=incident.run_mode)[0]

    def list_open_incidents(
        self,
        session: Session,
        *,
        run_mode: RunMode,
    ) -> tuple[StoredIncident, ...]:
        rows = session.execute(
            select(
                Incident.run_mode,
                Incident.incident_type,
                Incident.severity,
                Incident.status,
                Incident.summary,
                Incident.occurred_at,
                Incident.resolved_at,
                Incident.details,
            )
            .where(Incident.run_mode == run_mode, Incident.status != "resolved")
            .order_by(Incident.occurred_at.desc(), Incident.incident_type)
        ).all()
        return tuple(
            StoredIncident(
                run_mode=mode,
                incident_type=incident_type,
                severity=severity,
                status=status,
                summary=summary,
                occurred_at=occurred_at,
                resolved_at=resolved_at,
                details=details,
            )
            for (
                mode,
                incident_type,
                severity,
                status,
                summary,
                occurred_at,
                resolved_at,
                details,
            ) in rows
        )


def _strategy_run_from_model(model: StrategyRun) -> StoredStrategyRun:
    return StoredStrategyRun(
        id=model.id,
        run_mode=cast(OperationalRunMode, model.run_mode),
        strategy_name=model.strategy_name,
        config_version=model.config_version,
        config_hash=model.config_hash,
        signal_date=model.signal_date,
        execution_date=model.execution_date,
        status=model.status,
        started_at=model.started_at,
        completed_at=model.completed_at,
        metadata_json=model.metadata_json,
    )
