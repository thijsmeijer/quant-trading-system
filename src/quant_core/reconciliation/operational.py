"""Operational reconciliation across orders, fills, positions, and account state."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from sqlalchemy.orm import Session

from quant_core.broker import BrokerAccount, BrokerFill, BrokerGateway, BrokerOrder, BrokerPosition
from quant_core.data import (
    IncidentRepository,
    IncidentWrite,
    OperationalRunMode,
    OrderRepository,
    SnapshotRepository,
    StoredAccountSnapshot,
    StoredFill,
    StoredOrder,
    StoredPositionSnapshot,
)


class ReconciliationSeverity(StrEnum):
    """Severity levels for reconciliation mismatches."""

    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass(frozen=True, slots=True)
class OperationalReconciliationRow:
    """One reconciliation row across a concrete state category."""

    category: str
    key: str
    status: str
    severity: ReconciliationSeverity


@dataclass(frozen=True, slots=True)
class OperationalReconciliationSummary:
    """Aggregate reconciliation counts."""

    total_rows: int
    mismatched_rows: int
    critical_rows: int


@dataclass(frozen=True, slots=True)
class OperationalReconciliationReport:
    """Read-only reconciliation report over internal and broker state."""

    rows: tuple[OperationalReconciliationRow, ...]
    summary: OperationalReconciliationSummary


class OperationalReconciliationService:
    """Compare internal persisted state against broker state and raise incidents when severe."""

    def __init__(
        self,
        *,
        order_repository: OrderRepository | None = None,
        snapshot_repository: SnapshotRepository | None = None,
        incident_repository: IncidentRepository | None = None,
    ) -> None:
        self._order_repository = order_repository or OrderRepository()
        self._snapshot_repository = snapshot_repository or SnapshotRepository()
        self._incident_repository = incident_repository or IncidentRepository()

    def reconcile(
        self,
        session: Session,
        *,
        run_mode: OperationalRunMode,
        broker: BrokerGateway,
        occurred_at: datetime,
    ) -> OperationalReconciliationReport:
        """Build a reconciliation report and emit an incident on critical mismatches."""

        internal_orders = {
            order.internal_order_id: order
            for order in self._order_repository.list_orders(session, run_mode=run_mode)
        }
        broker_orders = {order.internal_order_id: order for order in broker.list_orders()}
        internal_fills = {
            fill.broker_fill_id or f"{fill.internal_order_id}:{fill.fill_at.isoformat()}": fill
            for fill in self._order_repository.list_fills(session, run_mode=run_mode)
        }
        broker_fills = {fill.broker_fill_id: fill for fill in broker.list_fills()}
        internal_positions = {
            position.symbol: position
            for position in self._snapshot_repository.latest_positions(session, run_mode=run_mode)
        }
        broker_positions = {position.symbol: position for position in broker.list_positions()}
        internal_account = self._snapshot_repository.latest_account_snapshot(
            session,
            run_mode=run_mode,
        )
        broker_account = broker.get_account()

        rows = [
            *self._order_rows(internal_orders=internal_orders, broker_orders=broker_orders),
            *self._fill_rows(internal_fills=internal_fills, broker_fills=broker_fills),
            *self._position_rows(
                internal_positions=internal_positions,
                broker_positions=broker_positions,
            ),
            *self._account_rows(internal_account=internal_account, broker_account=broker_account),
        ]
        rows_tuple = tuple(rows)
        summary = OperationalReconciliationSummary(
            total_rows=len(rows_tuple),
            mismatched_rows=sum(1 for row in rows_tuple if row.status != "matched"),
            critical_rows=sum(
                1 for row in rows_tuple if row.severity is ReconciliationSeverity.CRITICAL
            ),
        )
        if summary.critical_rows > 0:
            self._incident_repository.create_incident(
                session,
                IncidentWrite(
                    run_mode=run_mode,
                    incident_type="reconciliation_mismatch",
                    severity="critical",
                    status="open",
                    summary="critical reconciliation mismatch detected",
                    occurred_at=occurred_at,
                    details={
                        "critical_rows": summary.critical_rows,
                        "mismatched_rows": summary.mismatched_rows,
                    },
                ),
            )

        return OperationalReconciliationReport(rows=rows_tuple, summary=summary)

    def _order_rows(
        self,
        *,
        internal_orders: dict[str, StoredOrder],
        broker_orders: dict[str, BrokerOrder],
    ) -> list[OperationalReconciliationRow]:
        rows: list[OperationalReconciliationRow] = []
        for internal_order_id in sorted(set(internal_orders) | set(broker_orders)):
            if internal_order_id not in internal_orders or internal_order_id not in broker_orders:
                rows.append(
                    OperationalReconciliationRow(
                        category="orders",
                        key=internal_order_id,
                        status="missing",
                        severity=ReconciliationSeverity.WARNING,
                    )
                )
                continue
            internal_status = internal_orders[internal_order_id].status
            broker_status = broker_orders[internal_order_id].status.value
            rows.append(
                OperationalReconciliationRow(
                    category="orders",
                    key=internal_order_id,
                    status="matched" if internal_status == broker_status else "status_mismatch",
                    severity=ReconciliationSeverity.WARNING
                    if internal_status != broker_status
                    else ReconciliationSeverity.INFO,
                )
            )
        return rows

    def _fill_rows(
        self,
        *,
        internal_fills: dict[str, StoredFill],
        broker_fills: dict[str, BrokerFill],
    ) -> list[OperationalReconciliationRow]:
        rows: list[OperationalReconciliationRow] = []
        for fill_id in sorted(set(internal_fills) | set(broker_fills)):
            rows.append(
                OperationalReconciliationRow(
                    category="fills",
                    key=fill_id,
                    status=(
                        "matched"
                        if fill_id in internal_fills and fill_id in broker_fills
                        else "missing"
                    ),
                    severity=ReconciliationSeverity.WARNING
                    if fill_id not in internal_fills or fill_id not in broker_fills
                    else ReconciliationSeverity.INFO,
                )
            )
        return rows

    def _position_rows(
        self,
        *,
        internal_positions: dict[str, StoredPositionSnapshot],
        broker_positions: dict[str, BrokerPosition],
    ) -> list[OperationalReconciliationRow]:
        rows: list[OperationalReconciliationRow] = []
        for symbol in sorted(set(internal_positions) | set(broker_positions)):
            internal_position = internal_positions.get(symbol)
            broker_position = broker_positions.get(symbol)
            internal_quantity = (
                internal_position.quantity if internal_position is not None else None
            )
            broker_quantity = broker_position.quantity if broker_position is not None else None
            rows.append(
                OperationalReconciliationRow(
                    category="positions",
                    key=symbol,
                    status=(
                        "matched" if internal_quantity == broker_quantity else "quantity_mismatch"
                    ),
                    severity=ReconciliationSeverity.CRITICAL
                    if internal_quantity != broker_quantity
                    else ReconciliationSeverity.INFO,
                )
            )
        return rows

    def _account_rows(
        self,
        *,
        internal_account: StoredAccountSnapshot | None,
        broker_account: BrokerAccount,
    ) -> list[OperationalReconciliationRow]:
        if internal_account is None:
            return [
                OperationalReconciliationRow(
                    category="account",
                    key="paper_account",
                    status="missing_internal_account",
                    severity=ReconciliationSeverity.CRITICAL,
                )
            ]

        matched = (
            internal_account.cash == broker_account.cash
            and internal_account.equity == broker_account.equity
            and internal_account.buying_power == broker_account.buying_power
        )
        return [
            OperationalReconciliationRow(
                category="account",
                key="paper_account",
                status="matched" if matched else "account_mismatch",
                severity=(
                    ReconciliationSeverity.CRITICAL if not matched else ReconciliationSeverity.INFO
                ),
            )
        ]
