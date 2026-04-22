"""Read-only reconciliation views over internal and broker state."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from quant_core.broker import PaperBrokerOrderRequest
from quant_core.execution import PaperExecutionOrder


class ReconciliationRowStatus(StrEnum):
    """Read-only reconciliation row outcomes."""

    MATCHED = "matched"
    MISSING_BROKER_REQUEST = "missing_broker_request"
    ORPHANED_BROKER_REQUEST = "orphaned_broker_request"


@dataclass(frozen=True, slots=True)
class ReconciliationRow:
    """Single reconciliation row keyed by internal order identity."""

    internal_order_id: str
    symbol: str
    row_status: ReconciliationRowStatus


@dataclass(frozen=True, slots=True)
class ReconciliationReadModelSummary:
    """Aggregate counts over reconciliation rows."""

    total_rows: int
    matched_rows: int
    missing_broker_rows: int
    orphaned_broker_rows: int


@dataclass(frozen=True, slots=True)
class ReconciliationReadModel:
    """Read-only reconciliation view over orders and paper broker requests."""

    rows: tuple[ReconciliationRow, ...]
    summary: ReconciliationReadModelSummary


def build_reconciliation_read_model(
    *,
    orders: tuple[PaperExecutionOrder, ...],
    broker_requests: tuple[PaperBrokerOrderRequest, ...],
) -> ReconciliationReadModel:
    """Compare internal orders with paper broker requests read-only."""

    orders_by_id = {order.intent.internal_order_id: order for order in orders}
    requests_by_id = {request.internal_order_id: request for request in broker_requests}

    rows: list[ReconciliationRow] = []
    for internal_order_id in sorted(set(orders_by_id) | set(requests_by_id)):
        if internal_order_id in orders_by_id and internal_order_id in requests_by_id:
            rows.append(
                ReconciliationRow(
                    internal_order_id=internal_order_id,
                    symbol=orders_by_id[internal_order_id].intent.intent.symbol,
                    row_status=ReconciliationRowStatus.MATCHED,
                )
            )
            continue

        if internal_order_id in orders_by_id:
            rows.append(
                ReconciliationRow(
                    internal_order_id=internal_order_id,
                    symbol=orders_by_id[internal_order_id].intent.intent.symbol,
                    row_status=ReconciliationRowStatus.MISSING_BROKER_REQUEST,
                )
            )
            continue

        rows.append(
            ReconciliationRow(
                internal_order_id=internal_order_id,
                symbol=requests_by_id[internal_order_id].symbol,
                row_status=ReconciliationRowStatus.ORPHANED_BROKER_REQUEST,
            )
        )

    rows_tuple = tuple(
        sorted(
            rows,
            key=lambda row: (_row_priority(row.row_status), row.internal_order_id),
        )
    )
    return ReconciliationReadModel(
        rows=rows_tuple,
        summary=ReconciliationReadModelSummary(
            total_rows=len(rows_tuple),
            matched_rows=_count_rows(rows_tuple, ReconciliationRowStatus.MATCHED),
            missing_broker_rows=_count_rows(
                rows_tuple,
                ReconciliationRowStatus.MISSING_BROKER_REQUEST,
            ),
            orphaned_broker_rows=_count_rows(
                rows_tuple,
                ReconciliationRowStatus.ORPHANED_BROKER_REQUEST,
            ),
        ),
    )


def _count_rows(
    rows: tuple[ReconciliationRow, ...],
    status: ReconciliationRowStatus,
) -> int:
    return sum(1 for row in rows if row.row_status is status)


def _row_priority(status: ReconciliationRowStatus) -> int:
    priorities = {
        ReconciliationRowStatus.ORPHANED_BROKER_REQUEST: 0,
        ReconciliationRowStatus.MISSING_BROKER_REQUEST: 1,
        ReconciliationRowStatus.MATCHED: 2,
    }
    return priorities[status]
