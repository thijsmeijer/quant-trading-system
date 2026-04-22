"""Read-only execution reporting summaries."""

from __future__ import annotations

from dataclasses import dataclass

from quant_core.execution import PaperExecutionOrder, PaperExecutionStatus


@dataclass(frozen=True, slots=True)
class ExecutionReportSummary:
    """Read-only aggregate counts over internal execution orders."""

    total_orders: int
    pending_orders: int
    submitted_orders: int
    filled_orders: int
    canceled_orders: int
    rejected_orders: int


def build_execution_report_summary(
    *,
    orders: tuple[PaperExecutionOrder, ...],
) -> ExecutionReportSummary:
    """Summarize internal execution orders by paper execution status."""

    return ExecutionReportSummary(
        total_orders=len(orders),
        pending_orders=_count_orders(orders, PaperExecutionStatus.PENDING),
        submitted_orders=_count_orders(orders, PaperExecutionStatus.SUBMITTED),
        filled_orders=_count_orders(orders, PaperExecutionStatus.FILLED),
        canceled_orders=_count_orders(orders, PaperExecutionStatus.CANCELED),
        rejected_orders=_count_orders(orders, PaperExecutionStatus.REJECTED),
    )


def _count_orders(
    orders: tuple[PaperExecutionOrder, ...],
    status: PaperExecutionStatus,
) -> int:
    return sum(1 for order in orders if order.status is status)
