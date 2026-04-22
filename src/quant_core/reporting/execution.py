"""Read-only execution reporting summaries."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from quant_core.execution.paper import PaperExecutionOrder


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
        pending_orders=_count_orders(orders, "pending"),
        submitted_orders=_count_orders(orders, "submitted"),
        filled_orders=_count_orders(orders, "filled"),
        canceled_orders=_count_orders(orders, "canceled"),
        rejected_orders=_count_orders(orders, "rejected"),
    )


def _count_orders(
    orders: tuple[PaperExecutionOrder, ...],
    status: str,
) -> int:
    return sum(1 for order in orders if order.status.value == status)
