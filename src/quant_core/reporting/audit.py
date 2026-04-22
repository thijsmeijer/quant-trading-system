"""Read-only execution audit summaries."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class ExecutionAuditEntry:
    """Single internal execution audit event."""

    internal_order_id: str
    event_type: str
    occurred_at: datetime


@dataclass(frozen=True, slots=True)
class ExecutionAuditSummary:
    """Read-only aggregate counts over internal execution audit events."""

    total_events: int
    distinct_orders: int
    created_events: int
    submitted_events: int
    filled_events: int
    canceled_events: int
    rejected_events: int
    latest_event_at: datetime | None


def build_execution_audit_summary(
    *,
    entries: tuple[ExecutionAuditEntry, ...],
) -> ExecutionAuditSummary:
    """Summarize internal execution audit entries without mutating state."""

    return ExecutionAuditSummary(
        total_events=len(entries),
        distinct_orders=len({entry.internal_order_id for entry in entries}),
        created_events=_count_events(entries, "CREATED"),
        submitted_events=_count_events(entries, "SUBMITTED"),
        filled_events=_count_events(entries, "FILLED"),
        canceled_events=_count_events(entries, "CANCELED"),
        rejected_events=_count_events(entries, "REJECTED"),
        latest_event_at=max((entry.occurred_at for entry in entries), default=None),
    )


def _count_events(entries: tuple[ExecutionAuditEntry, ...], event_type: str) -> int:
    return sum(1 for entry in entries if entry.event_type == event_type)
