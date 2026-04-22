from __future__ import annotations

from datetime import UTC, datetime

from quant_core.reporting import (
    ExecutionAuditEntry,
    ExecutionAuditSummary,
    build_execution_audit_summary,
)


def test_build_execution_audit_summary_counts_events_and_tracks_latest_timestamp() -> None:
    summary = build_execution_audit_summary(
        entries=(
            ExecutionAuditEntry(
                internal_order_id="intent_1",
                event_type="CREATED",
                occurred_at=datetime(2026, 4, 22, 19, 0, tzinfo=UTC),
            ),
            ExecutionAuditEntry(
                internal_order_id="intent_1",
                event_type="SUBMITTED",
                occurred_at=datetime(2026, 4, 22, 19, 1, tzinfo=UTC),
            ),
            ExecutionAuditEntry(
                internal_order_id="intent_2",
                event_type="CREATED",
                occurred_at=datetime(2026, 4, 22, 19, 2, tzinfo=UTC),
            ),
        )
    )

    assert summary == ExecutionAuditSummary(
        total_events=3,
        distinct_orders=2,
        created_events=2,
        submitted_events=1,
        filled_events=0,
        canceled_events=0,
        rejected_events=0,
        latest_event_at=datetime(2026, 4, 22, 19, 2, tzinfo=UTC),
    )


def test_build_execution_audit_summary_handles_empty_entries() -> None:
    summary = build_execution_audit_summary(entries=())

    assert summary.total_events == 0
    assert summary.distinct_orders == 0
    assert summary.created_events == 0
    assert summary.submitted_events == 0
    assert summary.filled_events == 0
    assert summary.canceled_events == 0
    assert summary.rejected_events == 0
    assert summary.latest_event_at is None
