from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from quant_core.reporting import build_burnin_report
from quant_core.reporting.paper_runs import PaperRunReport
from quant_core.settings import PaperRunExpectationConfig


def test_build_burnin_report_tracks_anomalies_and_consecutive_clean_runs() -> None:
    report = build_burnin_report(
        reports=(
            _paper_run(run_id=3, signal_date=date(2026, 4, 24)),
            _paper_run(
                run_id=2,
                signal_date=date(2026, 4, 23),
                rejected_order_count=1,
                fill_count=0,
                open_incident_count=1,
            ),
            _paper_run(run_id=1, signal_date=date(2026, 4, 22)),
        ),
        expectation=PaperRunExpectationConfig(
            require_approved=True,
            min_fill_ratio=Decimal("1.0"),
            max_rejected_order_count=0,
            max_reconciliation_critical_rows=0,
            max_open_incident_count=0,
        ),
    )

    assert report.start_date == date(2026, 4, 22)
    assert report.end_date == date(2026, 4, 24)
    assert report.summary.total_runs == 3
    assert report.summary.completed_runs == 3
    assert report.summary.approved_runs == 3
    assert report.summary.clean_runs == 2
    assert report.summary.runs_with_anomalies == 1
    assert report.summary.runs_with_critical_issues == 1
    assert report.summary.consecutive_clean_runs == 1
    assert report.summary.total_rejected_orders == 1
    assert report.summary.total_open_incidents == 1
    assert report.summary.average_fill_ratio == Decimal("0.666667")
    assert report.rows[0].run_id == 3
    assert report.rows[0].anomaly_count == 0
    assert report.rows[1].run_id == 2
    assert report.rows[1].anomaly_count > 0
    assert report.rows[1].has_critical_issue is True


def _paper_run(
    *,
    run_id: int,
    signal_date: date,
    fill_count: int = 1,
    rejected_order_count: int = 0,
    open_incident_count: int = 0,
) -> PaperRunReport:
    return PaperRunReport(
        run_id=run_id,
        run_mode="paper",
        strategy_name="momentum_rotation",
        signal_date=signal_date,
        execution_date=signal_date,
        status="completed",
        approved=True,
        failed_reason_codes=(),
        order_count=1,
        fill_count=fill_count,
        rejected_order_count=rejected_order_count,
        open_incident_count=open_incident_count,
        reconciliation_total_rows=1,
        reconciliation_mismatched_rows=0,
        reconciliation_critical_rows=0,
        latest_account_equity=Decimal("100000.000000"),
        latest_gross_exposure=Decimal("0.100000"),
        generated_at=datetime(
            signal_date.year, signal_date.month, signal_date.day, 21, 0, tzinfo=UTC
        ),
    )
