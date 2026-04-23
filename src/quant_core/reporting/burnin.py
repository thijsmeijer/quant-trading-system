"""Multi-run paper burn-in summaries over archived paper reports."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from quant_core.reporting.paper_runs import PaperRunReport
from quant_core.reporting.readiness import PaperRunComparison, compare_paper_run_to_expectation
from quant_core.settings import PaperRunExpectationConfig


@dataclass(frozen=True, slots=True)
class BurnInRunRow:
    """One archived paper run annotated with burn-in anomaly flags."""

    run_id: int
    signal_date: date
    status: str
    approved: bool
    fill_ratio: Decimal
    anomaly_count: int
    has_critical_issue: bool
    comparison: PaperRunComparison


@dataclass(frozen=True, slots=True)
class BurnInSummary:
    """Aggregate counts over a set of archived paper runs."""

    total_runs: int
    completed_runs: int
    approved_runs: int
    clean_runs: int
    runs_with_anomalies: int
    runs_with_critical_issues: int
    consecutive_clean_runs: int
    total_rejected_orders: int
    total_open_incidents: int
    max_reconciliation_critical_rows: int
    average_fill_ratio: Decimal


@dataclass(frozen=True, slots=True)
class BurnInReport:
    """Archived multi-run burn-in report for operator review."""

    start_date: date | None
    end_date: date | None
    rows: tuple[BurnInRunRow, ...]
    summary: BurnInSummary


def build_burnin_report(
    *,
    reports: tuple[PaperRunReport, ...],
    expectation: PaperRunExpectationConfig,
) -> BurnInReport:
    """Build a burn-in report over archived paper runs."""

    if not reports:
        return BurnInReport(
            start_date=None,
            end_date=None,
            rows=(),
            summary=BurnInSummary(
                total_runs=0,
                completed_runs=0,
                approved_runs=0,
                clean_runs=0,
                runs_with_anomalies=0,
                runs_with_critical_issues=0,
                consecutive_clean_runs=0,
                total_rejected_orders=0,
                total_open_incidents=0,
                max_reconciliation_critical_rows=0,
                average_fill_ratio=Decimal("0.000000"),
            ),
        )

    rows = tuple(_row_for_report(report=report, expectation=expectation) for report in reports)
    fill_ratios = [row.fill_ratio for row in rows]
    clean_rows = [row for row in rows if row.anomaly_count == 0 and not row.has_critical_issue]
    summary = BurnInSummary(
        total_runs=len(rows),
        completed_runs=sum(1 for row in rows if row.status == "completed"),
        approved_runs=sum(1 for row in rows if row.approved),
        clean_runs=len(clean_rows),
        runs_with_anomalies=sum(1 for row in rows if row.anomaly_count > 0),
        runs_with_critical_issues=sum(1 for row in rows if row.has_critical_issue),
        consecutive_clean_runs=_consecutive_clean_runs(rows),
        total_rejected_orders=sum(report.rejected_order_count for report in reports),
        total_open_incidents=sum(report.open_incident_count for report in reports),
        max_reconciliation_critical_rows=max(
            report.reconciliation_critical_rows for report in reports
        ),
        average_fill_ratio=(
            sum(fill_ratios, start=Decimal("0")) / Decimal(len(fill_ratios))
        ).quantize(Decimal("0.000001")),
    )
    signal_dates = sorted(report.signal_date for report in reports)
    return BurnInReport(
        start_date=signal_dates[0],
        end_date=signal_dates[-1],
        rows=rows,
        summary=summary,
    )


def _row_for_report(
    *,
    report: PaperRunReport,
    expectation: PaperRunExpectationConfig,
) -> BurnInRunRow:
    comparison = compare_paper_run_to_expectation(report=report, expectation=expectation)
    anomaly_count = sum(1 for check in comparison.checks if check.status == "fail")
    has_critical_issue = (
        report.reconciliation_critical_rows > 0
        or report.open_incident_count > 0
        or report.status != "completed"
    )
    return BurnInRunRow(
        run_id=report.run_id,
        signal_date=report.signal_date,
        status=report.status,
        approved=report.approved,
        fill_ratio=comparison.fill_ratio,
        anomaly_count=anomaly_count,
        has_critical_issue=has_critical_issue,
        comparison=comparison,
    )


def _consecutive_clean_runs(rows: tuple[BurnInRunRow, ...]) -> int:
    count = 0
    for row in rows:
        if row.anomaly_count == 0 and not row.has_critical_issue:
            count += 1
            continue
        break
    return count
