from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from quant_core.reporting import PaperRunReport, compare_paper_run_to_expectation
from quant_core.settings import PaperRunExpectationConfig


def test_compare_paper_run_to_expectation_passes_when_metrics_are_inside_tolerance() -> None:
    comparison = compare_paper_run_to_expectation(
        report=_report(),
        expectation=PaperRunExpectationConfig(
            require_approved=True,
            min_fill_ratio=Decimal("1.0"),
            max_rejected_order_count=0,
            max_reconciliation_critical_rows=0,
            max_open_incident_count=0,
        ),
    )

    assert comparison.overall_status == "pass"
    assert comparison.fill_ratio == Decimal("1.000000")
    assert all(check.status == "pass" for check in comparison.checks)


def test_compare_paper_run_to_expectation_fails_when_run_is_outside_tolerance() -> None:
    comparison = compare_paper_run_to_expectation(
        report=_report(
            approved=False,
            fill_count=1,
            rejected_order_count=1,
            reconciliation_critical_rows=1,
            open_incident_count=1,
        ),
        expectation=PaperRunExpectationConfig(
            require_approved=True,
            min_fill_ratio=Decimal("1.0"),
            max_rejected_order_count=0,
            max_reconciliation_critical_rows=0,
            max_open_incident_count=0,
        ),
    )

    assert comparison.overall_status == "fail"
    assert comparison.fill_ratio == Decimal("0.500000")
    assert any(
        check.check_name == "approval" and check.status == "fail" for check in comparison.checks
    )
    assert any(
        check.check_name == "rejected_orders" and check.status == "fail"
        for check in comparison.checks
    )


def _report(
    *,
    approved: bool = True,
    fill_count: int = 2,
    rejected_order_count: int = 0,
    reconciliation_critical_rows: int = 0,
    open_incident_count: int = 0,
) -> PaperRunReport:
    return PaperRunReport(
        run_id=1,
        run_mode="paper",
        strategy_name="momentum_rotation",
        signal_date=date(2026, 4, 22),
        execution_date=date(2026, 4, 23),
        status="completed",
        approved=approved,
        failed_reason_codes=(),
        order_count=2,
        fill_count=fill_count,
        rejected_order_count=rejected_order_count,
        open_incident_count=open_incident_count,
        reconciliation_total_rows=2,
        reconciliation_mismatched_rows=reconciliation_critical_rows,
        reconciliation_critical_rows=reconciliation_critical_rows,
        latest_account_equity=Decimal("100000.000000"),
        latest_gross_exposure=Decimal("0.050000"),
        generated_at=datetime(2026, 4, 22, 21, 21, tzinfo=UTC),
    )
