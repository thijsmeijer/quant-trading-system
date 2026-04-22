"""Read-only comparison between paper-run behavior and modeled expectations."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from quant_core.reporting.paper_runs import PaperRunReport
from quant_core.settings import PaperRunExpectationConfig


@dataclass(frozen=True, slots=True)
class ExpectationCheck:
    """One readiness comparison check against expected paper behavior."""

    check_name: str
    status: str
    detail: str


@dataclass(frozen=True, slots=True)
class PaperRunComparison:
    """Comparison result for one paper run against modeled expectations."""

    overall_status: str
    fill_ratio: Decimal
    checks: tuple[ExpectationCheck, ...]


def compare_paper_run_to_expectation(
    *,
    report: PaperRunReport,
    expectation: PaperRunExpectationConfig,
) -> PaperRunComparison:
    """Compare one paper-run report to explicit modeled expectations."""

    fill_ratio = Decimal("1")
    if report.order_count > 0:
        fill_ratio = (Decimal(report.fill_count) / Decimal(report.order_count)).quantize(
            Decimal("0.000001")
        )

    checks = [
        _check(
            check_name="approval",
            passed=(not expectation.require_approved) or report.approved,
            detail="paper run was approved" if report.approved else "paper run was not approved",
        ),
        _check(
            check_name="fill_ratio",
            passed=fill_ratio >= expectation.min_fill_ratio,
            detail=f"fill_ratio={fill_ratio} expected>={expectation.min_fill_ratio}",
        ),
        _check(
            check_name="rejected_orders",
            passed=report.rejected_order_count <= expectation.max_rejected_order_count,
            detail=(
                f"rejected_order_count={report.rejected_order_count} "
                f"max={expectation.max_rejected_order_count}"
            ),
        ),
        _check(
            check_name="reconciliation",
            passed=(
                report.reconciliation_critical_rows <= expectation.max_reconciliation_critical_rows
            ),
            detail=(
                f"reconciliation_critical_rows={report.reconciliation_critical_rows} "
                f"max={expectation.max_reconciliation_critical_rows}"
            ),
        ),
        _check(
            check_name="open_incidents",
            passed=report.open_incident_count <= expectation.max_open_incident_count,
            detail=(
                f"open_incident_count={report.open_incident_count} "
                f"max={expectation.max_open_incident_count}"
            ),
        ),
    ]
    if expectation.expected_order_count is not None:
        checks.append(
            _check(
                check_name="order_count",
                passed=abs(report.order_count - expectation.expected_order_count)
                <= expectation.max_order_count_delta,
                detail=(
                    f"order_count={report.order_count} "
                    f"expected={expectation.expected_order_count} "
                    f"delta<={expectation.max_order_count_delta}"
                ),
            )
        )

    checks_tuple = tuple(checks)
    return PaperRunComparison(
        overall_status="pass" if all(check.status == "pass" for check in checks_tuple) else "fail",
        fill_ratio=fill_ratio,
        checks=checks_tuple,
    )


def _check(*, check_name: str, passed: bool, detail: str) -> ExpectationCheck:
    return ExpectationCheck(
        check_name=check_name,
        status="pass" if passed else "fail",
        detail=detail,
    )
