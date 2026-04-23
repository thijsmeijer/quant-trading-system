from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from quant_core.dashboard import (
    OperationsOverview,
    PromotionReadinessSummary,
    ServiceHealthCheck,
    ServiceHealthSummary,
)
from quant_core.dashboard.review import PaperOperatorReview
from quant_core.data import StoredPositionSnapshot
from quant_core.execution.alerts import ActiveAlertSummary
from quant_core.reporting import BurnInReport, BurnInSummary
from quant_core.reporting.paper_runs import PaperRunReport
from quant_core.reporting.readiness import ExpectationCheck, PaperRunComparison


def test_operator_review_payload_exposes_operator_critical_fields() -> None:
    from quant_core.dashboard.cli import _review_payload

    generated_at = datetime(2026, 4, 22, 21, 0, tzinfo=UTC)
    review = PaperOperatorReview(
        overview=OperationsOverview(
            latest_run=PaperRunReport(
                run_id=7,
                run_mode="paper",
                strategy_name="momentum_rotation",
                signal_date=date(2026, 4, 22),
                execution_date=date(2026, 4, 23),
                status="completed",
                approved=True,
                failed_reason_codes=(),
                order_count=2,
                fill_count=2,
                rejected_order_count=0,
                open_incident_count=0,
                reconciliation_total_rows=0,
                reconciliation_mismatched_rows=0,
                reconciliation_critical_rows=0,
                latest_account_equity=Decimal("100000.000000"),
                latest_gross_exposure=Decimal("0.500000"),
                generated_at=generated_at,
            ),
            positions=(
                StoredPositionSnapshot(
                    symbol="SPY",
                    quantity=Decimal("10.000000"),
                    market_value=Decimal("5080.000000"),
                    average_cost=None,
                    as_of=generated_at,
                ),
            ),
            orders=(),
            risk_state=None,
            incidents=(),
            alerts=ActiveAlertSummary(
                stale_data_alerts=0,
                failed_job_alerts=0,
                order_rejection_alerts=0,
                reconciliation_alerts=0,
            ),
        ),
        health=ServiceHealthSummary(
            overall_status="healthy",
            checks=(
                ServiceHealthCheck(
                    component="latest_run",
                    status="healthy",
                    detail="latest paper run completed",
                ),
            ),
        ),
        burnin=BurnInReport(
            start_date=date(2026, 4, 21),
            end_date=date(2026, 4, 22),
            rows=(),
            summary=BurnInSummary(
                total_runs=2,
                completed_runs=2,
                approved_runs=2,
                clean_runs=2,
                runs_with_anomalies=0,
                runs_with_critical_issues=0,
                consecutive_clean_runs=2,
                total_rejected_orders=0,
                total_open_incidents=0,
                max_reconciliation_critical_rows=0,
                average_fill_ratio=Decimal("1.000000"),
            ),
        ),
        readiness=PromotionReadinessSummary(
            status="awaiting_manual_approval",
            blocking_reasons=(),
            manual_approval_required=True,
            completed_run_count=2,
            open_critical_incidents=0,
            open_warning_incidents=0,
            latest_run_comparison=PaperRunComparison(
                overall_status="pass",
                checks=(
                    ExpectationCheck(
                        check_name="approval",
                        status="pass",
                        detail="latest run was approved",
                    ),
                ),
                fill_ratio=Decimal("1.000000"),
            ),
        ),
    )

    payload = _review_payload(review)

    assert payload["latest_run"]["run_id"] == 7
    assert payload["overview"]["position_count"] == 1
    assert payload["overview"]["open_incident_count"] == 0
    assert payload["health"]["overall_status"] == "healthy"
    assert payload["burnin"]["summary"]["consecutive_clean_runs"] == 2
    assert payload["readiness"]["status"] == "awaiting_manual_approval"
