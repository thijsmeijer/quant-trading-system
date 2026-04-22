from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from quant_core.data import (
    StoredAccountSnapshot,
    StoredFill,
    StoredOrder,
    StoredRiskSnapshot,
    StoredStrategyRun,
)
from quant_core.reconciliation import OperationalReconciliationSummary
from quant_core.reporting import PaperRunReport, build_paper_run_report


def test_build_paper_run_report_summarizes_persisted_operational_state() -> None:
    report = build_paper_run_report(
        run=StoredStrategyRun(
            id=7,
            run_mode="paper",
            strategy_name="momentum_rotation",
            config_version="v1",
            config_hash="abc123",
            signal_date=date(2026, 4, 22),
            execution_date=date(2026, 4, 23),
            status="completed",
            started_at=datetime(2026, 4, 22, 21, 15, tzinfo=UTC),
            completed_at=datetime(2026, 4, 22, 21, 21, tzinfo=UTC),
            metadata_json={"config": {"version": "v1"}},
        ),
        approved=True,
        failed_reason_codes=(),
        orders=(
            StoredOrder(
                id=1,
                internal_order_id="order_1",
                run_mode="paper",
                order_type="market",
                side="BUY",
                status="filled",
                requested_quantity=Decimal("10.000000"),
                requested_notional=Decimal("5080.000000"),
                time_in_force="day",
                broker_order_id="fake_order_0001",
                created_at=datetime(2026, 4, 22, 21, 18, tzinfo=UTC),
                submitted_at=datetime(2026, 4, 22, 21, 19, tzinfo=UTC),
                canceled_at=None,
                strategy_run_id=7,
                symbol="SPY",
            ),
        ),
        fills=(
            StoredFill(
                internal_order_id="order_1",
                fill_quantity=Decimal("10.000000"),
                fill_price=Decimal("508.000000"),
                fill_notional=Decimal("5080.000000"),
                fill_at=datetime(2026, 4, 22, 21, 19, tzinfo=UTC),
                broker_fill_id="fake_fill_0001",
            ),
        ),
        account_snapshot=StoredAccountSnapshot(
            run_mode="paper",
            cash=Decimal("94920.000000"),
            equity=Decimal("100000.000000"),
            buying_power=Decimal("94920.000000"),
            as_of=datetime(2026, 4, 22, 21, 20, tzinfo=UTC),
        ),
        risk_snapshot=StoredRiskSnapshot(
            run_mode="paper",
            gross_exposure=Decimal("0.050800"),
            net_exposure=Decimal("0.050800"),
            drawdown=None,
            open_order_count=0,
            as_of=datetime(2026, 4, 22, 21, 20, tzinfo=UTC),
        ),
        open_incident_count=0,
        reconciliation=OperationalReconciliationSummary(
            total_rows=3,
            mismatched_rows=0,
            critical_rows=0,
        ),
        generated_at=datetime(2026, 4, 22, 21, 21, tzinfo=UTC),
    )

    assert report == PaperRunReport(
        run_id=7,
        run_mode="paper",
        strategy_name="momentum_rotation",
        signal_date=date(2026, 4, 22),
        execution_date=date(2026, 4, 23),
        status="completed",
        approved=True,
        failed_reason_codes=(),
        order_count=1,
        fill_count=1,
        rejected_order_count=0,
        open_incident_count=0,
        reconciliation_total_rows=3,
        reconciliation_mismatched_rows=0,
        reconciliation_critical_rows=0,
        latest_account_equity=Decimal("100000.000000"),
        latest_gross_exposure=Decimal("0.050800"),
        generated_at=datetime(2026, 4, 22, 21, 21, tzinfo=UTC),
    )
    assert report.as_metadata()["generated_at"] == "2026-04-22T21:21:00+00:00"
