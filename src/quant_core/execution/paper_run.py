"""Ordered paper-run orchestration over the platform seams."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from sqlalchemy.orm import Session

from quant_core.broker import BrokerGateway
from quant_core.data import (
    IncidentRepository,
    OrderRepository,
    SnapshotRepository,
    StrategyRunRepository,
)
from quant_core.execution.alerts import OperationalAlertService
from quant_core.execution.oms import OrderManagementService
from quant_core.execution.state import OperationalStateRefresher
from quant_core.portfolio import PersistedTargetPositionBuilder
from quant_core.reconciliation import (
    OperationalReconciliationReport,
    OperationalReconciliationService,
    OperationalReconciliationSummary,
)
from quant_core.reporting.paper_runs import build_paper_run_report
from quant_core.research.daily_bars import ResearchDataset
from quant_core.risk import PersistedPreTradeRiskGate, PreTradeRiskConfig
from quant_core.strategy import MomentumRotationStrategy, MomentumStrategyConfig


@dataclass(frozen=True, slots=True)
class PaperRunTimestamps:
    """Explicit timestamps for one deterministic paper run."""

    strategy_started_at: datetime
    positions_generated_at: datetime
    risk_checked_at: datetime
    orders_created_at: datetime
    orders_submitted_at: datetime
    state_refreshed_at: datetime
    reconciliation_at: datetime


@dataclass(frozen=True, slots=True)
class PaperRunSummary:
    """Read-only summary for one paper run keyed by strategy run ID."""

    run_id: int
    signal_date: date
    approved: bool
    order_count: int
    fill_count: int
    reconciliation_critical_rows: int
    incident_count: int


class PaperRunOrchestrator:
    """Run the full paper path in one ordered, restart-safe flow."""

    def __init__(
        self,
        *,
        broker: BrokerGateway,
        strategy: MomentumRotationStrategy | None = None,
        target_position_builder: PersistedTargetPositionBuilder | None = None,
        risk_gate: PersistedPreTradeRiskGate | None = None,
        oms: OrderManagementService | None = None,
        state_refresher: OperationalStateRefresher | None = None,
        reconciliation: OperationalReconciliationService | None = None,
    ) -> None:
        self._broker = broker
        self._strategy = strategy or MomentumRotationStrategy()
        self._target_position_builder = target_position_builder or PersistedTargetPositionBuilder()
        self._risk_gate = risk_gate or PersistedPreTradeRiskGate()
        self._oms = oms or OrderManagementService(broker=broker)
        self._state_refresher = state_refresher or OperationalStateRefresher()
        self._reconciliation = reconciliation or OperationalReconciliationService()
        self._incident_repository = IncidentRepository()
        self._order_repository = OrderRepository()
        self._snapshot_repository = SnapshotRepository()
        self._strategy_repository = StrategyRunRepository()
        self._alert_service = OperationalAlertService()

    def run(
        self,
        session: Session,
        *,
        dataset: ResearchDataset,
        signal_date: date,
        execution_date: date | None = None,
        strategy_config: MomentumStrategyConfig,
        risk_config: PreTradeRiskConfig,
        timestamps: PaperRunTimestamps,
    ) -> PaperRunSummary:
        """Execute one end-to-end paper run and summarize the persisted result."""

        persisted_strategy = self._strategy.execute(
            session,
            dataset=dataset,
            signal_date=signal_date,
            run_mode="paper",
            config=strategy_config,
            started_at=timestamps.strategy_started_at,
            execution_date=execution_date,
        )
        self._target_position_builder.build_for_strategy_run(
            session,
            strategy_run_id=persisted_strategy.run.id,
            run_mode="paper",
            generated_at=timestamps.positions_generated_at,
        )
        risk_decision = self._risk_gate.evaluate_for_strategy_run(
            session,
            strategy_run_id=persisted_strategy.run.id,
            run_mode="paper",
            config=risk_config,
            checked_at=timestamps.risk_checked_at,
        )

        reconciliation_report = OperationalReconciliationReport(
            rows=(),
            summary=OperationalReconciliationSummary(
                total_rows=0,
                mismatched_rows=0,
                critical_rows=0,
            ),
        )
        if risk_decision.approved:
            created_orders = self._oms.create_orders_for_strategy_run(
                session,
                strategy_run_id=persisted_strategy.run.id,
                run_mode="paper",
                created_at=timestamps.orders_created_at,
            )
            self._oms.submit_orders(
                session,
                run_mode="paper",
                internal_order_ids=tuple(order.internal_order_id for order in created_orders),
                submitted_at=timestamps.orders_submitted_at,
            )
            self._oms.sync_broker_state(session, run_mode="paper")
            self._state_refresher.refresh_from_broker(
                session,
                run_mode="paper",
                broker=self._broker,
                as_of=timestamps.state_refreshed_at,
            )
            reconciliation_report = self._reconciliation.reconcile(
                session,
                run_mode="paper",
                broker=self._broker,
                occurred_at=timestamps.reconciliation_at,
            )

        open_incidents = self._incident_repository.list_open_incidents(session, run_mode="paper")
        paper_run_report = build_paper_run_report(
            run=self._strategy_repository.get_run(
                session,
                strategy_run_id=persisted_strategy.run.id,
            )
            or persisted_strategy.run,
            approved=risk_decision.approved,
            failed_reason_codes=risk_decision.failed_reason_codes,
            orders=self._order_repository.list_orders(
                session,
                run_mode="paper",
                strategy_run_id=persisted_strategy.run.id,
            ),
            fills=self._order_repository.list_fills(
                session,
                run_mode="paper",
                strategy_run_id=persisted_strategy.run.id,
            ),
            account_snapshot=self._snapshot_repository.latest_account_snapshot(
                session,
                run_mode="paper",
            ),
            risk_snapshot=self._snapshot_repository.latest_risk_snapshot(
                session,
                run_mode="paper",
            ),
            open_incident_count=len(open_incidents),
            reconciliation=reconciliation_report.summary,
            generated_at=(
                timestamps.reconciliation_at
                if risk_decision.approved
                else timestamps.risk_checked_at
            ),
        )
        self._strategy_repository.update_run_status(
            session,
            strategy_run_id=persisted_strategy.run.id,
            status="completed" if risk_decision.approved else "blocked",
            completed_at=(
                timestamps.reconciliation_at
                if risk_decision.approved
                else timestamps.risk_checked_at
            ),
            metadata_json={"paper_run_report": paper_run_report.as_metadata()},
        )
        self._alert_service.record_run_alerts(
            session,
            run_mode="paper",
            strategy_run_id=persisted_strategy.run.id,
            occurred_at=paper_run_report.generated_at,
        )

        return PaperRunSummary(
            run_id=persisted_strategy.run.id,
            signal_date=signal_date,
            approved=risk_decision.approved,
            order_count=paper_run_report.order_count,
            fill_count=paper_run_report.fill_count,
            reconciliation_critical_rows=paper_run_report.reconciliation_critical_rows,
            incident_count=paper_run_report.open_incident_count,
        )
