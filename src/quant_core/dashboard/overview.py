"""Operator-facing read models over current paper-run state."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from quant_core.data import (
    IncidentRepository,
    OperationalRunMode,
    OrderRepository,
    SnapshotRepository,
    StoredIncident,
    StoredOrder,
    StoredPositionSnapshot,
    StoredRiskSnapshot,
    StrategyRunRepository,
)
from quant_core.execution.alerts import ActiveAlertSummary, build_active_alert_summary
from quant_core.reporting.paper_runs import PaperRunReport, load_paper_run_report


@dataclass(frozen=True, slots=True)
class OperationsOverview:
    """Operator-facing snapshot of latest run, state, and incidents."""

    latest_run: PaperRunReport | None
    positions: tuple[StoredPositionSnapshot, ...]
    orders: tuple[StoredOrder, ...]
    risk_state: StoredRiskSnapshot | None
    incidents: tuple[StoredIncident, ...]
    alerts: ActiveAlertSummary


class OperationsOverviewService:
    """Build a dashboard-friendly overview from persisted paper state."""

    def __init__(
        self,
        *,
        strategy_repository: StrategyRunRepository | None = None,
        snapshot_repository: SnapshotRepository | None = None,
        order_repository: OrderRepository | None = None,
        incident_repository: IncidentRepository | None = None,
    ) -> None:
        self._strategy_repository = strategy_repository or StrategyRunRepository()
        self._snapshot_repository = snapshot_repository or SnapshotRepository()
        self._order_repository = order_repository or OrderRepository()
        self._incident_repository = incident_repository or IncidentRepository()

    def build(
        self,
        session: Session,
        *,
        run_mode: OperationalRunMode,
    ) -> OperationsOverview:
        """Build the latest operator overview for one environment."""

        latest_run = self._strategy_repository.latest_run(session, run_mode=run_mode)
        paper_run_report = None
        orders: tuple[StoredOrder, ...] = ()
        if latest_run is not None and latest_run.metadata_json is not None:
            paper_run_report = load_paper_run_report(
                latest_run.metadata_json.get("paper_run_report")
            )
            orders = self._order_repository.list_orders(
                session,
                run_mode=run_mode,
                strategy_run_id=latest_run.id,
            )

        incidents = self._incident_repository.list_open_incidents(session, run_mode=run_mode)
        return OperationsOverview(
            latest_run=paper_run_report,
            positions=self._snapshot_repository.latest_positions(session, run_mode=run_mode),
            orders=orders,
            risk_state=self._snapshot_repository.latest_risk_snapshot(session, run_mode=run_mode),
            incidents=incidents,
            alerts=build_active_alert_summary(incidents=incidents),
        )
