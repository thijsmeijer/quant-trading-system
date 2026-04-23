"""Operational alert generation for paper-run incidents."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy.orm import Session

from quant_core.data import (
    IncidentRepository,
    IncidentWrite,
    OperationalRunMode,
    OrderRepository,
    StoredIncident,
    StrategyRunRepository,
)
from quant_core.reporting.data_quality import DailyDataQualityReport
from quant_core.reporting.paper_runs import load_paper_run_report


@dataclass(frozen=True, slots=True)
class ActiveAlertSummary:
    """Aggregate counts for active operational alerts."""

    stale_data_alerts: int
    failed_job_alerts: int
    order_rejection_alerts: int
    reconciliation_alerts: int


def build_active_alert_summary(
    *,
    incidents: tuple[StoredIncident, ...],
) -> ActiveAlertSummary:
    """Summarize open incidents into operator-facing alert counts."""

    return ActiveAlertSummary(
        stale_data_alerts=sum(
            1 for incident in incidents if incident.incident_type == "stale_data"
        ),
        failed_job_alerts=sum(
            1 for incident in incidents if incident.incident_type == "failed_job"
        ),
        order_rejection_alerts=sum(
            1 for incident in incidents if incident.incident_type == "order_rejection"
        ),
        reconciliation_alerts=sum(
            1 for incident in incidents if incident.incident_type == "reconciliation_mismatch"
        ),
    )


class OperationalAlertService:
    """Generate alert incidents from data-quality and paper-run state."""

    def __init__(
        self,
        *,
        strategy_repository: StrategyRunRepository | None = None,
        order_repository: OrderRepository | None = None,
        incident_repository: IncidentRepository | None = None,
    ) -> None:
        self._strategy_repository = strategy_repository or StrategyRunRepository()
        self._order_repository = order_repository or OrderRepository()
        self._incident_repository = incident_repository or IncidentRepository()

    def record_data_quality_alerts(
        self,
        session: Session,
        *,
        run_mode: OperationalRunMode,
        report: DailyDataQualityReport,
        occurred_at: datetime,
    ) -> tuple[StoredIncident, ...]:
        """Create stale-data incidents from a daily data-quality report."""

        if not report.failing_symbols:
            return ()

        severity = "critical"
        incident = self._incident_repository.create_incident(
            session,
            IncidentWrite(
                run_mode=run_mode,
                incident_type="stale_data",
                severity=severity,
                status="open",
                summary="persisted ETF market data failed validation",
                occurred_at=occurred_at,
                details={
                    "checked_as_of": report.checked_as_of.isoformat(),
                    "duplicate_symbols": list(report.duplicate_symbols),
                    "missing_symbols": list(report.missing_symbols),
                    "price_sanity_symbols": list(report.price_sanity_symbols),
                    "stale_symbols": list(report.stale_symbols),
                },
            ),
        )
        return (incident,)

    def record_run_alerts(
        self,
        session: Session,
        *,
        run_mode: OperationalRunMode,
        strategy_run_id: int,
        occurred_at: datetime,
    ) -> tuple[StoredIncident, ...]:
        """Create incidents for failed runs, order rejections, and reconciliation alerts."""

        run = self._strategy_repository.get_run(session, strategy_run_id=strategy_run_id)
        if run is None:
            raise ValueError(f"Unknown strategy_run_id: {strategy_run_id}")

        incidents: list[StoredIncident] = []
        paper_run_report = None
        if run.metadata_json is not None:
            paper_run_report = load_paper_run_report(run.metadata_json.get("paper_run_report"))

        if run.status != "completed" or (
            paper_run_report is not None and not paper_run_report.approved
        ):
            incidents.append(
                self._incident_repository.create_incident(
                    session,
                    IncidentWrite(
                        run_mode=run_mode,
                        incident_type="failed_job",
                        severity="critical",
                        status="open",
                        summary="paper run did not complete cleanly",
                        occurred_at=occurred_at,
                        details={
                            "run_id": strategy_run_id,
                            "run_status": run.status,
                            "approved": (
                                paper_run_report.approved if paper_run_report is not None else None
                            ),
                        },
                    ),
                )
            )

        rejected_orders = tuple(
            order
            for order in self._order_repository.list_orders(
                session,
                run_mode=run_mode,
                strategy_run_id=strategy_run_id,
            )
            if order.status == "rejected"
        )
        if rejected_orders:
            incidents.append(
                self._incident_repository.create_incident(
                    session,
                    IncidentWrite(
                        run_mode=run_mode,
                        incident_type="order_rejection",
                        severity="critical",
                        status="open",
                        summary="broker rejected one or more paper orders",
                        occurred_at=occurred_at,
                        details={
                            "run_id": strategy_run_id,
                            "internal_order_ids": [
                                order.internal_order_id for order in rejected_orders
                            ],
                        },
                    ),
                )
            )

        if paper_run_report is not None and paper_run_report.reconciliation_critical_rows > 0:
            incidents.append(
                self._incident_repository.create_incident(
                    session,
                    IncidentWrite(
                        run_mode=run_mode,
                        incident_type="reconciliation_mismatch",
                        severity="critical",
                        status="open",
                        summary="paper reconciliation found critical mismatches",
                        occurred_at=occurred_at,
                        details={
                            "run_id": strategy_run_id,
                            "critical_rows": paper_run_report.reconciliation_critical_rows,
                            "mismatched_rows": paper_run_report.reconciliation_mismatched_rows,
                        },
                    ),
                )
            )

        return tuple(incidents)
