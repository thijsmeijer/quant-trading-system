"""Read-only operational health checks for the paper system."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from quant_core.dashboard.overview import OperationsOverview, OperationsOverviewService
from quant_core.data import OperationalRunMode


@dataclass(frozen=True, slots=True)
class ServiceHealthCheck:
    """One component-level health result."""

    component: str
    status: str
    detail: str


@dataclass(frozen=True, slots=True)
class ServiceHealthSummary:
    """Aggregate health status for the current paper environment."""

    overall_status: str
    checks: tuple[ServiceHealthCheck, ...]


class ServiceHealthService:
    """Build read-only health checks from persisted operational state."""

    def __init__(self, overview_service: OperationsOverviewService | None = None) -> None:
        self._overview_service = overview_service or OperationsOverviewService()

    def build(
        self,
        session: Session,
        *,
        run_mode: OperationalRunMode,
    ) -> ServiceHealthSummary:
        """Summarize health across latest run, data incidents, and state availability."""

        overview = self._overview_service.build(session, run_mode=run_mode)
        checks = (
            self._data_check(overview),
            self._run_check(overview),
            self._state_check(overview),
            self._incident_check(overview),
        )
        return ServiceHealthSummary(
            overall_status=_max_status(tuple(check.status for check in checks)),
            checks=checks,
        )

    def _data_check(self, overview: OperationsOverview) -> ServiceHealthCheck:
        stale_alerts = overview.alerts.stale_data_alerts
        if stale_alerts > 0:
            return ServiceHealthCheck(
                component="data",
                status="critical",
                detail="stale-data incidents are open",
            )
        return ServiceHealthCheck(component="data", status="healthy", detail="no stale-data alerts")

    def _run_check(self, overview: OperationsOverview) -> ServiceHealthCheck:
        if overview.latest_run is None:
            return ServiceHealthCheck(
                component="latest_run",
                status="critical",
                detail="no archived paper run is available",
            )
        if overview.latest_run.status != "completed":
            return ServiceHealthCheck(
                component="latest_run",
                status="critical",
                detail="latest paper run did not complete cleanly",
            )
        return ServiceHealthCheck(
            component="latest_run",
            status="healthy",
            detail="latest paper run completed",
        )

    def _state_check(self, overview: OperationsOverview) -> ServiceHealthCheck:
        if overview.risk_state is None or not overview.positions:
            return ServiceHealthCheck(
                component="state",
                status="warning",
                detail="risk snapshot or positions are missing",
            )
        return ServiceHealthCheck(
            component="state",
            status="healthy",
            detail="risk snapshot and positions are present",
        )

    def _incident_check(self, overview: OperationsOverview) -> ServiceHealthCheck:
        if any(incident.severity == "critical" for incident in overview.incidents):
            return ServiceHealthCheck(
                component="incidents",
                status="critical",
                detail="critical incidents are open",
            )
        if overview.incidents:
            return ServiceHealthCheck(
                component="incidents",
                status="warning",
                detail="non-critical incidents are open",
            )
        return ServiceHealthCheck(
            component="incidents",
            status="healthy",
            detail="no open incidents",
        )


def _max_status(statuses: tuple[str, ...]) -> str:
    priority = {"healthy": 0, "warning": 1, "critical": 2}
    return max(statuses, key=lambda status: priority[status])
