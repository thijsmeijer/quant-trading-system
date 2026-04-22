"""Read-only promotion-readiness summaries for paper trading."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from quant_core.data import IncidentRepository, OperationalRunMode, StrategyRunRepository
from quant_core.reporting import PaperRunComparison, compare_paper_run_to_expectation
from quant_core.reporting.paper_runs import load_paper_run_report
from quant_core.settings import PaperPromotionConfig


@dataclass(frozen=True, slots=True)
class PromotionReadinessSummary:
    """Operator-facing summary of whether paper promotion is blocked."""

    status: str
    blocking_reasons: tuple[str, ...]
    manual_approval_required: bool
    completed_run_count: int
    open_critical_incidents: int
    open_warning_incidents: int
    latest_run_comparison: PaperRunComparison | None


class PromotionReadinessService:
    """Evaluate paper promotion readiness from persisted reports and incidents."""

    def __init__(
        self,
        *,
        strategy_repository: StrategyRunRepository | None = None,
        incident_repository: IncidentRepository | None = None,
    ) -> None:
        self._strategy_repository = strategy_repository or StrategyRunRepository()
        self._incident_repository = incident_repository or IncidentRepository()

    def build(
        self,
        session: Session,
        *,
        run_mode: OperationalRunMode,
        config: PaperPromotionConfig,
    ) -> PromotionReadinessSummary:
        """Build the current paper-promotion readiness summary."""

        latest_run = self._strategy_repository.latest_run(session, run_mode=run_mode)
        comparison = None
        if latest_run is not None and latest_run.metadata_json is not None:
            latest_report = load_paper_run_report(latest_run.metadata_json.get("paper_run_report"))
            if latest_report is not None:
                comparison = compare_paper_run_to_expectation(
                    report=latest_report,
                    expectation=config.latest_run_expectation,
                )

        open_incidents = self._incident_repository.list_open_incidents(session, run_mode=run_mode)
        open_critical_incidents = sum(
            1 for incident in open_incidents if incident.severity == "critical"
        )
        open_warning_incidents = sum(
            1 for incident in open_incidents if incident.severity == "warning"
        )
        completed_run_count = self._strategy_repository.count_runs(
            session,
            run_mode=run_mode,
            statuses=("completed",),
        )

        blocking_reasons: list[str] = []
        if comparison is None:
            blocking_reasons.append("missing_latest_paper_run_report")
        elif comparison.overall_status != "pass":
            blocking_reasons.append("latest_run_outside_expectation")
        if completed_run_count < config.minimum_completed_runs:
            blocking_reasons.append("insufficient_completed_runs")
        if open_critical_incidents > config.maximum_open_critical_incidents:
            blocking_reasons.append("open_critical_incidents")
        if open_warning_incidents > config.maximum_open_warning_incidents:
            blocking_reasons.append("open_warning_incidents")

        status = "blocked"
        if not blocking_reasons:
            status = "awaiting_manual_approval" if config.manual_approval_required else "ready"

        return PromotionReadinessSummary(
            status=status,
            blocking_reasons=tuple(blocking_reasons),
            manual_approval_required=config.manual_approval_required,
            completed_run_count=completed_run_count,
            open_critical_incidents=open_critical_incidents,
            open_warning_incidents=open_warning_incidents,
            latest_run_comparison=comparison,
        )
