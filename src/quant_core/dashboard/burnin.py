"""Read-only burn-in review services for archived paper runs."""

from __future__ import annotations

from sqlalchemy.orm import Session

from quant_core.data import OperationalRunMode, StrategyRunRepository
from quant_core.reporting import BurnInReport, build_burnin_report, load_paper_run_report
from quant_core.settings import PaperRunExpectationConfig


class BurnInReportService:
    """Build burn-in review reports from persisted archived paper runs."""

    def __init__(self, strategy_repository: StrategyRunRepository | None = None) -> None:
        self._strategy_repository = strategy_repository or StrategyRunRepository()

    def build(
        self,
        session: Session,
        *,
        run_mode: OperationalRunMode,
        expectation: PaperRunExpectationConfig,
        limit: int = 60,
    ) -> BurnInReport:
        """Build a burn-in report over the most recent archived runs."""

        runs = self._strategy_repository.list_runs(
            session,
            run_mode=run_mode,
            limit=limit,
        )
        reports = tuple(
            report
            for run in runs
            if run.metadata_json is not None
            for report in [load_paper_run_report(run.metadata_json.get("paper_run_report"))]
            if report is not None
        )
        return build_burnin_report(reports=reports, expectation=expectation)
