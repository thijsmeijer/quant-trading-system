"""Operator-facing daily review summary for local paper runs."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from quant_core.dashboard.burnin import BurnInReportService
from quant_core.dashboard.health import ServiceHealthService, ServiceHealthSummary
from quant_core.dashboard.overview import OperationsOverview, OperationsOverviewService
from quant_core.dashboard.readiness import (
    PromotionReadinessService,
    PromotionReadinessSummary,
)
from quant_core.data import OperationalRunMode
from quant_core.reporting import BurnInReport
from quant_core.settings import PaperPromotionConfig


@dataclass(frozen=True, slots=True)
class PaperOperatorReview:
    """Combined operator view over the latest paper state."""

    overview: OperationsOverview
    health: ServiceHealthSummary
    burnin: BurnInReport
    readiness: PromotionReadinessSummary


class PaperOperatorReviewService:
    """Aggregate existing read-only services into one operator review."""

    def __init__(
        self,
        *,
        overview_service: OperationsOverviewService | None = None,
        health_service: ServiceHealthService | None = None,
        burnin_service: BurnInReportService | None = None,
        readiness_service: PromotionReadinessService | None = None,
    ) -> None:
        self._overview_service = overview_service or OperationsOverviewService()
        self._health_service = health_service or ServiceHealthService(
            overview_service=self._overview_service
        )
        self._burnin_service = burnin_service or BurnInReportService()
        self._readiness_service = readiness_service or PromotionReadinessService()

    def build(
        self,
        session: Session,
        *,
        run_mode: OperationalRunMode,
        config: PaperPromotionConfig,
        burnin_limit: int = 60,
    ) -> PaperOperatorReview:
        """Build the combined operator review from persisted state."""

        return PaperOperatorReview(
            overview=self._overview_service.build(session, run_mode=run_mode),
            health=self._health_service.build(session, run_mode=run_mode),
            burnin=self._burnin_service.build(
                session,
                run_mode=run_mode,
                expectation=config.latest_run_expectation,
                limit=burnin_limit,
            ),
            readiness=self._readiness_service.build(
                session,
                run_mode=run_mode,
                config=config,
            ),
        )
