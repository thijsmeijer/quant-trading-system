"""Pre-run validation gates for the paper operator path."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from sqlalchemy.orm import Session

from quant_core.execution.alerts import OperationalAlertService
from quant_core.reporting import DailyDataQualityReport, DailyDataQualityReportService


class PaperRunDataQualityError(ValueError):
    """Raised when persisted data fails validation for a paper run."""

    def __init__(self, report: DailyDataQualityReport) -> None:
        self.report = report
        failing_symbols = ", ".join(report.failing_symbols)
        detail = (
            "persisted market data failed validation"
            if not failing_symbols
            else f"persisted market data failed validation for symbols: {failing_symbols}"
        )
        super().__init__(detail)


@dataclass(frozen=True, slots=True)
class PaperRunPreflightResult:
    """Validated data-quality context for one paper run."""

    report: DailyDataQualityReport


class PaperRunPreflightService:
    """Run persisted data validation before the database-backed paper path."""

    def __init__(
        self,
        *,
        report_service: DailyDataQualityReportService | None = None,
        alert_service: OperationalAlertService | None = None,
    ) -> None:
        self._report_service = report_service or DailyDataQualityReportService()
        self._alert_service = alert_service or OperationalAlertService()

    def validate_for_paper_run(
        self,
        session: Session,
        *,
        universe_path: Path,
        signal_date: date,
        occurred_at: datetime,
    ) -> PaperRunPreflightResult:
        """Validate persisted daily bars and raise if they are unsafe to trade."""

        report = self._report_service.build_from_file(
            session,
            universe_path=universe_path,
            as_of=signal_date,
        )
        self._alert_service.record_data_quality_alerts(
            session,
            run_mode="paper",
            report=report,
            occurred_at=occurred_at,
        )
        if report.summary.failing_symbol_count > 0:
            raise PaperRunDataQualityError(report)
        return PaperRunPreflightResult(report=report)
