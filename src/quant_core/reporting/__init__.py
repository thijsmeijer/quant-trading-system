"""Reporting and operational summaries package."""

from quant_core.reporting.audit import (
    ExecutionAuditEntry,
    ExecutionAuditSummary,
    build_execution_audit_summary,
)
from quant_core.reporting.burnin import (
    BurnInReport,
    BurnInRunRow,
    BurnInSummary,
    build_burnin_report,
)
from quant_core.reporting.data_quality import (
    DailyDataQualityReport,
    DailyDataQualityReportBuilder,
    DailyDataQualityReportService,
    DailyDataQualitySummary,
)
from quant_core.reporting.execution import (
    ExecutionReportSummary,
    build_execution_report_summary,
)
from quant_core.reporting.paper_performance import (
    BenchmarkPrice,
    BenchmarkReturn,
    LatestRunTradingSummary,
    PaperPerformanceReport,
    PaperPerformanceReportService,
    PaperPerformanceUnavailableError,
    PositionPerformanceRow,
    calculate_period_return,
    calculate_turnover_ratio,
    calculate_weight,
    calculate_weighted_benchmark_return,
)
from quant_core.reporting.paper_runs import (
    PaperRunReport,
    build_paper_run_report,
    load_paper_run_report,
)
from quant_core.reporting.readiness import (
    ExpectationCheck,
    PaperRunComparison,
    compare_paper_run_to_expectation,
)

__all__ = [
    "BurnInReport",
    "BurnInRunRow",
    "BurnInSummary",
    "BenchmarkPrice",
    "BenchmarkReturn",
    "DailyDataQualityReport",
    "DailyDataQualityReportBuilder",
    "DailyDataQualityReportService",
    "DailyDataQualitySummary",
    "ExecutionAuditEntry",
    "ExecutionAuditSummary",
    "ExpectationCheck",
    "ExecutionReportSummary",
    "LatestRunTradingSummary",
    "PaperPerformanceReport",
    "PaperPerformanceReportService",
    "PaperPerformanceUnavailableError",
    "PaperRunReport",
    "PaperRunComparison",
    "PositionPerformanceRow",
    "build_burnin_report",
    "build_execution_audit_summary",
    "build_execution_report_summary",
    "build_paper_run_report",
    "calculate_period_return",
    "calculate_turnover_ratio",
    "calculate_weight",
    "calculate_weighted_benchmark_return",
    "compare_paper_run_to_expectation",
    "load_paper_run_report",
]
