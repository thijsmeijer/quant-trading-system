"""CLI entrypoint for local paper burn-in review."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from quant_core.dashboard import BurnInReportService
from quant_core.reporting import BurnInReport
from quant_core.settings import load_paper_promotion_config


def main() -> int:
    """Build and print a local burn-in summary from archived paper runs."""

    args = _build_parser().parse_args()
    config = load_paper_promotion_config(Path(args.config))
    engine = create_engine(args.database_url)
    try:
        with Session(engine) as session:
            report = BurnInReportService().build(
                session,
                run_mode=args.run_mode,
                expectation=config.latest_run_expectation,
                limit=args.limit,
            )
    finally:
        engine.dispose()

    print(json.dumps(_report_payload(report), sort_keys=True))
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Summarize archived paper burn-in runs.")
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--config", default="configs/paper_promotion.yaml")
    parser.add_argument("--run-mode", default="paper", choices=("dev", "paper", "live"))
    parser.add_argument("--limit", type=int, default=60)
    return parser


def _report_payload(report: BurnInReport) -> dict[str, Any]:
    rows = [
        {
            "run_id": row.run_id,
            "signal_date": row.signal_date.isoformat(),
            "status": row.status,
            "approved": row.approved,
            "fill_ratio": str(row.fill_ratio),
            "anomaly_count": row.anomaly_count,
            "has_critical_issue": row.has_critical_issue,
            "comparison_status": row.comparison.overall_status,
        }
        for row in report.rows
    ]
    return {
        "start_date": report.start_date.isoformat() if report.start_date is not None else None,
        "end_date": report.end_date.isoformat() if report.end_date is not None else None,
        "rows": rows,
        "summary": {
            "total_runs": report.summary.total_runs,
            "completed_runs": report.summary.completed_runs,
            "approved_runs": report.summary.approved_runs,
            "clean_runs": report.summary.clean_runs,
            "runs_with_anomalies": report.summary.runs_with_anomalies,
            "runs_with_critical_issues": report.summary.runs_with_critical_issues,
            "consecutive_clean_runs": report.summary.consecutive_clean_runs,
            "total_rejected_orders": report.summary.total_rejected_orders,
            "total_open_incidents": report.summary.total_open_incidents,
            "max_reconciliation_critical_rows": report.summary.max_reconciliation_critical_rows,
            "average_fill_ratio": str(report.summary.average_fill_ratio),
        },
    }


if __name__ == "__main__":
    raise SystemExit(main())
