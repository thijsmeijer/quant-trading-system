"""CLI entrypoint for daily paper-operator review."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from quant_core.dashboard.review import PaperOperatorReview, PaperOperatorReviewService
from quant_core.settings import load_paper_promotion_config


def main() -> int:
    """Print one operator-facing review payload from persisted paper state."""

    args = _build_parser().parse_args()
    config = load_paper_promotion_config(Path(args.config))
    engine = create_engine(args.database_url)
    try:
        with Session(engine) as session:
            review = PaperOperatorReviewService().build(
                session,
                run_mode=args.run_mode,
                config=config,
                burnin_limit=args.burnin_limit,
            )
    finally:
        engine.dispose()

    print(json.dumps(_review_payload(review), sort_keys=True))
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Summarize latest paper state, burn-in trend, and readiness."
    )
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--config", default="configs/paper_promotion.yaml")
    parser.add_argument("--run-mode", default="paper", choices=("dev", "paper", "live"))
    parser.add_argument("--burnin-limit", type=int, default=60)
    return parser


def _review_payload(review: PaperOperatorReview) -> dict[str, Any]:
    latest_run = None
    if review.overview.latest_run is not None:
        latest_run = {
            "run_id": review.overview.latest_run.run_id,
            "signal_date": review.overview.latest_run.signal_date.isoformat(),
            "execution_date": (
                review.overview.latest_run.execution_date.isoformat()
                if review.overview.latest_run.execution_date is not None
                else None
            ),
            "status": review.overview.latest_run.status,
            "approved": review.overview.latest_run.approved,
            "order_count": review.overview.latest_run.order_count,
            "fill_count": review.overview.latest_run.fill_count,
            "open_incident_count": review.overview.latest_run.open_incident_count,
            "reconciliation_critical_rows": (
                review.overview.latest_run.reconciliation_critical_rows
            ),
        }

    return {
        "latest_run": latest_run,
        "overview": {
            "position_count": len(review.overview.positions),
            "order_count": len(review.overview.orders),
            "open_incident_count": len(review.overview.incidents),
            "alerts": {
                "stale_data": review.overview.alerts.stale_data_alerts,
                "failed_jobs": review.overview.alerts.failed_job_alerts,
                "order_rejections": review.overview.alerts.order_rejection_alerts,
                "reconciliation": review.overview.alerts.reconciliation_alerts,
            },
        },
        "health": {
            "overall_status": review.health.overall_status,
            "checks": [
                {
                    "component": check.component,
                    "status": check.status,
                    "detail": check.detail,
                }
                for check in review.health.checks
            ],
        },
        "burnin": {
            "start_date": (
                review.burnin.start_date.isoformat()
                if review.burnin.start_date is not None
                else None
            ),
            "end_date": (
                review.burnin.end_date.isoformat() if review.burnin.end_date is not None else None
            ),
            "summary": {
                "total_runs": review.burnin.summary.total_runs,
                "completed_runs": review.burnin.summary.completed_runs,
                "approved_runs": review.burnin.summary.approved_runs,
                "clean_runs": review.burnin.summary.clean_runs,
                "runs_with_anomalies": review.burnin.summary.runs_with_anomalies,
                "runs_with_critical_issues": review.burnin.summary.runs_with_critical_issues,
                "consecutive_clean_runs": review.burnin.summary.consecutive_clean_runs,
                "total_rejected_orders": review.burnin.summary.total_rejected_orders,
                "total_open_incidents": review.burnin.summary.total_open_incidents,
                "max_reconciliation_critical_rows": (
                    review.burnin.summary.max_reconciliation_critical_rows
                ),
                "average_fill_ratio": str(review.burnin.summary.average_fill_ratio),
            },
        },
        "readiness": {
            "status": review.readiness.status,
            "blocking_reasons": list(review.readiness.blocking_reasons),
            "manual_approval_required": review.readiness.manual_approval_required,
            "completed_run_count": review.readiness.completed_run_count,
            "open_critical_incidents": review.readiness.open_critical_incidents,
            "open_warning_incidents": review.readiness.open_warning_incidents,
            "latest_run_comparison_status": (
                review.readiness.latest_run_comparison.overall_status
                if review.readiness.latest_run_comparison is not None
                else None
            ),
        },
    }


if __name__ == "__main__":
    raise SystemExit(main())
