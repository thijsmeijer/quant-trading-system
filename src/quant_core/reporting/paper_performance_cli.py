"""CLI entrypoint for paper performance reporting."""

from __future__ import annotations

import argparse
import json
from datetime import date
from decimal import Decimal
from typing import Any, cast

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from quant_core.data import OperationalRunMode
from quant_core.reporting.paper_performance import (
    PaperPerformanceReport,
    PaperPerformanceReportService,
)


def main(argv: list[str] | None = None) -> int:
    """Build and print a paper performance summary from persisted state."""

    args = _build_parser().parse_args(argv)
    engine = create_engine(args.database_url)
    try:
        with Session(engine) as session:
            report = PaperPerformanceReportService().build(
                session,
                run_mode=cast(OperationalRunMode, args.run_mode),
                benchmark_start_date=_optional_date(args.benchmark_start_date),
                benchmark_end_date=_optional_date(args.benchmark_end_date),
            )
    finally:
        engine.dispose()

    print(json.dumps(_report_payload(report), sort_keys=True))
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Summarize paper account performance.")
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--run-mode", default="paper", choices=("dev", "paper", "live"))
    parser.add_argument("--benchmark-start-date")
    parser.add_argument("--benchmark-end-date")
    return parser


def _optional_date(value: str | None) -> date | None:
    if value is None:
        return None
    return date.fromisoformat(value)


def _decimal_value(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return str(value)


def _report_payload(report: PaperPerformanceReport) -> dict[str, Any]:
    return {
        "run_mode": report.run_mode,
        "as_of": report.as_of.isoformat(),
        "account_as_of": report.account_as_of,
        "account": {
            "starting_equity": str(report.starting_equity),
            "latest_equity": str(report.latest_equity),
            "cash": str(report.cash),
            "buying_power": str(report.buying_power),
            "total_return": _decimal_value(report.total_return),
        },
        "pnl": {
            "realized_pnl": _decimal_value(report.realized_pnl),
            "unrealized_pnl": _decimal_value(report.unrealized_pnl),
            "total_pnl": _decimal_value(report.total_pnl),
        },
        "latest_run": {
            "run_id": report.latest_run.run_id,
            "signal_date": (
                report.latest_run.signal_date.isoformat()
                if report.latest_run.signal_date is not None
                else None
            ),
            "status": report.latest_run.status,
            "order_count": report.latest_run.order_count,
            "fill_count": report.latest_run.fill_count,
            "turnover_notional": str(report.latest_run.turnover_notional),
            "turnover_ratio": _decimal_value(report.latest_run.turnover_ratio),
        },
        "positions": [
            {
                "symbol": position.symbol,
                "quantity": str(position.quantity),
                "market_value": str(position.market_value),
                "weight": _decimal_value(position.weight),
                "average_cost": _decimal_value(position.average_cost),
            }
            for position in report.positions
        ],
        "benchmarks": [
            {
                "name": benchmark.name,
                "start_date": (
                    benchmark.start_date.isoformat() if benchmark.start_date is not None else None
                ),
                "end_date": (
                    benchmark.end_date.isoformat() if benchmark.end_date is not None else None
                ),
                "total_return": _decimal_value(benchmark.total_return),
                "missing_symbols": list(benchmark.missing_symbols),
            }
            for benchmark in report.benchmarks
        ],
    }


if __name__ == "__main__":
    raise SystemExit(main())
