"""CLI entrypoint for deterministic paper-run execution."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from quant_core.broker import FakeBrokerGateway
from quant_core.execution.paper_run import (
    PaperRunOrchestrator,
    PaperRunSummary,
    PaperRunTimestamps,
)
from quant_core.execution.preflight import PaperRunDataQualityError, PaperRunPreflightService
from quant_core.research import PersistedResearchDatasetLoader
from quant_core.research.daily_bars import ResearchDailyBar, ResearchDataset
from quant_core.risk import PreTradeRiskConfig
from quant_core.strategy import MomentumStrategyConfig


def main(argv: Sequence[str] | None = None) -> int:
    """Run one paper orchestration flow from CLI arguments."""

    args = _build_parser().parse_args(argv)
    timestamps = _default_timestamps()
    summary = run_paper_run(
        database_url=args.database_url,
        bars_json=(Path(args.bars_json) if args.bars_json is not None else None),
        signal_date=date.fromisoformat(args.signal_date),
        universe_path=Path(args.universe_path),
        strategy_config=MomentumStrategyConfig(
            version=args.strategy_version,
            lookback_bars=args.lookback_bars,
            trend_lookback_bars=args.trend_lookback_bars,
            top_n=args.top_n,
            minimum_momentum=Decimal(args.minimum_momentum),
        ),
        risk_config=PreTradeRiskConfig(
            max_gross_exposure=Decimal(args.max_gross_exposure),
            max_position_notional=Decimal(args.max_position_notional),
            max_open_orders=args.max_open_orders,
        ),
        timestamps=timestamps,
        auto_fill=args.auto_fill,
        fill_price_by_symbol=_parse_fill_prices(args.fill_price),
    )
    print(json.dumps(_summary_payload(summary), sort_keys=True))
    return 0


def run_paper_run(
    *,
    database_url: str,
    bars_json: Path | None,
    signal_date: date,
    universe_path: Path,
    strategy_config: MomentumStrategyConfig,
    risk_config: PreTradeRiskConfig,
    timestamps: PaperRunTimestamps,
    auto_fill: bool,
    fill_price_by_symbol: dict[str, Decimal],
) -> PaperRunSummary:
    """Execute one paper run against a target database."""

    engine = create_engine(database_url)
    try:
        with Session(engine) as session:
            try:
                dataset, execution_date = _load_runtime_inputs(
                    session,
                    signal_date=signal_date,
                    bars_json=bars_json,
                    universe_path=universe_path,
                    occurred_at=timestamps.strategy_started_at,
                )
                summary = PaperRunOrchestrator(
                    broker=FakeBrokerGateway(
                        auto_fill=auto_fill,
                        fill_price_by_symbol=fill_price_by_symbol,
                    )
                ).run(
                    session,
                    dataset=dataset,
                    signal_date=signal_date,
                    execution_date=execution_date,
                    strategy_config=strategy_config,
                    risk_config=risk_config,
                    timestamps=timestamps,
                )
                session.commit()
                return summary
            except PaperRunDataQualityError:
                session.commit()
                raise
    finally:
        engine.dispose()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one deterministic paper ETF workflow.")
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--bars-json")
    parser.add_argument("--signal-date", required=True)
    parser.add_argument("--universe-path", default="configs/universe.yaml")
    parser.add_argument("--strategy-version", default="v1")
    parser.add_argument("--lookback-bars", type=int, default=90)
    parser.add_argument("--trend-lookback-bars", type=int, default=200)
    parser.add_argument("--top-n", type=int, default=3)
    parser.add_argument("--minimum-momentum", default="0")
    parser.add_argument("--max-gross-exposure", default="1.000000")
    parser.add_argument("--max-position-notional", default="100000.000000")
    parser.add_argument("--max-open-orders", type=int, default=0)
    parser.add_argument("--fill-price", action="append", default=[])
    parser.add_argument("--auto-fill", action="store_true")
    return parser


def _load_dataset(path: Path) -> ResearchDataset:
    payload = json.loads(path.read_text())
    return ResearchDataset.from_bars([ResearchDailyBar.model_validate(item) for item in payload])


def _load_runtime_inputs(
    session: Session,
    *,
    signal_date: date,
    bars_json: Path | None,
    universe_path: Path,
    occurred_at: datetime,
) -> tuple[ResearchDataset, date | None]:
    if bars_json is not None:
        return _load_dataset(bars_json), None

    PaperRunPreflightService().validate_for_paper_run(
        session,
        universe_path=universe_path,
        signal_date=signal_date,
        occurred_at=occurred_at,
    )
    loaded = PersistedResearchDatasetLoader().load_for_signal_date(
        session,
        signal_date=signal_date,
    )
    return loaded.dataset, loaded.execution_date


def _parse_fill_prices(items: Sequence[str]) -> dict[str, Decimal]:
    prices: dict[str, Decimal] = {}
    for item in items:
        symbol, separator, raw_price = item.partition("=")
        if separator != "=" or not symbol or not raw_price:
            raise ValueError(f"fill prices must use SYMBOL=PRICE format: {item}")
        prices[symbol] = Decimal(raw_price)
    return prices


def _default_timestamps() -> PaperRunTimestamps:
    now = datetime.now(tz=UTC)
    return PaperRunTimestamps(
        strategy_started_at=now,
        positions_generated_at=now,
        risk_checked_at=now,
        orders_created_at=now,
        orders_submitted_at=now,
        state_refreshed_at=now,
        reconciliation_at=now,
    )


def _summary_payload(summary: PaperRunSummary) -> dict[str, int | bool | str]:
    return {
        "approved": summary.approved,
        "fill_count": summary.fill_count,
        "incident_count": summary.incident_count,
        "order_count": summary.order_count,
        "reconciliation_critical_rows": summary.reconciliation_critical_rows,
        "run_id": summary.run_id,
        "signal_date": summary.signal_date.isoformat(),
    }


if __name__ == "__main__":
    raise SystemExit(main())
