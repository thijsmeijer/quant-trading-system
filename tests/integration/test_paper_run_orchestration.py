from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import psycopg
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from quant_core.broker import FakeBrokerGateway
from quant_core.data import (
    AccountSnapshotWrite,
    OrderRepository,
    SnapshotRepository,
    StrategyRunRepository,
)
from quant_core.data.models import BarsDaily, Instrument, RawBarsDaily, TradingCalendar
from quant_core.execution import PaperRunOrchestrator, PaperRunTimestamps
from quant_core.research.daily_bars import ResearchDailyBar, ResearchDataset
from quant_core.risk import PreTradeRiskConfig
from quant_core.strategy import MomentumStrategyConfig

ROOT = Path(__file__).resolve().parents[2]
FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "canonical_daily_bars.json"


def test_paper_run_orchestrator_executes_the_full_paper_path() -> None:
    database_name = f"quant_core_paper_run_{uuid4().hex}"
    target_url = f"postgresql+psycopg://quant:quant@127.0.0.1:5432/{database_name}"
    engine = _migrated_engine(database_name=database_name, target_url=target_url)

    try:
        dataset = ResearchDataset.from_bars(
            [
                *_fixture_bars(),
                _future_bar("SPY", adjusted_close="509.000000"),
                _future_bar("BND", adjusted_close="72.500000"),
            ]
        )
        with Session(engine) as session:
            _seed_instruments(session)
            _seed_reference_bars(session)
            _seed_calendar(session)
            SnapshotRepository().store_account_snapshot(
                session,
                AccountSnapshotWrite(
                    run_mode="paper",
                    cash=Decimal("100000.000000"),
                    equity=Decimal("100000.000000"),
                    buying_power=Decimal("100000.000000"),
                    as_of=datetime(2026, 4, 22, 20, 1, tzinfo=UTC),
                ),
            )

            orchestrator = PaperRunOrchestrator(
                broker=FakeBrokerGateway(
                    auto_fill=True,
                    fill_price_by_symbol={"SPY": Decimal("508.000000")},
                )
            )
            summary = orchestrator.run(
                session,
                dataset=dataset,
                signal_date=date(2026, 4, 22),
                strategy_config=MomentumStrategyConfig(
                    version="v1",
                    lookback_bars=2,
                    trend_lookback_bars=3,
                    top_n=1,
                ),
                risk_config=PreTradeRiskConfig(
                    max_gross_exposure=Decimal("1.000000"),
                    max_position_notional=Decimal("100000.000000"),
                    max_open_orders=0,
                ),
                timestamps=PaperRunTimestamps(
                    strategy_started_at=datetime(2026, 4, 22, 21, 15, tzinfo=UTC),
                    positions_generated_at=datetime(2026, 4, 22, 21, 16, tzinfo=UTC),
                    risk_checked_at=datetime(2026, 4, 22, 21, 17, tzinfo=UTC),
                    orders_created_at=datetime(2026, 4, 22, 21, 18, tzinfo=UTC),
                    orders_submitted_at=datetime(2026, 4, 22, 21, 19, tzinfo=UTC),
                    state_refreshed_at=datetime(2026, 4, 22, 21, 20, tzinfo=UTC),
                    reconciliation_at=datetime(2026, 4, 22, 21, 21, tzinfo=UTC),
                ),
            )
            rerun_summary = orchestrator.run(
                session,
                dataset=dataset,
                signal_date=date(2026, 4, 22),
                strategy_config=MomentumStrategyConfig(
                    version="v1",
                    lookback_bars=2,
                    trend_lookback_bars=3,
                    top_n=1,
                ),
                risk_config=PreTradeRiskConfig(
                    max_gross_exposure=Decimal("1.000000"),
                    max_position_notional=Decimal("100000.000000"),
                    max_open_orders=0,
                ),
                timestamps=PaperRunTimestamps(
                    strategy_started_at=datetime(2026, 4, 22, 21, 30, tzinfo=UTC),
                    positions_generated_at=datetime(2026, 4, 22, 21, 31, tzinfo=UTC),
                    risk_checked_at=datetime(2026, 4, 22, 21, 32, tzinfo=UTC),
                    orders_created_at=datetime(2026, 4, 22, 21, 33, tzinfo=UTC),
                    orders_submitted_at=datetime(2026, 4, 22, 21, 34, tzinfo=UTC),
                    state_refreshed_at=datetime(2026, 4, 22, 21, 35, tzinfo=UTC),
                    reconciliation_at=datetime(2026, 4, 22, 21, 36, tzinfo=UTC),
                ),
            )
            strategy_run = StrategyRunRepository().get_run(
                session,
                strategy_run_id=summary.run_id,
            )
            orders = OrderRepository().list_orders(
                session,
                run_mode="paper",
                strategy_run_id=summary.run_id,
            )
            fills = OrderRepository().list_fills(
                session,
                run_mode="paper",
                strategy_run_id=summary.run_id,
            )
            session.commit()

        assert summary.signal_date == date(2026, 4, 22)
        assert summary.approved is True
        assert summary.order_count == 1
        assert summary.fill_count == 1
        assert summary.reconciliation_critical_rows == 0
        assert summary.incident_count == 0
        assert rerun_summary == summary
        assert strategy_run is not None
        assert strategy_run.id == summary.run_id
        assert strategy_run.status == "completed"
        assert len(orders) == 1
        assert len(fills) == 1
        assert strategy_run.metadata_json is not None
        paper_run_report = strategy_run.metadata_json["paper_run_report"]
        assert paper_run_report["run_id"] == summary.run_id
        assert paper_run_report["approved"] is True
        assert paper_run_report["order_count"] == 1
        assert paper_run_report["fill_count"] == 1
        assert paper_run_report["reconciliation_critical_rows"] == 0
    finally:
        _drop_database(engine=engine, database_name=database_name)


def _fixture_bars() -> list[ResearchDailyBar]:
    payload = json.loads(FIXTURE_PATH.read_text())
    return [ResearchDailyBar.model_validate(item) for item in payload]


def _future_bar(symbol: str, adjusted_close: str) -> ResearchDailyBar:
    return ResearchDailyBar.model_validate(
        {
            "symbol": symbol,
            "bar_date": "2026-04-23",
            "open": adjusted_close,
            "high": adjusted_close,
            "low": adjusted_close,
            "close": adjusted_close,
            "adjusted_close": adjusted_close,
            "volume": 999999,
        }
    )


def _migrated_engine(*, database_name: str, target_url: str) -> Engine:
    with psycopg.connect(
        "postgresql://quant:quant@127.0.0.1:5432/postgres",
        autocommit=True,
    ) as connection:
        connection.execute(f'CREATE DATABASE "{database_name}"')

    alembic_config = Config(str(ROOT / "alembic.ini"))
    alembic_config.set_main_option("script_location", str(ROOT / "migrations"))
    alembic_config.set_main_option("sqlalchemy.url", target_url)

    previous_url = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = target_url
    engine = create_engine(target_url)

    try:
        command.upgrade(alembic_config, "head")
    finally:
        if previous_url is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = previous_url

    return engine


def _seed_instruments(session: Session) -> None:
    session.add_all(
        [
            Instrument(
                symbol="SPY",
                name="SPY ETF",
                category="broad_us_equity",
                exchange="ARCA",
                is_active=True,
            ),
            Instrument(
                symbol="BND",
                name="BND ETF",
                category="fixed_income",
                exchange="ARCA",
                is_active=True,
            ),
        ]
    )
    session.commit()


def _seed_reference_bars(session: Session) -> None:
    instruments = {
        instrument.symbol: instrument.id for instrument in session.query(Instrument).all()
    }

    raw_spy = RawBarsDaily(
        instrument_id=instruments["SPY"],
        vendor="test_vendor",
        bar_date=date(2026, 4, 22),
        payload={"symbol": "SPY"},
        fetched_at=datetime(2026, 4, 22, 20, 0, tzinfo=UTC),
    )
    raw_bnd = RawBarsDaily(
        instrument_id=instruments["BND"],
        vendor="test_vendor",
        bar_date=date(2026, 4, 22),
        payload={"symbol": "BND"},
        fetched_at=datetime(2026, 4, 22, 20, 0, tzinfo=UTC),
    )
    session.add_all([raw_spy, raw_bnd])
    session.flush()

    session.add_all(
        [
            BarsDaily(
                instrument_id=instruments["SPY"],
                raw_bar_id=raw_spy.id,
                bar_date=date(2026, 4, 22),
                open=Decimal("506.000000"),
                high=Decimal("509.000000"),
                low=Decimal("505.000000"),
                close=Decimal("508.000000"),
                adjusted_close=Decimal("508.000000"),
                volume=1200000,
            ),
            BarsDaily(
                instrument_id=instruments["BND"],
                raw_bar_id=raw_bnd.id,
                bar_date=date(2026, 4, 22),
                open=Decimal("72.300000"),
                high=Decimal("72.500000"),
                low=Decimal("72.200000"),
                close=Decimal("72.400000"),
                adjusted_close=Decimal("72.400000"),
                volume=830000,
            ),
        ]
    )
    session.commit()


def _seed_calendar(session: Session) -> None:
    session.add(
        TradingCalendar(
            trading_date=date(2026, 4, 23),
            market_open_utc=datetime(2026, 4, 23, 13, 30, tzinfo=UTC),
            market_close_utc=datetime(2026, 4, 23, 20, 0, tzinfo=UTC),
            is_open=True,
            is_early_close=False,
        )
    )
    session.commit()


def _drop_database(*, engine: Engine, database_name: str) -> None:
    engine.dispose()
    with psycopg.connect(
        "postgresql://quant:quant@127.0.0.1:5432/postgres",
        autocommit=True,
    ) as connection:
        connection.execute(
            f"""
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = '{database_name}'
              AND pid <> pg_backend_pid()
            """
        )
        connection.execute(f'DROP DATABASE IF EXISTS "{database_name}"')
