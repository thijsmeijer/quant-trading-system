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

from quant_core.data import AccountSnapshotWrite, SnapshotRepository
from quant_core.data.models import BarsDaily, Instrument, RawBarsDaily
from quant_core.portfolio import PersistedTargetPositionBuilder
from quant_core.research.daily_bars import ResearchDailyBar, ResearchDataset
from quant_core.strategy import MomentumRotationStrategy, MomentumStrategyConfig

ROOT = Path(__file__).resolve().parents[2]
FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "canonical_daily_bars.json"


def test_persisted_target_position_builder_uses_account_state_and_close_prices() -> None:
    database_name = f"quant_core_target_positions_{uuid4().hex}"
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
        config = MomentumStrategyConfig(
            version="v1",
            lookback_bars=2,
            trend_lookback_bars=3,
            top_n=1,
        )

        with Session(engine) as session:
            _seed_instruments(session)
            _seed_reference_bars(session)

            snapshot_repository = SnapshotRepository()
            snapshot_repository.store_account_snapshot(
                session,
                AccountSnapshotWrite(
                    run_mode="paper",
                    cash=Decimal("100000.000000"),
                    equity=Decimal("100000.000000"),
                    buying_power=Decimal("100000.000000"),
                    as_of=datetime(2026, 4, 22, 20, 1, tzinfo=UTC),
                ),
            )

            strategy = MomentumRotationStrategy()
            persisted_strategy = strategy.execute(
                session,
                dataset=dataset,
                signal_date=date(2026, 4, 22),
                run_mode="paper",
                config=config,
                started_at=datetime(2026, 4, 22, 21, 15, tzinfo=UTC),
            )

            builder = PersistedTargetPositionBuilder()
            stored_positions = builder.build_for_strategy_run(
                session,
                strategy_run_id=persisted_strategy.run.id,
                run_mode="paper",
                generated_at=datetime(2026, 4, 22, 21, 16, tzinfo=UTC),
            )
            session.commit()

        assert len(stored_positions) == 1
        assert stored_positions[0].allocation_key == "SPY"
        assert stored_positions[0].target_weight == Decimal("1.000000")
        assert stored_positions[0].target_notional == Decimal("100000.000000")
        assert stored_positions[0].target_quantity == Decimal("196.850394")
        assert stored_positions[0].reference_price == Decimal("508.000000")
        assert stored_positions[0].symbol == "SPY"
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
