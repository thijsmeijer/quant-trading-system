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

from quant_core.data.models import Instrument
from quant_core.research.daily_bars import ResearchDailyBar, ResearchDataset
from quant_core.strategy import MomentumRotationStrategy, MomentumStrategyConfig

ROOT = Path(__file__).resolve().parents[2]
FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "canonical_daily_bars.json"


def test_momentum_strategy_execute_persists_run_signals_and_target_weights() -> None:
    database_name = f"quant_core_momentum_strategy_{uuid4().hex}"
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
            strategy = MomentumRotationStrategy()
            persisted = strategy.execute(
                session,
                dataset=dataset,
                signal_date=date(2026, 4, 22),
                run_mode="paper",
                config=config,
                started_at=datetime(2026, 4, 22, 21, 15, tzinfo=UTC),
            )
            session.commit()

        with Session(engine) as session:
            strategy = MomentumRotationStrategy()
            persisted_again = strategy.execute(
                session,
                dataset=dataset,
                signal_date=date(2026, 4, 22),
                run_mode="paper",
                config=config,
                started_at=datetime(2026, 4, 22, 21, 16, tzinfo=UTC),
                execution_date=date(2026, 4, 23),
            )
            session.rollback()

        assert persisted.run.strategy_name == "momentum_rotation"
        assert persisted.run.run_mode == "paper"
        assert persisted.run.status == "completed"
        assert persisted.run.config_version == "v1"
        assert persisted.run.config_hash == config.config_hash()
        assert persisted.run.metadata_json == {
            "config": config.metadata(),
            "eligible_symbol_count": 2,
            "selected_symbols": ["SPY"],
        }
        assert [
            (signal.symbol, signal.rank, signal.is_selected) for signal in persisted.signals
        ] == [
            ("SPY", 1, True),
            ("BND", 2, False),
        ]
        assert [
            (weight.allocation_key, weight.target_weight, weight.symbol)
            for weight in persisted.target_weights
        ] == [("SPY", Decimal("1.000000"), "SPY")]
        assert persisted_again.run.id != persisted.run.id
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
