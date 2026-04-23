from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from quant_core.data import AccountSnapshotWrite, SnapshotRepository, StrategyRunRepository
from quant_core.data.models import BarsDaily, Instrument, RawBarsDaily, TradingCalendar
from quant_core.execution.cli import main

ROOT = Path(__file__).resolve().parents[2]


def test_paper_run_cli_can_load_persisted_bars_without_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_name = f"quant_core_paper_cli_{uuid4().hex}"
    target_url = f"postgresql+psycopg://quant:quant@127.0.0.1:5432/{database_name}"
    engine = _migrated_engine(database_name=database_name, target_url=target_url)

    try:
        with Session(engine) as session:
            _seed_instruments(session)
            _seed_bars(session)
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
            session.commit()

        exit_code = main(
            [
                "--database-url",
                target_url,
                "--signal-date",
                "2026-04-22",
                "--lookback-bars",
                "2",
                "--trend-lookback-bars",
                "3",
                "--top-n",
                "1",
                "--auto-fill",
                "--fill-price",
                "SPY=508.000000",
            ]
        )
        output = json.loads(capsys.readouterr().out)

        with Session(engine) as session:
            latest_run = StrategyRunRepository().latest_run(session, run_mode="paper")
            session.commit()

        assert exit_code == 0
        assert output["approved"] is True
        assert output["order_count"] == 1
        assert output["fill_count"] == 1
        assert latest_run is not None
        assert latest_run.execution_date == date(2026, 4, 23)
    finally:
        _drop_database(engine=engine, database_name=database_name)


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


def _seed_bars(session: Session) -> None:
    instruments = {
        instrument.symbol: instrument.id for instrument in session.query(Instrument).all()
    }
    for symbol, prices in {
        "SPY": ("500.000000", "504.000000", "508.000000"),
        "BND": ("72.500000", "72.450000", "72.400000"),
    }.items():
        for offset, close in enumerate(prices):
            bar_date = date(2026, 4, 20 + offset)
            raw_bar = RawBarsDaily(
                instrument_id=instruments[symbol],
                vendor="test_vendor",
                bar_date=bar_date,
                payload={"symbol": symbol, "bar_date": bar_date.isoformat()},
                fetched_at=datetime(2026, 4, 22, 20, 0, tzinfo=UTC),
            )
            session.add(raw_bar)
            session.flush()
            session.add(
                BarsDaily(
                    instrument_id=instruments[symbol],
                    raw_bar_id=raw_bar.id,
                    bar_date=bar_date,
                    open=Decimal(close),
                    high=Decimal(close),
                    low=Decimal(close),
                    close=Decimal(close),
                    adjusted_close=Decimal(close),
                    volume=1_000_000,
                )
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
