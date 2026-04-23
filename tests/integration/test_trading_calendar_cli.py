from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import psycopg
from sqlalchemy import create_engine, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from quant_core.data.bootstrap_cli import LocalBootstrapService
from quant_core.data.ingestion.trading_calendar_cli import main
from quant_core.data.models import TradingCalendar


def test_trading_calendar_cli_loads_and_upserts_rows(tmp_path: Path) -> None:
    database_name = f"quant_core_calendar_{uuid4().hex}"
    target_url = f"postgresql+psycopg://quant:quant@127.0.0.1:5432/{database_name}"
    calendar_path = tmp_path / "trading_calendar.json"
    universe_path = tmp_path / "universe.yaml"
    _write_trading_calendar(calendar_path)
    _write_universe(universe_path)
    engine = _create_database(database_name=database_name, target_url=target_url)

    try:
        LocalBootstrapService().bootstrap(
            database_url=target_url,
            universe_path=universe_path,
        )

        first_exit = main(["--database-url", target_url, "--input-json", str(calendar_path)])
        second_exit = main(["--database-url", target_url, "--input-json", str(calendar_path)])

        with Session(engine) as session:
            rows = session.scalars(
                select(TradingCalendar).order_by(TradingCalendar.trading_date)
            ).all()

        assert first_exit == 0
        assert second_exit == 0
        assert [row.trading_date.isoformat() for row in rows] == [
            "2026-04-23",
            "2026-04-24",
        ]
        assert rows[0].market_open_utc == datetime(2026, 4, 23, 13, 30, tzinfo=UTC)
        assert rows[1].market_close_utc == datetime(2026, 4, 24, 20, 0, tzinfo=UTC)
    finally:
        _drop_database(engine=engine, database_name=database_name)


def _write_trading_calendar(path: Path) -> None:
    path.write_text(
        json.dumps(
            [
                {
                    "trading_date": "2026-04-23",
                    "market_open_utc": "2026-04-23T15:30:00+02:00",
                    "market_close_utc": "2026-04-23T22:00:00+02:00",
                    "is_open": True,
                },
                {
                    "trading_date": "2026-04-24",
                    "market_open_utc": "2026-04-24T13:30:00+00:00",
                    "market_close_utc": "2026-04-24T20:00:00+00:00",
                    "is_open": True,
                    "is_early_close": False,
                },
            ]
        )
    )


def _write_universe(path: Path) -> None:
    path.write_text(
        """
version: 1
as_of: 2026-04-23
universe:
  name: core_us_etfs
  venue: us_equities
  bar_frequency: daily
  regular_hours_only: true
eligibility:
  min_price: 20
  min_average_daily_volume: 1000000
  min_history_days: 252
  excluded_flags:
    - leveraged
    - inverse
instruments:
  - symbol: SPY
    name: SPDR S&P 500 ETF Trust
    category: broad_us_equity
    exchange: ARCA
    is_active: true
    flags: []
""".strip()
    )


def _create_database(*, database_name: str, target_url: str) -> Engine:
    del target_url
    with psycopg.connect(
        "postgresql://quant:quant@127.0.0.1:5432/postgres",
        autocommit=True,
    ) as connection:
        connection.execute(f'CREATE DATABASE "{database_name}"')

    return create_engine(f"postgresql+psycopg://quant:quant@127.0.0.1:5432/{database_name}")


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
