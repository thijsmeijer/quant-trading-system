from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from quant_core.data import AccountSnapshotWrite, SnapshotRepository, StrategyRunRepository
from quant_core.data.bootstrap_cli import main as bootstrap_main
from quant_core.data.ingestion.daily_bars_cli import main as import_main
from quant_core.data.ingestion.trading_calendar_cli import main as trading_calendar_main
from quant_core.data.models import Instrument
from quant_core.execution.cli import main as paper_run_main

ROOT = Path(__file__).resolve().parents[2]


def test_local_bootstrap_cli_applies_schema_and_loads_universe(tmp_path: Path) -> None:
    database_name = f"quant_core_bootstrap_{uuid4().hex}"
    target_url = f"postgresql+psycopg://quant:quant@127.0.0.1:5432/{database_name}"
    universe_path = tmp_path / "universe.yaml"
    _write_universe(universe_path)
    engine = _create_database(database_name=database_name, target_url=target_url)

    try:
        first_exit = bootstrap_main(
            ["--database-url", target_url, "--universe-path", str(universe_path)]
        )
        second_exit = bootstrap_main(
            ["--database-url", target_url, "--universe-path", str(universe_path)]
        )

        with Session(engine) as session:
            instrument_count = session.scalar(select(func.count()).select_from(Instrument))
            symbols = [
                instrument.symbol
                for instrument in session.scalars(
                    select(Instrument).order_by(Instrument.symbol)
                ).all()
            ]

        assert first_exit == 0
        assert second_exit == 0
        assert instrument_count == 2
        assert symbols == ["BND", "SPY"]
    finally:
        _drop_database(engine=engine, database_name=database_name)


def test_local_bootstrap_can_prepare_instruments_for_import_and_paper_run(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_name = f"quant_core_bootstrap_sequence_{uuid4().hex}"
    target_url = f"postgresql+psycopg://quant:quant@127.0.0.1:5432/{database_name}"
    universe_path = tmp_path / "universe.yaml"
    calendar_path = tmp_path / "trading_calendar.json"
    input_path = tmp_path / "vendor_daily_bars.json"
    _write_universe(universe_path)
    _write_trading_calendar(calendar_path)
    _write_vendor_bars(input_path)
    engine = _create_database(database_name=database_name, target_url=target_url)

    try:
        bootstrap_exit = bootstrap_main(
            ["--database-url", target_url, "--universe-path", str(universe_path)]
        )
        calendar_exit = trading_calendar_main(
            ["--database-url", target_url, "--input-json", str(calendar_path)]
        )

        with Session(engine) as session:
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

        import_exit = import_main(["--database-url", target_url, "--input-json", str(input_path)])
        paper_exit = paper_run_main(
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
        output = json.loads(capsys.readouterr().out.strip().splitlines()[-1])

        with Session(engine) as session:
            latest_run = StrategyRunRepository().latest_run(session, run_mode="paper")
            session.commit()

        assert bootstrap_exit == 0
        assert calendar_exit == 0
        assert import_exit == 0
        assert paper_exit == 0
        assert output["approved"] is True
        assert latest_run is not None
        assert latest_run.execution_date == date(2026, 4, 23)
    finally:
        _drop_database(engine=engine, database_name=database_name)


def _write_universe(path: Path) -> None:
    path.write_text(
        """
version: 1
as_of: 2026-04-20
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
  - symbol: BND
    name: Vanguard Total Bond Market ETF
    category: aggregate_bonds
    exchange: NASDAQ
    is_active: true
    flags: []
""".strip()
    )


def _write_vendor_bars(path: Path) -> None:
    payload: list[dict[str, object]] = []
    for symbol, prices in {
        "SPY": ("500.000000", "504.000000", "508.000000"),
        "BND": ("72.500000", "72.450000", "72.400000"),
    }.items():
        for offset, close in enumerate(prices):
            bar_date = date(2026, 4, 20 + offset)
            payload.append(
                {
                    "symbol": symbol,
                    "vendor": "test_vendor",
                    "bar_date": bar_date.isoformat(),
                    "open": close,
                    "high": close,
                    "low": close,
                    "close": close,
                    "adjusted_close": close,
                    "volume": 1_000_000,
                    "fetched_at": datetime(2026, 4, 22, 20, 0, tzinfo=UTC).isoformat(),
                    "source_payload": {
                        "symbol": symbol,
                        "bar_date": bar_date.isoformat(),
                        "close": close,
                    },
                }
            )
    path.write_text(json.dumps(payload))


def _write_trading_calendar(path: Path) -> None:
    path.write_text(
        json.dumps(
            [
                {
                    "trading_date": "2026-04-20",
                    "market_open_utc": "2026-04-20T13:30:00+00:00",
                    "market_close_utc": "2026-04-20T20:00:00+00:00",
                    "is_open": True,
                    "is_early_close": False,
                },
                {
                    "trading_date": "2026-04-21",
                    "market_open_utc": "2026-04-21T13:30:00+00:00",
                    "market_close_utc": "2026-04-21T20:00:00+00:00",
                    "is_open": True,
                    "is_early_close": False,
                },
                {
                    "trading_date": "2026-04-22",
                    "market_open_utc": "2026-04-22T13:30:00+00:00",
                    "market_close_utc": "2026-04-22T20:00:00+00:00",
                    "is_open": True,
                    "is_early_close": False,
                },
                {
                    "trading_date": "2026-04-23",
                    "market_open_utc": "2026-04-23T13:30:00+00:00",
                    "market_close_utc": "2026-04-23T20:00:00+00:00",
                    "is_open": True,
                    "is_early_close": False,
                },
            ]
        )
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
