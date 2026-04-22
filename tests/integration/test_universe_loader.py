from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

import psycopg
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from quant_core.data.ingestion.universe import UniverseLoaderService
from quant_core.data.models.market_data import Instrument

ROOT = Path(__file__).resolve().parents[2]


def test_universe_loader_upserts_canonical_instruments_from_yaml(tmp_path: Path) -> None:
    database_name = f"quant_core_universe_{uuid4().hex}"
    target_url = f"postgresql+psycopg://quant:quant@127.0.0.1:5432/{database_name}"
    universe_path = tmp_path / "universe.yaml"

    with psycopg.connect(
        "postgresql://quant:quant@127.0.0.1:5432/postgres", autocommit=True
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

        universe_path.write_text(
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

        service = UniverseLoaderService()

        with Session(engine) as session:
            result = service.load_from_file(session, universe_path)
            session.commit()

        assert result.version == 1
        assert result.upserted_instruments == 2

        universe_path.write_text(
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
    name: Vanguard Total Bond Market ETF Updated
    category: aggregate_bonds
    exchange: NASDAQ
    is_active: false
    flags: []
""".strip()
        )

        with Session(engine) as session:
            result = service.load_from_file(session, universe_path)
            session.commit()

        with Session(engine) as session:
            instrument_count = session.scalar(select(func.count()).select_from(Instrument))
            instruments = session.scalars(select(Instrument).order_by(Instrument.symbol)).all()

        assert result.upserted_instruments == 2
        assert instrument_count == 2
        assert [
            (instrument.symbol, instrument.name, instrument.is_active) for instrument in instruments
        ] == [
            ("BND", "Vanguard Total Bond Market ETF Updated", False),
            ("SPY", "SPDR S&P 500 ETF Trust", True),
        ]
    finally:
        engine.dispose()

        if previous_url is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = previous_url

        with psycopg.connect(
            "postgresql://quant:quant@127.0.0.1:5432/postgres", autocommit=True
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
