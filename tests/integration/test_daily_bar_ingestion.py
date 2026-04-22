from __future__ import annotations

import os
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import psycopg
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from quant_core.data.ingestion.daily_bars import DailyBarIngestionService, VendorDailyBar
from quant_core.data.models.market_data import BarsDaily, Instrument, RawBarsDaily

ROOT = Path(__file__).resolve().parents[2]


def test_daily_bar_ingestion_is_idempotent_for_raw_and_normalized_rows() -> None:
    database_name = f"quant_core_ingest_{uuid4().hex}"
    target_url = f"postgresql+psycopg://quant:quant@127.0.0.1:5432/{database_name}"

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

        with Session(engine) as session:
            session.add(
                Instrument(
                    symbol="SPY",
                    name="SPDR S&P 500 ETF Trust",
                    category="broad_us_equity",
                    exchange="ARCA",
                    is_active=True,
                )
            )
            session.commit()

        bar = VendorDailyBar(
            symbol="SPY",
            vendor="test_vendor",
            bar_date=date(2026, 4, 20),
            open=Decimal("598.120000"),
            high=Decimal("603.000000"),
            low=Decimal("597.400000"),
            close=Decimal("602.330000"),
            adjusted_close=Decimal("602.330000"),
            volume=123456789,
            fetched_at=datetime(2026, 4, 20, 21, 15, tzinfo=UTC),
            source_payload={
                "ticker": "SPY",
                "date": "2026-04-20",
                "prices": {"o": "598.12", "h": "603.00", "l": "597.40", "c": "602.33"},
                "volume": 123456789,
            },
        )

        service = DailyBarIngestionService()

        with Session(engine) as session:
            service.ingest(session, [bar])
            session.commit()

        with Session(engine) as session:
            service.ingest(session, [bar])
            session.commit()

        with Session(engine) as session:
            raw_count = session.scalar(select(func.count()).select_from(RawBarsDaily))
            normalized_count = session.scalar(select(func.count()).select_from(BarsDaily))
            raw_row = session.execute(select(RawBarsDaily)).scalar_one()
            normalized_row = session.execute(select(BarsDaily)).scalar_one()

        assert raw_count == 1
        assert normalized_count == 1
        assert raw_row.vendor == "test_vendor"
        assert raw_row.bar_date == date(2026, 4, 20)
        assert raw_row.payload["symbol"] == "SPY"
        assert normalized_row.raw_bar_id == raw_row.id
        assert normalized_row.close == Decimal("602.330000")
        assert normalized_row.adjusted_close == Decimal("602.330000")
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
