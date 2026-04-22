from __future__ import annotations

import os
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import psycopg
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from quant_core.data.models.market_data import BarsDaily, Instrument, RawBarsDaily, TradingCalendar
from quant_core.data.validation.daily_bars import DailyBarValidationService

ROOT = Path(__file__).resolve().parents[2]


def test_daily_bar_validator_reports_missing_stale_and_price_sanity_issues() -> None:
    database_name = f"quant_core_validate_{uuid4().hex}"
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
            spy = Instrument(
                symbol="SPY",
                name="SPDR S&P 500 ETF Trust",
                category="broad_us_equity",
                exchange="ARCA",
                is_active=True,
            )
            qqq = Instrument(
                symbol="QQQ",
                name="Invesco QQQ Trust",
                category="broad_us_equity",
                exchange="NASDAQ",
                is_active=True,
            )
            session.add_all([spy, qqq])
            session.flush()

            session.add_all(
                [
                    TradingCalendar(
                        trading_date=date(2026, 4, 20),
                        market_open_utc=datetime(2026, 4, 20, 13, 30, tzinfo=UTC),
                        market_close_utc=datetime(2026, 4, 20, 20, 0, tzinfo=UTC),
                        is_open=True,
                        is_early_close=False,
                    ),
                    TradingCalendar(
                        trading_date=date(2026, 4, 21),
                        market_open_utc=datetime(2026, 4, 21, 13, 30, tzinfo=UTC),
                        market_close_utc=datetime(2026, 4, 21, 20, 0, tzinfo=UTC),
                        is_open=True,
                        is_early_close=False,
                    ),
                    TradingCalendar(
                        trading_date=date(2026, 4, 22),
                        market_open_utc=datetime(2026, 4, 22, 13, 30, tzinfo=UTC),
                        market_close_utc=datetime(2026, 4, 22, 20, 0, tzinfo=UTC),
                        is_open=True,
                        is_early_close=False,
                    ),
                ]
            )
            session.flush()

            raw_row = RawBarsDaily(
                instrument_id=spy.id,
                vendor="test_vendor",
                bar_date=date(2026, 4, 20),
                payload={
                    "symbol": "SPY",
                    "vendor": "test_vendor",
                    "bar_date": "2026-04-20",
                },
                fetched_at=datetime(2026, 4, 22, 21, 0, tzinfo=UTC),
            )
            session.add(raw_row)
            session.flush()

            session.add(
                BarsDaily(
                    instrument_id=spy.id,
                    raw_bar_id=raw_row.id,
                    bar_date=date(2026, 4, 20),
                    open=Decimal("100.000000"),
                    high=Decimal("99.000000"),
                    low=Decimal("101.000000"),
                    close=Decimal("98.000000"),
                    adjusted_close=Decimal("98.000000"),
                    volume=1234,
                )
            )
            session.commit()

        with Session(engine) as session:
            report = DailyBarValidationService().validate(
                session,
                as_of=date(2026, 4, 22),
            )

        assert report.checked_as_of == date(2026, 4, 22)
        assert report.raw_duplicates == []
        assert report.normalized_duplicates == []
        assert ("QQQ", date(2026, 4, 20)) in {
            (issue.symbol, issue.bar_date) for issue in report.missing_bars
        }
        assert ("SPY", date(2026, 4, 21)) in {
            (issue.symbol, issue.bar_date) for issue in report.missing_bars
        }
        assert {issue.symbol for issue in report.stale_symbols} == {"SPY", "QQQ"}
        assert report.price_sanity_issues[0].symbol == "SPY"
        assert report.price_sanity_issues[0].bar_date == date(2026, 4, 20)
        assert "high" in report.price_sanity_issues[0].detail
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
