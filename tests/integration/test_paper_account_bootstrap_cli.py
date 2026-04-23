from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import psycopg
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from quant_core.data import SnapshotRepository
from quant_core.execution.paper_account_cli import main

ROOT = Path(__file__).resolve().parents[2]


def test_paper_account_bootstrap_cli_stores_latest_paper_snapshot() -> None:
    database_name = f"quant_core_paper_account_{uuid4().hex}"
    target_url = f"postgresql+psycopg://quant:quant@127.0.0.1:5432/{database_name}"
    engine = _migrated_engine(database_name=database_name, target_url=target_url)

    try:
        first_exit = main(
            [
                "--database-url",
                target_url,
                "--cash",
                "100000.000000",
                "--equity",
                "100000.000000",
                "--buying-power",
                "100000.000000",
                "--as-of",
                "2026-04-23T22:05:00+02:00",
            ]
        )
        second_exit = main(
            [
                "--database-url",
                target_url,
                "--cash",
                "95000.000000",
                "--equity",
                "99000.000000",
                "--buying-power",
                "97000.000000",
                "--as-of",
                "2026-04-23T22:05:00+02:00",
            ]
        )

        with Session(engine) as session:
            latest = SnapshotRepository().latest_account_snapshot(session, run_mode="paper")

        assert first_exit == 0
        assert second_exit == 0
        assert latest is not None
        assert latest.cash == 95000
        assert latest.equity == 99000
        assert latest.buying_power == 97000
        assert latest.as_of == datetime(2026, 4, 23, 20, 5, tzinfo=UTC)
    finally:
        _drop_database(engine=engine, database_name=database_name)


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
