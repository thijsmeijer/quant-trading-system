from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

import psycopg
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect

ROOT = Path(__file__).resolve().parents[2]


def test_phase1_migration_builds_core_tables() -> None:
    database_name = f"quant_core_test_{uuid4().hex}"
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

    try:
        command.upgrade(alembic_config, "head")

        engine = create_engine(target_url)
        try:
            inspector = inspect(engine)
            assert {
                "instruments",
                "trading_calendar",
                "raw_bars_daily",
                "bars_daily",
                "alembic_version",
            }.issubset(set(inspector.get_table_names()))
        finally:
            engine.dispose()
    finally:
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
