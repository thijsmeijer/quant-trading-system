from __future__ import annotations

import os
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

import psycopg
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from quant_core.dashboard import BurnInReportService
from quant_core.data import StrategyRunCreate, StrategyRunRepository
from quant_core.reporting.paper_runs import PaperRunReport
from quant_core.settings import PaperRunExpectationConfig

ROOT = Path(__file__).resolve().parents[2]


def test_burnin_report_service_reads_archived_runs_from_persistence() -> None:
    database_name = f"quant_core_burnin_{uuid4().hex}"
    target_url = f"postgresql+psycopg://quant:quant@127.0.0.1:5432/{database_name}"
    engine = _migrated_engine(database_name=database_name, target_url=target_url)

    try:
        with Session(engine) as session:
            _store_run(
                session,
                config_hash="cfg-1",
                signal_date=date(2026, 4, 23),
                fill_count=1,
                rejected_order_count=0,
                open_incident_count=0,
            )
            _store_run(
                session,
                config_hash="cfg-2",
                signal_date=date(2026, 4, 24),
                fill_count=0,
                rejected_order_count=1,
                open_incident_count=1,
            )

            report = BurnInReportService().build(
                session,
                run_mode="paper",
                expectation=PaperRunExpectationConfig(
                    require_approved=True,
                    min_fill_ratio=1,
                    max_rejected_order_count=0,
                    max_reconciliation_critical_rows=0,
                    max_open_incident_count=0,
                ),
                limit=60,
            )
            session.commit()

        assert report.summary.total_runs == 2
        assert report.summary.runs_with_anomalies == 1
        assert report.summary.runs_with_critical_issues == 1
        assert report.summary.consecutive_clean_runs == 0
        assert report.rows[0].signal_date == date(2026, 4, 24)
        assert report.rows[0].anomaly_count > 0
        assert report.rows[1].signal_date == date(2026, 4, 23)
    finally:
        _drop_database(engine=engine, database_name=database_name)


def _store_run(
    session: Session,
    *,
    config_hash: str,
    signal_date: date,
    fill_count: int,
    rejected_order_count: int,
    open_incident_count: int,
) -> None:
    repository = StrategyRunRepository()
    run = repository.create_run(
        session,
        StrategyRunCreate(
            run_mode="paper",
            strategy_name="momentum_rotation",
            config_version="v1",
            config_hash=config_hash,
            signal_date=signal_date,
            execution_date=signal_date,
            status="running",
            started_at=datetime(
                signal_date.year, signal_date.month, signal_date.day, 21, 0, tzinfo=UTC
            ),
            metadata_json={"config": {"version": "v1"}},
        ),
    )
    persisted_run = repository.update_run_status(
        session,
        strategy_run_id=run.id,
        status="completed",
        completed_at=datetime(
            signal_date.year, signal_date.month, signal_date.day, 21, 15, tzinfo=UTC
        ),
    )
    report = PaperRunReport(
        run_id=persisted_run.id,
        run_mode=persisted_run.run_mode,
        strategy_name=persisted_run.strategy_name,
        signal_date=persisted_run.signal_date,
        execution_date=persisted_run.execution_date,
        status=persisted_run.status,
        approved=True,
        failed_reason_codes=(),
        order_count=1,
        fill_count=fill_count,
        rejected_order_count=rejected_order_count,
        open_incident_count=open_incident_count,
        reconciliation_total_rows=0,
        reconciliation_mismatched_rows=0,
        reconciliation_critical_rows=0,
        latest_account_equity=None,
        latest_gross_exposure=None,
        generated_at=datetime(
            signal_date.year, signal_date.month, signal_date.day, 21, 15, tzinfo=UTC
        ),
    )
    repository.update_run_status(
        session,
        strategy_run_id=run.id,
        status="completed",
        completed_at=datetime(
            signal_date.year, signal_date.month, signal_date.day, 21, 15, tzinfo=UTC
        ),
        metadata_json={"paper_run_report": report.as_metadata()},
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
