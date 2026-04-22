from __future__ import annotations

import os
from datetime import UTC, date, datetime
from pathlib import Path

import psycopg
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from quant_core.dashboard import PromotionReadinessService
from quant_core.data import (
    IncidentRepository,
    IncidentWrite,
    StrategyRunCreate,
    StrategyRunRepository,
)
from quant_core.reconciliation import OperationalReconciliationSummary
from quant_core.reporting.paper_runs import build_paper_run_report
from quant_core.settings import PaperPromotionConfig, PaperRunExpectationConfig

ROOT = Path(__file__).resolve().parents[2]


def test_promotion_readiness_blocks_on_open_critical_incidents() -> None:
    database_name = "quant_core_promotion_" + datetime.now(tz=UTC).strftime("%Y%m%d%H%M%S%f")
    target_url = f"postgresql+psycopg://quant:quant@127.0.0.1:5432/{database_name}"
    engine = _migrated_engine(database_name=database_name, target_url=target_url)

    try:
        with Session(engine) as session:
            _store_completed_run(
                session, run_id_suffix="a", started_at=datetime(2026, 4, 21, 21, 0, tzinfo=UTC)
            )
            _store_completed_run(
                session, run_id_suffix="b", started_at=datetime(2026, 4, 22, 21, 0, tzinfo=UTC)
            )
            readiness_service = PromotionReadinessService()
            config = PaperPromotionConfig(
                minimum_completed_runs=2,
                maximum_open_critical_incidents=0,
                maximum_open_warning_incidents=0,
                manual_approval_required=True,
                latest_run_expectation=PaperRunExpectationConfig(
                    require_approved=True,
                    min_fill_ratio=1,
                    max_rejected_order_count=0,
                    max_reconciliation_critical_rows=0,
                    max_open_incident_count=0,
                ),
            )

            ready_summary = readiness_service.build(session, run_mode="paper", config=config)
            IncidentRepository().create_incident(
                session,
                IncidentWrite(
                    run_mode="paper",
                    incident_type="manual_blocker",
                    severity="critical",
                    status="open",
                    summary="operator detected unresolved issue",
                    occurred_at=datetime(2026, 4, 22, 22, 0, tzinfo=UTC),
                    details={"note": "blocking promotion"},
                ),
            )
            blocked_summary = readiness_service.build(session, run_mode="paper", config=config)
            session.commit()

        assert ready_summary.status == "awaiting_manual_approval"
        assert ready_summary.blocking_reasons == ()
        assert ready_summary.manual_approval_required is True
        assert ready_summary.completed_run_count == 2
        assert blocked_summary.status == "blocked"
        assert "open_critical_incidents" in blocked_summary.blocking_reasons
        assert blocked_summary.manual_approval_required is True
        assert blocked_summary.open_critical_incidents == 1
        assert blocked_summary.latest_run_comparison is not None
        assert blocked_summary.latest_run_comparison.overall_status == "pass"
    finally:
        _drop_database(engine=engine, database_name=database_name)


def _store_completed_run(session: Session, *, run_id_suffix: str, started_at: datetime) -> None:
    repository = StrategyRunRepository()
    run = repository.create_run(
        session,
        StrategyRunCreate(
            run_mode="paper",
            strategy_name="momentum_rotation",
            config_version="v1",
            config_hash=f"cfg-{run_id_suffix}",
            signal_date=date(2026, 4, 22),
            execution_date=date(2026, 4, 23),
            status="running",
            started_at=started_at,
            metadata_json={"config": {"version": "v1"}},
        ),
    )
    report = build_paper_run_report(
        run=repository.update_run_status(
            session,
            strategy_run_id=run.id,
            status="completed",
            completed_at=started_at,
        ),
        approved=True,
        failed_reason_codes=(),
        orders=(),
        fills=(),
        account_snapshot=None,
        risk_snapshot=None,
        open_incident_count=0,
        reconciliation=OperationalReconciliationSummary(
            total_rows=0,
            mismatched_rows=0,
            critical_rows=0,
        ),
        generated_at=started_at,
    )
    repository.update_run_status(
        session,
        strategy_run_id=run.id,
        status="completed",
        completed_at=started_at,
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
