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
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from quant_core.dashboard import PaperOperatorReviewService
from quant_core.data import (
    AccountSnapshotWrite,
    PositionSnapshotWrite,
    RiskSnapshotWrite,
    SnapshotRepository,
    StrategyRunCreate,
    StrategyRunRepository,
)
from quant_core.data.models import Instrument
from quant_core.reconciliation import OperationalReconciliationSummary
from quant_core.reporting.paper_runs import build_paper_run_report
from quant_core.settings import PaperPromotionConfig, PaperRunExpectationConfig

ROOT = Path(__file__).resolve().parents[2]


def test_operator_review_service_combines_persisted_operational_state() -> None:
    database_name = f"quant_core_operator_review_{uuid4().hex}"
    target_url = f"postgresql+psycopg://quant:quant@127.0.0.1:5432/{database_name}"
    engine = _migrated_engine(database_name=database_name, target_url=target_url)

    try:
        with Session(engine) as session:
            repository = StrategyRunRepository()
            started_at = datetime(2026, 4, 22, 21, 0, tzinfo=UTC)
            run = repository.create_run(
                session,
                StrategyRunCreate(
                    run_mode="paper",
                    strategy_name="momentum_rotation",
                    config_version="v1",
                    config_hash="cfg-review",
                    signal_date=date(2026, 4, 22),
                    execution_date=date(2026, 4, 23),
                    status="running",
                    started_at=started_at,
                    metadata_json={"config": {"version": "v1"}},
                ),
            )

            _seed_instruments(session)
            _seed_snapshots(session, as_of=started_at)

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
                account_snapshot=SnapshotRepository().latest_account_snapshot(
                    session,
                    run_mode="paper",
                ),
                risk_snapshot=SnapshotRepository().latest_risk_snapshot(
                    session,
                    run_mode="paper",
                ),
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

            review = PaperOperatorReviewService().build(
                session,
                run_mode="paper",
                config=PaperPromotionConfig(
                    minimum_completed_runs=1,
                    maximum_open_critical_incidents=0,
                    maximum_open_warning_incidents=0,
                    manual_approval_required=True,
                    latest_run_expectation=PaperRunExpectationConfig(
                        require_approved=True,
                        min_fill_ratio=Decimal("0.000000"),
                        max_rejected_order_count=0,
                        max_reconciliation_critical_rows=0,
                        max_open_incident_count=0,
                    ),
                ),
                burnin_limit=60,
            )
            session.commit()

        assert review.overview.latest_run is not None
        assert review.overview.latest_run.run_id == run.id
        assert review.health.overall_status == "healthy"
        assert review.burnin.summary.total_runs == 1
        assert review.burnin.summary.consecutive_clean_runs == 1
        assert review.readiness.status == "awaiting_manual_approval"
        assert review.readiness.blocking_reasons == ()
    finally:
        _drop_database(engine=engine, database_name=database_name)


def _seed_snapshots(session: Session, *, as_of: datetime) -> None:
    repository = SnapshotRepository()
    repository.replace_positions(
        session,
        run_mode="paper",
        as_of=as_of,
        positions=[
            PositionSnapshotWrite(
                symbol="SPY",
                quantity=Decimal("10.000000"),
                market_value=Decimal("5080.000000"),
                as_of=as_of,
            )
        ],
    )
    repository.store_account_snapshot(
        session,
        AccountSnapshotWrite(
            run_mode="paper",
            cash=Decimal("94920.000000"),
            equity=Decimal("100000.000000"),
            buying_power=Decimal("94920.000000"),
            as_of=as_of,
        ),
    )
    repository.store_risk_snapshot(
        session,
        RiskSnapshotWrite(
            run_mode="paper",
            gross_exposure=Decimal("0.050800"),
            net_exposure=Decimal("0.050800"),
            drawdown=None,
            open_order_count=0,
            as_of=as_of,
        ),
    )


def _seed_instruments(session: Session) -> None:
    session.add(
        Instrument(
            symbol="SPY",
            name="SPY ETF",
            category="broad_us_equity",
            exchange="ARCA",
            is_active=True,
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
