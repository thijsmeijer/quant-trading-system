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

from quant_core.dashboard import OperationsOverviewService, ServiceHealthService
from quant_core.data import (
    AccountSnapshotWrite,
    IncidentRepository,
    OrderCreate,
    OrderRepository,
    PositionSnapshotWrite,
    RiskSnapshotWrite,
    StrategyRunCreate,
    StrategyRunRepository,
)
from quant_core.data.models import Instrument
from quant_core.execution import OperationalAlertService
from quant_core.reconciliation import OperationalReconciliationSummary
from quant_core.reporting.data_quality import DailyDataQualityReport, DailyDataQualitySummary
from quant_core.reporting.paper_runs import build_paper_run_report

ROOT = Path(__file__).resolve().parents[2]


def test_operational_alerts_feed_dashboard_overview_and_health() -> None:
    database_name = f"quant_core_alerts_{uuid4().hex}"
    target_url = f"postgresql+psycopg://quant:quant@127.0.0.1:5432/{database_name}"
    engine = _migrated_engine(database_name=database_name, target_url=target_url)

    try:
        with Session(engine) as session:
            _seed_instruments(session)
            run = StrategyRunRepository().create_run(
                session,
                StrategyRunCreate(
                    run_mode="paper",
                    strategy_name="momentum_rotation",
                    config_version="v1",
                    config_hash="cfg123",
                    signal_date=date(2026, 4, 22),
                    execution_date=date(2026, 4, 23),
                    status="running",
                    started_at=datetime(2026, 4, 22, 21, 15, tzinfo=UTC),
                    metadata_json={"config": {"version": "v1"}},
                ),
            )
            order = OrderRepository().create_order(
                session,
                OrderCreate(
                    internal_order_id="order_rejected_1",
                    strategy_run_id=run.id,
                    run_mode="paper",
                    symbol="SPY",
                    order_type="market",
                    side="BUY",
                    status="rejected",
                    requested_quantity=Decimal("10.000000"),
                    requested_notional=Decimal("5080.000000"),
                    time_in_force="day",
                    created_at=datetime(2026, 4, 22, 21, 18, tzinfo=UTC),
                ),
            )
            snapshot_repository = _seed_snapshots(session)
            report = build_paper_run_report(
                run=StrategyRunRepository().update_run_status(
                    session,
                    strategy_run_id=run.id,
                    status="blocked",
                ),
                approved=False,
                failed_reason_codes=("stale_data",),
                orders=(order,),
                fills=(),
                account_snapshot=snapshot_repository.latest_account_snapshot(
                    session,
                    run_mode="paper",
                ),
                risk_snapshot=snapshot_repository.latest_risk_snapshot(
                    session,
                    run_mode="paper",
                ),
                open_incident_count=0,
                reconciliation=OperationalReconciliationSummary(
                    total_rows=3,
                    mismatched_rows=2,
                    critical_rows=1,
                ),
                generated_at=datetime(2026, 4, 22, 21, 21, tzinfo=UTC),
            )
            StrategyRunRepository().update_run_status(
                session,
                strategy_run_id=run.id,
                status="blocked",
                completed_at=datetime(2026, 4, 22, 21, 21, tzinfo=UTC),
                metadata_json={"paper_run_report": report.as_metadata()},
            )

            alert_service = OperationalAlertService()
            run_incidents = alert_service.record_run_alerts(
                session,
                run_mode="paper",
                strategy_run_id=run.id,
                occurred_at=datetime(2026, 4, 22, 21, 21, tzinfo=UTC),
            )
            data_incidents = alert_service.record_data_quality_alerts(
                session,
                run_mode="paper",
                report=DailyDataQualityReport(
                    checked_as_of=date(2026, 4, 22),
                    universe_name="core_us_etfs",
                    active_universe_symbols=("SPY",),
                    duplicate_symbols=(),
                    missing_symbols=("SPY",),
                    stale_symbols=("SPY",),
                    price_sanity_symbols=(),
                    failing_symbols=("SPY",),
                    summary=DailyDataQualitySummary(
                        raw_duplicate_group_count=0,
                        normalized_duplicate_group_count=0,
                        duplicate_symbol_count=0,
                        missing_bar_count=1,
                        stale_symbol_count=1,
                        price_sanity_issue_count=0,
                        failing_symbol_count=1,
                    ),
                ),
                occurred_at=datetime(2026, 4, 22, 20, 5, tzinfo=UTC),
            )

            overview = OperationsOverviewService().build(session, run_mode="paper")
            health = ServiceHealthService().build(session, run_mode="paper")
            all_incidents = IncidentRepository().list_open_incidents(session, run_mode="paper")
            session.commit()

        assert len(run_incidents) == 3
        assert len(data_incidents) == 1
        assert overview.latest_run is not None
        assert overview.latest_run.run_id == run.id
        assert overview.latest_run.status == "blocked"
        assert len(overview.positions) == 1
        assert len(overview.orders) == 1
        assert overview.alerts.stale_data_alerts == 1
        assert overview.alerts.failed_job_alerts == 1
        assert overview.alerts.order_rejection_alerts == 1
        assert overview.alerts.reconciliation_alerts == 1
        assert len(all_incidents) == 4
        assert health.overall_status == "critical"
        assert {check.component: check.status for check in health.checks} == {
            "data": "critical",
            "latest_run": "critical",
            "state": "healthy",
            "incidents": "critical",
        }
    finally:
        _drop_database(engine=engine, database_name=database_name)


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


def _seed_snapshots(session: Session):
    from quant_core.data import SnapshotRepository

    repository = SnapshotRepository()
    as_of = datetime(2026, 4, 22, 21, 20, tzinfo=UTC)
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
    return repository


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
