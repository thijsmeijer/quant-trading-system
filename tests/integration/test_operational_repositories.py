from __future__ import annotations

import os
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from quant_core.data import (
    AccountSnapshotWrite,
    FillWrite,
    IncidentRepository,
    IncidentWrite,
    OrderCreate,
    OrderEventWrite,
    OrderRepository,
    PnlSnapshotWrite,
    PositionSnapshotWrite,
    RiskCheckWrite,
    RiskSnapshotWrite,
    SignalWrite,
    SnapshotRepository,
    StrategyRunCreate,
    StrategyRunRepository,
    TargetPositionWrite,
    TargetWeightWrite,
    UnknownOperationalInstrumentError,
)
from quant_core.data.models import Instrument

ROOT = Path(__file__).resolve().parents[2]


def test_strategy_run_repository_persists_and_replaces_operational_rows() -> None:
    database_name = f"quant_core_operational_repos_{uuid4().hex}"
    target_url = f"postgresql+psycopg://quant:quant@127.0.0.1:5432/{database_name}"
    engine = _migrated_engine(database_name=database_name, target_url=target_url)

    try:
        with Session(engine) as session:
            _seed_instruments(session, ("SPY", "TLT"))

            repository = StrategyRunRepository()
            run = repository.create_run(
                session,
                StrategyRunCreate(
                    run_mode="paper",
                    strategy_name="momentum_rotation",
                    config_version="v1",
                    config_hash="cfg-001",
                    signal_date=date(2026, 4, 22),
                    execution_date=date(2026, 4, 23),
                    status="running",
                    started_at=datetime(2026, 4, 22, 21, 0, tzinfo=UTC),
                    metadata_json={"rebalance": "daily"},
                ),
            )

            repository.replace_signals(
                session,
                strategy_run_id=run.id,
                signals=[
                    SignalWrite(
                        symbol="SPY",
                        signal_name="momentum_rank",
                        rank=1,
                        score=Decimal("0.145000"),
                        is_selected=True,
                        generated_at=datetime(2026, 4, 22, 21, 1, tzinfo=UTC),
                    ),
                    SignalWrite(
                        symbol="TLT",
                        signal_name="momentum_rank",
                        rank=2,
                        score=Decimal("0.010000"),
                        is_selected=False,
                        generated_at=datetime(2026, 4, 22, 21, 1, tzinfo=UTC),
                    ),
                ],
            )
            repository.replace_target_weights(
                session,
                strategy_run_id=run.id,
                target_weights=[
                    TargetWeightWrite(
                        allocation_key="SPY",
                        symbol="SPY",
                        target_weight=Decimal("0.600000"),
                        generated_at=datetime(2026, 4, 22, 21, 2, tzinfo=UTC),
                    ),
                    TargetWeightWrite(
                        allocation_key="cash",
                        symbol=None,
                        target_weight=Decimal("0.400000"),
                        generated_at=datetime(2026, 4, 22, 21, 2, tzinfo=UTC),
                    ),
                ],
            )
            repository.replace_target_positions(
                session,
                strategy_run_id=run.id,
                target_positions=[
                    TargetPositionWrite(
                        allocation_key="SPY",
                        symbol="SPY",
                        target_weight=Decimal("0.600000"),
                        target_notional=Decimal("60000.000000"),
                        target_quantity=Decimal("100.000000"),
                        reference_price=Decimal("600.000000"),
                        generated_at=datetime(2026, 4, 22, 21, 3, tzinfo=UTC),
                    ),
                    TargetPositionWrite(
                        allocation_key="cash",
                        symbol=None,
                        target_weight=Decimal("0.400000"),
                        target_notional=Decimal("40000.000000"),
                        target_quantity=Decimal("0.000000"),
                        reference_price=None,
                        generated_at=datetime(2026, 4, 22, 21, 3, tzinfo=UTC),
                    ),
                ],
            )
            repository.replace_risk_checks(
                session,
                strategy_run_id=run.id,
                risk_checks=[
                    RiskCheckWrite(
                        check_scope="portfolio",
                        check_name="gross_exposure",
                        status="pass",
                        reason_code=None,
                        checked_at=datetime(2026, 4, 22, 21, 4, tzinfo=UTC),
                        details={"gross_exposure": "0.600000"},
                    )
                ],
            )
            completed_run = repository.update_run_status(
                session,
                strategy_run_id=run.id,
                status="completed",
                completed_at=datetime(2026, 4, 22, 21, 5, tzinfo=UTC),
                metadata_json={"rebalance": "daily", "selection_count": 1},
            )

            repository.replace_target_weights(
                session,
                strategy_run_id=run.id,
                target_weights=[
                    TargetWeightWrite(
                        allocation_key="TLT",
                        symbol="TLT",
                        target_weight=Decimal("1.000000"),
                        generated_at=datetime(2026, 4, 22, 21, 6, tzinfo=UTC),
                    )
                ],
            )
            session.commit()

        with Session(engine) as session:
            repository = StrategyRunRepository()
            loaded_run = repository.get_run(session, strategy_run_id=run.id)
            signals = repository.list_signals(session, strategy_run_id=run.id)
            weights = repository.list_target_weights(session, strategy_run_id=run.id)
            positions = repository.list_target_positions(session, strategy_run_id=run.id)
            checks = repository.list_risk_checks(session, strategy_run_id=run.id)

        assert completed_run.status == "completed"
        assert completed_run.metadata_json == {"rebalance": "daily", "selection_count": 1}
        assert loaded_run == completed_run
        assert signals[0].symbol == "SPY"
        assert signals[1].symbol == "TLT"
        assert len(weights) == 1
        assert weights[0].allocation_key == "TLT"
        assert weights[0].target_weight == Decimal("1.000000")
        assert weights[0].symbol == "TLT"
        assert {position.allocation_key for position in positions} == {"SPY", "cash"}
        assert checks[0].check_name == "gross_exposure"
    finally:
        _drop_database(engine=engine, database_name=database_name)


def test_operational_repositories_keep_idempotency_and_mode_separation() -> None:
    database_name = f"quant_core_operational_state_{uuid4().hex}"
    target_url = f"postgresql+psycopg://quant:quant@127.0.0.1:5432/{database_name}"
    engine = _migrated_engine(database_name=database_name, target_url=target_url)

    try:
        with Session(engine) as session:
            _seed_instruments(session, ("SPY", "BND"))
            run_repository = StrategyRunRepository()
            order_repository = OrderRepository()
            snapshot_repository = SnapshotRepository()
            incident_repository = IncidentRepository()

            run = run_repository.create_run(
                session,
                StrategyRunCreate(
                    run_mode="paper",
                    strategy_name="momentum_rotation",
                    config_version="v1",
                    config_hash="cfg-001",
                    signal_date=date(2026, 4, 22),
                    execution_date=date(2026, 4, 23),
                    status="completed",
                    started_at=datetime(2026, 4, 22, 21, 0, tzinfo=UTC),
                    metadata_json=None,
                ),
            )

            created_order = order_repository.create_order(
                session,
                OrderCreate(
                    internal_order_id="ord-paper-001",
                    strategy_run_id=run.id,
                    run_mode="paper",
                    symbol="SPY",
                    order_type="market",
                    side="BUY",
                    status="pending",
                    requested_quantity=Decimal("10.000000"),
                    requested_notional=Decimal("6000.000000"),
                    time_in_force="day",
                    created_at=datetime(2026, 4, 23, 13, 25, tzinfo=UTC),
                ),
            )
            repeated_order = order_repository.create_order(
                session,
                OrderCreate(
                    internal_order_id="ord-paper-001",
                    strategy_run_id=run.id,
                    run_mode="paper",
                    symbol="SPY",
                    order_type="market",
                    side="BUY",
                    status="pending",
                    requested_quantity=Decimal("10.000000"),
                    requested_notional=Decimal("6000.000000"),
                    time_in_force="day",
                    created_at=datetime(2026, 4, 23, 13, 25, tzinfo=UTC),
                ),
            )
            submitted_order = order_repository.update_order_status(
                session,
                run_mode="paper",
                internal_order_id="ord-paper-001",
                status="submitted",
                broker_order_id="broker-paper-001",
                submitted_at=datetime(2026, 4, 23, 13, 30, tzinfo=UTC),
            )

            order_repository.record_events(
                session,
                run_mode="paper",
                events=[
                    OrderEventWrite(
                        internal_order_id="ord-paper-001",
                        event_type="submitted",
                        event_at=datetime(2026, 4, 23, 13, 30, tzinfo=UTC),
                        broker_event_id="evt-001",
                        details={"status": "submitted"},
                    ),
                    OrderEventWrite(
                        internal_order_id="ord-paper-001",
                        event_type="submitted",
                        event_at=datetime(2026, 4, 23, 13, 30, tzinfo=UTC),
                        broker_event_id="evt-001",
                        details={"status": "submitted"},
                    ),
                ],
            )
            order_repository.record_fills(
                session,
                run_mode="paper",
                fills=[
                    FillWrite(
                        internal_order_id="ord-paper-001",
                        broker_fill_id="fill-001",
                        fill_quantity=Decimal("10.000000"),
                        fill_price=Decimal("601.250000"),
                        fill_notional=Decimal("6012.500000"),
                        fill_at=datetime(2026, 4, 23, 13, 31, tzinfo=UTC),
                    ),
                    FillWrite(
                        internal_order_id="ord-paper-001",
                        broker_fill_id="fill-001",
                        fill_quantity=Decimal("10.000000"),
                        fill_price=Decimal("601.250000"),
                        fill_notional=Decimal("6012.500000"),
                        fill_at=datetime(2026, 4, 23, 13, 31, tzinfo=UTC),
                    ),
                ],
            )

            paper_account = snapshot_repository.store_account_snapshot(
                session,
                AccountSnapshotWrite(
                    run_mode="paper",
                    cash=Decimal("40000.000000"),
                    equity=Decimal("100000.000000"),
                    buying_power=Decimal("40000.000000"),
                    as_of=datetime(2026, 4, 23, 20, 0, tzinfo=UTC),
                ),
            )
            live_account = snapshot_repository.store_account_snapshot(
                session,
                AccountSnapshotWrite(
                    run_mode="live",
                    cash=Decimal("9000.000000"),
                    equity=Decimal("10000.000000"),
                    buying_power=Decimal("9000.000000"),
                    as_of=datetime(2026, 4, 23, 20, 0, tzinfo=UTC),
                ),
            )
            paper_positions = snapshot_repository.replace_positions(
                session,
                run_mode="paper",
                as_of=datetime(2026, 4, 23, 20, 0, tzinfo=UTC),
                positions=[
                    PositionSnapshotWrite(
                        symbol="SPY",
                        quantity=Decimal("10.000000"),
                        average_cost=Decimal("601.250000"),
                        market_value=Decimal("6012.500000"),
                        as_of=datetime(2026, 4, 23, 20, 0, tzinfo=UTC),
                    )
                ],
            )
            live_positions = snapshot_repository.replace_positions(
                session,
                run_mode="live",
                as_of=datetime(2026, 4, 23, 20, 0, tzinfo=UTC),
                positions=[
                    PositionSnapshotWrite(
                        symbol="BND",
                        quantity=Decimal("50.000000"),
                        average_cost=Decimal("72.000000"),
                        market_value=Decimal("3600.000000"),
                        as_of=datetime(2026, 4, 23, 20, 0, tzinfo=UTC),
                    )
                ],
            )
            paper_pnl = snapshot_repository.store_pnl_snapshot(
                session,
                PnlSnapshotWrite(
                    run_mode="paper",
                    realized_pnl=Decimal("12.500000"),
                    unrealized_pnl=Decimal("25.000000"),
                    total_pnl=Decimal("37.500000"),
                    as_of=datetime(2026, 4, 23, 20, 0, tzinfo=UTC),
                ),
            )
            paper_risk = snapshot_repository.store_risk_snapshot(
                session,
                RiskSnapshotWrite(
                    run_mode="paper",
                    gross_exposure=Decimal("0.601250"),
                    net_exposure=Decimal("0.601250"),
                    drawdown=Decimal("0.010000"),
                    open_order_count=0,
                    as_of=datetime(2026, 4, 23, 20, 0, tzinfo=UTC),
                ),
            )
            incident_repository.create_incident(
                session,
                IncidentWrite(
                    run_mode="paper",
                    incident_type="order_rejection",
                    severity="warning",
                    status="open",
                    summary="paper order rejected",
                    occurred_at=datetime(2026, 4, 23, 13, 32, tzinfo=UTC),
                    details={"internal_order_id": "ord-paper-001"},
                ),
            )
            incident_repository.create_incident(
                session,
                IncidentWrite(
                    run_mode="live",
                    incident_type="stale_data",
                    severity="critical",
                    status="open",
                    summary="live data feed stale",
                    occurred_at=datetime(2026, 4, 23, 13, 33, tzinfo=UTC),
                    details=None,
                ),
            )
            session.commit()

        with Session(engine) as session:
            order_repository = OrderRepository()
            snapshot_repository = SnapshotRepository()
            incident_repository = IncidentRepository()

            paper_orders = order_repository.list_orders(session, run_mode="paper")
            paper_events = order_repository.list_events(session, run_mode="paper")
            paper_fills = order_repository.list_fills(session, run_mode="paper")
            paper_account = snapshot_repository.latest_account_snapshot(session, run_mode="paper")
            live_account = snapshot_repository.latest_account_snapshot(session, run_mode="live")
            loaded_paper_positions = snapshot_repository.latest_positions(session, run_mode="paper")
            loaded_live_positions = snapshot_repository.latest_positions(session, run_mode="live")
            loaded_paper_pnl = snapshot_repository.latest_pnl_snapshot(session, run_mode="paper")
            loaded_paper_risk = snapshot_repository.latest_risk_snapshot(session, run_mode="paper")
            paper_incidents = incident_repository.list_open_incidents(session, run_mode="paper")
            live_incidents = incident_repository.list_open_incidents(session, run_mode="live")

        assert created_order.id == repeated_order.id
        assert submitted_order.status == "submitted"
        assert submitted_order.broker_order_id == "broker-paper-001"
        assert len(paper_orders) == 1
        assert len(paper_events) == 1
        assert len(paper_fills) == 1
        assert paper_account is not None
        assert paper_account.equity == Decimal("100000.000000")
        assert live_account is not None
        assert live_account.equity == Decimal("10000.000000")
        assert paper_positions[0].symbol == "SPY"
        assert live_positions[0].symbol == "BND"
        assert loaded_paper_positions[0].symbol == "SPY"
        assert loaded_live_positions[0].symbol == "BND"
        assert paper_pnl.total_pnl == Decimal("37.500000")
        assert loaded_paper_pnl == paper_pnl
        assert paper_risk.drawdown == Decimal("0.010000")
        assert loaded_paper_risk == paper_risk
        assert paper_incidents[0].incident_type == "order_rejection"
        assert live_incidents[0].incident_type == "stale_data"
    finally:
        _drop_database(engine=engine, database_name=database_name)


def test_strategy_run_repository_rejects_unknown_symbols() -> None:
    database_name = f"quant_core_operational_missing_symbol_{uuid4().hex}"
    target_url = f"postgresql+psycopg://quant:quant@127.0.0.1:5432/{database_name}"
    engine = _migrated_engine(database_name=database_name, target_url=target_url)

    try:
        with Session(engine) as session:
            _seed_instruments(session, ("SPY",))
            repository = StrategyRunRepository()
            run = repository.create_run(
                session,
                StrategyRunCreate(
                    run_mode="paper",
                    strategy_name="momentum_rotation",
                    config_version="v1",
                    config_hash="cfg-001",
                    signal_date=date(2026, 4, 22),
                    execution_date=date(2026, 4, 23),
                    status="running",
                    started_at=datetime(2026, 4, 22, 21, 0, tzinfo=UTC),
                    metadata_json=None,
                ),
            )

            with pytest.raises(UnknownOperationalInstrumentError, match="Missing instruments"):
                repository.replace_signals(
                    session,
                    strategy_run_id=run.id,
                    signals=[
                        SignalWrite(
                            symbol="QQQ",
                            signal_name="momentum_rank",
                            rank=1,
                            score=Decimal("0.100000"),
                            is_selected=True,
                            generated_at=datetime(2026, 4, 22, 21, 1, tzinfo=UTC),
                        )
                    ],
                )
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


def _seed_instruments(session: Session, symbols: tuple[str, ...]) -> None:
    session.add_all(
        Instrument(
            symbol=symbol,
            name=f"{symbol} ETF",
            category="broad_us_equity",
            exchange="ARCA",
            is_active=True,
        )
        for symbol in symbols
    )
    session.commit()


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
