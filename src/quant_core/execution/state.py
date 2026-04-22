"""State refresh from broker snapshots and persisted fills."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from sqlalchemy.orm import Session

from quant_core.broker import BrokerGateway
from quant_core.data import (
    AccountSnapshotWrite,
    OperationalRunMode,
    PnlSnapshotWrite,
    PositionSnapshotWrite,
    RiskSnapshotWrite,
    SnapshotRepository,
)


@dataclass(frozen=True, slots=True)
class RefreshedOperationalState:
    """Internal state refreshed from broker snapshots."""

    position_count: int
    cash: Decimal
    equity: Decimal
    buying_power: Decimal


class OperationalStateRefresher:
    """Refresh internal positions and snapshots from a broker adapter."""

    def __init__(self, snapshot_repository: SnapshotRepository | None = None) -> None:
        self._snapshot_repository = snapshot_repository or SnapshotRepository()

    def refresh_from_broker(
        self,
        session: Session,
        *,
        run_mode: OperationalRunMode,
        broker: BrokerGateway,
        as_of: datetime,
    ) -> RefreshedOperationalState:
        """Persist broker positions, account, PnL, and risk snapshots."""

        previous_account = self._snapshot_repository.latest_account_snapshot(
            session,
            run_mode=run_mode,
        )
        broker_positions = broker.list_positions()
        broker_account = broker.get_account()

        self._snapshot_repository.replace_positions(
            session,
            run_mode=run_mode,
            as_of=as_of,
            positions=[
                PositionSnapshotWrite(
                    symbol=position.symbol,
                    quantity=position.quantity,
                    market_value=position.market_value,
                    average_cost=None,
                    as_of=as_of,
                )
                for position in broker_positions
            ],
        )
        self._snapshot_repository.store_account_snapshot(
            session,
            AccountSnapshotWrite(
                run_mode=run_mode,
                cash=broker_account.cash,
                equity=broker_account.equity,
                buying_power=broker_account.buying_power,
                as_of=as_of,
            ),
        )

        baseline_equity = (
            previous_account.equity if previous_account is not None else broker_account.equity
        )
        total_pnl = (broker_account.equity - baseline_equity).quantize(Decimal("0.000001"))
        self._snapshot_repository.store_pnl_snapshot(
            session,
            PnlSnapshotWrite(
                run_mode=run_mode,
                realized_pnl=Decimal("0.000000"),
                unrealized_pnl=total_pnl,
                total_pnl=total_pnl,
                as_of=as_of,
            ),
        )

        gross_exposure = Decimal("0.000000")
        if broker_account.equity > Decimal("0"):
            gross_exposure = (
                sum((position.market_value for position in broker_positions), start=Decimal("0"))
                / broker_account.equity
            ).quantize(Decimal("0.000001"))
        self._snapshot_repository.store_risk_snapshot(
            session,
            RiskSnapshotWrite(
                run_mode=run_mode,
                gross_exposure=gross_exposure,
                net_exposure=gross_exposure,
                drawdown=None,
                open_order_count=0,
                as_of=as_of,
            ),
        )
        return RefreshedOperationalState(
            position_count=len(broker_positions),
            cash=broker_account.cash,
            equity=broker_account.equity,
            buying_power=broker_account.buying_power,
        )
