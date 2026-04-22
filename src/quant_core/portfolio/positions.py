"""Portfolio target-position sizing over persisted strategy weights."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Final

from sqlalchemy import select
from sqlalchemy.orm import Session

from quant_core.data import (
    OperationalRunMode,
    SnapshotRepository,
    StoredTargetPosition,
    StrategyRunRepository,
    TargetPositionWrite,
)
from quant_core.data.models import BarsDaily, Instrument

POSITION_PRECISION: Final = Decimal("0.000001")


class MissingPortfolioAccountStateError(ValueError):
    """Raised when target-position sizing has no account snapshot to use."""


class MissingPortfolioPriceError(ValueError):
    """Raised when target-position sizing lacks a canonical price for a symbol."""


class MissingTargetWeightStateError(ValueError):
    """Raised when a strategy run has no persisted target weights."""


@dataclass(frozen=True, slots=True)
class PortfolioTargetAllocation:
    """One strategy-produced target weight allocation."""

    allocation_key: str
    target_weight: Decimal
    symbol: str | None


@dataclass(frozen=True, slots=True)
class PortfolioTargetPosition:
    """One sized target position derived from account equity and reference prices."""

    allocation_key: str
    target_weight: Decimal
    target_notional: Decimal
    target_quantity: Decimal
    reference_price: Decimal | None
    symbol: str | None


def build_target_positions(
    *,
    allocations: Sequence[PortfolioTargetAllocation],
    account_equity: Decimal,
    price_by_symbol: Mapping[str, Decimal],
) -> tuple[PortfolioTargetPosition, ...]:
    """Size target positions from target weights, account equity, and prices."""

    if account_equity <= Decimal("0"):
        raise ValueError("account_equity must be positive")
    if not allocations:
        raise ValueError("allocations must not be empty")

    equity_total = account_equity.quantize(POSITION_PRECISION, rounding=ROUND_HALF_UP)
    target_notionals = _target_notionals(
        allocations=allocations,
        account_equity=equity_total,
    )

    positions: list[PortfolioTargetPosition] = []
    for allocation in allocations:
        target_notional = target_notionals[allocation.allocation_key]
        if allocation.symbol is None:
            positions.append(
                PortfolioTargetPosition(
                    allocation_key=allocation.allocation_key,
                    target_weight=allocation.target_weight,
                    target_notional=target_notional,
                    target_quantity=Decimal("0.000000"),
                    reference_price=None,
                    symbol=None,
                )
            )
            continue

        reference_price = price_by_symbol.get(allocation.symbol)
        if reference_price is None:
            raise MissingPortfolioPriceError(
                f"Missing canonical reference price for symbol: {allocation.symbol}"
            )
        if reference_price <= Decimal("0"):
            raise MissingPortfolioPriceError(
                f"Reference price must be positive for symbol: {allocation.symbol}"
            )

        target_quantity = (target_notional / reference_price).quantize(
            POSITION_PRECISION,
            rounding=ROUND_HALF_UP,
        )
        positions.append(
            PortfolioTargetPosition(
                allocation_key=allocation.allocation_key,
                target_weight=allocation.target_weight,
                target_notional=target_notional,
                target_quantity=target_quantity,
                reference_price=reference_price,
                symbol=allocation.symbol,
            )
        )

    return tuple(sorted(positions, key=lambda item: item.allocation_key))


class PersistedTargetPositionBuilder:
    """Build and persist target positions from stored strategy weights and account state."""

    def __init__(
        self,
        *,
        strategy_repository: StrategyRunRepository | None = None,
        snapshot_repository: SnapshotRepository | None = None,
    ) -> None:
        self._strategy_repository = strategy_repository or StrategyRunRepository()
        self._snapshot_repository = snapshot_repository or SnapshotRepository()

    def build_for_strategy_run(
        self,
        session: Session,
        *,
        strategy_run_id: int,
        run_mode: OperationalRunMode,
        generated_at: datetime,
        price_date: date | None = None,
    ) -> tuple[StoredTargetPosition, ...]:
        """Build and persist target positions for one stored strategy run."""

        run = self._strategy_repository.get_run(session, strategy_run_id=strategy_run_id)
        if run is None:
            raise ValueError(f"Unknown strategy_run_id: {strategy_run_id}")
        if run.run_mode != run_mode:
            raise ValueError("strategy run mode does not match target-position request mode")

        stored_weights = self._strategy_repository.list_target_weights(
            session,
            strategy_run_id=strategy_run_id,
        )
        if not stored_weights:
            raise MissingTargetWeightStateError(
                f"strategy run has no persisted target weights: {strategy_run_id}"
            )

        account_snapshot = self._snapshot_repository.latest_account_snapshot(
            session,
            run_mode=run_mode,
        )
        if account_snapshot is None:
            raise MissingPortfolioAccountStateError(
                f"Missing account snapshot for run mode: {run_mode}"
            )

        effective_price_date = price_date or run.signal_date
        price_by_symbol = self._close_prices_by_symbol(
            session,
            symbols=sorted(
                {weight.symbol for weight in stored_weights if weight.symbol is not None}
            ),
            bar_date=effective_price_date,
        )
        positions = build_target_positions(
            allocations=[
                PortfolioTargetAllocation(
                    allocation_key=weight.allocation_key,
                    target_weight=weight.target_weight,
                    symbol=weight.symbol,
                )
                for weight in stored_weights
            ],
            account_equity=account_snapshot.equity,
            price_by_symbol=price_by_symbol,
        )

        return self._strategy_repository.replace_target_positions(
            session,
            strategy_run_id=strategy_run_id,
            target_positions=[
                TargetPositionWrite(
                    allocation_key=position.allocation_key,
                    target_weight=position.target_weight,
                    target_notional=position.target_notional,
                    target_quantity=position.target_quantity,
                    reference_price=position.reference_price,
                    generated_at=generated_at,
                    symbol=position.symbol,
                )
                for position in positions
            ],
        )

    def _close_prices_by_symbol(
        self,
        session: Session,
        *,
        symbols: Sequence[str],
        bar_date: date,
    ) -> dict[str, Decimal]:
        if not symbols:
            return {}

        rows = session.execute(
            select(Instrument.symbol, BarsDaily.close)
            .join(Instrument, Instrument.id == BarsDaily.instrument_id)
            .where(Instrument.symbol.in_(symbols), BarsDaily.bar_date == bar_date)
            .order_by(Instrument.symbol)
        ).all()
        prices = {symbol: close for symbol, close in rows}

        missing_symbols = sorted(set(symbols) - set(prices))
        if missing_symbols:
            joined = ", ".join(missing_symbols)
            raise MissingPortfolioPriceError(
                f"Missing canonical reference prices for symbols: {joined}"
            )

        return prices


def _target_notionals(
    *,
    allocations: Sequence[PortfolioTargetAllocation],
    account_equity: Decimal,
) -> dict[str, Decimal]:
    notionals = {
        allocation.allocation_key: (account_equity * allocation.target_weight).quantize(
            POSITION_PRECISION,
            rounding=ROUND_HALF_UP,
        )
        for allocation in allocations
    }
    rounded_total = sum(notionals.values(), start=Decimal("0"))
    residual = account_equity - rounded_total
    if residual != Decimal("0"):
        receiver = max(
            allocations,
            key=lambda allocation: (allocation.target_weight, allocation.allocation_key),
        )
        notionals[receiver.allocation_key] = (
            notionals[receiver.allocation_key] + residual
        ).quantize(POSITION_PRECISION, rounding=ROUND_HALF_UP)

    return notionals
