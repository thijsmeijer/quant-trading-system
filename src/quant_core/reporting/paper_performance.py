"""Read-only paper performance reporting from persisted operational state."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.orm import Session

from quant_core.data import (
    OperationalRunMode,
    OrderRepository,
    SnapshotRepository,
    StrategyRunRepository,
)
from quant_core.data.models import AccountSnapshot, BarsDaily, Instrument

_SIX_PLACES = Decimal("0.000001")


class PaperPerformanceUnavailableError(RuntimeError):
    """Raised when required paper state has not been initialized."""


@dataclass(frozen=True, slots=True)
class BenchmarkPrice:
    """Start and end prices used for one benchmark constituent."""

    symbol: str
    start_date: date
    start_price: Decimal
    end_date: date
    end_price: Decimal


@dataclass(frozen=True, slots=True)
class BenchmarkReturn:
    """Return for one benchmark over the report period."""

    name: str
    start_date: date | None
    end_date: date | None
    total_return: Decimal | None
    missing_symbols: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PositionPerformanceRow:
    """Latest position row with allocation weight."""

    symbol: str
    quantity: Decimal
    market_value: Decimal
    weight: Decimal | None
    average_cost: Decimal | None


@dataclass(frozen=True, slots=True)
class LatestRunTradingSummary:
    """Order and fill activity for the latest strategy run."""

    run_id: int | None
    signal_date: date | None
    status: str | None
    order_count: int
    fill_count: int
    turnover_notional: Decimal
    turnover_ratio: Decimal | None


@dataclass(frozen=True, slots=True)
class PaperPerformanceReport:
    """Operator-facing paper performance summary."""

    run_mode: OperationalRunMode
    as_of: date
    account_as_of: str
    starting_equity: Decimal
    latest_equity: Decimal
    cash: Decimal
    buying_power: Decimal
    total_return: Decimal | None
    realized_pnl: Decimal | None
    unrealized_pnl: Decimal | None
    total_pnl: Decimal | None
    positions: tuple[PositionPerformanceRow, ...]
    latest_run: LatestRunTradingSummary
    benchmarks: tuple[BenchmarkReturn, ...]


class PaperPerformanceReportService:
    """Build daily paper performance reports from persisted state."""

    def __init__(
        self,
        *,
        snapshot_repository: SnapshotRepository | None = None,
        strategy_run_repository: StrategyRunRepository | None = None,
        order_repository: OrderRepository | None = None,
    ) -> None:
        self._snapshots = snapshot_repository or SnapshotRepository()
        self._strategy_runs = strategy_run_repository or StrategyRunRepository()
        self._orders = order_repository or OrderRepository()

    def build(
        self,
        session: Session,
        *,
        run_mode: OperationalRunMode = "paper",
        benchmark_start_date: date | None = None,
        benchmark_end_date: date | None = None,
    ) -> PaperPerformanceReport:
        """Build a report for one operational mode."""

        latest_account = self._snapshots.latest_account_snapshot(session, run_mode=run_mode)
        if latest_account is None:
            raise PaperPerformanceUnavailableError(
                f"No account snapshots are available for run_mode={run_mode!r}"
            )

        starting_equity = self._starting_equity(session, run_mode=run_mode)
        latest_pnl = self._snapshots.latest_pnl_snapshot(session, run_mode=run_mode)
        positions = tuple(
            PositionPerformanceRow(
                symbol=position.symbol,
                quantity=position.quantity,
                market_value=position.market_value,
                weight=calculate_weight(
                    market_value=position.market_value,
                    equity=latest_account.equity,
                ),
                average_cost=position.average_cost,
            )
            for position in self._snapshots.latest_positions(session, run_mode=run_mode)
        )

        latest_run = self._strategy_runs.latest_run(session, run_mode=run_mode)
        trading_summary = self._latest_run_trading_summary(
            session,
            run_mode=run_mode,
            latest_equity=latest_account.equity,
            strategy_run_id=latest_run.id if latest_run is not None else None,
        )
        completed_runs = self._strategy_runs.list_runs(
            session,
            run_mode=run_mode,
            statuses=("completed",),
        )
        benchmark_start = benchmark_start_date or (
            min(run.signal_date for run in completed_runs)
            if completed_runs
            else latest_account.as_of.date()
        )
        benchmark_end = benchmark_end_date or (
            max(run.signal_date for run in completed_runs)
            if completed_runs
            else latest_account.as_of.date()
        )

        return PaperPerformanceReport(
            run_mode=run_mode,
            as_of=latest_account.as_of.date(),
            account_as_of=latest_account.as_of.isoformat(),
            starting_equity=starting_equity,
            latest_equity=latest_account.equity,
            cash=latest_account.cash,
            buying_power=latest_account.buying_power,
            total_return=calculate_period_return(starting_equity, latest_account.equity),
            realized_pnl=latest_pnl.realized_pnl if latest_pnl is not None else None,
            unrealized_pnl=latest_pnl.unrealized_pnl if latest_pnl is not None else None,
            total_pnl=latest_pnl.total_pnl if latest_pnl is not None else None,
            positions=positions,
            latest_run=trading_summary,
            benchmarks=self._benchmark_returns(
                session,
                start_date=benchmark_start,
                end_date=benchmark_end,
            ),
        )

    def _starting_equity(self, session: Session, *, run_mode: OperationalRunMode) -> Decimal:
        equity = session.scalar(
            select(AccountSnapshot.equity)
            .where(AccountSnapshot.run_mode == run_mode)
            .order_by(AccountSnapshot.as_of.asc())
            .limit(1)
        )
        if equity is None:
            raise PaperPerformanceUnavailableError(
                f"No account snapshots are available for run_mode={run_mode!r}"
            )
        return equity

    def _latest_run_trading_summary(
        self,
        session: Session,
        *,
        run_mode: OperationalRunMode,
        latest_equity: Decimal,
        strategy_run_id: int | None,
    ) -> LatestRunTradingSummary:
        if strategy_run_id is None:
            return LatestRunTradingSummary(
                run_id=None,
                signal_date=None,
                status=None,
                order_count=0,
                fill_count=0,
                turnover_notional=Decimal("0.000000"),
                turnover_ratio=Decimal("0.000000"),
            )

        run = self._strategy_runs.get_run(session, strategy_run_id=strategy_run_id)
        orders = self._orders.list_orders(
            session,
            run_mode=run_mode,
            strategy_run_id=strategy_run_id,
        )
        fills = self._orders.list_fills(
            session,
            run_mode=run_mode,
            strategy_run_id=strategy_run_id,
        )
        turnover_notional = sum(
            (abs(fill.fill_notional) for fill in fills),
            start=Decimal("0.000000"),
        ).quantize(_SIX_PLACES)
        return LatestRunTradingSummary(
            run_id=strategy_run_id,
            signal_date=run.signal_date if run is not None else None,
            status=run.status if run is not None else None,
            order_count=len(orders),
            fill_count=len(fills),
            turnover_notional=turnover_notional,
            turnover_ratio=calculate_turnover_ratio(
                turnover_notional=turnover_notional,
                equity=latest_equity,
            ),
        )

    def _benchmark_returns(
        self,
        session: Session,
        *,
        start_date: date,
        end_date: date,
    ) -> tuple[BenchmarkReturn, ...]:
        spy_price = self._benchmark_price(
            session, symbol="SPY", start_date=start_date, end_date=end_date
        )
        bnd_price = self._benchmark_price(
            session, symbol="BND", start_date=start_date, end_date=end_date
        )
        prices = {price.symbol: price for price in (spy_price, bnd_price) if price is not None}
        return (
            BenchmarkReturn(
                name="cash",
                start_date=start_date,
                end_date=end_date,
                total_return=Decimal("0.000000"),
                missing_symbols=(),
            ),
            calculate_weighted_benchmark_return(
                name="spy_buy_and_hold",
                start_date=start_date,
                end_date=end_date,
                prices=prices,
                weights={"SPY": Decimal("1.000000")},
            ),
            calculate_weighted_benchmark_return(
                name="sixty_forty_spy_bnd",
                start_date=start_date,
                end_date=end_date,
                prices=prices,
                weights={"SPY": Decimal("0.600000"), "BND": Decimal("0.400000")},
            ),
        )

    def _benchmark_price(
        self,
        session: Session,
        *,
        symbol: str,
        start_date: date,
        end_date: date,
    ) -> BenchmarkPrice | None:
        start = _price_on_or_before(session, symbol=symbol, target_date=start_date)
        end = _price_on_or_before(session, symbol=symbol, target_date=end_date)
        if start is None or end is None:
            return None
        return BenchmarkPrice(
            symbol=symbol,
            start_date=start[0],
            start_price=start[1],
            end_date=end[0],
            end_price=end[1],
        )


def calculate_period_return(start_value: Decimal, end_value: Decimal) -> Decimal | None:
    """Calculate `(end / start) - 1`, rounded to six decimal places."""

    if start_value <= 0:
        return None
    return ((end_value / start_value) - Decimal("1")).quantize(_SIX_PLACES)


def calculate_turnover_ratio(
    *,
    turnover_notional: Decimal,
    equity: Decimal,
) -> Decimal | None:
    """Calculate latest-run turnover as traded notional divided by latest equity."""

    if equity <= 0:
        return None
    return (turnover_notional / equity).quantize(_SIX_PLACES)


def calculate_weight(
    *,
    market_value: Decimal,
    equity: Decimal,
) -> Decimal | None:
    """Calculate current position weight as market value divided by latest equity."""

    if equity <= 0:
        return None
    return (market_value / equity).quantize(_SIX_PLACES)


def calculate_weighted_benchmark_return(
    *,
    name: str,
    start_date: date,
    end_date: date,
    prices: Mapping[str, BenchmarkPrice],
    weights: Mapping[str, Decimal],
) -> BenchmarkReturn:
    """Calculate a weighted benchmark return from constituent price returns."""

    missing_symbols = tuple(symbol for symbol in sorted(weights) if symbol not in prices)
    if missing_symbols:
        return BenchmarkReturn(
            name=name,
            start_date=start_date,
            end_date=end_date,
            total_return=None,
            missing_symbols=missing_symbols,
        )

    total_return = Decimal("0.000000")
    for symbol, weight in weights.items():
        price = prices[symbol]
        symbol_return = calculate_period_return(price.start_price, price.end_price)
        if symbol_return is None:
            return BenchmarkReturn(
                name=name,
                start_date=start_date,
                end_date=end_date,
                total_return=None,
                missing_symbols=(symbol,),
            )
        total_return += weight * symbol_return

    return BenchmarkReturn(
        name=name,
        start_date=start_date,
        end_date=end_date,
        total_return=total_return.quantize(_SIX_PLACES),
        missing_symbols=(),
    )


def _price_on_or_before(
    session: Session,
    *,
    symbol: str,
    target_date: date,
) -> tuple[date, Decimal] | None:
    row = session.execute(
        select(BarsDaily.bar_date, BarsDaily.adjusted_close)
        .join(Instrument, Instrument.id == BarsDaily.instrument_id)
        .where(Instrument.symbol == symbol, BarsDaily.bar_date <= target_date)
        .order_by(BarsDaily.bar_date.desc())
        .limit(1)
    ).one_or_none()
    if row is None:
        return None
    return row.bar_date, row.adjusted_close
