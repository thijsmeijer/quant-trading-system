"""Persisted pre-trade risk gate over target positions."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Final

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from quant_core.data import (
    OperationalRunMode,
    RiskCheckWrite,
    StoredRiskCheck,
    StrategyRunRepository,
)
from quant_core.data.models import BarsDaily, Instrument, Order, TradingCalendar
from quant_core.portfolio import PortfolioTargetPosition

OPEN_ORDER_STATUSES: Final[tuple[str, ...]] = ("pending", "submitted")


class MissingTargetPositionStateError(ValueError):
    """Raised when persisted risk evaluation has no target positions to inspect."""


@dataclass(frozen=True, slots=True)
class PreTradeRiskConfig:
    """Risk limits and operator controls for one pre-trade decision."""

    max_gross_exposure: Decimal
    max_position_notional: Decimal
    max_open_orders: int


@dataclass(frozen=True, slots=True)
class PreTradeRiskCheck:
    """One explicit pre-trade risk check result."""

    check_name: str
    status: str
    reason_code: str | None
    details: dict[str, str | int]


@dataclass(frozen=True, slots=True)
class PreTradeRiskDecision:
    """Overall pre-trade approval or rejection across all configured controls."""

    approved: bool
    failed_reason_codes: tuple[str, ...]
    checks: tuple[PreTradeRiskCheck, ...]


@dataclass(frozen=True, slots=True)
class PersistedPreTradeRiskDecision:
    """Persisted risk evaluation plus stored table-backed checks."""

    approved: bool
    failed_reason_codes: tuple[str, ...]
    checks: tuple[StoredRiskCheck, ...]


def evaluate_pretrade_risk(
    *,
    target_positions: Sequence[PortfolioTargetPosition],
    active_symbols: set[str],
    latest_price_date_by_symbol: Mapping[str, date],
    expected_price_date: date,
    execution_session_is_open: bool,
    kill_switch_active: bool,
    open_order_count: int,
    config: PreTradeRiskConfig,
) -> PreTradeRiskDecision:
    """Evaluate configured pre-trade controls over target positions."""

    checks: list[PreTradeRiskCheck] = []

    symbol_positions = [position for position in target_positions if position.symbol is not None]
    position_symbols = {
        position.symbol for position in symbol_positions if position.symbol is not None
    }
    inactive_symbols = sorted(symbol for symbol in position_symbols if symbol not in active_symbols)
    checks.append(
        _check(
            name="symbol_validity",
            passed=not inactive_symbols,
            reason_code="inactive_symbol" if inactive_symbols else None,
            details={"inactive_symbols": ",".join(inactive_symbols)} if inactive_symbols else {},
        )
    )

    gross_exposure = sum(
        (position.target_weight for position in symbol_positions),
        start=Decimal("0"),
    )
    checks.append(
        _check(
            name="gross_exposure",
            passed=gross_exposure <= config.max_gross_exposure,
            reason_code="gross_exposure_limit"
            if gross_exposure > config.max_gross_exposure
            else None,
            details={
                "gross_exposure": str(gross_exposure),
                "max_gross_exposure": str(config.max_gross_exposure),
            },
        )
    )

    oversized_symbols = sorted(
        position.symbol
        for position in symbol_positions
        if position.symbol is not None and position.target_notional > config.max_position_notional
    )
    checks.append(
        _check(
            name="position_notional_limit",
            passed=not oversized_symbols,
            reason_code="position_notional_limit" if oversized_symbols else None,
            details={
                "oversized_symbols": ",".join(oversized_symbols),
                "max_position_notional": str(config.max_position_notional),
            },
        )
    )

    stale_symbols = sorted(
        symbol
        for symbol in position_symbols
        if latest_price_date_by_symbol.get(symbol) != expected_price_date
    )
    checks.append(
        _check(
            name="stale_data",
            passed=not stale_symbols,
            reason_code="stale_data" if stale_symbols else None,
            details={
                "expected_price_date": expected_price_date.isoformat(),
                "stale_symbols": ",".join(stale_symbols),
            },
        )
    )

    checks.append(
        _check(
            name="session_validity",
            passed=execution_session_is_open,
            reason_code="invalid_session" if not execution_session_is_open else None,
            details={},
        )
    )
    checks.append(
        _check(
            name="kill_switch",
            passed=not kill_switch_active,
            reason_code="kill_switch_active" if kill_switch_active else None,
            details={},
        )
    )
    checks.append(
        _check(
            name="open_order_count",
            passed=open_order_count <= config.max_open_orders,
            reason_code="open_order_limit" if open_order_count > config.max_open_orders else None,
            details={
                "open_order_count": open_order_count,
                "max_open_orders": config.max_open_orders,
            },
        )
    )

    failed_reason_codes = tuple(
        check.reason_code for check in checks if check.reason_code is not None
    )
    return PreTradeRiskDecision(
        approved=not failed_reason_codes,
        failed_reason_codes=failed_reason_codes,
        checks=tuple(checks),
    )


class PersistedPreTradeRiskGate:
    """Evaluate and persist risk checks for one stored strategy run."""

    def __init__(self, repository: StrategyRunRepository | None = None) -> None:
        self._repository = repository or StrategyRunRepository()

    def evaluate_for_strategy_run(
        self,
        session: Session,
        *,
        strategy_run_id: int,
        run_mode: OperationalRunMode,
        config: PreTradeRiskConfig,
        checked_at: datetime,
        kill_switch_active: bool = False,
    ) -> PersistedPreTradeRiskDecision:
        """Evaluate pre-trade controls over persisted target positions and store the result."""

        run = self._repository.get_run(session, strategy_run_id=strategy_run_id)
        if run is None:
            raise ValueError(f"Unknown strategy_run_id: {strategy_run_id}")
        if run.run_mode != run_mode:
            raise ValueError("strategy run mode does not match risk evaluation mode")
        if run.execution_date is None:
            raise ValueError("strategy run must include an execution_date for risk evaluation")

        stored_positions = self._repository.list_target_positions(
            session,
            strategy_run_id=strategy_run_id,
        )
        if not stored_positions:
            raise MissingTargetPositionStateError(
                f"strategy run has no persisted target positions: {strategy_run_id}"
            )

        target_positions = tuple(
            PortfolioTargetPosition(
                allocation_key=position.allocation_key,
                target_weight=position.target_weight,
                target_notional=position.target_notional,
                target_quantity=position.target_quantity,
                reference_price=position.reference_price,
                symbol=position.symbol,
            )
            for position in stored_positions
        )
        symbols = sorted(
            {position.symbol for position in target_positions if position.symbol is not None}
        )
        decision = evaluate_pretrade_risk(
            target_positions=target_positions,
            active_symbols=self._active_symbols(session, symbols=symbols),
            latest_price_date_by_symbol=self._latest_price_dates(session, symbols=symbols),
            expected_price_date=run.signal_date,
            execution_session_is_open=self._execution_session_is_open(
                session,
                execution_date=run.execution_date,
            ),
            kill_switch_active=kill_switch_active,
            open_order_count=self._open_order_count(session, run_mode=run_mode),
            config=config,
        )
        stored_checks = self._repository.replace_risk_checks(
            session,
            strategy_run_id=strategy_run_id,
            risk_checks=[
                RiskCheckWrite(
                    check_scope="pre_trade",
                    check_name=check.check_name,
                    status=check.status,
                    reason_code=check.reason_code,
                    checked_at=checked_at,
                    details=check.details,
                )
                for check in decision.checks
            ],
        )
        return PersistedPreTradeRiskDecision(
            approved=decision.approved,
            failed_reason_codes=decision.failed_reason_codes,
            checks=stored_checks,
        )

    def _active_symbols(self, session: Session, *, symbols: Sequence[str]) -> set[str]:
        if not symbols:
            return set()
        rows = session.execute(
            select(Instrument.symbol)
            .where(Instrument.symbol.in_(symbols), Instrument.is_active.is_(True))
            .order_by(Instrument.symbol)
        ).all()
        return {symbol for (symbol,) in rows}

    def _latest_price_dates(
        self,
        session: Session,
        *,
        symbols: Sequence[str],
    ) -> dict[str, date]:
        if not symbols:
            return {}
        rows = session.execute(
            select(Instrument.symbol, func.max(BarsDaily.bar_date))
            .join(Instrument, Instrument.id == BarsDaily.instrument_id)
            .where(Instrument.symbol.in_(symbols))
            .group_by(Instrument.symbol)
            .order_by(Instrument.symbol)
        ).all()
        return {symbol: latest_bar_date for symbol, latest_bar_date in rows}

    def _execution_session_is_open(self, session: Session, *, execution_date: date) -> bool:
        row = session.execute(
            select(TradingCalendar.is_open).where(TradingCalendar.trading_date == execution_date)
        ).one_or_none()
        if row is None:
            return False
        return bool(row.is_open)

    def _open_order_count(self, session: Session, *, run_mode: OperationalRunMode) -> int:
        count = session.scalar(
            select(func.count())
            .select_from(Order)
            .where(Order.run_mode == run_mode, Order.status.in_(OPEN_ORDER_STATUSES))
        )
        return int(count or 0)


def _check(
    *,
    name: str,
    passed: bool,
    reason_code: str | None,
    details: dict[str, str | int],
) -> PreTradeRiskCheck:
    return PreTradeRiskCheck(
        check_name=name,
        status="pass" if passed else "fail",
        reason_code=reason_code,
        details=details,
    )
