"""Momentum strategy seam with persisted run artifacts."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Final

from sqlalchemy.orm import Session

from quant_core.data import (
    OperationalRunMode,
    SignalWrite,
    StoredSignal,
    StoredStrategyRun,
    StoredTargetWeight,
    StrategyRunCreate,
    StrategyRunRepository,
    TargetWeightWrite,
)
from quant_core.features.daily_bars import build_momentum_snapshot
from quant_core.research.daily_bars import ResearchDailyBar, ResearchDataset

TARGET_WEIGHT_PRECISION: Final = Decimal("0.000001")
TARGET_WEIGHT_TOTAL: Final = Decimal("1.000000")


class InvalidMomentumStrategyConfigError(ValueError):
    """Raised when the momentum strategy configuration is invalid."""


class UnavailableSignalDateError(ValueError):
    """Raised when the strategy is asked to run on an unavailable date."""


@dataclass(frozen=True, slots=True)
class MomentumStrategyConfig:
    """Configuration for the MVP ETF momentum strategy."""

    version: str
    lookback_bars: int
    trend_lookback_bars: int
    top_n: int
    minimum_momentum: Decimal = Decimal("0")
    cash_allocation_key: str = "cash"

    def __post_init__(self) -> None:
        if self.lookback_bars <= 0:
            raise InvalidMomentumStrategyConfigError("lookback_bars must be positive")
        if self.trend_lookback_bars <= 0:
            raise InvalidMomentumStrategyConfigError("trend_lookback_bars must be positive")
        if self.top_n <= 0:
            raise InvalidMomentumStrategyConfigError("top_n must be positive")
        if not self.version.strip():
            raise InvalidMomentumStrategyConfigError("version must not be empty")
        if not self.cash_allocation_key.strip():
            raise InvalidMomentumStrategyConfigError("cash_allocation_key must not be empty")

    def config_hash(self) -> str:
        """Build a deterministic config hash for persisted strategy runs."""

        payload = json.dumps(
            {
                "cash_allocation_key": self.cash_allocation_key,
                "lookback_bars": self.lookback_bars,
                "minimum_momentum": str(self.minimum_momentum),
                "top_n": self.top_n,
                "trend_lookback_bars": self.trend_lookback_bars,
                "version": self.version,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def metadata(self) -> dict[str, str | int]:
        """Expose persisted config metadata for strategy-run records."""

        return {
            "cash_allocation_key": self.cash_allocation_key,
            "lookback_bars": self.lookback_bars,
            "minimum_momentum": str(self.minimum_momentum),
            "top_n": self.top_n,
            "trend_lookback_bars": self.trend_lookback_bars,
            "version": self.version,
        }


@dataclass(frozen=True, slots=True)
class MomentumSignal:
    """One ranked momentum signal with trend-filter state."""

    symbol: str
    rank: int
    score: Decimal
    passes_trend_filter: bool
    is_selected: bool


@dataclass(frozen=True, slots=True)
class StrategyTargetWeight:
    """One target-weight row emitted by the momentum strategy."""

    allocation_key: str
    target_weight: Decimal
    symbol: str | None


@dataclass(frozen=True, slots=True)
class MomentumStrategyDecision:
    """Pure strategy decision before persistence."""

    signal_date: date
    execution_date: date
    signals: tuple[MomentumSignal, ...]
    target_weights: tuple[StrategyTargetWeight, ...]


@dataclass(frozen=True, slots=True)
class PersistedMomentumStrategyRun:
    """Persisted strategy run plus stored signals and target weights."""

    run: StoredStrategyRun
    signals: tuple[StoredSignal, ...]
    target_weights: tuple[StoredTargetWeight, ...]


class MomentumRotationStrategy:
    """Daily-bar ETF momentum strategy with a trend filter and cash fallback."""

    name = "momentum_rotation"

    def __init__(self, repository: StrategyRunRepository | None = None) -> None:
        self._repository = repository or StrategyRunRepository()

    def build_decision(
        self,
        *,
        dataset: ResearchDataset,
        signal_date: date,
        config: MomentumStrategyConfig,
        execution_date: date | None = None,
    ) -> MomentumStrategyDecision:
        """Build the pure strategy outputs without touching persistence."""

        available_dates = dataset.available_dates()
        if signal_date not in available_dates:
            detail = signal_date.isoformat()
            raise UnavailableSignalDateError(
                f"signal_date is not available in the dataset: {detail}"
            )

        resolved_execution_date = execution_date or dataset.next_available_date(signal_date)
        if resolved_execution_date is None or resolved_execution_date <= signal_date:
            raise UnavailableSignalDateError(
                "strategy execution requires a trading date after the signal date"
            )

        history = dataset.history_up_to(signal_date)
        momentum = build_momentum_snapshot(
            dataset=history,
            signal_date=signal_date,
            lookback_bars=config.lookback_bars,
        )
        ranked_scores = sorted(momentum.values.items(), key=lambda item: (-item[1], item[0]))

        eligible_symbols: list[str] = []
        raw_signals: list[MomentumSignal] = []
        for index, (symbol, score) in enumerate(ranked_scores, start=1):
            bars = history.bars_for_symbol(symbol)
            passes_trend_filter = _passes_trend_filter(
                bars=bars,
                trend_lookback_bars=config.trend_lookback_bars,
            )
            if passes_trend_filter and score > config.minimum_momentum:
                eligible_symbols.append(symbol)

            raw_signals.append(
                MomentumSignal(
                    symbol=symbol,
                    rank=index,
                    score=score,
                    passes_trend_filter=passes_trend_filter,
                    is_selected=False,
                )
            )

        selected_symbols = set(eligible_symbols[: config.top_n])
        signals = tuple(
            MomentumSignal(
                symbol=signal.symbol,
                rank=signal.rank,
                score=signal.score,
                passes_trend_filter=signal.passes_trend_filter,
                is_selected=signal.symbol in selected_symbols,
            )
            for signal in raw_signals
        )

        if selected_symbols:
            target_weights = _equal_weight_targets(selected_symbols)
        else:
            target_weights = (
                StrategyTargetWeight(
                    allocation_key=config.cash_allocation_key,
                    target_weight=TARGET_WEIGHT_TOTAL,
                    symbol=None,
                ),
            )

        return MomentumStrategyDecision(
            signal_date=signal_date,
            execution_date=resolved_execution_date,
            signals=signals,
            target_weights=target_weights,
        )

    def execute(
        self,
        session: Session,
        *,
        dataset: ResearchDataset,
        signal_date: date,
        run_mode: OperationalRunMode,
        config: MomentumStrategyConfig,
        started_at: datetime,
        execution_date: date | None = None,
    ) -> PersistedMomentumStrategyRun:
        """Persist one strategy run, its signals, and its target weights."""

        decision = self.build_decision(
            dataset=dataset,
            signal_date=signal_date,
            config=config,
            execution_date=execution_date,
        )

        metadata = {
            "config": config.metadata(),
            "eligible_symbol_count": sum(
                1
                for signal in decision.signals
                if signal.passes_trend_filter and signal.score > config.minimum_momentum
            ),
            "selected_symbols": [
                signal.symbol for signal in decision.signals if signal.is_selected
            ],
        }
        run = self._repository.create_run(
            session,
            StrategyRunCreate(
                run_mode=run_mode,
                strategy_name=self.name,
                config_version=config.version,
                config_hash=config.config_hash(),
                signal_date=decision.signal_date,
                execution_date=decision.execution_date,
                status="running",
                started_at=started_at,
                metadata_json=metadata,
            ),
        )

        stored_signals = self._repository.replace_signals(
            session,
            strategy_run_id=run.id,
            signals=[
                SignalWrite(
                    symbol=signal.symbol,
                    signal_name="momentum_rank",
                    rank=signal.rank,
                    score=signal.score,
                    is_selected=signal.is_selected,
                    generated_at=started_at,
                )
                for signal in decision.signals
            ],
        )
        stored_target_weights = self._repository.replace_target_weights(
            session,
            strategy_run_id=run.id,
            target_weights=[
                TargetWeightWrite(
                    allocation_key=target_weight.allocation_key,
                    target_weight=target_weight.target_weight,
                    generated_at=started_at,
                    symbol=target_weight.symbol,
                )
                for target_weight in decision.target_weights
            ],
        )
        completed_run = self._repository.update_run_status(
            session,
            strategy_run_id=run.id,
            status="completed",
            completed_at=started_at,
            metadata_json=metadata,
        )

        return PersistedMomentumStrategyRun(
            run=completed_run,
            signals=stored_signals,
            target_weights=stored_target_weights,
        )


def _passes_trend_filter(
    *,
    bars: tuple[ResearchDailyBar, ...],
    trend_lookback_bars: int,
) -> bool:
    if len(bars) < trend_lookback_bars:
        return False

    trailing_bars = bars[-trend_lookback_bars:]
    current_price = trailing_bars[-1].adjusted_close
    average_price = sum(
        (bar.adjusted_close for bar in trailing_bars), start=Decimal("0")
    ) / Decimal(len(trailing_bars))
    return current_price > average_price


def _equal_weight_targets(symbols: set[str]) -> tuple[StrategyTargetWeight, ...]:
    sorted_symbols = sorted(symbols)
    raw_weight = (TARGET_WEIGHT_TOTAL / Decimal(len(sorted_symbols))).quantize(
        TARGET_WEIGHT_PRECISION,
        rounding=ROUND_HALF_UP,
    )
    rows = [
        StrategyTargetWeight(
            allocation_key=symbol,
            target_weight=raw_weight,
            symbol=symbol,
        )
        for symbol in sorted_symbols
    ]
    rounded_total = sum((row.target_weight for row in rows), start=Decimal("0"))
    residual = TARGET_WEIGHT_TOTAL - rounded_total
    if residual != Decimal("0"):
        largest_index = max(
            range(len(rows)),
            key=lambda index: (rows[index].target_weight, rows[index].allocation_key),
        )
        row = rows[largest_index]
        rows[largest_index] = StrategyTargetWeight(
            allocation_key=row.allocation_key,
            target_weight=(row.target_weight + residual).quantize(
                TARGET_WEIGHT_PRECISION,
                rounding=ROUND_HALF_UP,
            ),
            symbol=row.symbol,
        )

    return tuple(rows)
