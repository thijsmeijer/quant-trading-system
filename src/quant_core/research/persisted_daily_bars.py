"""Load canonical research datasets from persisted daily-bar tables."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from sqlalchemy import select
from sqlalchemy.orm import Session

from quant_core.data.models.market_data import BarsDaily, Instrument, TradingCalendar
from quant_core.research.daily_bars import ResearchDailyBar, ResearchDataset


class PersistedMarketDataUnavailableError(ValueError):
    """Raised when persisted market data cannot support a paper run."""


@dataclass(frozen=True, slots=True)
class PersistedResearchWindow:
    """Research dataset plus the next execution date resolved from persistence."""

    dataset: ResearchDataset
    execution_date: date


class PersistedResearchDatasetLoader:
    """Build a research dataset from persisted normalized daily bars."""

    def load_for_signal_date(
        self,
        session: Session,
        *,
        signal_date: date,
    ) -> PersistedResearchWindow:
        """Load active daily bars through the signal date and resolve next execution date."""

        execution_date = self._next_execution_date(session, signal_date=signal_date)
        rows = session.execute(
            select(
                Instrument.symbol,
                BarsDaily.bar_date,
                BarsDaily.open,
                BarsDaily.high,
                BarsDaily.low,
                BarsDaily.close,
                BarsDaily.adjusted_close,
                BarsDaily.volume,
            )
            .join(Instrument, Instrument.id == BarsDaily.instrument_id)
            .where(
                Instrument.is_active.is_(True),
                BarsDaily.bar_date <= signal_date,
            )
            .order_by(Instrument.symbol, BarsDaily.bar_date)
        ).all()
        if not rows:
            detail = signal_date.isoformat()
            raise PersistedMarketDataUnavailableError(
                f"no persisted active daily bars are available through signal_date {detail}"
            )

        dataset = ResearchDataset.from_bars(
            ResearchDailyBar(
                symbol=symbol,
                bar_date=bar_date,
                open=open_price,
                high=high_price,
                low=low_price,
                close=close_price,
                adjusted_close=adjusted_close,
                volume=volume,
            )
            for (
                symbol,
                bar_date,
                open_price,
                high_price,
                low_price,
                close_price,
                adjusted_close,
                volume,
            ) in rows
        )
        if signal_date not in dataset.available_dates():
            detail = signal_date.isoformat()
            raise PersistedMarketDataUnavailableError(
                f"persisted daily bars do not include signal_date {detail}"
            )
        return PersistedResearchWindow(dataset=dataset, execution_date=execution_date)

    def _next_execution_date(self, session: Session, *, signal_date: date) -> date:
        execution_date = session.execute(
            select(TradingCalendar.trading_date)
            .where(
                TradingCalendar.is_open.is_(True),
                TradingCalendar.trading_date > signal_date,
            )
            .order_by(TradingCalendar.trading_date)
            .limit(1)
        ).scalar_one_or_none()
        if execution_date is None:
            detail = signal_date.isoformat()
            raise PersistedMarketDataUnavailableError(
                f"trading calendar does not contain an execution date after signal_date {detail}"
            )
        return execution_date
