"""Trading-calendar loading into canonical calendar storage."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from quant_core.data.models import TradingCalendar


class TradingCalendarValidationError(ValueError):
    """Raised when a trading-calendar input file is invalid."""


@dataclass(frozen=True, slots=True)
class TradingCalendarEntry:
    """One canonical trading-calendar row normalized to UTC."""

    trading_date: date
    market_open_utc: datetime | None
    market_close_utc: datetime | None
    is_open: bool
    is_early_close: bool = False


@dataclass(frozen=True, slots=True)
class TradingCalendarLoadResult:
    """Summary of one trading-calendar load run."""

    processed_rows: int


def load_trading_calendar_file(path: Path) -> tuple[TradingCalendarEntry, ...]:
    """Load and validate a trading-calendar JSON array from disk."""

    import json

    payload = json.loads(path.read_text())
    if not isinstance(payload, list):
        raise TradingCalendarValidationError("trading-calendar file must be a JSON array")
    return tuple(_parse_entry(item) for item in payload)


class TradingCalendarLoaderService:
    """Idempotently load canonical trading-calendar rows."""

    def load(
        self,
        session: Session,
        entries: tuple[TradingCalendarEntry, ...],
    ) -> TradingCalendarLoadResult:
        """Upsert canonical calendar rows keyed by trading date."""

        for entry in entries:
            session.execute(
                insert(TradingCalendar)
                .values(
                    trading_date=entry.trading_date,
                    market_open_utc=entry.market_open_utc,
                    market_close_utc=entry.market_close_utc,
                    is_open=entry.is_open,
                    is_early_close=entry.is_early_close,
                )
                .on_conflict_do_update(
                    index_elements=[TradingCalendar.trading_date],
                    set_={
                        "market_open_utc": entry.market_open_utc,
                        "market_close_utc": entry.market_close_utc,
                        "is_open": entry.is_open,
                        "is_early_close": entry.is_early_close,
                    },
                )
            )

        return TradingCalendarLoadResult(processed_rows=len(entries))

    def load_from_file(self, session: Session, path: Path) -> TradingCalendarLoadResult:
        """Load canonical calendar rows from a validated local file."""

        return self.load(session, load_trading_calendar_file(path))


def _parse_entry(item: object) -> TradingCalendarEntry:
    if not isinstance(item, dict):
        raise TradingCalendarValidationError("each trading-calendar row must be a JSON object")

    trading_date = date.fromisoformat(str(item["trading_date"]))
    is_open = bool(item["is_open"])
    market_open = _parse_utc_datetime(item.get("market_open_utc"), field_name="market_open_utc")
    market_close = _parse_utc_datetime(
        item.get("market_close_utc"),
        field_name="market_close_utc",
    )
    is_early_close = bool(item.get("is_early_close", False))

    if is_open and (market_open is None or market_close is None):
        raise TradingCalendarValidationError(
            "open trading-calendar rows require both market_open_utc and market_close_utc"
        )
    if market_open is not None and market_close is not None and market_open >= market_close:
        raise TradingCalendarValidationError(
            "market_open_utc must be earlier than market_close_utc"
        )

    return TradingCalendarEntry(
        trading_date=trading_date,
        market_open_utc=market_open,
        market_close_utc=market_close,
        is_open=is_open,
        is_early_close=is_early_close,
    )


def _parse_utc_datetime(value: object, *, field_name: str) -> datetime | None:
    if value is None:
        return None

    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        raise TradingCalendarValidationError(f"{field_name} must be timezone-aware")
    return parsed.astimezone(UTC)
