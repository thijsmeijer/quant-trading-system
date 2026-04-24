"""Alpaca trading-calendar fetch adapter for paper operation scheduling."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from quant_core.data.ingestion.trading_calendar import TradingCalendarEntry

ALPACA_CALENDAR_URL = "https://paper-api.alpaca.markets/v2/calendar"
NEW_YORK = ZoneInfo("America/New_York")
REGULAR_MARKET_CLOSE = time(16, 0)


class AlpacaTradingCalendarResponseError(ValueError):
    """Raised when Alpaca returns an unusable trading-calendar payload."""


@dataclass(frozen=True, slots=True)
class AlpacaTradingCalendarRequest:
    """Request parameters for Alpaca's trading calendar endpoint."""

    start_date: date
    end_date: date


def build_alpaca_calendar_url(request: AlpacaTradingCalendarRequest) -> str:
    """Build the Alpaca trading calendar URL."""

    query = urlencode(
        {
            "start": request.start_date.isoformat(),
            "end": request.end_date.isoformat(),
        }
    )
    return f"{ALPACA_CALENDAR_URL}?{query}"


def fetch_alpaca_trading_calendar_payload(
    request: AlpacaTradingCalendarRequest,
    *,
    api_key_id: str,
    api_secret_key: str,
) -> Sequence[Mapping[str, Any]]:
    """Fetch raw Alpaca trading-calendar JSON."""

    http_request = Request(
        build_alpaca_calendar_url(request),
        headers={
            "APCA-API-KEY-ID": api_key_id,
            "APCA-API-SECRET-KEY": api_secret_key,
            "Accept": "application/json",
        },
    )
    try:
        with urlopen(http_request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        raise AlpacaTradingCalendarResponseError(
            f"alpaca calendar request failed with HTTP {exc.code}: {exc.reason}"
        ) from exc
    except URLError as exc:
        raise AlpacaTradingCalendarResponseError(
            f"alpaca calendar request failed: {exc.reason}"
        ) from exc

    if not isinstance(payload, list):
        raise AlpacaTradingCalendarResponseError("alpaca calendar response must be a JSON array")
    return tuple(_require_mapping(item) for item in payload)


def parse_alpaca_trading_calendar_payload(
    payload: Sequence[Mapping[str, Any]],
) -> tuple[TradingCalendarEntry, ...]:
    """Convert Alpaca calendar rows into canonical UTC trading-calendar entries."""

    return tuple(_parse_calendar_row(row) for row in payload)


def _parse_calendar_row(row: Mapping[str, Any]) -> TradingCalendarEntry:
    trading_date = date.fromisoformat(str(row["date"]))
    open_time = _parse_hhmm(str(row["open"]))
    close_time = _parse_hhmm(str(row["close"]))
    market_open = datetime.combine(trading_date, open_time, tzinfo=NEW_YORK).astimezone(UTC)
    market_close = datetime.combine(trading_date, close_time, tzinfo=NEW_YORK).astimezone(UTC)

    return TradingCalendarEntry(
        trading_date=trading_date,
        market_open_utc=market_open,
        market_close_utc=market_close,
        is_open=True,
        is_early_close=close_time < REGULAR_MARKET_CLOSE,
    )


def _parse_hhmm(value: str) -> time:
    return time.fromisoformat(value)


def _require_mapping(item: object) -> Mapping[str, Any]:
    if not isinstance(item, Mapping):
        raise AlpacaTradingCalendarResponseError("each Alpaca calendar row must be an object")
    return item
