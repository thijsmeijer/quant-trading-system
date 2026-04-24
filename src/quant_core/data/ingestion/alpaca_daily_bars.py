"""Alpaca daily-bar fetch adapter for real-data paper workflows."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from quant_core.data.ingestion.daily_bars import VendorDailyBar

ALPACA_BARS_URL = "https://data.alpaca.markets/v2/stocks/bars"
PRICE_PRECISION = Decimal("0.000001")


class AlpacaDailyBarResponseError(ValueError):
    """Raised when Alpaca returns an unusable daily-bar payload."""


@dataclass(frozen=True, slots=True)
class AlpacaDailyBarFetchRequest:
    """Request parameters for Alpaca daily bars."""

    symbols: tuple[str, ...]
    start_date: date
    end_date: date
    feed: str = "iex"
    adjustment: str = "all"
    page_token: str | None = None


def build_alpaca_bars_url(request: AlpacaDailyBarFetchRequest) -> str:
    """Build the Alpaca historical bars URL for daily ETF bars."""

    query_params = {
        "symbols": ",".join(request.symbols),
        "timeframe": "1Day",
        "start": request.start_date.isoformat(),
        "end": request.end_date.isoformat(),
        "feed": request.feed,
        "adjustment": request.adjustment,
        "limit": 10000,
    }
    if request.page_token is not None:
        query_params["page_token"] = request.page_token

    query = urlencode(query_params)
    return f"{ALPACA_BARS_URL}?{query}"


def fetch_alpaca_daily_bars_payload(
    request: AlpacaDailyBarFetchRequest,
    *,
    api_key_id: str,
    api_secret_key: str,
) -> Mapping[str, Any]:
    """Fetch raw Alpaca daily-bar JSON, following page tokens when present."""

    merged: dict[str, Any] = {"bars": {}}
    page_token = request.page_token

    while True:
        page_request = AlpacaDailyBarFetchRequest(
            symbols=request.symbols,
            start_date=request.start_date,
            end_date=request.end_date,
            feed=request.feed,
            adjustment=request.adjustment,
            page_token=page_token,
        )
        payload = _fetch_alpaca_daily_bars_page(
            page_request,
            api_key_id=api_key_id,
            api_secret_key=api_secret_key,
        )
        _merge_bars_payload(merged, payload)
        next_page_token = payload.get("next_page_token")
        if not next_page_token:
            return merged
        page_token = str(next_page_token)


def _fetch_alpaca_daily_bars_page(
    request: AlpacaDailyBarFetchRequest,
    *,
    api_key_id: str,
    api_secret_key: str,
) -> Mapping[str, Any]:
    http_request = Request(
        build_alpaca_bars_url(request),
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
        raise AlpacaDailyBarResponseError(
            f"alpaca request failed with HTTP {exc.code}: {exc.reason}"
        ) from exc
    except URLError as exc:
        raise AlpacaDailyBarResponseError(f"alpaca request failed: {exc.reason}") from exc
    if not isinstance(payload, Mapping):
        raise AlpacaDailyBarResponseError("alpaca response must be a JSON object")
    return payload


def _merge_bars_payload(merged: dict[str, Any], payload: Mapping[str, Any]) -> None:
    bars_by_symbol = payload.get("bars")
    if not isinstance(bars_by_symbol, Mapping):
        raise AlpacaDailyBarResponseError("alpaca response missing bars object")

    merged_bars = merged["bars"]
    if not isinstance(merged_bars, dict):
        raise AlpacaDailyBarResponseError("internal merged bars container is invalid")

    for symbol, bars in bars_by_symbol.items():
        if not isinstance(bars, list):
            raise AlpacaDailyBarResponseError(f"alpaca bars for {symbol} must be a list")
        merged_bars.setdefault(str(symbol), []).extend(bars)


def parse_alpaca_daily_bars_payload(
    payload: Mapping[str, Any],
    *,
    expected_symbols: Sequence[str],
    fetched_at: datetime,
    adjustment: str,
) -> list[VendorDailyBar]:
    """Convert an Alpaca bars payload into canonical vendor daily bars."""

    if fetched_at.tzinfo is None:
        raise AlpacaDailyBarResponseError("fetched_at must be timezone-aware")

    bars_by_symbol = payload.get("bars")
    if not isinstance(bars_by_symbol, Mapping):
        raise AlpacaDailyBarResponseError("alpaca response missing bars object")
    if payload.get("next_page_token"):
        raise AlpacaDailyBarResponseError(
            "pagination is not yet supported; narrow the date range before importing"
        )

    parsed: list[VendorDailyBar] = []
    missing_symbols: list[str] = []
    for symbol in expected_symbols:
        raw_bars = bars_by_symbol.get(symbol)
        if not isinstance(raw_bars, list) or not raw_bars:
            missing_symbols.append(symbol)
            continue
        for raw_bar in raw_bars:
            if not isinstance(raw_bar, Mapping):
                raise AlpacaDailyBarResponseError(f"alpaca bar for {symbol} must be an object")
            parsed.append(_parse_one_bar(symbol, raw_bar, fetched_at, adjustment))

    if missing_symbols:
        joined = ", ".join(sorted(missing_symbols))
        raise AlpacaDailyBarResponseError(f"missing bars for symbols: {joined}")

    return sorted(parsed, key=lambda item: (item.symbol, item.bar_date))


def _parse_one_bar(
    symbol: str,
    raw_bar: Mapping[str, Any],
    fetched_at: datetime,
    adjustment: str,
) -> VendorDailyBar:
    timestamp = datetime.fromisoformat(str(raw_bar["t"]).replace("Z", "+00:00"))
    bar_date = timestamp.astimezone(UTC).date()
    close = _decimal(raw_bar["c"])
    return VendorDailyBar(
        symbol=symbol,
        vendor="alpaca",
        bar_date=bar_date,
        open=_decimal(raw_bar["o"]),
        high=_decimal(raw_bar["h"]),
        low=_decimal(raw_bar["l"]),
        close=close,
        adjusted_close=close,
        volume=int(raw_bar["v"]),
        fetched_at=fetched_at.astimezone(UTC),
        source_payload={
            "adjustment": adjustment,
            "alpaca_bar": dict(raw_bar),
        },
    )


def _decimal(value: object) -> Decimal:
    return Decimal(str(value)).quantize(PRICE_PRECISION)
