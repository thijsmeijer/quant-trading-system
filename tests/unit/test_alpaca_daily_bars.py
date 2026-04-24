from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from urllib.error import HTTPError

import pytest

from quant_core.data.ingestion.alpaca_daily_bars import (
    AlpacaDailyBarFetchRequest,
    AlpacaDailyBarResponseError,
    build_alpaca_bars_url,
    fetch_alpaca_daily_bars_payload,
    parse_alpaca_daily_bars_payload,
)


def test_build_alpaca_bars_url_uses_daily_timeframe_symbols_and_adjustment() -> None:
    request = AlpacaDailyBarFetchRequest(
        symbols=("SPY", "BND"),
        start_date=date(2026, 4, 23),
        end_date=date(2026, 4, 23),
        feed="iex",
        adjustment="all",
    )

    url = build_alpaca_bars_url(request)

    assert url.startswith("https://data.alpaca.markets/v2/stocks/bars?")
    assert "symbols=SPY%2CBND" in url
    assert "timeframe=1Day" in url
    assert "start=2026-04-23" in url
    assert "end=2026-04-23" in url
    assert "feed=iex" in url
    assert "adjustment=all" in url


def test_build_alpaca_bars_url_includes_page_token_when_present() -> None:
    request = AlpacaDailyBarFetchRequest(
        symbols=("SPY",),
        start_date=date(2026, 4, 23),
        end_date=date(2026, 4, 23),
        page_token="next page",
    )

    url = build_alpaca_bars_url(request)

    assert "page_token=next+page" in url


def test_fetch_alpaca_daily_bars_payload_merges_paginated_responses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = [
        _FakeResponse(
            {
                "bars": {
                    "SPY": [
                        {
                            "t": "2026-04-22T04:00:00Z",
                            "o": 500.0,
                            "h": 505.0,
                            "l": 499.0,
                            "c": 504.0,
                            "v": 1000,
                        }
                    ]
                },
                "next_page_token": "page-2",
            }
        ),
        _FakeResponse(
            {
                "bars": {
                    "SPY": [
                        {
                            "t": "2026-04-23T04:00:00Z",
                            "o": 504.0,
                            "h": 509.0,
                            "l": 503.0,
                            "c": 508.0,
                            "v": 1100,
                        }
                    ]
                }
            }
        ),
    ]
    requested_urls: list[str] = []

    def fake_urlopen(request: object, timeout: int) -> _FakeResponse:
        del timeout
        requested_urls.append(request.full_url)  # type: ignore[attr-defined]
        return responses.pop(0)

    monkeypatch.setattr("quant_core.data.ingestion.alpaca_daily_bars.urlopen", fake_urlopen)

    payload = fetch_alpaca_daily_bars_payload(
        AlpacaDailyBarFetchRequest(
            symbols=("SPY",),
            start_date=date(2026, 4, 22),
            end_date=date(2026, 4, 23),
        ),
        api_key_id="key",
        api_secret_key="secret",
    )

    assert len(payload["bars"]["SPY"]) == 2
    assert "page_token=page-2" in requested_urls[1]
    assert "next_page_token" not in payload


def test_fetch_alpaca_daily_bars_payload_wraps_http_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_urlopen(request: object, timeout: int) -> object:
        del request, timeout
        raise HTTPError(
            url="https://data.alpaca.markets/v2/stocks/bars",
            code=403,
            msg="Forbidden",
            hdrs={},
            fp=None,
        )

    monkeypatch.setattr("quant_core.data.ingestion.alpaca_daily_bars.urlopen", fake_urlopen)

    with pytest.raises(AlpacaDailyBarResponseError, match="HTTP 403: Forbidden"):
        fetch_alpaca_daily_bars_payload(
            AlpacaDailyBarFetchRequest(
                symbols=("SPY",),
                start_date=date(2026, 4, 22),
                end_date=date(2026, 4, 23),
            ),
            api_key_id="key",
            api_secret_key="secret",
        )


def test_parse_alpaca_daily_bars_payload_returns_vendor_daily_bars() -> None:
    payload = {
        "bars": {
            "SPY": [
                {
                    "t": "2026-04-23T04:00:00Z",
                    "o": 500.0,
                    "h": 508.0,
                    "l": 499.5,
                    "c": 507.25,
                    "v": 12345678,
                    "n": 90000,
                    "vw": 506.1,
                }
            ]
        }
    }
    fetched_at = datetime(2026, 4, 23, 20, 20, tzinfo=UTC)

    bars = parse_alpaca_daily_bars_payload(
        payload,
        expected_symbols=("SPY",),
        fetched_at=fetched_at,
        adjustment="all",
    )

    assert len(bars) == 1
    assert bars[0].symbol == "SPY"
    assert bars[0].vendor == "alpaca"
    assert bars[0].bar_date == date(2026, 4, 23)
    assert bars[0].open == Decimal("500.000000")
    assert bars[0].high == Decimal("508.000000")
    assert bars[0].low == Decimal("499.500000")
    assert bars[0].close == Decimal("507.250000")
    assert bars[0].adjusted_close == Decimal("507.250000")
    assert bars[0].volume == 12345678
    assert bars[0].fetched_at == fetched_at
    assert bars[0].source_payload["adjustment"] == "all"
    assert bars[0].source_payload["alpaca_bar"]["c"] == 507.25


def test_parse_alpaca_daily_bars_payload_rejects_missing_symbols() -> None:
    with pytest.raises(AlpacaDailyBarResponseError, match="missing bars for symbols: BND"):
        parse_alpaca_daily_bars_payload(
            {"bars": {"SPY": []}},
            expected_symbols=("SPY", "BND"),
            fetched_at=datetime(2026, 4, 23, 20, 20, tzinfo=UTC),
            adjustment="all",
        )


def test_parse_alpaca_daily_bars_payload_rejects_unhandled_pagination() -> None:
    with pytest.raises(AlpacaDailyBarResponseError, match="pagination is not yet supported"):
        parse_alpaca_daily_bars_payload(
            {"bars": {"SPY": []}, "next_page_token": "next-page"},
            expected_symbols=("SPY",),
            fetched_at=datetime(2026, 4, 23, 20, 20, tzinfo=UTC),
            adjustment="all",
        )


def test_parse_alpaca_daily_bars_payload_rejects_naive_fetched_at() -> None:
    with pytest.raises(AlpacaDailyBarResponseError, match="timezone-aware"):
        parse_alpaca_daily_bars_payload(
            {"bars": {"SPY": [{"t": "2026-04-23T04:00:00Z"}]}},
            expected_symbols=("SPY",),
            fetched_at=datetime(2026, 4, 23, 20, 20),
            adjustment="all",
        )


class _FakeResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, *args: object) -> None:
        del args

    def read(self) -> bytes:
        import json

        return json.dumps(self._payload).encode("utf-8")
