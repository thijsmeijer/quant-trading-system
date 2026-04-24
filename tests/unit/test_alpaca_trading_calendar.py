from __future__ import annotations

from datetime import UTC, date, datetime
from urllib.error import HTTPError

import pytest

from quant_core.data.ingestion.alpaca_trading_calendar import (
    AlpacaTradingCalendarRequest,
    AlpacaTradingCalendarResponseError,
    build_alpaca_calendar_url,
    fetch_alpaca_trading_calendar_payload,
    parse_alpaca_trading_calendar_payload,
)


def test_build_alpaca_calendar_url_uses_start_and_end_dates() -> None:
    url = build_alpaca_calendar_url(
        AlpacaTradingCalendarRequest(
            start_date=date(2026, 4, 24),
            end_date=date(2026, 4, 28),
        )
    )

    assert url.startswith("https://paper-api.alpaca.markets/v2/calendar?")
    assert "start=2026-04-24" in url
    assert "end=2026-04-28" in url


def test_parse_alpaca_calendar_payload_returns_canonical_utc_rows() -> None:
    rows = parse_alpaca_trading_calendar_payload(
        [
            {
                "date": "2026-04-24",
                "open": "09:30",
                "close": "16:00",
                "session_open": "0400",
                "session_close": "2000",
            },
            {
                "date": "2026-04-27",
                "open": "09:30",
                "close": "16:00",
            },
        ]
    )

    assert len(rows) == 2
    assert rows[0].trading_date == date(2026, 4, 24)
    assert rows[0].market_open_utc == datetime(2026, 4, 24, 13, 30, tzinfo=UTC)
    assert rows[0].market_close_utc == datetime(2026, 4, 24, 20, 0, tzinfo=UTC)
    assert rows[0].is_open is True
    assert rows[0].is_early_close is False
    assert rows[1].trading_date == date(2026, 4, 27)


def test_parse_alpaca_calendar_payload_marks_early_close() -> None:
    rows = parse_alpaca_trading_calendar_payload(
        [
            {
                "date": "2026-11-27",
                "open": "09:30",
                "close": "13:00",
            }
        ]
    )

    assert rows[0].market_close_utc == datetime(2026, 11, 27, 18, 0, tzinfo=UTC)
    assert rows[0].is_early_close is True


def test_fetch_alpaca_trading_calendar_payload_wraps_http_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_urlopen(request: object, timeout: int) -> object:
        del request, timeout
        raise HTTPError(
            url="https://paper-api.alpaca.markets/v2/calendar",
            code=403,
            msg="Forbidden",
            hdrs={},
            fp=None,
        )

    monkeypatch.setattr("quant_core.data.ingestion.alpaca_trading_calendar.urlopen", fake_urlopen)

    with pytest.raises(AlpacaTradingCalendarResponseError, match="HTTP 403: Forbidden"):
        fetch_alpaca_trading_calendar_payload(
            AlpacaTradingCalendarRequest(
                start_date=date(2026, 4, 24),
                end_date=date(2026, 4, 28),
            ),
            api_key_id="key",
            api_secret_key="secret",
        )
