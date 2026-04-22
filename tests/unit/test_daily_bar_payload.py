from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from quant_core.data.ingestion.daily_bars import VendorDailyBar, build_canonical_payload


def test_build_canonical_payload_preserves_vendor_source_and_core_fields() -> None:
    fetched_at = datetime(2026, 4, 20, 21, 15, tzinfo=UTC)
    bar = VendorDailyBar(
        symbol="SPY",
        vendor="test_vendor",
        bar_date=date(2026, 4, 20),
        open=Decimal("598.120000"),
        high=Decimal("603.000000"),
        low=Decimal("597.400000"),
        close=Decimal("602.330000"),
        adjusted_close=Decimal("602.330000"),
        volume=123456789,
        fetched_at=fetched_at,
        source_payload={
            "ticker": "SPY",
            "date": "2026-04-20",
            "prices": {"o": "598.12", "h": "603.00", "l": "597.40", "c": "602.33"},
            "volume": 123456789,
        },
    )

    payload = build_canonical_payload(bar)

    assert payload["symbol"] == "SPY"
    assert payload["vendor"] == "test_vendor"
    assert payload["bar_date"] == "2026-04-20"
    assert payload["fetched_at"] == fetched_at.isoformat()
    assert payload["prices"] == {
        "open": "598.120000",
        "high": "603.000000",
        "low": "597.400000",
        "close": "602.330000",
        "adjusted_close": "602.330000",
    }
    assert payload["volume"] == 123456789
    assert payload["source_payload"] == {
        "ticker": "SPY",
        "date": "2026-04-20",
        "prices": {"o": "598.12", "h": "603.00", "l": "597.40", "c": "602.33"},
        "volume": 123456789,
    }
