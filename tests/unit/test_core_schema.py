from quant_core.data.models import Base


def test_core_metadata_exposes_market_data_tables() -> None:
    tables = Base.metadata.tables

    assert {
        "instruments",
        "trading_calendar",
        "raw_bars_daily",
        "bars_daily",
    }.issubset(tables.keys())

    instruments = tables["instruments"].c
    assert {"id", "symbol", "name", "category", "exchange", "is_active"}.issubset(
        instruments.keys()
    )

    trading_calendar = tables["trading_calendar"].c
    assert {
        "trading_date",
        "market_open_utc",
        "market_close_utc",
        "is_open",
        "is_early_close",
    }.issubset(trading_calendar.keys())

    raw_bars = tables["raw_bars_daily"].c
    assert {
        "id",
        "instrument_id",
        "vendor",
        "bar_date",
        "payload",
        "fetched_at",
    }.issubset(raw_bars.keys())

    normalized_bars = tables["bars_daily"].c
    assert {
        "id",
        "instrument_id",
        "bar_date",
        "open",
        "high",
        "low",
        "close",
        "adjusted_close",
        "volume",
        "raw_bar_id",
    }.issubset(normalized_bars.keys())
