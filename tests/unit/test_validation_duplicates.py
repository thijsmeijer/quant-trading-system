from __future__ import annotations

from datetime import date

from quant_core.data.validation.daily_bars import DailyBarValidationService, DuplicateKey


def test_find_duplicate_keys_returns_only_duplicate_groups() -> None:
    keys = [
        ("SPY", date(2026, 4, 20)),
        ("SPY", date(2026, 4, 20)),
        ("QQQ", date(2026, 4, 20)),
        ("TLT", date(2026, 4, 21)),
        ("TLT", date(2026, 4, 21)),
        ("TLT", date(2026, 4, 21)),
    ]

    duplicates = DailyBarValidationService.find_duplicate_keys(keys)

    assert duplicates == [
        DuplicateKey(key=("SPY", date(2026, 4, 20)), occurrences=2),
        DuplicateKey(key=("TLT", date(2026, 4, 21)), occurrences=3),
    ]
