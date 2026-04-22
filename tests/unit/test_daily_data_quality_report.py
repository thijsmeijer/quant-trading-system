from __future__ import annotations

from datetime import date

from quant_core.data.ingestion.universe import UniverseDefinition
from quant_core.data.validation.daily_bars import (
    DailyBarValidationReport,
    DuplicateKey,
    MissingBarIssue,
    PriceSanityIssue,
    StaleSymbolIssue,
)
from quant_core.reporting.data_quality import DailyDataQualityReportBuilder


def test_daily_data_quality_report_scopes_validation_output_to_active_universe() -> None:
    universe = UniverseDefinition.model_validate(
        {
            "version": 1,
            "as_of": "2026-04-22",
            "universe": {
                "name": "core_us_etfs",
                "venue": "us_equities",
                "bar_frequency": "daily",
                "regular_hours_only": True,
            },
            "eligibility": {
                "min_price": 20,
                "min_average_daily_volume": 1_000_000,
                "min_history_days": 252,
                "excluded_flags": ["leveraged", "inverse"],
            },
            "instruments": [
                {
                    "symbol": "SPY",
                    "name": "SPDR S&P 500 ETF Trust",
                    "category": "broad_us_equity",
                    "exchange": "ARCA",
                    "is_active": True,
                    "flags": [],
                },
                {
                    "symbol": "QQQ",
                    "name": "Invesco QQQ Trust",
                    "category": "broad_us_equity",
                    "exchange": "NASDAQ",
                    "is_active": True,
                    "flags": [],
                },
            ],
        }
    )
    validation_report = DailyBarValidationReport(
        checked_as_of=date(2026, 4, 22),
        raw_duplicates=[
            DuplicateKey(key=("SPY", "vendor_a", date(2026, 4, 21)), occurrences=2),
            DuplicateKey(key=("TQQQ", "vendor_a", date(2026, 4, 21)), occurrences=2),
        ],
        normalized_duplicates=[
            DuplicateKey(key=("QQQ", date(2026, 4, 21)), occurrences=2),
        ],
        missing_bars=[
            MissingBarIssue(symbol="SPY", bar_date=date(2026, 4, 22)),
            MissingBarIssue(symbol="TQQQ", bar_date=date(2026, 4, 22)),
        ],
        stale_symbols=[
            StaleSymbolIssue(
                symbol="QQQ",
                latest_bar_date=date(2026, 4, 21),
                expected_bar_date=date(2026, 4, 22),
            ),
            StaleSymbolIssue(
                symbol="TQQQ",
                latest_bar_date=None,
                expected_bar_date=date(2026, 4, 22),
            ),
        ],
        price_sanity_issues=[
            PriceSanityIssue(
                symbol="SPY",
                bar_date=date(2026, 4, 21),
                detail="high/low envelope violated",
            )
        ],
    )

    report = DailyDataQualityReportBuilder().build(universe, validation_report)

    assert report.checked_as_of == date(2026, 4, 22)
    assert report.universe_name == "core_us_etfs"
    assert report.active_universe_symbols == ("QQQ", "SPY")
    assert report.duplicate_symbols == ("QQQ", "SPY")
    assert report.missing_symbols == ("SPY",)
    assert report.stale_symbols == ("QQQ",)
    assert report.price_sanity_symbols == ("SPY",)
    assert report.failing_symbols == ("QQQ", "SPY")
    assert report.summary.raw_duplicate_group_count == 1
    assert report.summary.normalized_duplicate_group_count == 1
    assert report.summary.duplicate_symbol_count == 2
    assert report.summary.missing_bar_count == 1
    assert report.summary.stale_symbol_count == 1
    assert report.summary.price_sanity_issue_count == 1
    assert report.summary.failing_symbol_count == 2
