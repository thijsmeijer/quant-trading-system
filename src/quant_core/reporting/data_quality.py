"""Daily data-quality reporting built from validation output."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from sqlalchemy.orm import Session

from quant_core.data.ingestion.universe import UniverseDefinition, load_universe_definition
from quant_core.data.validation.daily_bars import (
    DailyBarValidationReport,
    DailyBarValidationService,
)


@dataclass(frozen=True, slots=True)
class DailyDataQualitySummary:
    """Compact counts for the daily data-quality report."""

    raw_duplicate_group_count: int
    normalized_duplicate_group_count: int
    duplicate_symbol_count: int
    missing_bar_count: int
    stale_symbol_count: int
    price_sanity_issue_count: int
    failing_symbol_count: int


@dataclass(frozen=True, slots=True)
class DailyDataQualityReport:
    """Operator-facing daily summary of market-data validation output."""

    checked_as_of: date
    universe_name: str
    active_universe_symbols: tuple[str, ...]
    duplicate_symbols: tuple[str, ...]
    missing_symbols: tuple[str, ...]
    stale_symbols: tuple[str, ...]
    price_sanity_symbols: tuple[str, ...]
    failing_symbols: tuple[str, ...]
    summary: DailyDataQualitySummary


class DailyDataQualityReportBuilder:
    """Build a deterministic daily data-quality report from validation output."""

    def build(
        self,
        universe: UniverseDefinition,
        validation_report: DailyBarValidationReport,
    ) -> DailyDataQualityReport:
        active_universe_symbols = tuple(
            sorted(instrument.symbol for instrument in universe.instruments if instrument.is_active)
        )
        scoped_symbols = set(active_universe_symbols)

        raw_duplicate_groups = [
            duplicate
            for duplicate in validation_report.raw_duplicates
            if duplicate.key[0] in scoped_symbols
        ]
        normalized_duplicate_groups = [
            duplicate
            for duplicate in validation_report.normalized_duplicates
            if duplicate.key[0] in scoped_symbols
        ]
        duplicate_symbols = self._symbols(
            [duplicate.key[0] for duplicate in raw_duplicate_groups]
            + [duplicate.key[0] for duplicate in normalized_duplicate_groups]
        )
        missing_symbols = self._symbols(
            issue.symbol
            for issue in validation_report.missing_bars
            if issue.symbol in scoped_symbols
        )
        stale_symbols = self._symbols(
            issue.symbol
            for issue in validation_report.stale_symbols
            if issue.symbol in scoped_symbols
        )
        price_sanity_symbols = self._symbols(
            issue.symbol
            for issue in validation_report.price_sanity_issues
            if issue.symbol in scoped_symbols
        )
        failing_symbols = self._symbols(
            [*duplicate_symbols, *missing_symbols, *stale_symbols, *price_sanity_symbols]
        )

        return DailyDataQualityReport(
            checked_as_of=validation_report.checked_as_of,
            universe_name=universe.universe.name,
            active_universe_symbols=active_universe_symbols,
            duplicate_symbols=duplicate_symbols,
            missing_symbols=missing_symbols,
            stale_symbols=stale_symbols,
            price_sanity_symbols=price_sanity_symbols,
            failing_symbols=failing_symbols,
            summary=DailyDataQualitySummary(
                raw_duplicate_group_count=len(raw_duplicate_groups),
                normalized_duplicate_group_count=len(normalized_duplicate_groups),
                duplicate_symbol_count=len(duplicate_symbols),
                missing_bar_count=sum(
                    1 for issue in validation_report.missing_bars if issue.symbol in scoped_symbols
                ),
                stale_symbol_count=len(stale_symbols),
                price_sanity_issue_count=sum(
                    1
                    for issue in validation_report.price_sanity_issues
                    if issue.symbol in scoped_symbols
                ),
                failing_symbol_count=len(failing_symbols),
            ),
        )

    @staticmethod
    def _symbols(symbols: Iterable[str]) -> tuple[str, ...]:
        return tuple(sorted(set(symbols)))


class DailyDataQualityReportService:
    """Build daily data-quality reports directly from the canonical universe file."""

    def __init__(
        self,
        *,
        validator: DailyBarValidationService | None = None,
        builder: DailyDataQualityReportBuilder | None = None,
    ) -> None:
        self._validator = validator or DailyBarValidationService()
        self._builder = builder or DailyDataQualityReportBuilder()

    def build_from_file(
        self,
        session: Session,
        *,
        universe_path: Path,
        as_of: date,
    ) -> DailyDataQualityReport:
        universe = load_universe_definition(universe_path)
        validation_report = self._validator.validate(session, as_of=as_of)
        return self._builder.build(universe, validation_report)
