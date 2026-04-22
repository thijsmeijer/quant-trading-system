"""Versioned ETF universe definition parsing and canonical instrument loading."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from quant_core.data.models.market_data import Instrument


class UniverseValidationError(ValueError):
    """Raised when the versioned ETF universe source fails validation."""


class UniverseMetadata(BaseModel):
    """High-level metadata for the versioned ETF universe source."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str
    venue: str
    bar_frequency: str
    regular_hours_only: bool


class UniverseEligibility(BaseModel):
    """Research eligibility filters attached to a universe version."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    min_price: Decimal = Field(gt=0)
    min_average_daily_volume: int = Field(gt=0)
    min_history_days: int = Field(gt=0)
    excluded_flags: tuple[str, ...] = ("inverse", "leveraged")

    @field_validator("excluded_flags", mode="before")
    @classmethod
    def normalize_excluded_flags(cls, value: Any) -> tuple[str, ...]:
        if value is None:
            return ("inverse", "leveraged")
        if not isinstance(value, list | tuple):
            raise TypeError("excluded_flags must be a list or tuple of strings")

        normalized = sorted({str(flag).strip().lower() for flag in value if str(flag).strip()})
        if not normalized:
            raise ValueError("excluded_flags must not be empty")
        return tuple(normalized)


class UniverseInstrumentDefinition(BaseModel):
    """One ETF entry from the versioned source file."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    symbol: str
    name: str
    category: str
    exchange: str
    is_active: bool
    flags: tuple[str, ...] = ()

    @field_validator("symbol")
    @classmethod
    def uppercase_symbol(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("symbol must not be empty")
        return normalized

    @field_validator("flags", mode="before")
    @classmethod
    def normalize_flags(cls, value: Any) -> tuple[str, ...]:
        if value is None:
            return ()
        if not isinstance(value, list | tuple):
            raise TypeError("flags must be a list or tuple of strings")

        normalized = sorted({str(flag).strip().lower() for flag in value if str(flag).strip()})
        return tuple(normalized)


class UniverseDefinition(BaseModel):
    """The full versioned ETF universe definition owned by the repo."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    version: int = Field(ge=1)
    as_of: date
    universe: UniverseMetadata
    eligibility: UniverseEligibility
    instruments: tuple[UniverseInstrumentDefinition, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def ensure_excluded_flags_are_absent(self) -> UniverseDefinition:
        excluded = set(self.eligibility.excluded_flags)

        offenders = []
        for instrument in self.instruments:
            blocked = sorted(excluded.intersection(instrument.flags))
            if blocked:
                offenders.append(f"{instrument.symbol} ({', '.join(blocked)})")

        if offenders:
            joined = ", ".join(offenders)
            raise ValueError(f"instruments contain excluded flags: {joined}")

        return self


@dataclass(frozen=True, slots=True)
class UniverseLoadResult:
    """Minimal summary for a universe load into canonical instruments."""

    version: int
    upserted_instruments: int


def load_universe_definition(path: Path) -> UniverseDefinition:
    """Load and validate the versioned ETF universe file."""

    raw_payload = yaml.safe_load(path.read_text())
    if not isinstance(raw_payload, dict):
        raise UniverseValidationError("universe file must contain a top-level mapping")

    try:
        return UniverseDefinition.model_validate(raw_payload)
    except ValidationError as exc:
        raise UniverseValidationError(str(exc)) from exc


class UniverseLoaderService:
    """Load a versioned ETF universe source into canonical instrument metadata."""

    def load_from_file(self, session: Session, path: Path) -> UniverseLoadResult:
        definition = load_universe_definition(path)
        return self.sync(session, definition)

    def sync(self, session: Session, definition: UniverseDefinition) -> UniverseLoadResult:
        for instrument in definition.instruments:
            statement = (
                insert(Instrument)
                .values(
                    symbol=instrument.symbol,
                    name=instrument.name,
                    category=instrument.category,
                    exchange=instrument.exchange,
                    is_active=instrument.is_active,
                )
                .on_conflict_do_update(
                    index_elements=[Instrument.symbol],
                    set_={
                        "name": instrument.name,
                        "category": instrument.category,
                        "exchange": instrument.exchange,
                        "is_active": instrument.is_active,
                    },
                )
            )
            session.execute(statement)

        return UniverseLoadResult(
            version=definition.version,
            upserted_instruments=len(definition.instruments),
        )
