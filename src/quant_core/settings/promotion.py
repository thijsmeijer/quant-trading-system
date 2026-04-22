"""Versioned paper-promotion configuration."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]


class InvalidPaperPromotionConfigError(ValueError):
    """Raised when paper-promotion configuration is invalid."""


@dataclass(frozen=True, slots=True)
class PaperRunExpectationConfig:
    """Expected paper-run behavior before promotion is considered."""

    require_approved: bool
    min_fill_ratio: Decimal
    max_rejected_order_count: int
    max_reconciliation_critical_rows: int
    max_open_incident_count: int
    expected_order_count: int | None = None
    max_order_count_delta: int = 0

    def __post_init__(self) -> None:
        if self.min_fill_ratio < Decimal("0") or self.min_fill_ratio > Decimal("1"):
            raise InvalidPaperPromotionConfigError("min_fill_ratio must be between 0 and 1")
        if self.max_rejected_order_count < 0:
            raise InvalidPaperPromotionConfigError("max_rejected_order_count must be non-negative")
        if self.max_reconciliation_critical_rows < 0:
            raise InvalidPaperPromotionConfigError(
                "max_reconciliation_critical_rows must be non-negative"
            )
        if self.max_open_incident_count < 0:
            raise InvalidPaperPromotionConfigError("max_open_incident_count must be non-negative")
        if self.expected_order_count is not None and self.expected_order_count < 0:
            raise InvalidPaperPromotionConfigError("expected_order_count must be non-negative")
        if self.max_order_count_delta < 0:
            raise InvalidPaperPromotionConfigError("max_order_count_delta must be non-negative")


@dataclass(frozen=True, slots=True)
class PaperPromotionConfig:
    """Versioned promotion gates for paper-to-live readiness."""

    minimum_completed_runs: int
    maximum_open_critical_incidents: int
    maximum_open_warning_incidents: int
    manual_approval_required: bool
    latest_run_expectation: PaperRunExpectationConfig

    def __post_init__(self) -> None:
        if self.minimum_completed_runs <= 0:
            raise InvalidPaperPromotionConfigError("minimum_completed_runs must be positive")
        if self.maximum_open_critical_incidents < 0:
            raise InvalidPaperPromotionConfigError(
                "maximum_open_critical_incidents must be non-negative"
            )
        if self.maximum_open_warning_incidents < 0:
            raise InvalidPaperPromotionConfigError(
                "maximum_open_warning_incidents must be non-negative"
            )


def load_paper_promotion_config(path: Path) -> PaperPromotionConfig:
    """Load paper-promotion config from a versioned YAML file."""

    payload = yaml.safe_load(path.read_text())
    if not isinstance(payload, dict):
        raise InvalidPaperPromotionConfigError("promotion config must be a mapping")

    root = payload.get("paper_promotion")
    if not isinstance(root, dict):
        raise InvalidPaperPromotionConfigError("paper_promotion section must be a mapping")

    latest_run_expectation = _expectation_config(root.get("latest_run_expectation"))
    return PaperPromotionConfig(
        minimum_completed_runs=int(root["minimum_completed_runs"]),
        maximum_open_critical_incidents=int(root["maximum_open_critical_incidents"]),
        maximum_open_warning_incidents=int(root["maximum_open_warning_incidents"]),
        manual_approval_required=bool(root.get("manual_approval_required", True)),
        latest_run_expectation=latest_run_expectation,
    )


def _expectation_config(payload: Any) -> PaperRunExpectationConfig:
    if not isinstance(payload, dict):
        raise InvalidPaperPromotionConfigError("latest_run_expectation section must be a mapping")

    expected_order_count = payload.get("expected_order_count")
    return PaperRunExpectationConfig(
        require_approved=bool(payload["require_approved"]),
        min_fill_ratio=Decimal(str(payload["min_fill_ratio"])),
        max_rejected_order_count=int(payload["max_rejected_order_count"]),
        max_reconciliation_critical_rows=int(payload["max_reconciliation_critical_rows"]),
        max_open_incident_count=int(payload["max_open_incident_count"]),
        expected_order_count=(
            int(expected_order_count) if expected_order_count is not None else None
        ),
        max_order_count_delta=int(payload.get("max_order_count_delta", 0)),
    )
