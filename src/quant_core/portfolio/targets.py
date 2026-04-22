"""Portfolio target seams over strategy-produced weights."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from typing import Final

TARGET_WEIGHT_PRECISION: Final = Decimal("0.000001")


class DuplicateTargetWeightError(ValueError):
    """Raised when a target includes the same symbol more than once."""


class InvalidTargetNormalizationError(ValueError):
    """Raised when a target cannot be normalized into usable weights."""


@dataclass(frozen=True, slots=True)
class PortfolioTarget:
    """Read-only target weights at one decision timestamp."""

    as_of: date
    weights: tuple[tuple[str, Decimal], ...]

    @property
    def gross_exposure(self) -> Decimal:
        return sum((weight for _, weight in self.weights), start=Decimal("0")).quantize(
            TARGET_WEIGHT_PRECISION,
            rounding=ROUND_HALF_UP,
        )

    def weights_by_symbol(self) -> dict[str, Decimal]:
        return dict(self.weights)


def build_portfolio_target(
    *,
    as_of: date,
    weights: Mapping[str, Decimal] | Iterable[tuple[str, Decimal]],
) -> PortfolioTarget:
    """Normalize a long-only target-weight proposal into a stable portfolio seam."""

    normalized_weights: list[tuple[str, Decimal]] = []
    seen_symbols: set[str] = set()

    for raw_symbol, raw_weight in _iter_weights(weights):
        symbol = raw_symbol.strip().upper()
        if not symbol:
            raise ValueError("symbol must not be empty")
        if symbol in seen_symbols:
            raise DuplicateTargetWeightError(f"duplicate target weight for symbol: {symbol}")

        quantized_weight = raw_weight.quantize(
            TARGET_WEIGHT_PRECISION,
            rounding=ROUND_HALF_UP,
        )
        if quantized_weight < Decimal("0"):
            raise ValueError("target weights must be non-negative")

        seen_symbols.add(symbol)
        normalized_weights.append((symbol, quantized_weight))

    return PortfolioTarget(
        as_of=as_of,
        weights=tuple(sorted(normalized_weights, key=lambda item: item[0])),
    )


def normalize_portfolio_target(target: PortfolioTarget) -> PortfolioTarget:
    """Scale a proposed target into deterministic long-only weights summing to one."""

    positive_weights = [
        (symbol, weight) for symbol, weight in target.weights if weight > Decimal("0")
    ]
    total_weight = sum((weight for _, weight in positive_weights), start=Decimal("0"))
    if total_weight <= Decimal("0"):
        raise InvalidTargetNormalizationError(
            "portfolio target must have a positive total weight to normalize"
        )

    normalized_weights: list[tuple[str, Decimal]] = []
    for symbol, weight in positive_weights:
        normalized_weight = (weight / total_weight).quantize(
            TARGET_WEIGHT_PRECISION,
            rounding=ROUND_HALF_UP,
        )
        normalized_weights.append((symbol, normalized_weight))

    rounded_total = sum((weight for _, weight in normalized_weights), start=Decimal("0"))
    residual = Decimal("1.000000") - rounded_total
    if residual != Decimal("0"):
        index = _residual_receiver_index(normalized_weights)
        symbol, weight = normalized_weights[index]
        normalized_weights[index] = (
            symbol,
            (weight + residual).quantize(
                TARGET_WEIGHT_PRECISION,
                rounding=ROUND_HALF_UP,
            ),
        )

    return PortfolioTarget(
        as_of=target.as_of,
        weights=tuple(sorted(normalized_weights, key=lambda item: item[0])),
    )


def _iter_weights(
    weights: Mapping[str, Decimal] | Iterable[tuple[str, Decimal]],
) -> Iterable[tuple[str, Decimal]]:
    if isinstance(weights, Mapping):
        return weights.items()
    return weights


def _residual_receiver_index(weights: list[tuple[str, Decimal]]) -> int:
    return max(
        range(len(weights)),
        key=lambda index: (weights[index][1], weights[index][0]),
    )
