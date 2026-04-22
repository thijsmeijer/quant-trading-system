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


def _iter_weights(
    weights: Mapping[str, Decimal] | Iterable[tuple[str, Decimal]],
) -> Iterable[tuple[str, Decimal]]:
    if isinstance(weights, Mapping):
        return weights.items()
    return weights
