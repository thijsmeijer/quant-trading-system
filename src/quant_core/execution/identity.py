"""Internal identity and idempotency for trade intents."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256

from quant_core.execution.intents import TradeIntent


@dataclass(frozen=True, slots=True)
class IdentifiedTradeIntent:
    """Trade intent with stable internal identity and idempotency material."""

    internal_order_id: str
    idempotency_key: str
    intent: TradeIntent


def identify_trade_intents(
    intents: tuple[TradeIntent, ...],
) -> tuple[IdentifiedTradeIntent, ...]:
    """Attach deterministic internal identity to broker-agnostic trade intents."""

    identified: list[IdentifiedTradeIntent] = []
    for intent in sorted(intents, key=lambda item: (item.symbol, item.side)):
        digest = sha256(_intent_fingerprint(intent).encode("ascii")).hexdigest()
        identified.append(
            IdentifiedTradeIntent(
                internal_order_id=f"intent_{digest[:16]}",
                idempotency_key=f"intent:{digest}",
                intent=intent,
            )
        )

    return tuple(identified)


def _intent_fingerprint(intent: TradeIntent) -> str:
    return "|".join(
        [
            intent.as_of.isoformat(),
            intent.symbol,
            intent.side,
            f"{intent.current_weight:.6f}",
            f"{intent.target_weight:.6f}",
            f"{intent.delta_weight:.6f}",
        ]
    )
