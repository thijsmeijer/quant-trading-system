from __future__ import annotations

from datetime import date
from decimal import Decimal

from quant_core.execution import (
    IdentifiedTradeIntent,
    TradeIntent,
    identify_trade_intents,
)


def test_identify_trade_intents_assigns_stable_internal_ids_and_keys() -> None:
    intents = (
        TradeIntent(
            as_of=date(2026, 4, 22),
            symbol="SPY",
            side="BUY",
            current_weight=Decimal("0.500000"),
            target_weight=Decimal("0.750000"),
            delta_weight=Decimal("0.250000"),
        ),
        TradeIntent(
            as_of=date(2026, 4, 22),
            symbol="GLD",
            side="SELL",
            current_weight=Decimal("0.100000"),
            target_weight=Decimal("0.000000"),
            delta_weight=Decimal("-0.100000"),
        ),
    )

    identified_once = identify_trade_intents(intents)
    identified_twice = identify_trade_intents(intents)

    assert identified_once == identified_twice
    assert identified_once[0] == IdentifiedTradeIntent(
        internal_order_id=identified_once[0].internal_order_id,
        idempotency_key=identified_once[0].idempotency_key,
        intent=intents[1],
    )
    assert identified_once[1] == IdentifiedTradeIntent(
        internal_order_id=identified_once[1].internal_order_id,
        idempotency_key=identified_once[1].idempotency_key,
        intent=intents[0],
    )
    assert identified_once[0].internal_order_id.startswith("intent_")
    assert identified_once[0].idempotency_key.startswith("intent:")


def test_identify_trade_intents_orders_output_by_symbol_and_side() -> None:
    intents = (
        TradeIntent(
            as_of=date(2026, 4, 22),
            symbol="SPY",
            side="BUY",
            current_weight=Decimal("0.500000"),
            target_weight=Decimal("0.750000"),
            delta_weight=Decimal("0.250000"),
        ),
        TradeIntent(
            as_of=date(2026, 4, 22),
            symbol="BND",
            side="BUY",
            current_weight=Decimal("0.000000"),
            target_weight=Decimal("0.250000"),
            delta_weight=Decimal("0.250000"),
        ),
    )

    identified = identify_trade_intents(intents)

    assert [item.intent.symbol for item in identified] == ["BND", "SPY"]


def test_identify_trade_intents_changes_key_when_intent_payload_changes() -> None:
    first = identify_trade_intents(
        (
            TradeIntent(
                as_of=date(2026, 4, 22),
                symbol="SPY",
                side="BUY",
                current_weight=Decimal("0.500000"),
                target_weight=Decimal("0.750000"),
                delta_weight=Decimal("0.250000"),
            ),
        )
    )
    second = identify_trade_intents(
        (
            TradeIntent(
                as_of=date(2026, 4, 22),
                symbol="SPY",
                side="BUY",
                current_weight=Decimal("0.400000"),
                target_weight=Decimal("0.750000"),
                delta_weight=Decimal("0.350000"),
            ),
        )
    )

    assert first[0].internal_order_id != second[0].internal_order_id
    assert first[0].idempotency_key != second[0].idempotency_key
