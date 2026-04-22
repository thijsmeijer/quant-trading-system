from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from quant_core.execution import (
    IdentifiedTradeIntent,
    InvalidPaperExecutionTransitionError,
    PaperExecutionStatus,
    create_paper_execution_order,
    transition_paper_execution_order,
)
from quant_core.execution.identity import identify_trade_intents
from quant_core.execution.intents import TradeIntent


def test_create_paper_execution_order_starts_pending() -> None:
    identified_intent = _identified_intent()

    order = create_paper_execution_order(intent=identified_intent)

    assert order.status is PaperExecutionStatus.PENDING
    assert order.intent == identified_intent


def test_transition_paper_execution_order_allows_happy_path() -> None:
    order = create_paper_execution_order(intent=_identified_intent())

    submitted = transition_paper_execution_order(
        order=order,
        new_status=PaperExecutionStatus.SUBMITTED,
    )
    filled = transition_paper_execution_order(
        order=submitted,
        new_status=PaperExecutionStatus.FILLED,
    )

    assert submitted.status is PaperExecutionStatus.SUBMITTED
    assert filled.status is PaperExecutionStatus.FILLED


def test_transition_paper_execution_order_rejects_invalid_jump() -> None:
    order = create_paper_execution_order(intent=_identified_intent())

    with pytest.raises(InvalidPaperExecutionTransitionError, match="PENDING"):
        transition_paper_execution_order(
            order=order,
            new_status=PaperExecutionStatus.FILLED,
        )


def test_transition_paper_execution_order_allows_cancel_from_pending_or_submitted() -> None:
    order = create_paper_execution_order(intent=_identified_intent())

    pending_canceled = transition_paper_execution_order(
        order=order,
        new_status=PaperExecutionStatus.CANCELED,
    )
    submitted = transition_paper_execution_order(
        order=order,
        new_status=PaperExecutionStatus.SUBMITTED,
    )
    submitted_canceled = transition_paper_execution_order(
        order=submitted,
        new_status=PaperExecutionStatus.CANCELED,
    )

    assert pending_canceled.status is PaperExecutionStatus.CANCELED
    assert submitted_canceled.status is PaperExecutionStatus.CANCELED


def _identified_intent() -> IdentifiedTradeIntent:
    return identify_trade_intents(
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
    )[0]
