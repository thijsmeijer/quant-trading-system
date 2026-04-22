"""Paper-only execution state transitions."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from quant_core.execution.identity import IdentifiedTradeIntent


class PaperExecutionStatus(StrEnum):
    """Internal paper-mode execution states."""

    PENDING = "pending"
    SUBMITTED = "submitted"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"


class InvalidPaperExecutionTransitionError(ValueError):
    """Raised when a paper execution transition is not allowed."""


@dataclass(frozen=True, slots=True)
class PaperExecutionOrder:
    """Internal paper-mode execution record for one identified intent."""

    intent: IdentifiedTradeIntent
    status: PaperExecutionStatus


def create_paper_execution_order(*, intent: IdentifiedTradeIntent) -> PaperExecutionOrder:
    """Create the initial paper-mode execution record for an identified intent."""

    return PaperExecutionOrder(
        intent=intent,
        status=PaperExecutionStatus.PENDING,
    )


def transition_paper_execution_order(
    *,
    order: PaperExecutionOrder,
    new_status: PaperExecutionStatus,
) -> PaperExecutionOrder:
    """Apply one allowed paper-mode lifecycle transition."""

    allowed_transitions = {
        PaperExecutionStatus.PENDING: {
            PaperExecutionStatus.SUBMITTED,
            PaperExecutionStatus.CANCELED,
            PaperExecutionStatus.REJECTED,
        },
        PaperExecutionStatus.SUBMITTED: {
            PaperExecutionStatus.FILLED,
            PaperExecutionStatus.CANCELED,
            PaperExecutionStatus.REJECTED,
        },
        PaperExecutionStatus.FILLED: set(),
        PaperExecutionStatus.CANCELED: set(),
        PaperExecutionStatus.REJECTED: set(),
    }

    current_status = order.status
    if new_status not in allowed_transitions[current_status]:
        raise InvalidPaperExecutionTransitionError(
            "cannot transition paper execution order from "
            f"{current_status.name} to {new_status.name}"
        )

    return PaperExecutionOrder(
        intent=order.intent,
        status=new_status,
    )
