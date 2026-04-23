"""Paper-mode account bootstrap into canonical runtime snapshot storage."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy.orm import Session

from quant_core.data import AccountSnapshotWrite, SnapshotRepository, StoredAccountSnapshot


class InvalidPaperAccountBootstrapError(ValueError):
    """Raised when the initial paper account snapshot is invalid."""


@dataclass(frozen=True, slots=True)
class PaperAccountBootstrapInput:
    """One paper-mode account snapshot owned by the operator bootstrap path."""

    cash: Decimal
    equity: Decimal
    buying_power: Decimal
    as_of: datetime


class PaperAccountBootstrapService:
    """Persist one explicit paper account snapshot for local operator setup."""

    def __init__(self, snapshot_repository: SnapshotRepository | None = None) -> None:
        self._snapshot_repository = snapshot_repository or SnapshotRepository()

    def bootstrap(
        self,
        session: Session,
        *,
        snapshot: PaperAccountBootstrapInput,
    ) -> StoredAccountSnapshot:
        """Store an idempotent paper account snapshot normalized to UTC."""

        if snapshot.cash < Decimal("0"):
            raise InvalidPaperAccountBootstrapError("cash must be non-negative")
        if snapshot.equity <= Decimal("0"):
            raise InvalidPaperAccountBootstrapError("equity must be positive")
        if snapshot.buying_power < Decimal("0"):
            raise InvalidPaperAccountBootstrapError("buying_power must be non-negative")
        if snapshot.as_of.tzinfo is None:
            raise InvalidPaperAccountBootstrapError("as_of must be timezone-aware")

        return self._snapshot_repository.store_account_snapshot(
            session,
            AccountSnapshotWrite(
                run_mode="paper",
                cash=snapshot.cash,
                equity=snapshot.equity,
                buying_power=snapshot.buying_power,
                as_of=snapshot.as_of.astimezone(UTC),
            ),
        )
