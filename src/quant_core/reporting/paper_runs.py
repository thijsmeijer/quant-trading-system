"""Archived read models for one paper-run execution."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import cast

from quant_core.data import (
    OperationalRunMode,
    StoredAccountSnapshot,
    StoredFill,
    StoredOrder,
    StoredRiskSnapshot,
    StoredStrategyRun,
)
from quant_core.reconciliation import OperationalReconciliationSummary


@dataclass(frozen=True, slots=True)
class PaperRunReport:
    """Archived operator-facing summary for one paper run."""

    run_id: int
    run_mode: OperationalRunMode
    strategy_name: str
    signal_date: date
    execution_date: date | None
    status: str
    approved: bool
    failed_reason_codes: tuple[str, ...]
    order_count: int
    fill_count: int
    rejected_order_count: int
    open_incident_count: int
    reconciliation_total_rows: int
    reconciliation_mismatched_rows: int
    reconciliation_critical_rows: int
    latest_account_equity: Decimal | None
    latest_gross_exposure: Decimal | None
    generated_at: datetime

    def as_metadata(self) -> dict[str, object]:
        """Serialize the archived report into JSON-compatible metadata."""

        return {
            "run_id": self.run_id,
            "run_mode": self.run_mode,
            "strategy_name": self.strategy_name,
            "signal_date": self.signal_date.isoformat(),
            "execution_date": (
                self.execution_date.isoformat() if self.execution_date is not None else None
            ),
            "status": self.status,
            "approved": self.approved,
            "failed_reason_codes": list(self.failed_reason_codes),
            "order_count": self.order_count,
            "fill_count": self.fill_count,
            "rejected_order_count": self.rejected_order_count,
            "open_incident_count": self.open_incident_count,
            "reconciliation_total_rows": self.reconciliation_total_rows,
            "reconciliation_mismatched_rows": self.reconciliation_mismatched_rows,
            "reconciliation_critical_rows": self.reconciliation_critical_rows,
            "latest_account_equity": (
                str(self.latest_account_equity) if self.latest_account_equity is not None else None
            ),
            "latest_gross_exposure": (
                str(self.latest_gross_exposure) if self.latest_gross_exposure is not None else None
            ),
            "generated_at": self.generated_at.isoformat(),
        }


def build_paper_run_report(
    *,
    run: StoredStrategyRun,
    approved: bool,
    failed_reason_codes: tuple[str, ...],
    orders: tuple[StoredOrder, ...],
    fills: tuple[StoredFill, ...],
    account_snapshot: StoredAccountSnapshot | None,
    risk_snapshot: StoredRiskSnapshot | None,
    open_incident_count: int,
    reconciliation: OperationalReconciliationSummary,
    generated_at: datetime,
) -> PaperRunReport:
    """Build a paper-run report from persisted operational state."""

    rejected_order_count = sum(1 for order in orders if order.status == "rejected")
    return PaperRunReport(
        run_id=run.id,
        run_mode=run.run_mode,
        strategy_name=run.strategy_name,
        signal_date=run.signal_date,
        execution_date=run.execution_date,
        status=run.status,
        approved=approved,
        failed_reason_codes=failed_reason_codes,
        order_count=len(orders),
        fill_count=len(fills),
        rejected_order_count=rejected_order_count,
        open_incident_count=open_incident_count,
        reconciliation_total_rows=reconciliation.total_rows,
        reconciliation_mismatched_rows=reconciliation.mismatched_rows,
        reconciliation_critical_rows=reconciliation.critical_rows,
        latest_account_equity=(account_snapshot.equity if account_snapshot is not None else None),
        latest_gross_exposure=(risk_snapshot.gross_exposure if risk_snapshot is not None else None),
        generated_at=generated_at,
    )


def load_paper_run_report(metadata: Mapping[str, object] | None) -> PaperRunReport | None:
    """Load an archived paper-run report from stored metadata."""

    if metadata is None:
        return None

    failed_reason_codes = cast(Sequence[object], metadata["failed_reason_codes"])
    return PaperRunReport(
        run_id=int(str(metadata["run_id"])),
        run_mode=str(metadata["run_mode"]),  # type: ignore[arg-type]
        strategy_name=str(metadata["strategy_name"]),
        signal_date=date.fromisoformat(str(metadata["signal_date"])),
        execution_date=(
            date.fromisoformat(str(metadata["execution_date"]))
            if metadata["execution_date"] is not None
            else None
        ),
        status=str(metadata["status"]),
        approved=bool(metadata["approved"]),
        failed_reason_codes=tuple(str(item) for item in failed_reason_codes),
        order_count=int(str(metadata["order_count"])),
        fill_count=int(str(metadata["fill_count"])),
        rejected_order_count=int(str(metadata["rejected_order_count"])),
        open_incident_count=int(str(metadata["open_incident_count"])),
        reconciliation_total_rows=int(str(metadata["reconciliation_total_rows"])),
        reconciliation_mismatched_rows=int(str(metadata["reconciliation_mismatched_rows"])),
        reconciliation_critical_rows=int(str(metadata["reconciliation_critical_rows"])),
        latest_account_equity=(
            Decimal(str(metadata["latest_account_equity"]))
            if metadata["latest_account_equity"] is not None
            else None
        ),
        latest_gross_exposure=(
            Decimal(str(metadata["latest_gross_exposure"]))
            if metadata["latest_gross_exposure"] is not None
            else None
        ),
        generated_at=datetime.fromisoformat(str(metadata["generated_at"])),
    )
