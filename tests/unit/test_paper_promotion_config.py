from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from quant_core.settings import load_paper_promotion_config


def test_load_paper_promotion_config_reads_versioned_yaml() -> None:
    config = load_paper_promotion_config(Path("configs/paper_promotion.yaml"))

    assert config.minimum_completed_runs == 20
    assert config.maximum_open_critical_incidents == 0
    assert config.maximum_open_warning_incidents == 0
    assert config.manual_approval_required is True
    assert config.latest_run_expectation.require_approved is True
    assert config.latest_run_expectation.min_fill_ratio == Decimal("1.0")
    assert config.latest_run_expectation.max_rejected_order_count == 0
    assert config.latest_run_expectation.max_reconciliation_critical_rows == 0
    assert config.latest_run_expectation.max_open_incident_count == 0
