from __future__ import annotations

import os
import subprocess
import sys


def test_makefile_backed_cli_modules_execute_under_python_m() -> None:
    modules = (
        "quant_core.data.bootstrap_cli",
        "quant_core.data.ingestion.alpaca_trading_calendar_cli",
        "quant_core.data.ingestion.trading_calendar_cli",
        "quant_core.execution.paper_account_cli",
        "quant_core.data.ingestion.alpaca_daily_bars_cli",
        "quant_core.data.ingestion.daily_bars_cli",
        "quant_core.execution.cli",
        "quant_core.reporting.burnin_cli",
        "quant_core.reporting.paper_performance_cli",
        "quant_core.dashboard.cli",
    )
    env = os.environ.copy()
    env["PYTHONPATH"] = "src"

    for module in modules:
        completed = subprocess.run(
            [sys.executable, "-m", module, "--help"],
            check=False,
            cwd=".",
            env=env,
            capture_output=True,
            text=True,
        )

        assert completed.returncode == 0, completed.stderr
        assert "usage:" in completed.stdout
