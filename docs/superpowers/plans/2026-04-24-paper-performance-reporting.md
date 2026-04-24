# Paper Performance Reporting Plan

## Goal

Add the immediate reporting needed to run the next 60 paper-trading days with real data:

- daily account equity, cash, buying power, and PnL
- current paper positions and allocation weights
- latest-run order/fill counts and turnover
- benchmark comparison against cash, SPY, and a 60/40 SPY/BND mix
- one JSON command that can be run after every `paper-run`

## Non-Goals

- no strategy changes
- no broker submission changes
- no live trading
- no new database tables
- no universe expansion
- no performance conclusions before multiple real paper runs exist

## Constraints

- Use only persisted PostgreSQL state.
- Keep paper, dev, and live modes separated by `run_mode`.
- Use adjusted daily closes for benchmark comparisons.
- Avoid lookahead by using bars on or before the requested benchmark dates.
- Treat missing benchmark prices as reportable data gaps instead of inventing returns.

## Implementation Steps

1. Add a read-only reporting module under `src/quant_core/reporting/`.
2. Add tests for:
   - total return from account snapshots
   - latest-run turnover from fills
   - benchmark return calculations
   - missing benchmark price handling
3. Add a CLI entrypoint and Makefile target:
   - `quant-paper-performance-report`
   - `make paper-performance-report`
4. Export the report types from `quant_core.reporting`.
5. Add the command to the Phase 6 local runbook immediately after `paper-review`.
6. Run focused tests, lint, format check, type check, then `make verify`.
7. Run the report against `quant_core_real_paper` and summarize the first real-paper result.

## Operator Command

After each paper run:

```bash
make paper-performance-report ARGS="--database-url postgresql+psycopg://quant:quant@127.0.0.1:5432/quant_core_real_paper --run-mode paper"
```

## Expected First-Run Interpretation

The first real run should mostly tell us that the machine can select, size, fill, reconcile, and report positions from real Alpaca daily bars. It will not yet prove profitability. With only one completed real paper run, portfolio and benchmark returns should normally be near zero because there is no multi-day performance window yet.

## Exit Criteria

- The report command works against migrated databases.
- The report command works against `quant_core_real_paper`.
- Verification passes.
- The runbook contains the command the operator should use daily.
