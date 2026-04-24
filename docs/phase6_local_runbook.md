# Phase 6 Local Runbook

Phase 6 is a sustained paper-trading burn-in from your local environment. The objective is to run the full paper machine repeatedly, archive the evidence, and review anomalies over time.

## What You Need Running

- local PostgreSQL through Docker Compose
- local Python environment through `.venv`
- the paper workflow command
- the burn-in review command
- the operator review command

## U.S. Session Timing

This system uses regular U.S. market hours only.

- New York market open: `09:30`
- New York market close: `16:00`

For Amsterdam, when New York is on EDT and Amsterdam is on CEST, that is usually:

- Amsterdam open: `15:30`
- Amsterdam close: `22:00`

Do not run the daily paper workflow before the close data is final. You do not need to run exactly at market open or exactly at market close.

## Recommended Local Schedule

Use one of these windows every trading day:

1. Same evening in Amsterdam:
   - `22:15` to `23:00`
2. Next morning or early afternoon in Amsterdam:
   - any time after the prior close data is final
   - finish before the next U.S. open at `15:30` Amsterdam time when DST alignment is EDT/CEST

The most practical local routine is the same-evening window.

## Daily Operator Checklist

1. Start local services if needed.

```bash
make postgres-up
```

2. Prepare the local database if this is a fresh environment or the universe changed.

```bash
make local-bootstrap ARGS="--database-url postgresql+psycopg://quant:quant@127.0.0.1:5432/quant_core_real_paper --universe-path configs/universe.yaml"
```

3. Load the trading calendar used for next-session execution dates.

```bash
make trading-calendar-fetch ARGS="--start-date 2026-04-24 --end-date 2026-12-31 --output-json /tmp/alpaca_trading_calendar_2026-04-24_2026-12-31.json"
make trading-calendar-import ARGS="--database-url postgresql+psycopg://quant:quant@127.0.0.1:5432/quant_core_real_paper --input-json /tmp/alpaca_trading_calendar_2026-04-24_2026-12-31.json"
```

`trading-calendar-fetch` reads Alpaca credentials from environment variables or `.env` keys named `ALPACA_API_KEY_ID` and `ALPACA_API_SECRET_KEY`. The generated input file is a JSON array of canonical trading-calendar rows with `trading_date`, `market_open_utc`, `market_close_utc`, `is_open`, and optional `is_early_close`. Timestamps are normalized to UTC.

4. Seed the initial paper account state.

```bash
make paper-account-bootstrap ARGS="--database-url postgresql+psycopg://quant:quant@127.0.0.1:5432/quant_core_real_paper --cash 100000.000000 --equity 100000.000000 --buying-power 100000.000000 --as-of 2026-04-23T20:05:00+00:00"
```

Use an explicit timezone-aware `as_of` timestamp. Rerunning the same command for the same timestamp updates the same paper snapshot instead of creating duplicate state.

5. Fetch and import the latest real daily bars.

For the first real paper run, backfill enough daily history for the default strategy lookbacks:

```bash
make daily-bars-fetch ARGS="--symbols SPY,VEA,XLV,BND,IEF,SHY --start-date 2010-01-01 --end-date 2026-04-23 --feed iex --adjustment all --output-json /tmp/alpaca_daily_bars_backfill_2010-01-01_2026-04-23.json"
make daily-bars-import ARGS="--database-url postgresql+psycopg://quant:quant@127.0.0.1:5432/quant_core_real_paper --input-json /tmp/alpaca_daily_bars_backfill_2010-01-01_2026-04-23.json"
```

After the first backfill, fetch only the just-closed signal date:

```bash
make daily-bars-fetch ARGS="--symbols SPY,VEA,XLV,BND,IEF,SHY --date 2026-04-24 --feed iex --adjustment all --output-json /tmp/alpaca_daily_bars_2026-04-24.json"
make daily-bars-import ARGS="--database-url postgresql+psycopg://quant:quant@127.0.0.1:5432/quant_core_real_paper --input-json /tmp/alpaca_daily_bars_2026-04-24.json"
```

`daily-bars-fetch` reads Alpaca credentials from environment variables or `.env` keys named `ALPACA_API_KEY_ID` and `ALPACA_API_SECRET_KEY`. The generated import file is a JSON array of `VendorDailyBar`-shaped objects with `symbol`, `vendor`, `bar_date`, `open`, `high`, `low`, `close`, `adjusted_close`, `volume`, `fetched_at`, and `source_payload`.

6. Run the daily paper workflow.

```bash
make paper-run ARGS="--database-url postgresql+psycopg://quant:quant@127.0.0.1:5432/quant_core_real_paper --signal-date 2026-04-24 --auto-fill"
```

If you need a fixture-driven run for deterministic testing, `--bars-json /absolute/path/to/daily_bars.json` is still supported. The normal operator path should use persisted PostgreSQL bars.

The normal database-backed path validates persisted market data before strategy execution. If daily bars are missing, stale, duplicated, or fail price-sanity checks, the run will stop and record an operator-visible incident instead of placing paper orders.

7. Review the latest burn-in summary.

```bash
make paper-burnin-report ARGS="--database-url postgresql+psycopg://quant:quant@127.0.0.1:5432/quant_core_real_paper --config configs/paper_promotion.yaml --run-mode paper --limit 60"
```

8. Run the operator review command.

```bash
make paper-review ARGS="--database-url postgresql+psycopg://quant:quant@127.0.0.1:5432/quant_core_real_paper --config configs/paper_promotion.yaml --run-mode paper --burnin-limit 60"
```

9. Review paper performance and benchmark comparison.

```bash
make paper-performance-report ARGS="--database-url postgresql+psycopg://quant:quant@127.0.0.1:5432/quant_core_real_paper --run-mode paper"
```

The first real-data run proves the machine can ingest real bars, select positions, fill paper orders, reconcile, and report state. Profitability only becomes meaningful after repeated daily runs.

10. Check what matters after every run:
   - latest run completed cleanly
   - no new critical incidents
   - no reconciliation critical rows
   - latest run still inside modeled expectation
   - anomaly count is not increasing in a repeating pattern

11. If there is a critical incident, do not treat the run as healthy just because the command completed.

## Weekly Review Checklist

At least once per week, review the last several runs together:

- consecutive clean runs
- total rejected orders
- total open incidents
- max reconciliation critical rows
- repeated stale-data or failed-job patterns
- drift between paper behavior and modeled expectation

## Burn-In Target

The roadmap target is:

- roughly `60` trading days in paper mode
- zero critical unresolved reconciliation issues
- zero critical unresolved execution issues

## Local Reality

You can start Phase 6 entirely on your machine. A remote always-on environment is optional at first.

What a remote machine helps with later:

- runs continue when your laptop is off
- scheduling is more production-like
- burn-in becomes less dependent on personal machine uptime

It is not required to begin Phase 6.
