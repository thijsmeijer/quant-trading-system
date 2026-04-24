# Real Data Paper Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the missing real-market-data path so the local paper workflow can run every evening from actual daily ETF bars instead of synthetic JSON.

**Architecture:** Keep the existing paper pipeline intact: fetch vendor data into a `VendorDailyBar` JSON file, import through `daily-bars-import`, validate persisted bars, then run paper execution. Add a small Alpaca daily-bars adapter under `src/quant_core/data/ingestion/` that preserves source payloads, emits timezone-aware UTC `fetched_at` values, and supports both historical backfill and single-day nightly fetches.

**Tech Stack:** Python 3.12, stdlib `urllib`/`json` for HTTP to avoid a new dependency, SQLAlchemy/Alembic existing persistence, pytest, ruff, mypy, Docker Compose PostgreSQL, Alpaca Market Data API.

---

## Operator Prerequisites

Before implementation can produce real paper runs, the operator needs:

- Alpaca account with market data access.
- Environment variables:

```bash
export ALPACA_API_KEY_ID="your_key_id"
export ALPACA_API_SECRET_KEY="your_secret_key"
```

- Local services:

```bash
make postgres-up
make local-bootstrap ARGS="--database-url postgresql+psycopg://quant:quant@127.0.0.1:5432/quant_core --universe-path configs/universe.yaml"
make trading-calendar-import ARGS="--database-url postgresql+psycopg://quant:quant@127.0.0.1:5432/quant_core --input-json configs/trading_calendar_us_2026-04-20_2026-04-24.json"
make paper-account-bootstrap ARGS="--database-url postgresql+psycopg://quant:quant@127.0.0.1:5432/quant_core --cash 100000.000000 --equity 100000.000000 --buying-power 100000.000000 --as-of 2026-04-23T20:05:00+00:00"
```

The first real paper run needs enough history for production settings. The current defaults require `trend_lookback_bars=200`, but for more stable research context the real-data backfill should start at `2010-01-01` for the MVP ETF universe before trusting the default paper run.

## File Structure

- Create `src/quant_core/data/ingestion/alpaca_daily_bars.py`
  - Alpaca request building, response parsing, and conversion into the existing `VendorDailyBar` model.
  - No database writes in this file.
- Create `src/quant_core/data/ingestion/alpaca_daily_bars_cli.py`
  - CLI that fetches Alpaca bars and writes a JSON file accepted by `daily-bars-import`.
  - Supports `--date` for nightly fetch and `--start-date`/`--end-date` for backfill.
- Modify `Makefile`
  - Add `daily-bars-fetch` target.
- Modify `pyproject.toml`
  - Add optional script entrypoint `quant-daily-bars-fetch`.
- Create `tests/unit/test_alpaca_daily_bars.py`
  - Unit tests for request construction, response parsing, adjustment assumptions, missing symbol handling, and JSON shape.
- Create `tests/unit/test_alpaca_daily_bars_cli.py`
  - CLI-level tests with injected fake response payloads.
- Modify `tests/integration/test_local_operator_sequence.py`
  - Add or extend an integration path that uses the fetcher output shape before import.
- Modify `docs/phase6_local_runbook.md`
  - Replace “operator supplies vendor JSON” with concrete fetch/backfill/nightly commands.
- Modify `.ai/tasks/todo.md`
  - Track the real-data paper-testing slice, verification, and residual risks.

## Task 1: Add Alpaca Daily-Bar Fetch Domain

**Files:**
- Create: `src/quant_core/data/ingestion/alpaca_daily_bars.py`
- Test: `tests/unit/test_alpaca_daily_bars.py`

- [ ] **Step 1: Write failing tests for Alpaca response conversion**

Create `tests/unit/test_alpaca_daily_bars.py`:

```python
from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from quant_core.data.ingestion.alpaca_daily_bars import (
    AlpacaDailyBarFetchRequest,
    AlpacaDailyBarResponseError,
    build_alpaca_bars_url,
    parse_alpaca_daily_bars_payload,
)


def test_build_alpaca_bars_url_uses_daily_timeframe_symbols_and_adjustment() -> None:
    request = AlpacaDailyBarFetchRequest(
        symbols=("SPY", "BND"),
        start_date=date(2026, 4, 23),
        end_date=date(2026, 4, 23),
        feed="iex",
        adjustment="all",
    )

    url = build_alpaca_bars_url(request)

    assert url.startswith("https://data.alpaca.markets/v2/stocks/bars?")
    assert "symbols=SPY%2CBND" in url
    assert "timeframe=1Day" in url
    assert "start=2026-04-23" in url
    assert "end=2026-04-23" in url
    assert "feed=iex" in url
    assert "adjustment=all" in url


def test_parse_alpaca_daily_bars_payload_returns_vendor_daily_bars() -> None:
    payload = {
        "bars": {
            "SPY": [
                {
                    "t": "2026-04-23T04:00:00Z",
                    "o": 500.0,
                    "h": 508.0,
                    "l": 499.5,
                    "c": 507.25,
                    "v": 12345678,
                    "n": 90000,
                    "vw": 506.1,
                }
            ]
        }
    }
    fetched_at = datetime(2026, 4, 23, 20, 20, tzinfo=UTC)

    bars = parse_alpaca_daily_bars_payload(
        payload,
        expected_symbols=("SPY",),
        fetched_at=fetched_at,
        adjustment="all",
    )

    assert len(bars) == 1
    assert bars[0].symbol == "SPY"
    assert bars[0].vendor == "alpaca"
    assert bars[0].bar_date == date(2026, 4, 23)
    assert bars[0].open == Decimal("500.000000")
    assert bars[0].high == Decimal("508.000000")
    assert bars[0].low == Decimal("499.500000")
    assert bars[0].close == Decimal("507.250000")
    assert bars[0].adjusted_close == Decimal("507.250000")
    assert bars[0].volume == 12345678
    assert bars[0].fetched_at == fetched_at
    assert bars[0].source_payload["adjustment"] == "all"
    assert bars[0].source_payload["alpaca_bar"]["c"] == 507.25


def test_parse_alpaca_daily_bars_payload_rejects_missing_symbols() -> None:
    with pytest.raises(AlpacaDailyBarResponseError, match="missing bars for symbols: BND"):
        parse_alpaca_daily_bars_payload(
            {"bars": {"SPY": []}},
            expected_symbols=("SPY", "BND"),
            fetched_at=datetime(2026, 4, 23, 20, 20, tzinfo=UTC),
            adjustment="all",
        )


def test_parse_alpaca_daily_bars_payload_rejects_unhandled_pagination() -> None:
    with pytest.raises(AlpacaDailyBarResponseError, match="pagination is not yet supported"):
        parse_alpaca_daily_bars_payload(
            {"bars": {"SPY": []}, "next_page_token": "next-page"},
            expected_symbols=("SPY",),
            fetched_at=datetime(2026, 4, 23, 20, 20, tzinfo=UTC),
            adjustment="all",
        )
```

- [ ] **Step 2: Run tests and confirm failure**

```bash
.venv/bin/pytest tests/unit/test_alpaca_daily_bars.py
```

Expected: import failure because `quant_core.data.ingestion.alpaca_daily_bars` does not exist.

- [ ] **Step 3: Implement Alpaca fetch domain**

Create `src/quant_core/data/ingestion/alpaca_daily_bars.py`:

```python
"""Alpaca daily-bar fetch adapter for real-data paper workflows."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from quant_core.data.ingestion.daily_bars import VendorDailyBar

ALPACA_BARS_URL = "https://data.alpaca.markets/v2/stocks/bars"
PRICE_PRECISION = Decimal("0.000001")


class AlpacaDailyBarResponseError(ValueError):
    """Raised when Alpaca returns an unusable daily-bar payload."""


@dataclass(frozen=True, slots=True)
class AlpacaDailyBarFetchRequest:
    """Request parameters for Alpaca daily bars."""

    symbols: tuple[str, ...]
    start_date: date
    end_date: date
    feed: str = "iex"
    adjustment: str = "all"


def build_alpaca_bars_url(request: AlpacaDailyBarFetchRequest) -> str:
    """Build the Alpaca historical bars URL for daily ETF bars."""

    query = urlencode(
        {
            "symbols": ",".join(request.symbols),
            "timeframe": "1Day",
            "start": request.start_date.isoformat(),
            "end": request.end_date.isoformat(),
            "feed": request.feed,
            "adjustment": request.adjustment,
            "limit": 10000,
        }
    )
    return f"{ALPACA_BARS_URL}?{query}"


def fetch_alpaca_daily_bars_payload(
    request: AlpacaDailyBarFetchRequest,
    *,
    api_key_id: str,
    api_secret_key: str,
) -> Mapping[str, Any]:
    """Fetch raw Alpaca daily-bar JSON."""

    http_request = Request(
        build_alpaca_bars_url(request),
        headers={
            "APCA-API-KEY-ID": api_key_id,
            "APCA-API-SECRET-KEY": api_secret_key,
            "Accept": "application/json",
        },
    )
    with urlopen(http_request, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, Mapping):
        raise AlpacaDailyBarResponseError("alpaca response must be a JSON object")
    return payload


def parse_alpaca_daily_bars_payload(
    payload: Mapping[str, Any],
    *,
    expected_symbols: Sequence[str],
    fetched_at: datetime,
    adjustment: str,
) -> list[VendorDailyBar]:
    """Convert an Alpaca bars payload into canonical vendor daily bars."""

    if fetched_at.tzinfo is None:
        raise AlpacaDailyBarResponseError("fetched_at must be timezone-aware")

    bars_by_symbol = payload.get("bars")
    if not isinstance(bars_by_symbol, Mapping):
        raise AlpacaDailyBarResponseError("alpaca response missing bars object")
    if payload.get("next_page_token"):
        raise AlpacaDailyBarResponseError(
            "pagination is not yet supported; narrow the date range before importing"
        )

    parsed: list[VendorDailyBar] = []
    missing_symbols: list[str] = []
    for symbol in expected_symbols:
        raw_bars = bars_by_symbol.get(symbol)
        if not isinstance(raw_bars, list) or not raw_bars:
            missing_symbols.append(symbol)
            continue
        for raw_bar in raw_bars:
            if not isinstance(raw_bar, Mapping):
                raise AlpacaDailyBarResponseError(f"alpaca bar for {symbol} must be an object")
            parsed.append(_parse_one_bar(symbol, raw_bar, fetched_at, adjustment))

    if missing_symbols:
        joined = ", ".join(sorted(missing_symbols))
        raise AlpacaDailyBarResponseError(f"missing bars for symbols: {joined}")

    return sorted(parsed, key=lambda item: (item.symbol, item.bar_date))


def _parse_one_bar(
    symbol: str,
    raw_bar: Mapping[str, Any],
    fetched_at: datetime,
    adjustment: str,
) -> VendorDailyBar:
    timestamp = datetime.fromisoformat(str(raw_bar["t"]).replace("Z", "+00:00"))
    bar_date = timestamp.astimezone(UTC).date()
    close = _decimal(raw_bar["c"])
    return VendorDailyBar(
        symbol=symbol,
        vendor="alpaca",
        bar_date=bar_date,
        open=_decimal(raw_bar["o"]),
        high=_decimal(raw_bar["h"]),
        low=_decimal(raw_bar["l"]),
        close=close,
        adjusted_close=close,
        volume=int(raw_bar["v"]),
        fetched_at=fetched_at.astimezone(UTC),
        source_payload={
            "adjustment": adjustment,
            "alpaca_bar": dict(raw_bar),
        },
    )


def _decimal(value: object) -> Decimal:
    return Decimal(str(value)).quantize(PRICE_PRECISION)
```

- [ ] **Step 4: Run tests and confirm pass**

```bash
.venv/bin/pytest tests/unit/test_alpaca_daily_bars.py
```

Expected: pass.

## Task 2: Add Fetch CLI That Writes Importable JSON

**Files:**
- Create: `src/quant_core/data/ingestion/alpaca_daily_bars_cli.py`
- Modify: `Makefile`
- Modify: `pyproject.toml`
- Test: `tests/unit/test_alpaca_daily_bars_cli.py`

- [ ] **Step 1: Write failing CLI tests**

Create `tests/unit/test_alpaca_daily_bars_cli.py`:

```python
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from quant_core.data.ingestion import alpaca_daily_bars_cli


def test_cli_writes_importable_vendor_daily_bars(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_path = tmp_path / "bars.json"

    monkeypatch.setenv("ALPACA_API_KEY_ID", "key")
    monkeypatch.setenv("ALPACA_API_SECRET_KEY", "secret")
    monkeypatch.setattr(
        alpaca_daily_bars_cli,
        "fetch_alpaca_daily_bars_payload",
        lambda request, api_key_id, api_secret_key: {
            "bars": {
                "SPY": [
                    {
                        "t": "2026-04-23T04:00:00Z",
                        "o": 500.0,
                        "h": 508.0,
                        "l": 499.5,
                        "c": 507.25,
                        "v": 12345678,
                    }
                ]
            }
        },
    )

    exit_code = alpaca_daily_bars_cli.main(
        [
            "--symbols",
            "SPY",
            "--date",
            "2026-04-23",
            "--output-json",
            str(output_path),
        ]
    )

    payload = json.loads(output_path.read_text())
    assert exit_code == 0
    assert payload[0]["symbol"] == "SPY"
    assert payload[0]["vendor"] == "alpaca"
    assert payload[0]["bar_date"] == "2026-04-23"
    assert payload[0]["adjusted_close"] == "507.250000"
    assert payload[0]["source_payload"]["adjustment"] == "all"


def test_parse_date_range_rejects_mixed_date_modes() -> None:
    with pytest.raises(SystemExit):
        alpaca_daily_bars_cli.main(
            [
                "--symbols",
                "SPY",
                "--date",
                "2026-04-23",
                "--start-date",
                "2026-04-01",
                "--end-date",
                "2026-04-23",
                "--output-json",
                "/tmp/out.json",
            ]
        )
```

- [ ] **Step 2: Run tests and confirm failure**

```bash
.venv/bin/pytest tests/unit/test_alpaca_daily_bars_cli.py
```

Expected: import failure because CLI module does not exist.

- [ ] **Step 3: Implement CLI**

Create `src/quant_core/data/ingestion/alpaca_daily_bars_cli.py`:

```python
"""CLI for fetching Alpaca daily bars into the repo's vendor-bar JSON format."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Sequence
from datetime import UTC, date, datetime
from pathlib import Path

from quant_core.data.ingestion.alpaca_daily_bars import (
    AlpacaDailyBarFetchRequest,
    fetch_alpaca_daily_bars_payload,
    parse_alpaca_daily_bars_payload,
)


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    start_date, end_date = _parse_dates(args)
    symbols = _parse_symbols(args.symbols)
    request = AlpacaDailyBarFetchRequest(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        feed=args.feed,
        adjustment=args.adjustment,
    )
    payload = fetch_alpaca_daily_bars_payload(
        request,
        api_key_id=_required_env("ALPACA_API_KEY_ID"),
        api_secret_key=_required_env("ALPACA_API_SECRET_KEY"),
    )
    bars = parse_alpaca_daily_bars_payload(
        payload,
        expected_symbols=symbols,
        fetched_at=datetime.now(tz=UTC),
        adjustment=args.adjustment,
    )
    output = [
        {
            "symbol": bar.symbol,
            "vendor": bar.vendor,
            "bar_date": bar.bar_date.isoformat(),
            "open": str(bar.open),
            "high": str(bar.high),
            "low": str(bar.low),
            "close": str(bar.close),
            "adjusted_close": str(bar.adjusted_close),
            "volume": bar.volume,
            "fetched_at": bar.fetched_at.isoformat(),
            "source_payload": dict(bar.source_payload),
        }
        for bar in bars
    ]
    Path(args.output_json).write_text(json.dumps(output, indent=2, sort_keys=True))
    print(json.dumps({"output_json": args.output_json, "processed_bars": len(output)}))
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch Alpaca daily bars into JSON.")
    parser.add_argument("--symbols", required=True, help="Comma-separated ETF symbols.")
    parser.add_argument("--date", help="Single trading date to fetch.")
    parser.add_argument("--start-date", help="Backfill start date.")
    parser.add_argument("--end-date", help="Backfill end date.")
    parser.add_argument("--feed", default="iex")
    parser.add_argument("--adjustment", default="all")
    parser.add_argument("--output-json", required=True)
    return parser


def _parse_dates(args: argparse.Namespace) -> tuple[date, date]:
    if args.date and (args.start_date or args.end_date):
        raise SystemExit("use either --date or --start-date/--end-date, not both")
    if args.date:
        parsed = date.fromisoformat(args.date)
        return parsed, parsed
    if not args.start_date or not args.end_date:
        raise SystemExit("provide --date or both --start-date and --end-date")
    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)
    if end_date < start_date:
        raise SystemExit("--end-date must be on or after --start-date")
    return start_date, end_date


def _parse_symbols(raw_symbols: str) -> tuple[str, ...]:
    symbols = tuple(symbol.strip().upper() for symbol in raw_symbols.split(",") if symbol.strip())
    if not symbols:
        raise SystemExit("--symbols must include at least one symbol")
    return symbols


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"missing required environment variable: {name}")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Add Makefile and script entrypoint**

Modify `Makefile` `.PHONY` line to include `daily-bars-fetch`, and add:

```make
daily-bars-fetch:
	PYTHONPATH=src $(BIN)/python -m quant_core.data.ingestion.alpaca_daily_bars_cli $(ARGS)
```

Modify `pyproject.toml` `[project.scripts]`:

```toml
quant-daily-bars-fetch = "quant_core.data.ingestion.alpaca_daily_bars_cli:main"
```

- [ ] **Step 5: Run focused tests**

```bash
.venv/bin/pytest tests/unit/test_alpaca_daily_bars.py tests/unit/test_alpaca_daily_bars_cli.py tests/unit/test_cli_module_entrypoints.py
.venv/bin/ruff check src/quant_core/data/ingestion/alpaca_daily_bars.py src/quant_core/data/ingestion/alpaca_daily_bars_cli.py tests/unit/test_alpaca_daily_bars.py tests/unit/test_alpaca_daily_bars_cli.py Makefile pyproject.toml
.venv/bin/ruff format --check src/quant_core/data/ingestion/alpaca_daily_bars.py src/quant_core/data/ingestion/alpaca_daily_bars_cli.py tests/unit/test_alpaca_daily_bars.py tests/unit/test_alpaca_daily_bars_cli.py
```

Expected: tests pass; ruff passes.

## Task 3: Backfill Real Historical Data

**Files:**
- Modify: `docs/phase6_local_runbook.md`
- No production code required if Task 2 is complete.

- [ ] **Step 1: Export Alpaca credentials**

```bash
export ALPACA_API_KEY_ID="your_key_id"
export ALPACA_API_SECRET_KEY="your_secret_key"
```

- [ ] **Step 2: Fetch at least 252 trading days for all MVP symbols**

Use a broad calendar range that covers more than 252 sessions:

```bash
make daily-bars-fetch ARGS="--symbols SPY,VEA,XLV,BND,IEF,SHY --start-date 2010-01-01 --end-date 2026-04-23 --feed iex --adjustment all --output-json /tmp/alpaca_daily_bars_backfill_2010-01-01_2026-04-23.json"
```

Expected output:

```json
{"output_json": "/tmp/alpaca_daily_bars_backfill_2010-01-01_2026-04-23.json", "processed_bars": 20000}
```

Exact bar count can vary with holidays and symbol availability, but it should be much higher than `6 * 252` for the long-history backfill.

- [ ] **Step 3: Import backfill**

```bash
make daily-bars-import ARGS="--database-url postgresql+psycopg://quant:quant@127.0.0.1:5432/quant_core --input-json /tmp/alpaca_daily_bars_backfill_2010-01-01_2026-04-23.json"
```

Expected: JSON with `processed_bars` matching the fetch output.

- [ ] **Step 4: Run production-default paper workflow**

```bash
make paper-run ARGS="--database-url postgresql+psycopg://quant:quant@127.0.0.1:5432/quant_core --signal-date 2026-04-23 --auto-fill"
```

Expected:

```json
{"approved": true, "incident_count": 0, "reconciliation_critical_rows": 0, "...": "..."}
```

If this fails with stale or missing data, do not force the run. Inspect the incident output through `paper-review`.

## Task 4: Add Nightly Real-Data Runbook

**Files:**
- Modify: `docs/phase6_local_runbook.md`

- [ ] **Step 1: Add nightly fetch/import/run section**

Insert after the existing “Import the latest daily bars” section:

```markdown
### Real Alpaca Data Path

For the first real run, backfill enough history for the default strategy settings:

```bash
make daily-bars-fetch ARGS="--symbols SPY,VEA,XLV,BND,IEF,SHY --start-date 2010-01-01 --end-date 2026-04-23 --feed iex --adjustment all --output-json /tmp/alpaca_daily_bars_backfill_2010-01-01_2026-04-23.json"
make daily-bars-import ARGS="--database-url postgresql+psycopg://quant:quant@127.0.0.1:5432/quant_core --input-json /tmp/alpaca_daily_bars_backfill_2010-01-01_2026-04-23.json"
```

After the first backfill, fetch only the just-closed signal date:

```bash
make daily-bars-fetch ARGS="--symbols SPY,VEA,XLV,BND,IEF,SHY --date 2026-04-24 --feed iex --adjustment all --output-json /tmp/alpaca_daily_bars_2026-04-24.json"
make daily-bars-import ARGS="--database-url postgresql+psycopg://quant:quant@127.0.0.1:5432/quant_core --input-json /tmp/alpaca_daily_bars_2026-04-24.json"
make paper-run ARGS="--database-url postgresql+psycopg://quant:quant@127.0.0.1:5432/quant_core --signal-date 2026-04-24 --auto-fill"
make paper-burnin-report ARGS="--database-url postgresql+psycopg://quant:quant@127.0.0.1:5432/quant_core --config configs/paper_promotion.yaml --run-mode paper --limit 60"
make paper-review ARGS="--database-url postgresql+psycopg://quant:quant@127.0.0.1:5432/quant_core --config configs/paper_promotion.yaml --run-mode paper --burnin-limit 60"
```

Do not count a run as healthy when `paper-review` reports open critical incidents, stale-data alerts, order rejection alerts, or reconciliation alerts.
```

- [ ] **Step 2: Review the runbook directly**

```bash
sed -n '1,180p' docs/phase6_local_runbook.md
```

Expected: no stale synthetic-data instructions in the normal real-data path.

## Task 5: Add Integration Proof For Fetch Output Through Import

**Files:**
- Modify: `tests/integration/test_daily_bar_import_cli.py`

- [ ] **Step 1: Add test that fetch-shaped Alpaca JSON imports and feeds paper run**

Append a test that writes the same JSON shape emitted by `alpaca_daily_bars_cli`, imports it, then runs the existing database-backed paper path with short lookbacks.

```python
def test_alpaca_fetch_output_shape_imports_and_feeds_paper_run(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_name = f"quant_core_alpaca_shape_{uuid4().hex}"
    target_url = f"postgresql+psycopg://quant:quant@127.0.0.1:5432/{database_name}"
    engine = _migrated_engine(database_name=database_name, target_url=target_url)
    input_path = tmp_path / "alpaca_daily_bars.json"
    _write_alpaca_fetch_output_bars(input_path)

    try:
        with Session(engine) as session:
            _seed_instruments(session)
            _seed_calendar(session)
            SnapshotRepository().store_account_snapshot(
                session,
                AccountSnapshotWrite(
                    run_mode="paper",
                    cash=Decimal("100000.000000"),
                    equity=Decimal("100000.000000"),
                    buying_power=Decimal("100000.000000"),
                    as_of=datetime(2026, 4, 22, 20, 1, tzinfo=UTC),
                ),
            )
            session.commit()

        import_exit = import_main(["--database-url", target_url, "--input-json", str(input_path)])
        paper_exit = paper_run_main(
            [
                "--database-url",
                target_url,
                "--signal-date",
                "2026-04-22",
                "--lookback-bars",
                "2",
                "--trend-lookback-bars",
                "3",
                "--top-n",
                "1",
                "--auto-fill",
            ]
        )
        output = json.loads(capsys.readouterr().out.strip().splitlines()[-1])

        assert import_exit == 0
        assert paper_exit == 0
        assert output["approved"] is True
        assert output["incident_count"] == 0
    finally:
        _drop_database(engine=engine, database_name=database_name)
```

Add helper:

```python
def _write_alpaca_fetch_output_bars(path: Path) -> None:
    payload: list[dict[str, object]] = []
    for symbol, prices in {
        "SPY": ("500.000000", "504.000000", "508.000000"),
        "BND": ("72.500000", "72.450000", "72.400000"),
    }.items():
        for offset, close in enumerate(prices):
            bar_date = date(2026, 4, 20 + offset)
            payload.append(
                {
                    "symbol": symbol,
                    "vendor": "alpaca",
                    "bar_date": bar_date.isoformat(),
                    "open": close,
                    "high": close,
                    "low": close,
                    "close": close,
                    "adjusted_close": close,
                    "volume": 1_000_000,
                    "fetched_at": datetime(2026, 4, 22, 20, 0, tzinfo=UTC).isoformat(),
                    "source_payload": {
                        "adjustment": "all",
                        "alpaca_bar": {"c": close},
                    },
                }
            )
    path.write_text(json.dumps(payload))
```

- [ ] **Step 2: Run integration proof**

```bash
.venv/bin/pytest tests/integration/test_daily_bar_import_cli.py
```

Expected: pass.

## Task 6: Verification And First Real Run

**Files:**
- No new files unless tests reveal a bug.

- [ ] **Step 1: Run focused verification**

```bash
.venv/bin/pytest tests/unit/test_alpaca_daily_bars.py tests/unit/test_alpaca_daily_bars_cli.py tests/unit/test_cli_module_entrypoints.py tests/integration/test_daily_bar_import_cli.py
.venv/bin/ruff check src/quant_core/data/ingestion/alpaca_daily_bars.py src/quant_core/data/ingestion/alpaca_daily_bars_cli.py tests/unit/test_alpaca_daily_bars.py tests/unit/test_alpaca_daily_bars_cli.py tests/integration/test_daily_bar_import_cli.py
.venv/bin/ruff format --check src/quant_core/data/ingestion/alpaca_daily_bars.py src/quant_core/data/ingestion/alpaca_daily_bars_cli.py tests/unit/test_alpaca_daily_bars.py tests/unit/test_alpaca_daily_bars_cli.py tests/integration/test_daily_bar_import_cli.py
.venv/bin/mypy -p quant_core
```

- [ ] **Step 2: Run real backfill and paper run**

```bash
make daily-bars-fetch ARGS="--symbols SPY,VEA,XLV,BND,IEF,SHY --start-date 2010-01-01 --end-date 2026-04-23 --feed iex --adjustment all --output-json /tmp/alpaca_daily_bars_backfill_2010-01-01_2026-04-23.json"
make daily-bars-import ARGS="--database-url postgresql+psycopg://quant:quant@127.0.0.1:5432/quant_core --input-json /tmp/alpaca_daily_bars_backfill_2010-01-01_2026-04-23.json"
make paper-run ARGS="--database-url postgresql+psycopg://quant:quant@127.0.0.1:5432/quant_core --signal-date 2026-04-23 --auto-fill"
make paper-burnin-report ARGS="--database-url postgresql+psycopg://quant:quant@127.0.0.1:5432/quant_core --config configs/paper_promotion.yaml --run-mode paper --limit 60"
make paper-review ARGS="--database-url postgresql+psycopg://quant:quant@127.0.0.1:5432/quant_core --config configs/paper_promotion.yaml --run-mode paper --burnin-limit 60"
```

- [ ] **Step 3: Acceptance criteria for first real-data paper test**

The first real-data run is acceptable only when:

- `daily-bars-import` processes real Alpaca bars for all active universe symbols.
- `paper-run` returns `incident_count: 0`.
- `paper-run` returns `reconciliation_critical_rows: 0`.
- `paper-review` health is `healthy`.
- `paper-review` readiness is blocked only by expected burn-in criteria such as `insufficient_completed_runs`, not by data or execution incidents.

## Task 7: Ongoing Daily Operator Routine

After the first backfill, run this after every U.S. market close once final bars are available:

```bash
SIGNAL_DATE=2026-04-24
make daily-bars-fetch ARGS="--symbols SPY,VEA,XLV,BND,IEF,SHY --date ${SIGNAL_DATE} --feed iex --adjustment all --output-json /tmp/alpaca_daily_bars_${SIGNAL_DATE}.json"
make daily-bars-import ARGS="--database-url postgresql+psycopg://quant:quant@127.0.0.1:5432/quant_core --input-json /tmp/alpaca_daily_bars_${SIGNAL_DATE}.json"
make paper-run ARGS="--database-url postgresql+psycopg://quant:quant@127.0.0.1:5432/quant_core --signal-date ${SIGNAL_DATE} --auto-fill"
make paper-burnin-report ARGS="--database-url postgresql+psycopg://quant:quant@127.0.0.1:5432/quant_core --config configs/paper_promotion.yaml --run-mode paper --limit 60"
make paper-review ARGS="--database-url postgresql+psycopg://quant:quant@127.0.0.1:5432/quant_core --config configs/paper_promotion.yaml --run-mode paper --burnin-limit 60"
```

Record each day:

- signal date
- execution date
- approved status
- order count
- fill count
- open incident count
- reconciliation critical rows
- paper-review health

Do not promote to live while readiness is blocked.

## Self-Review

- Spec coverage: The plan covers real vendor data acquisition, backfill, nightly fetch, import, paper run, validation, reporting, and daily operator routine.
- Placeholder scan: No forbidden placeholder terms remain.
- Type consistency: The plan reuses existing `VendorDailyBar`, `daily-bars-import`, `paper-run`, `paper-burnin-report`, and `paper-review` seams.
- Residual design note: The first Alpaca version uses `adjustment=all` and sets `adjusted_close` equal to Alpaca’s adjusted close. A later hardening slice can fetch both raw and adjusted series if we need separate unadjusted close provenance in normalized data.
- Pagination note: The MVP universe backfill is expected to stay under Alpaca's `limit=10000`; the first implementation should fail loudly if Alpaca returns `next_page_token` so we never silently import partial data.
