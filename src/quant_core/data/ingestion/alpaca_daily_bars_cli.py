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
    AlpacaDailyBarResponseError,
    fetch_alpaca_daily_bars_payload,
    parse_alpaca_daily_bars_payload,
)


def main(argv: Sequence[str] | None = None) -> int:
    """Fetch Alpaca daily bars and write importable vendor-bar JSON."""

    args = _build_parser().parse_args(argv)
    start_date, end_date = _parse_dates(args)
    symbols = _parse_symbols(args.symbols)
    credentials = _load_credentials(Path(args.env_file))
    request = AlpacaDailyBarFetchRequest(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        feed=args.feed,
        adjustment=args.adjustment,
    )
    try:
        payload = fetch_alpaca_daily_bars_payload(
            request,
            api_key_id=credentials["ALPACA_API_KEY_ID"],
            api_secret_key=credentials["ALPACA_API_SECRET_KEY"],
        )
        bars = parse_alpaca_daily_bars_payload(
            payload,
            expected_symbols=symbols,
            fetched_at=datetime.now(tz=UTC),
            adjustment=args.adjustment,
        )
    except AlpacaDailyBarResponseError as exc:
        raise SystemExit(str(exc)) from exc
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
    parser.add_argument("--env-file", default=".env")
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


def _load_credentials(env_file: Path) -> dict[str, str]:
    dotenv_values = _read_dotenv(env_file)
    return {
        "ALPACA_API_KEY_ID": _required_secret("ALPACA_API_KEY_ID", dotenv_values),
        "ALPACA_API_SECRET_KEY": _required_secret("ALPACA_API_SECRET_KEY", dotenv_values),
    }


def _required_secret(name: str, dotenv_values: dict[str, str]) -> str:
    value = os.environ.get(name) or dotenv_values.get(name)
    if not value:
        raise SystemExit(f"missing required environment variable or .env key: {name}")
    return value


def _read_dotenv(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}

    values: dict[str, str] = {}
    for line in path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        key, separator, raw_value = stripped.partition("=")
        if separator != "=":
            continue
        values[key.strip()] = _unquote_env_value(raw_value.strip())
    return values


def _unquote_env_value(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


if __name__ == "__main__":
    raise SystemExit(main())
