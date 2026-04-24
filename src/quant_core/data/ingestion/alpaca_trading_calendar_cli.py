"""CLI for fetching Alpaca trading calendar rows into canonical JSON."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Sequence
from datetime import date
from pathlib import Path

from quant_core.data.ingestion.alpaca_trading_calendar import (
    AlpacaTradingCalendarRequest,
    AlpacaTradingCalendarResponseError,
    fetch_alpaca_trading_calendar_payload,
    parse_alpaca_trading_calendar_payload,
)


def main(argv: Sequence[str] | None = None) -> int:
    """Fetch Alpaca trading calendar rows and write canonical import JSON."""

    args = _build_parser().parse_args(argv)
    credentials = _load_credentials(Path(args.env_file))
    request = AlpacaTradingCalendarRequest(
        start_date=date.fromisoformat(args.start_date),
        end_date=date.fromisoformat(args.end_date),
    )
    try:
        payload = fetch_alpaca_trading_calendar_payload(
            request,
            api_key_id=credentials["ALPACA_API_KEY_ID"],
            api_secret_key=credentials["ALPACA_API_SECRET_KEY"],
        )
        entries = parse_alpaca_trading_calendar_payload(payload)
    except AlpacaTradingCalendarResponseError as exc:
        raise SystemExit(str(exc)) from exc

    output = [
        {
            "trading_date": entry.trading_date.isoformat(),
            "market_open_utc": (
                entry.market_open_utc.isoformat() if entry.market_open_utc is not None else None
            ),
            "market_close_utc": (
                entry.market_close_utc.isoformat() if entry.market_close_utc is not None else None
            ),
            "is_open": entry.is_open,
            "is_early_close": entry.is_early_close,
        }
        for entry in entries
    ]
    Path(args.output_json).write_text(json.dumps(output, indent=2, sort_keys=True))
    print(json.dumps({"output_json": args.output_json, "processed_rows": len(output)}))
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch Alpaca trading calendar into JSON.")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--output-json", required=True)
    return parser


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
