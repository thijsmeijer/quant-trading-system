"""CLI entrypoint for loading canonical trading-calendar rows from a local JSON file."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from quant_core.data.ingestion.trading_calendar import TradingCalendarLoaderService


def main(argv: list[str] | None = None) -> int:
    """Load canonical trading-calendar rows into persistent storage."""

    args = _build_parser().parse_args(argv)
    engine = create_engine(args.database_url)
    try:
        with Session(engine) as session:
            result = TradingCalendarLoaderService().load_from_file(
                session,
                Path(args.input_json),
            )
            session.commit()
    finally:
        engine.dispose()

    print(json.dumps({"processed_rows": result.processed_rows}, sort_keys=True))
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Load canonical trading-calendar rows from a local JSON file."
    )
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--input-json", required=True)
    return parser


if __name__ == "__main__":
    raise SystemExit(main())
