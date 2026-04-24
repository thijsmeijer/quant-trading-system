"""CLI entrypoint for importing vendor-shaped daily bars from a local JSON file."""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from quant_core.data.ingestion.daily_bars import DailyBarIngestionService, VendorDailyBar


def main(argv: list[str] | None = None) -> int:
    """Import vendor-shaped daily bars into canonical raw and normalized tables."""

    args = _build_parser().parse_args(argv)
    bars = _load_bars(Path(args.input_json))
    engine = create_engine(args.database_url)
    try:
        with Session(engine) as session:
            result = DailyBarIngestionService().ingest(session, bars)
            session.commit()
    finally:
        engine.dispose()

    print(json.dumps({"processed_bars": result.processed_bars}, sort_keys=True))
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Import vendor-shaped daily bars from a local JSON file."
    )
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--input-json", required=True)
    return parser


def _load_bars(path: Path) -> list[VendorDailyBar]:
    payload = json.loads(path.read_text())
    if not isinstance(payload, list):
        raise ValueError("daily-bar import file must be a JSON array")
    return [_parse_bar(item) for item in payload]


def _parse_bar(item: object) -> VendorDailyBar:
    if not isinstance(item, dict):
        raise ValueError("each imported daily bar must be a JSON object")

    source_payload = cast(dict[str, Any], item.get("source_payload", {}))
    return VendorDailyBar(
        symbol=str(item["symbol"]),
        vendor=str(item["vendor"]),
        bar_date=date.fromisoformat(str(item["bar_date"])),
        open=Decimal(str(item["open"])),
        high=Decimal(str(item["high"])),
        low=Decimal(str(item["low"])),
        close=Decimal(str(item["close"])),
        adjusted_close=Decimal(str(item["adjusted_close"])),
        volume=int(item["volume"]),
        fetched_at=datetime.fromisoformat(str(item["fetched_at"])),
        source_payload=source_payload,
    )


if __name__ == "__main__":
    raise SystemExit(main())
