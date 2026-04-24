"""CLI entrypoint for local database bootstrap in paper mode."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from quant_core.data.ingestion.universe import UniverseLoaderService
from quant_core.data.models import Instrument

ROOT = Path(__file__).resolve().parents[3]


@dataclass(frozen=True, slots=True)
class LocalBootstrapResult:
    """Summary of one local bootstrap run."""

    upserted_instruments: int
    total_instruments: int


def main(argv: list[str] | None = None) -> int:
    """Apply schema migrations and load the ETF universe for local paper mode."""

    args = _build_parser().parse_args(argv)
    result = LocalBootstrapService().bootstrap(
        database_url=args.database_url,
        universe_path=Path(args.universe_path),
    )
    print(
        json.dumps(
            {
                "total_instruments": result.total_instruments,
                "upserted_instruments": result.upserted_instruments,
            },
            sort_keys=True,
        )
    )
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Apply schema migrations and load the ETF universe for local paper mode."
    )
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--universe-path", default="configs/universe.yaml")
    return parser


class LocalBootstrapService:
    """Prepare a local database for paper-mode operator workflows."""

    def bootstrap(
        self,
        *,
        database_url: str,
        universe_path: Path,
    ) -> LocalBootstrapResult:
        self._upgrade_schema(database_url)
        engine = create_engine(database_url)
        try:
            with Session(engine) as session:
                result = UniverseLoaderService().load_from_file(session, universe_path)
                total_instruments = (
                    session.scalar(select(func.count()).select_from(Instrument)) or 0
                )
                session.commit()
        finally:
            engine.dispose()

        return LocalBootstrapResult(
            upserted_instruments=result.upserted_instruments,
            total_instruments=total_instruments,
        )

    def _upgrade_schema(self, database_url: str) -> None:
        config = Config(str(ROOT / "alembic.ini"))
        config.set_main_option("script_location", str(ROOT / "migrations"))
        config.set_main_option("sqlalchemy.url", database_url)

        previous_url = os.environ.get("DATABASE_URL")
        os.environ["DATABASE_URL"] = database_url
        try:
            command.upgrade(config, "head")
        finally:
            if previous_url is None:
                os.environ.pop("DATABASE_URL", None)
            else:
                os.environ["DATABASE_URL"] = previous_url


if __name__ == "__main__":
    raise SystemExit(main())
