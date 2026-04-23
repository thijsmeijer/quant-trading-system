"""CLI entrypoint for bootstrapping the initial paper account snapshot."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from quant_core.execution.paper_account import (
    PaperAccountBootstrapInput,
    PaperAccountBootstrapService,
)


def main(argv: list[str] | None = None) -> int:
    """Store one explicit paper account snapshot for local operator setup."""

    args = _build_parser().parse_args(argv)
    engine = create_engine(args.database_url)
    try:
        with Session(engine) as session:
            stored = PaperAccountBootstrapService().bootstrap(
                session,
                snapshot=PaperAccountBootstrapInput(
                    cash=Decimal(args.cash),
                    equity=Decimal(args.equity),
                    buying_power=Decimal(args.buying_power),
                    as_of=datetime.fromisoformat(args.as_of),
                ),
            )
            session.commit()
    finally:
        engine.dispose()

    print(
        json.dumps(
            {
                "as_of": stored.as_of.isoformat(),
                "buying_power": str(stored.buying_power),
                "cash": str(stored.cash),
                "equity": str(stored.equity),
                "run_mode": stored.run_mode,
            },
            sort_keys=True,
        )
    )
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Store one explicit paper account snapshot for local operator setup."
    )
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--cash", required=True)
    parser.add_argument("--equity", required=True)
    parser.add_argument("--buying-power", required=True)
    parser.add_argument("--as-of", required=True)
    return parser


if __name__ == "__main__":
    raise SystemExit(main())
