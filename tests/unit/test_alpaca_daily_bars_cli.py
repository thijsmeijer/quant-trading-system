from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from quant_core.data.ingestion import alpaca_daily_bars_cli
from quant_core.data.ingestion.alpaca_daily_bars import AlpacaDailyBarResponseError


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


def test_cli_loads_credentials_from_dotenv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_path = tmp_path / "bars.json"
    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text(
        '\nALPACA_API_KEY_ID="key-from-dotenv"\nALPACA_API_SECRET_KEY=secret-from-dotenv\n'
    )
    observed_credentials: dict[str, str] = {}

    monkeypatch.delenv("ALPACA_API_KEY_ID", raising=False)
    monkeypatch.delenv("ALPACA_API_SECRET_KEY", raising=False)

    def fake_fetch(request: object, *, api_key_id: str, api_secret_key: str) -> dict[str, object]:
        del request
        observed_credentials["key"] = api_key_id
        observed_credentials["secret"] = api_secret_key
        return {
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
        }

    monkeypatch.setattr(alpaca_daily_bars_cli, "fetch_alpaca_daily_bars_payload", fake_fetch)

    exit_code = alpaca_daily_bars_cli.main(
        [
            "--symbols",
            "SPY",
            "--date",
            "2026-04-23",
            "--env-file",
            str(dotenv_path),
            "--output-json",
            str(output_path),
        ]
    )

    assert exit_code == 0
    assert observed_credentials == {
        "key": "key-from-dotenv",
        "secret": "secret-from-dotenv",
    }


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


def test_parse_dates_accepts_backfill_range() -> None:
    namespace = alpaca_daily_bars_cli._build_parser().parse_args(
        [
            "--symbols",
            "SPY",
            "--start-date",
            "2026-04-01",
            "--end-date",
            "2026-04-23",
            "--output-json",
            "/tmp/out.json",
        ]
    )

    assert alpaca_daily_bars_cli._parse_dates(namespace) == (
        date(2026, 4, 1),
        date(2026, 4, 23),
    )


def test_cli_reports_alpaca_errors_without_traceback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_path = tmp_path / "bars.json"

    monkeypatch.setenv("ALPACA_API_KEY_ID", "key")
    monkeypatch.setenv("ALPACA_API_SECRET_KEY", "secret")

    def fake_fetch(request: object, *, api_key_id: str, api_secret_key: str) -> object:
        del request, api_key_id, api_secret_key
        raise AlpacaDailyBarResponseError("alpaca request failed with HTTP 403: Forbidden")

    monkeypatch.setattr(alpaca_daily_bars_cli, "fetch_alpaca_daily_bars_payload", fake_fetch)

    with pytest.raises(SystemExit, match="HTTP 403"):
        alpaca_daily_bars_cli.main(
            [
                "--symbols",
                "SPY",
                "--date",
                "2026-04-23",
                "--output-json",
                str(output_path),
            ]
        )
