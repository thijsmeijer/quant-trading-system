from __future__ import annotations

import json
from pathlib import Path

import pytest

from quant_core.data.ingestion import alpaca_trading_calendar_cli
from quant_core.data.ingestion.alpaca_trading_calendar import AlpacaTradingCalendarResponseError


def test_cli_writes_importable_trading_calendar(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_path = tmp_path / "calendar.json"

    monkeypatch.setenv("ALPACA_API_KEY_ID", "key")
    monkeypatch.setenv("ALPACA_API_SECRET_KEY", "secret")
    monkeypatch.setattr(
        alpaca_trading_calendar_cli,
        "fetch_alpaca_trading_calendar_payload",
        lambda request, api_key_id, api_secret_key: [
            {"date": "2026-04-24", "open": "09:30", "close": "16:00"},
            {"date": "2026-04-27", "open": "09:30", "close": "16:00"},
        ],
    )

    exit_code = alpaca_trading_calendar_cli.main(
        [
            "--start-date",
            "2026-04-24",
            "--end-date",
            "2026-04-27",
            "--output-json",
            str(output_path),
        ]
    )

    payload = json.loads(output_path.read_text())
    assert exit_code == 0
    assert payload == [
        {
            "trading_date": "2026-04-24",
            "market_open_utc": "2026-04-24T13:30:00+00:00",
            "market_close_utc": "2026-04-24T20:00:00+00:00",
            "is_open": True,
            "is_early_close": False,
        },
        {
            "trading_date": "2026-04-27",
            "market_open_utc": "2026-04-27T13:30:00+00:00",
            "market_close_utc": "2026-04-27T20:00:00+00:00",
            "is_open": True,
            "is_early_close": False,
        },
    ]


def test_cli_loads_credentials_from_dotenv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_path = tmp_path / "calendar.json"
    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text("ALPACA_API_KEY_ID=key-from-dotenv\nALPACA_API_SECRET_KEY=secret\n")
    observed_credentials: dict[str, str] = {}

    monkeypatch.delenv("ALPACA_API_KEY_ID", raising=False)
    monkeypatch.delenv("ALPACA_API_SECRET_KEY", raising=False)

    def fake_fetch(
        request: object, *, api_key_id: str, api_secret_key: str
    ) -> list[dict[str, str]]:
        del request
        observed_credentials["key"] = api_key_id
        observed_credentials["secret"] = api_secret_key
        return [{"date": "2026-04-24", "open": "09:30", "close": "16:00"}]

    monkeypatch.setattr(
        alpaca_trading_calendar_cli,
        "fetch_alpaca_trading_calendar_payload",
        fake_fetch,
    )

    exit_code = alpaca_trading_calendar_cli.main(
        [
            "--start-date",
            "2026-04-24",
            "--end-date",
            "2026-04-24",
            "--env-file",
            str(dotenv_path),
            "--output-json",
            str(output_path),
        ]
    )

    assert exit_code == 0
    assert observed_credentials == {
        "key": "key-from-dotenv",
        "secret": "secret",
    }


def test_cli_reports_alpaca_errors_without_traceback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_path = tmp_path / "calendar.json"

    monkeypatch.setenv("ALPACA_API_KEY_ID", "key")
    monkeypatch.setenv("ALPACA_API_SECRET_KEY", "secret")

    def fake_fetch(request: object, *, api_key_id: str, api_secret_key: str) -> object:
        del request, api_key_id, api_secret_key
        raise AlpacaTradingCalendarResponseError("alpaca calendar failed with HTTP 403")

    monkeypatch.setattr(
        alpaca_trading_calendar_cli,
        "fetch_alpaca_trading_calendar_payload",
        fake_fetch,
    )

    with pytest.raises(SystemExit, match="HTTP 403"):
        alpaca_trading_calendar_cli.main(
            [
                "--start-date",
                "2026-04-24",
                "--end-date",
                "2026-04-24",
                "--output-json",
                str(output_path),
            ]
        )
