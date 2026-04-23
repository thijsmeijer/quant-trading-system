PYTHON ?= python3
VENV ?= .venv
BIN := $(VENV)/bin
PIP := $(BIN)/pip
PYTEST := $(BIN)/pytest
RUFF := $(BIN)/ruff
MYPY := $(BIN)/mypy

.PHONY: venv install test lint format-check type-check verify postgres-up postgres-down local-bootstrap daily-bars-import paper-run paper-burnin-report paper-review

venv:
	$(PYTHON) -m venv $(VENV)

install: venv
	$(PIP) install --upgrade pip
	$(PIP) install -e .[dev]

test:
	$(PYTEST) tests/unit

lint:
	$(RUFF) check .

format-check:
	$(RUFF) format --check .

type-check:
	$(MYPY) -p quant_core

verify: test lint format-check type-check

postgres-up:
	docker compose up -d postgres

postgres-down:
	docker compose down

local-bootstrap:
	PYTHONPATH=src $(BIN)/python -m quant_core.data.bootstrap_cli $(ARGS)

daily-bars-import:
	PYTHONPATH=src $(BIN)/python -m quant_core.data.ingestion.daily_bars_cli $(ARGS)

paper-run:
	PYTHONPATH=src $(BIN)/python -m quant_core.execution.cli $(ARGS)

paper-burnin-report:
	PYTHONPATH=src $(BIN)/python -m quant_core.reporting.burnin_cli $(ARGS)

paper-review:
	PYTHONPATH=src $(BIN)/python -m quant_core.dashboard.cli $(ARGS)
