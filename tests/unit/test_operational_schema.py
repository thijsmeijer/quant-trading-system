from quant_core.data.models import Base


def test_operational_metadata_exposes_strategy_portfolio_and_risk_tables() -> None:
    tables = Base.metadata.tables

    assert {
        "features_daily",
        "strategy_runs",
        "signals",
        "target_weights",
        "target_positions",
        "risk_checks",
    }.issubset(tables.keys())

    features_daily = tables["features_daily"].c
    assert {
        "id",
        "instrument_id",
        "bar_date",
        "feature_name",
        "feature_version",
        "feature_value",
        "computed_at",
    }.issubset(features_daily.keys())

    strategy_runs = tables["strategy_runs"].c
    assert {
        "id",
        "run_mode",
        "strategy_name",
        "config_version",
        "config_hash",
        "signal_date",
        "execution_date",
        "status",
        "started_at",
        "completed_at",
        "metadata_json",
    }.issubset(strategy_runs.keys())

    signals = tables["signals"].c
    assert {
        "id",
        "strategy_run_id",
        "instrument_id",
        "signal_name",
        "rank",
        "score",
        "is_selected",
        "generated_at",
    }.issubset(signals.keys())

    target_weights = tables["target_weights"].c
    assert {
        "id",
        "strategy_run_id",
        "instrument_id",
        "allocation_key",
        "target_weight",
        "generated_at",
    }.issubset(target_weights.keys())

    target_positions = tables["target_positions"].c
    assert {
        "id",
        "strategy_run_id",
        "instrument_id",
        "allocation_key",
        "target_weight",
        "target_notional",
        "target_quantity",
        "reference_price",
        "generated_at",
    }.issubset(target_positions.keys())

    risk_checks = tables["risk_checks"].c
    assert {
        "id",
        "strategy_run_id",
        "check_scope",
        "check_name",
        "status",
        "reason_code",
        "checked_at",
        "details",
    }.issubset(risk_checks.keys())
