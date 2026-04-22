from quant_core.data.models import Base


def test_runtime_state_metadata_exposes_oms_account_and_incident_tables() -> None:
    tables = Base.metadata.tables

    assert {
        "orders",
        "order_events",
        "fills",
        "positions",
        "account_snapshots",
        "pnl_snapshots",
        "risk_snapshots",
        "incidents",
    }.issubset(tables.keys())

    orders = tables["orders"].c
    assert {
        "id",
        "strategy_run_id",
        "instrument_id",
        "internal_order_id",
        "run_mode",
        "order_type",
        "side",
        "status",
        "requested_quantity",
        "requested_notional",
        "time_in_force",
        "broker_order_id",
        "created_at",
        "submitted_at",
        "canceled_at",
    }.issubset(orders.keys())

    order_events = tables["order_events"].c
    assert {
        "id",
        "order_id",
        "event_type",
        "event_at",
        "broker_event_id",
        "details",
    }.issubset(order_events.keys())

    fills = tables["fills"].c
    assert {
        "id",
        "order_id",
        "broker_fill_id",
        "fill_quantity",
        "fill_price",
        "fill_notional",
        "fill_at",
    }.issubset(fills.keys())

    positions = tables["positions"].c
    assert {
        "id",
        "run_mode",
        "instrument_id",
        "quantity",
        "average_cost",
        "market_value",
        "as_of",
    }.issubset(positions.keys())

    account_snapshots = tables["account_snapshots"].c
    assert {"id", "run_mode", "cash", "equity", "buying_power", "as_of"}.issubset(
        account_snapshots.keys()
    )

    pnl_snapshots = tables["pnl_snapshots"].c
    assert {"id", "run_mode", "realized_pnl", "unrealized_pnl", "total_pnl", "as_of"}.issubset(
        pnl_snapshots.keys()
    )

    risk_snapshots = tables["risk_snapshots"].c
    assert {
        "id",
        "run_mode",
        "gross_exposure",
        "net_exposure",
        "drawdown",
        "open_order_count",
        "as_of",
    }.issubset(risk_snapshots.keys())

    incidents = tables["incidents"].c
    assert {
        "id",
        "run_mode",
        "incident_type",
        "severity",
        "status",
        "summary",
        "occurred_at",
        "resolved_at",
        "details",
    }.issubset(incidents.keys())
