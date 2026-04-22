from quant_core.data.models import Base


def test_corporate_actions_metadata_exposes_raw_and_normalized_tables() -> None:
    tables = Base.metadata.tables

    assert {"raw_corporate_actions", "corporate_actions"}.issubset(tables.keys())

    raw_actions = tables["raw_corporate_actions"].c
    assert {
        "id",
        "instrument_id",
        "vendor",
        "action_type",
        "ex_date",
        "payload",
        "fetched_at",
    }.issubset(raw_actions.keys())

    corporate_actions = tables["corporate_actions"].c
    assert {
        "id",
        "instrument_id",
        "raw_action_id",
        "action_type",
        "ex_date",
        "effective_date",
        "cash_amount",
        "currency",
        "split_from",
        "split_to",
    }.issubset(corporate_actions.keys())
