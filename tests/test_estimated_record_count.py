"""Tests for estimated record count behavior."""

from __future__ import annotations

from unittest.mock import MagicMock

from hotglue_singer_sdk.streams.core import REPLICATION_FULL_TABLE

from tap_databricks.client import DatabricksConnector
from tap_databricks.streams import DynamicStream
from tap_databricks.tap import Tapdatabricks
from tap_databricks.utils import _uc_table_schema, build_catalog_entry_from_uc

CONFIG_START_DATE = "2020-01-01T00:00:00Z"
STATE_BOOKMARK = "2024-02-03T04:05:06Z"

SAMPLE_COLUMNS = [
    {"name": "id", "type_name": "INT", "nullable": True},
    {"name": "name", "type_name": "STRING", "nullable": True},
    {"name": "created_at", "type_name": "TIMESTAMP", "nullable": True},
]

SAMPLE_TABLE = {
    "name": "dummy_table",
    "table_type": "MANAGED",
    "columns": SAMPLE_COLUMNS,
    "properties": {"spark.sql.statistics.numRows": "42"},
}


def _config_with_table_selection() -> dict:
    return {
        "host": "dbc-example.cloud.databricks.com",
        "client_id": "client-id",
        "client_secret": "client-secret",
        "http_path": "/sql/1.0/warehouses/warehouse_id",
        "start_date": CONFIG_START_DATE,
        "catalog": "workspace",
        "default_target_schema": "default",
        "table_selection": [
            {
                "name": "dummy_table",
                "replication_key": "created_at",
            }
        ],
    }


def _make_stream(*, config: dict | None = None, state: dict | None = None) -> DynamicStream:
    tap_config = config or {
        "host": "dbc-example.cloud.databricks.com",
        "client_id": "client-id",
        "client_secret": "client-secret",
        "http_path": "/sql/1.0/warehouses/warehouse_id",
        "start_date": CONFIG_START_DATE,
    }
    replication_key = next(
        (
            entry.get("replication_key")
            for entry in tap_config.get("table_selection", [])
            if entry.get("name") == "dummy_table"
        ),
        None,
    )
    schema_dict, unsupported = _uc_table_schema(SAMPLE_COLUMNS)
    entry = build_catalog_entry_from_uc(
        uc_catalog_name="workspace",
        uc_schema_name="default",
        uc_table_name="dummy_table",
        schema_dict=schema_dict,
        table_meta=SAMPLE_TABLE,
        unsupported_columns=unsupported,
        replication_key=replication_key,
        primary_keys=["id"],
    )
    tap = Tapdatabricks(config=tap_config, state=state)
    connector = DatabricksConnector(dict(tap.config))
    connector.register_table("workspace.default.dummy_table", schema_dict)
    return DynamicStream(tap=tap, catalog_entry=entry, connector=connector)


def _estimated_count_query(stream: DynamicStream):
    mock_result = MagicMock()
    mock_result.scalar_one.return_value = 2
    mock_connection = MagicMock()
    mock_connection.execute.return_value = mock_result
    stream.connector._connection = mock_connection

    assert stream.get_estimated_record_count() == 2

    return mock_connection.execute.call_args[0][0]


def test_get_estimated_record_count_for_full_table():
    stream = _make_stream()

    assert stream._singer_catalog_entry.replication_method == REPLICATION_FULL_TABLE
    assert stream.get_estimated_record_count() == 42


def test_get_estimated_record_count_for_incremental_stream():
    stream = _make_stream(config=_config_with_table_selection())

    assert stream.replication_key == "created_at"
    _estimated_count_query(stream)


def test_incremental_estimated_count_uses_config_start_date():
    stream = _make_stream(config=_config_with_table_selection())

    query = _estimated_count_query(stream)

    assert CONFIG_START_DATE in query.compile().params.values()


def test_incremental_estimated_count_uses_state_bookmark():
    state = {
        "bookmarks": {
            "workspace.default.dummy_table": {
                "replication_key": "created_at",
                "replication_key_value": STATE_BOOKMARK,
            }
        }
    }
    stream = _make_stream(config=_config_with_table_selection(), state=state)

    query = _estimated_count_query(stream)

    assert STATE_BOOKMARK in query.compile().params.values()
