"""Tests for SQLStream sync via DatabricksConnector."""

from __future__ import annotations

from unittest.mock import MagicMock

from hotglue_singer_sdk.helpers._singer import CatalogEntry
from hotglue_singer_sdk.streams.core import REPLICATION_INCREMENTAL
from sqlalchemy.sql.selectable import Select

from tap_databricks.client import DatabricksConnector
from tap_databricks.streams import DynamicStream
from tap_databricks.tap import Tapdatabricks
from tap_databricks.utils import _uc_table_schema, build_catalog_entry_from_uc, tap_stream_id

CONFIG_START_DATE = "2020-01-01T00:00:00Z"
STATE_BOOKMARK = "2024-02-03T04:05:06Z"

SAMPLE_COLUMNS = [
    {"name": "id", "type_name": "INT", "nullable": True},
    {"name": "name", "type_name": "STRING", "nullable": True},
    {"name": "created_at", "type_name": "DATE", "nullable": True},
]

SAMPLE_TABLE = {
    "name": "dummy_table",
    "table_type": "MANAGED",
    "columns": SAMPLE_COLUMNS,
}


def _config_with_table_selection(*, start_date: str = CONFIG_START_DATE) -> dict:
    return {
        "host": "dbc-example.cloud.databricks.com",
        "client_id": "client-id",
        "client_secret": "client-secret",
        "http_path": "/sql/1.0/warehouses/warehouse_id",
        "start_date": start_date,
        "catalog": "workspace",
        "default_target_schema": "default",
        "table_selection": [
            {
                "name": "dummy_table",
                "replication_key": "created_at",
            }
        ],
    }


def _make_stream(
    *,
    config: dict | None = None,
    state: dict | None = None,
) -> DynamicStream:
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


def _executed_query(stream: DynamicStream):
    mock_result = MagicMock()
    mock_result.mappings.return_value = []
    mock_connection = MagicMock()
    mock_connection.execute.return_value = mock_result
    stream._write_starting_replication_value(context=None)
    stream.connector._connection = mock_connection

    list(stream.get_records(context=None))

    return mock_connection.execute.call_args[0][0]


def test_get_sqlalchemy_url():
    connector = DatabricksConnector(
        {
            "host": "dbc-example.cloud.databricks.com",
            "http_path": "/sql/1.0/warehouses/abc123",
        }
    )
    url = connector.get_sqlalchemy_url(connector.config)
    assert url == (
        "databricks://token:dummy@dbc-example.cloud.databricks.com"
        "?http_path=/sql/1.0/warehouses/abc123"
    )


def test_catalog_entry_round_trip():
    schema_dict, unsupported = _uc_table_schema(SAMPLE_COLUMNS)
    entry_dict = build_catalog_entry_from_uc(
        uc_catalog_name="workspace",
        uc_schema_name="default",
        uc_table_name="dummy_table",
        schema_dict=schema_dict,
        table_meta=SAMPLE_TABLE,
        unsupported_columns=unsupported,
        replication_key="created_at",
        primary_keys=["id"],
    )
    entry = CatalogEntry.from_dict(entry_dict)
    assert entry.tap_stream_id == tap_stream_id("workspace", "default", "dummy_table")
    assert entry.replication_method == REPLICATION_INCREMENTAL
    assert entry.database == "workspace"
    assert entry.table == "dummy_table"


def test_get_records_yields_rows(monkeypatch):
    stream = _make_stream()
    mock_rows = [
        {"id": 1, "name": "Alice", "created_at": "2026-06-01"},
        {"id": 2, "name": "Bob", "created_at": "2026-06-02"},
    ]
    mock_result = MagicMock()
    mock_result.mappings.return_value = mock_rows
    mock_execute = MagicMock(return_value=mock_result)
    monkeypatch.setattr(stream.connector, "_connection", MagicMock())
    monkeypatch.setattr(stream.connector.connection, "execute", mock_execute)

    records = list(stream.get_records(context=None))

    assert records == mock_rows
    mock_execute.assert_called_once()
    query = mock_execute.call_args[0][0]
    assert isinstance(query, Select)


def test_get_records_incremental_where(monkeypatch):
    stream = _make_stream(config=_config_with_table_selection())
    stream._write_starting_replication_value(context=None)
    mock_result = MagicMock()
    mock_result.mappings.return_value = []
    mock_execute = MagicMock(return_value=mock_result)
    monkeypatch.setattr(stream.connector, "_connection", MagicMock())
    monkeypatch.setattr(stream.connector.connection, "execute", mock_execute)

    list(stream.get_records(context=None))

    query = mock_execute.call_args[0][0]
    assert query.whereclause is not None
    assert stream.get_starting_replication_key_value(context=None) == CONFIG_START_DATE


def test_config_start_date_is_passed_to_incremental_query():
    stream = _make_stream(config=_config_with_table_selection())

    query = _executed_query(stream)

    assert CONFIG_START_DATE in query.compile().params.values()


def test_state_bookmark_is_passed_to_incremental_query():
    state = {
        "bookmarks": {
            "workspace.default.dummy_table": {
                "replication_key": "created_at",
                "replication_key_value": STATE_BOOKMARK,
            }
        }
    }
    stream = _make_stream(config=_config_with_table_selection(), state=state)

    query = _executed_query(stream)

    assert STATE_BOOKMARK in query.compile().params.values()
